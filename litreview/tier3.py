"""Tier-3 pipeline: select transferable papers and fill the target table.

End-to-end:
  1. Gather candidate papers near the Carolinian Zone, querying BOTH OpenAlex
     and Semantic Scholar, then deduplicating across the two (see below),
  2. Screen them against the keyword vocabularies and flag peer-review status,
  3. Score transferability (transferability.py) and keep the best,
  4. Deep-extract each into a target-table row (extract.py, full text + LLM),
  5. Write a CSV with the exact columns of
     research/Ecosystem Services Mapping(Data).csv.

Steps 1-3 need no LLM; step 4 uses Azure OpenAI (falls back to scaffolding-only
rows if Azure isn't configured, so the selection pipeline is still usable).
"""

from __future__ import annotations

import csv
import os
import time
from typing import Optional

from .config import SETTINGS
from .models import Paper, TableRow
from .sources import openalex, semantic_scholar, ebsco, wos
from . import screening, peer_review, transferability, extract as extract_mod, dedup
from . import report as report_mod

# Primary sources for tier-3, run independently (one table per source) so the
# corpora stay comparable rather than merged. OpenAlex carries Semantic Scholar
# as an in-run enrichment layer (abstracts / OA PDFs, merged via dedup); EBSCO
# and WoS stand alone. Add future primary sources here; `available_sources()`
# drops any whose credentials aren't configured.
PRIMARY_SOURCES = ["openalex", "ebsco", "wos"]

# Sources gated on optional credentials -> their is_configured() module.
_OPTIONAL_SOURCES = {"ebsco": ebsco, "wos": wos}


def available_sources() -> list[str]:
    """PRIMARY_SOURCES minus any whose credentials aren't configured."""
    out = []
    for name in PRIMARY_SOURCES:
        mod = _OPTIONAL_SOURCES.get(name)
        if mod is not None and not mod.is_configured():
            continue
        out.append(name)
    return out

# Study-site region for the optional --llm-location-filter pass (mirrors the
# "carolinian" profile in topic_table). Judged by the LLM against each paper's
# actual study site, complementing the deterministic transferability gate.
CAROLINIAN_REGION = (
    "the Carolinian Zone of southern Ontario, Canada, or the adjacent "
    "Great Lakes / northeastern United States"
)

# Region queries, broad -> these gather candidates; transferability.py then
# ranks by actual proximity. Phrases are quoted for exact matching.
DEFAULT_REGION_TERMS = [
    '"carolinian"',
    '"southern ontario"',
    '"southwestern ontario"',
    '"great lakes"',
    '"lake erie"',
    '"ontario" AND "canada"',
]

# OpenAlex ISO-2 codes to constrain candidate institutions to North America.
CANDIDATE_COUNTRIES = ["CA", "US"]


def gather_candidates(
    *,
    source: str = "openalex",
    region_terms: Optional[list[str]] = None,
    per_term: int = 60,
    year_from: Optional[int] = None,
    require_ecosystem: bool = False,
    sleep: float = 0.2,
) -> list[Paper]:
    """Search, screen, and peer-review-classify regional candidate papers.

    Runs against a single primary `source` so the per-source tables stay
    independent. For "openalex" it also pulls Semantic Scholar and lets
    dedup.deduplicate merge cross-source duplicates (by DOI/title) into one
    record whose `contributing_sources` tracks every backing source — S2 stays
    an enrichment layer, never its own table. For "ebsco" it queries EDS only;
    for "wos" it queries Web of Science only (constrained to North America via
    the CU field). Dedup still runs to collapse within-source duplicates.
    """
    region_terms = region_terms or DEFAULT_REGION_TERMS
    candidates: list[Paper] = []
    for region in region_terms:
        query = f'"ecosystem services" AND {region}'
        if source == "openalex":
            papers = list(openalex.search(
                search=query, year_from=year_from,
                country_codes=CANDIDATE_COUNTRIES, max_results=per_term,
            ))
            try:
                papers += list(semantic_scholar.search(
                    query=query, year_from=year_from, max_results=per_term))
            except Exception:
                pass  # S2 is an enrichment layer; OpenAlex alone still works.
        elif source == "ebsco":
            # EDS has no author-country facet; the region terms constrain
            # geography textually, same as Semantic Scholar.
            papers = list(ebsco.search(
                query=query, year_from=year_from, max_results=per_term))
        elif source == "wos":
            # WoS supports an author-country facet, so constrain to North
            # America like the OpenAlex run does.
            papers = list(wos.search(
                query=query, year_from=year_from,
                country_codes=CANDIDATE_COUNTRIES, max_results=per_term))
        else:
            raise ValueError(f"unknown tier-3 source: {source!r}")
        for paper in papers:
            screening.screen(paper)
            if not screening.is_relevant(paper, require_ecosystem=require_ecosystem):
                continue
            peer_review.classify(paper)
            paper.region = "carolinian"
            candidates.append(paper)
        time.sleep(sleep)
    return dedup.deduplicate(candidates)


def select(
    papers: list[Paper],
    *,
    top_n: int = 25,
    min_score: float = 0.5,
    peer_reviewed_only: bool = False,
) -> list[tuple[Paper, dict]]:
    """Rank candidates by transferability and keep the best."""
    ranked = transferability.rank(papers)
    out = []
    for paper, sc in ranked:
        if peer_reviewed_only and not paper.is_peer_reviewed:
            continue
        if sc["transferability_score"] < min_score:
            continue
        out.append((paper, sc))
        if len(out) >= top_n:
            break
    return out


def write_table(rows: list[TableRow], path: str) -> tuple[str, str]:
    """Write the raw outputs/ CSV and a cleaned reports/filtered/ copy.

    Returns (out_path, report_path). The cleaned copy gets the same tier-3
    treatment report.py applies (drops the raw score, strips inline subscores),
    so reports/filtered/ is populated without a separate `python -m
    litreview.report` run.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    labels = [label for _, label in TableRow.CSV_COLUMNS]
    extra = ["Data Source(s)", "Peer Reviewed", "Transferability Score"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=labels + extra)
        w.writeheader()
        for r in rows:
            d = r.to_csv_dict()
            d["Data Source(s)"] = r.data_source
            d["Peer Reviewed"] = r.peer_reviewed
            d["Transferability Score"] = r.transferability_score
            w.writerow(d)

    # Mirror the output's path under outputs/ into reports/ (preserving the
    # per-source subdir, e.g. filtered/ebsco/tier3_...csv). Falls back to a flat
    # filtered/ path for out_paths written outside the standard outputs tree.
    rel = os.path.relpath(path, SETTINGS.output_dir)
    if rel.startswith(".."):
        rel = os.path.join("filtered", os.path.basename(path))
    report_path = os.path.join(report_mod.REPORTS_DIR, rel)
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    fields, data = report_mod._read_csv(path)
    out_fields, out_rows = report_mod._process_tier3(fields, data)
    with open(report_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=out_fields)
        w.writeheader()
        w.writerows(out_rows)
    return path, report_path


def _source_out_path(source: str) -> str:
    """Default per-source output path: outputs/filtered/<source>/tier3_...csv."""
    return os.path.join(SETTINGS.output_dir, "filtered", source,
                        "tier3_carolinian_table.csv")


def run_source(
    source: str,
    *,
    region_terms: Optional[list[str]] = None,
    per_term: int = 60,
    top_n: int = 25,
    min_score: float = 0.5,
    year_from: Optional[int] = None,
    peer_reviewed_only: bool = False,
    use_fulltext: bool = True,
    llm_location_filter: bool = False,
    out_path: Optional[str] = None,
    do_print: bool = True,
) -> list[TableRow]:
    """Full tier-3 pipeline for a single primary source -> one table."""
    cands = gather_candidates(source=source, region_terms=region_terms,
                              per_term=per_term, year_from=year_from)
    if do_print:
        print(f"[{source}] Gathered {len(cands)} screened regional candidates.")
    chosen = select(cands, top_n=top_n, min_score=min_score,
                    peer_reviewed_only=peer_reviewed_only)
    if do_print:
        print(f"[{source}] Selected {len(chosen)} papers (score >= {min_score}"
              f"{', peer-reviewed only' if peer_reviewed_only else ''}).")

    # Optional LLM layer: judge each paper's STUDY SITE and drop those outside
    # the Carolinian region. Complements the transferability-score gate, which
    # ranks by textual proximity cues rather than reading the paper.
    if llm_location_filter:
        from . import llm_azure
        if not llm_azure.is_configured():
            if do_print:
                print("NOTE: --llm-location-filter requested but Azure is not "
                      "configured; skipping LLM location filter.")
        else:
            kept: list[tuple[Paper, dict]] = []
            for paper, sc in chosen:
                verdict = extract_mod.in_region(
                    paper, CAROLINIAN_REGION, use_fulltext=use_fulltext)
                if verdict["in_region"]:
                    kept.append((paper, sc))
                elif do_print:
                    print(f"  [drop: outside region] {paper.title[:58]} "
                          f"(site: {verdict['study_location'] or 'unspecified'})")
            if do_print:
                print(f"[{source}] LLM location filter: kept {len(kept)}/"
                      f"{len(chosen)} in-region.")
            chosen = kept

    rows: list[TableRow] = []
    for i, (paper, sc) in enumerate(chosen, 1):
        if do_print:
            print(f"  [{i}/{len(chosen)}] {sc['transferability_score']:.2f}  "
                  f"{paper.title[:70]}")
        rows.append(extract_mod.extract_row(paper, use_fulltext=use_fulltext))

    out_path = out_path or _source_out_path(source)
    _, report_path = write_table(rows, out_path)
    if do_print:
        print(f"[{source}] Wrote {len(rows)} rows:")
        print(f"  {out_path}")
        print(f"  {report_path}  (cleaned)")
    return rows


def run(
    *,
    sources: Optional[list[str]] = None,
    region_terms: Optional[list[str]] = None,
    per_term: int = 60,
    top_n: int = 25,
    min_score: float = 0.5,
    year_from: Optional[int] = None,
    peer_reviewed_only: bool = False,
    use_fulltext: bool = True,
    llm_location_filter: bool = False,
    out_path: Optional[str] = None,
    do_print: bool = True,
) -> dict[str, list[TableRow]]:
    """Run tier-3 independently per primary source, one table each.

    Returns {source: rows}. `out_path` is honored only for a single-source run
    (it names one file); multi-source runs use the per-source default paths.
    """
    active = sources or available_sources()
    if do_print:
        print(f"Tier-3 sources: {', '.join(active)}.")
        from . import llm_azure
        if not llm_azure.is_configured():
            print("NOTE: Azure not configured — rows will contain deterministic "
                  "scaffolding only (no LLM-extracted values).")
    if out_path and len(active) > 1:
        raise ValueError("--out names a single file but multiple sources are "
                         "active; drop --out or pass one source via --sources.")

    results: dict[str, list[TableRow]] = {}
    for source in active:
        results[source] = run_source(
            source, region_terms=region_terms, per_term=per_term, top_n=top_n,
            min_score=min_score, year_from=year_from,
            peer_reviewed_only=peer_reviewed_only, use_fulltext=use_fulltext,
            llm_location_filter=llm_location_filter, out_path=out_path,
            do_print=do_print)
    return results


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(
        description="Select transferable Carolinian-Zone papers and fill the "
                    "target ecosystem-services table.")
    p.add_argument("--per-term", type=int, default=60)
    p.add_argument("--top-n", type=int, default=25)
    p.add_argument("--min-score", type=float, default=0.5)
    p.add_argument("--year-from", type=int, default=None,
                   help="publication year floor (default: no year filter)")
    p.add_argument("--peer-reviewed-only", action="store_true")
    p.add_argument("--no-fulltext", action="store_true",
                   help="use abstracts only (skip PDF download/parse)")
    p.add_argument("--llm-location-filter", action="store_true",
                   help="extra LLM pass that drops papers whose study SITE is "
                        "outside the Carolinian region (needs Azure)")
    p.add_argument("--sources", default=None,
                   help="comma-separated primary sources to run "
                        f"(default: configured subset of {','.join(PRIMARY_SOURCES)}). "
                        "Each writes its own outputs/filtered/<source>/ table.")
    p.add_argument("--out", default=None,
                   help="single output file; only valid with one --sources value")
    args = p.parse_args()
    sources = ([s.strip() for s in args.sources.split(",") if s.strip()]
               if args.sources else None)
    run(sources=sources, per_term=args.per_term, top_n=args.top_n,
        min_score=args.min_score, year_from=args.year_from,
        peer_reviewed_only=args.peer_reviewed_only,
        use_fulltext=not args.no_fulltext,
        llm_location_filter=args.llm_location_filter, out_path=args.out)


if __name__ == "__main__":
    main()
