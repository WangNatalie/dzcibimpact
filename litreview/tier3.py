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
from .sources import openalex, semantic_scholar
from . import screening, peer_review, transferability, extract as extract_mod, dedup

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
    region_terms: Optional[list[str]] = None,
    per_term: int = 60,
    year_from: Optional[int] = None,
    require_ecosystem: bool = False,
    sleep: float = 0.2,
) -> list[Paper]:
    """Search, screen, and peer-review-classify regional candidate papers.

    Pulls from OpenAlex and Semantic Scholar, then dedup.deduplicate merges
    cross-source duplicates (by DOI/title) into one record whose
    `contributing_sources` records every source that backed it. S2 has no
    country filter, but the region terms constrain geography textually.
    """
    region_terms = region_terms or DEFAULT_REGION_TERMS
    candidates: list[Paper] = []
    for region in region_terms:
        query = f'"ecosystem services" AND {region}'
        papers = list(openalex.search(
            search=query, year_from=year_from,
            country_codes=CANDIDATE_COUNTRIES, max_results=per_term,
        ))
        try:
            papers += list(semantic_scholar.search(
                query=query, year_from=year_from, max_results=per_term))
        except Exception:
            pass  # S2 is an enrichment layer; OpenAlex alone still works.
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


def write_table(rows: list[TableRow], path: str) -> str:
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
    return path


def run(
    *,
    region_terms: Optional[list[str]] = None,
    per_term: int = 60,
    top_n: int = 25,
    min_score: float = 0.5,
    year_from: Optional[int] = None,
    peer_reviewed_only: bool = False,
    use_fulltext: bool = True,
    out_path: Optional[str] = None,
    do_print: bool = True,
) -> list[TableRow]:
    cands = gather_candidates(region_terms=region_terms, per_term=per_term,
                              year_from=year_from)
    if do_print:
        print(f"Gathered {len(cands)} screened regional candidates.")
    chosen = select(cands, top_n=top_n, min_score=min_score,
                    peer_reviewed_only=peer_reviewed_only)
    if do_print:
        print(f"Selected {len(chosen)} papers (score >= {min_score}"
              f"{', peer-reviewed only' if peer_reviewed_only else ''}).")
        from . import llm_azure
        if not llm_azure.is_configured():
            print("NOTE: Azure not configured — rows will contain deterministic "
                  "scaffolding only (no LLM-extracted values).")

    rows: list[TableRow] = []
    for i, (paper, sc) in enumerate(chosen, 1):
        if do_print:
            print(f"  [{i}/{len(chosen)}] {sc['transferability_score']:.2f}  "
                  f"{paper.title[:70]}")
        rows.append(extract_mod.extract_row(paper, use_fulltext=use_fulltext))

    out_path = out_path or os.path.join(SETTINGS.output_dir, "filtered",
                                        "tier3_carolinian_table.csv")
    write_table(rows, out_path)
    if do_print:
        print(f"\nWrote {len(rows)} rows to {out_path}")
    return rows


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
    p.add_argument("--out", default=None)
    args = p.parse_args()
    run(per_term=args.per_term, top_n=args.top_n, min_score=args.min_score,
        year_from=args.year_from, peer_reviewed_only=args.peer_reviewed_only,
        use_fulltext=not args.no_fulltext, out_path=args.out)


if __name__ == "__main__":
    main()
