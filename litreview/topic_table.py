"""Build a tier-3-style table for every paper in an OpenAlex Topic.

Generalizes tier3.py from the hard-wired "ecosystem services" + Carolinian
search to *any* term in keywords_topics.csv. Those terms are OpenAlex Topic
*display names* (the CSV was built by faceting the corpus on `primary_topic`),
so selection here is by Topic id, not free-text search:

  1. resolve the term to its OpenAlex Topic id (openalex.resolve_topic_id),
  2. enumerate every work in that Topic (optionally country-filtered by the
     --location profile), pulling Semantic Scholar in as an enrichment layer,
  3. screen + peer-review-classify + dedup (all deterministic),
  4. fill the target-table row in one of two modes:
       --llm  : Azure OpenAI extraction over full text (extract.extract_row),
       (default) heuristic: NO LLM — regex-parse value figures / lat-long out
                 of the OpenAlex abstract + S2 `tldr` (AI summary) + full text,
                 take the tldr as key findings, cue-sentences as limitations.
     The heuristic path is best-effort and lower-precision than the LLM; every
     auto-filled cell is prefixed so the provenance stays auditable.
  5. write the same columns as tier3_carolinian_table.csv.

The Carolinian SOLRIS-overlap and transferability columns are kept for any
term (both are computed deterministically and remain meaningful as a
"how usable for southern Ontario" signal regardless of topic).
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import time
from typing import Optional

from .config import SETTINGS
from .models import Paper, TableRow
from .sources import openalex, semantic_scholar
from . import screening, peer_review, transferability, dedup
from . import extract as extract_mod
from . import report as report_mod
from .fulltext import fetch_fulltext

# --------------------------------------------------------------------------
# Location profiles: how --location constrains the candidate pull.
#   country_codes  -> OpenAlex institutions.country_code filter (None = global)
#   min_score      -> drop papers below this transferability score (None = keep)
# Add a profile here to support a new --location value.
# --------------------------------------------------------------------------
LOCATION_PROFILES: dict[str, dict] = {
    "global": {"country_codes": None, "min_score": None},
    "north_america": {"country_codes": ["CA", "US", "MX"], "min_score": None},
    "carolinian": {"country_codes": ["CA", "US"], "min_score": 0.5},
}


# --------------------------------------------------------------------------
# Heuristic (no-LLM) extraction. Regex over abstract + S2 tldr + full text.
# --------------------------------------------------------------------------
# Monetary figures, optionally with a per-unit/-time scale.
_CURRENCY_RE = re.compile(
    r"(?:US\$|CA\$|Int\$|USD|CAD|EUR|GBP|\$|€|£)\s?\d[\d,]*(?:\.\d+)?"
    r"(?:\s?(?:million|billion|thousand|bn|k))?"
    r"(?:\s?(?:per|/)\s?(?:ha|hectare|acre|km2|km²|capita|household|"
    r"person|year|yr|annum))*",
    re.IGNORECASE,
)
# Physical rate figures, e.g. "243 g C m-2 yr-1", "2.5 t C ha-1 yr-1".
_RATE_RE = re.compile(
    r"\d[\d,]*(?:\.\d+)?\s?"
    r"(?:t|kg|g|Mg|mg|tonnes?|tons?)\s?(?:C|CO2|CO₂|N|carbon)?\s?"
    r"(?:ha-?1|ha−1|m-?2|m−2|km-?2|/ha|per\s?ha|per\s?hectare)\s?"
    r"(?:yr-?1|yr−1|year-?1|/yr|/year|per\s?year)?",
    re.IGNORECASE,
)
# Coordinates given with a hemisphere letter (safe to sign), e.g. "43.5°N, 80.2°W".
_LATLON_RE = re.compile(
    r"(\d{1,3}(?:\.\d+)?)\s?°?\s?([NSns])[ ,;]+"
    r"(\d{1,3}(?:\.\d+)?)\s?°?\s?([EWew])"
)
_FINDING_CUES = re.compile(
    r"\b(we (?:find|found|show|estimate|conclude)|results? (?:show|indicate|"
    r"suggest)|estimated? at|valued at|amounted to|significant|increased?|"
    r"decreased?|declined?|concluded?)\b",
    re.IGNORECASE,
)
_LIMIT_CUES = re.compile(
    r"\b(limitation|uncertaint|caveat|however|further research|future (?:work|"
    r"research)|should be interpreted|with caution|did not|were not able|"
    r"data (?:gap|were (?:scarce|limited|lacking)))\b",
    re.IGNORECASE,
)
_SENT_SPLIT = re.compile(r"(?<=[.!?])\s+")


def _tldr(paper: Paper) -> str:
    """Semantic Scholar's AI summary, if this record carried one."""
    t = (paper.raw or {}).get("tldr") or {}
    return (t.get("text") or "").strip() if isinstance(t, dict) else ""


def _dedup_join(values: list[str], cap: int) -> str:
    seen: list[str] = []
    for v in values:
        v = re.sub(r"\s{2,}", " ", v).strip(" .,;")
        if v and v.lower() not in {s.lower() for s in seen}:
            seen.append(v)
        if len(seen) >= cap:
            break
    return "; ".join(seen)


def _extract_values(text: str) -> str:
    hits = [m.group(0) for m in _CURRENCY_RE.finditer(text)]
    hits += [m.group(0) for m in _RATE_RE.finditer(text)]
    return _dedup_join(hits, cap=6)


def _extract_latlon(text: str) -> tuple[Optional[float], Optional[float]]:
    m = _LATLON_RE.search(text)
    if not m:
        return None, None
    lat = float(m.group(1)) * (-1 if m.group(2).upper() == "S" else 1)
    lon = float(m.group(3)) * (-1 if m.group(4).upper() == "W" else 1)
    return lat, lon


def _cue_sentences(text: str, cue: re.Pattern, limit: int) -> str:
    out = [s.strip() for s in _SENT_SPLIT.split(text) if cue.search(s)]
    return _dedup_join(out, cap=limit)


def _region_mention(paper: Paper) -> str:
    """First Carolinian/region place name found in the text (title-cased)."""
    text = f"{paper.title} {paper.abstract} {paper.location_text}".lower()
    for _, terms in transferability._REGION_TIERS:
        for t in terms:
            if re.search(rf"(?<![a-z]){re.escape(t)}(?![a-z])", text):
                return t.title()
    return ""


def heuristic_row(paper: Paper, *, use_fulltext: bool, max_chars: int) -> TableRow:
    """Fill a TableRow with regex/heuristic extraction — no LLM calls."""
    if use_fulltext:
        text, source = fetch_fulltext(paper, max_chars=max_chars)
    else:
        text, source = paper.abstract or "", "abstract"

    tldr = _tldr(paper)
    blob = "\n".join(p for p in (paper.title, paper.abstract, tldr, text) if p)
    tscore = transferability.score(paper)
    lat, lon = _extract_latlon(blob)

    value = _extract_values(blob)
    findings = tldr or _cue_sentences(paper.abstract or text, _FINDING_CUES, 2)
    limits = _cue_sentences(blob, _LIMIT_CUES, 2)
    link = (f"https://doi.org/{paper.doi}" if paper.doi
            else (paper.oa_pdf_url or ""))

    return TableRow(
        source=paper.title,
        location=_region_mention(paper) or paper.location_text,
        lat=lat if lat is not None else paper.lat,
        lon=lon if lon is not None else paper.lon,
        ecosystem=", ".join(paper.ecosystems),
        solris_overlap=extract_mod.solris_overlap(paper),
        ecosystem_service=", ".join(paper.ecosystem_services),
        value_measurement_scale=(f"[heuristic] {value}" if value else ""),
        method=", ".join(paper.methods),
        limitations_uncertainty=(f"[heuristic] {limits}" if limits else ""),
        link=link,
        key_findings=(
            f"[tldr] {findings}" if (findings and tldr)
            else (f"[heuristic] {findings}" if findings else "")
        ),
        utility_transferability=f"[auto] {tscore['rationale']}",
        data_source=paper.source_label,
        peer_reviewed=paper.is_peer_reviewed,
        transferability_score=tscore["transferability_score"],
    )


# --------------------------------------------------------------------------
# Candidate gathering
# --------------------------------------------------------------------------
def gather_topic_papers(
    topic_id: str,
    term: str,
    *,
    seed_query: str,
    country_codes: Optional[list[str]],
    year_from: Optional[int],
    max_results: int,
    require_ecosystem: bool = False,
    sleep: float = 0.2,
) -> list[Paper]:
    """Enumerate a Topic from OpenAlex (+ S2 enrichment), screen, and dedup.

    `seed_query` is AND-ed with the Topic id so the pull matches the corpus
    that built keywords_topics.csv (those counts are already conditioned on the
    seed search). Pass seed_query="" to enumerate the whole Topic instead.
    """
    candidates: list[Paper] = []
    papers = list(openalex.search(
        search=seed_query or None,
        extra={"primary_topic.id": topic_id},
        year_from=year_from, country_codes=country_codes,
        max_results=max_results,
    ))
    try:  # S2 has no topic ids; seed + term as a relevance query is the bridge.
        s2_query = " ".join(q for q in (f'"{seed_query}"' if seed_query else "",
                                        term) if q).strip()
        papers += list(semantic_scholar.search(
            query=s2_query, year_from=year_from, max_results=max_results))
    except Exception:
        pass  # enrichment only — OpenAlex alone still works.
    time.sleep(sleep)
    for paper in papers:
        screening.screen(paper)
        if not screening.is_relevant(paper, require_ecosystem=require_ecosystem):
            continue
        peer_review.classify(paper)
        candidates.append(paper)
    return dedup.deduplicate(candidates)


def select(
    papers: list[Paper],
    *,
    min_score: Optional[float],
    peer_reviewed_only: bool,
) -> list[Paper]:
    """Rank by transferability; drop below min_score / non-peer-reviewed."""
    out: list[Paper] = []
    for paper, sc in transferability.rank(papers):
        if peer_reviewed_only and not paper.is_peer_reviewed:
            continue
        if min_score is not None and sc["transferability_score"] < min_score:
            continue
        out.append(paper)
    return out


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------
def _slug(term: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", term.lower()).strip("_") or "topic"


def write_tables(rows: list[TableRow], term: str, location: str) -> tuple[str, str]:
    """Write the raw outputs/ CSV and a cleaned reports/ CSV. Returns both paths."""
    base = f"topic_table_{_slug(term)}_{location}.csv"
    out_dir = os.path.join(SETTINGS.output_dir, "filtered")
    out_path = os.path.join(out_dir, base)
    os.makedirs(out_dir, exist_ok=True)

    labels = [label for _, label in TableRow.CSV_COLUMNS]
    extra = ["Data Source(s)", "Peer Reviewed", "Transferability Score"]
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=labels + extra)
        w.writeheader()
        for r in rows:
            d = r.to_csv_dict()
            d["Data Source(s)"] = r.data_source
            d["Peer Reviewed"] = r.peer_reviewed
            d["Transferability Score"] = r.transferability_score
            w.writerow(d)

    # Cleaned, human-readable copy (drops the raw score, strips inline subscores)
    # — same treatment report.py gives tier3 tables.
    rep_dir = os.path.join(report_mod.REPORTS_DIR, "filtered")
    os.makedirs(rep_dir, exist_ok=True)
    report_path = os.path.join(rep_dir, base)
    fields, data = report_mod._read_csv(out_path)
    out_fields, out_rows = report_mod._process_tier3(fields, data)
    with open(report_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=out_fields)
        w.writeheader()
        w.writerows(out_rows)
    return out_path, report_path


def run(
    term: str,
    *,
    location: str = "global",
    use_llm: bool = False,
    seed_query: str = "ecosystem service",
    year_from: Optional[int] = None,
    max_results: int = 200,
    peer_reviewed_only: bool = False,
    use_fulltext: bool = False,
    max_chars: int = 60000,
    do_print: bool = True,
) -> list[TableRow]:
    if location not in LOCATION_PROFILES:
        raise ValueError(
            f"--location must be one of {sorted(LOCATION_PROFILES)}, got {location!r}"
        )
    profile = LOCATION_PROFILES[location]

    topic = openalex.resolve_topic_id(term)
    if not topic:
        raise SystemExit(
            f"No OpenAlex Topic matched {term!r}. Use a term from "
            f"keywords_topics.csv (a Topic display name).")
    # Universe actually targeted: seed query AND this topic, under the location
    # filter. This is the keywords_topics.csv-style count (seed-conditioned).
    universe = openalex.total_count(
        search=seed_query or None,
        extra={"primary_topic.id": topic["id"]},
        year_from=year_from, country_codes=profile["country_codes"])
    if do_print:
        print(f"Topic: {topic['display_name']} ({topic['id']}, "
              f"{topic['works_count']:,} works in topic)")
        print(f"Seed query: {seed_query or '(none — whole topic)'}")
        print(f"Location profile: {location} "
              f"(countries={profile['country_codes'] or 'any'}, "
              f"min_score={profile['min_score']})")
        print(f"Targetable universe (seed AND topic): {universe:,}", end="")
        print(f"  — capped at max_results={max_results:,}"
              if universe > max_results else "")

    cands = gather_topic_papers(
        topic["id"], term, seed_query=seed_query,
        country_codes=profile["country_codes"],
        year_from=year_from, max_results=max_results,
    )
    chosen = select(cands, min_score=profile["min_score"],
                    peer_reviewed_only=peer_reviewed_only)
    if do_print:
        print(f"Screened {len(cands)} relevant; kept {len(chosen)} after "
              f"location/peer-review filters.")
        mode = "LLM (Azure)" if use_llm else "heuristic regex (no LLM)"
        print(f"Extraction mode: {mode}")
        if use_llm:
            from . import llm_azure
            if not llm_azure.is_configured():
                print("NOTE: Azure not configured — falling back to "
                      "deterministic scaffolding for LLM columns.")

    rows: list[TableRow] = []
    for i, paper in enumerate(chosen, 1):
        if do_print:
            print(f"  [{i}/{len(chosen)}] {paper.title[:72]}")
        if use_llm:
            rows.append(extract_mod.extract_row(paper, use_fulltext=use_fulltext,
                                                max_chars=max_chars))
        else:
            rows.append(heuristic_row(paper, use_fulltext=use_fulltext,
                                      max_chars=max_chars))

    out_path, report_path = write_tables(rows, term, location)
    if do_print:
        print(f"\nWrote {len(rows)} rows:")
        print(f"  {out_path}")
        print(f"  {report_path}  (cleaned)")
    return rows


def main() -> None:
    p = argparse.ArgumentParser(
        description="Build a tier-3-style table for an OpenAlex Topic (a term "
                    "from keywords_topics.csv), with or without an LLM.")
    p.add_argument("--term", required=True,
                   help="Topic display name from keywords_topics.csv, e.g. "
                        "'Land Use and Ecosystem Services'")
    p.add_argument("--location", default="global",
                   choices=sorted(LOCATION_PROFILES),
                   help="geographic profile (default: global)")
    p.add_argument("--llm", action="store_true",
                   help="use Azure OpenAI extraction (default: heuristic, no LLM)")
    p.add_argument("--seed-query", default="ecosystem service",
                   help="search AND-ed with the topic; matches the corpus that "
                        "built keywords_topics.csv (default: %(default)r). Pass "
                        "'' to enumerate the whole topic.")
    p.add_argument("--year-from", type=int, default=None,
                   help="publication year floor (default: no year filter)")
    p.add_argument("--max-results", type=int, default=200,
                   help="cap papers pulled per source (default: 200)")
    p.add_argument("--peer-reviewed-only", action="store_true")
    p.add_argument("--fulltext", action="store_true",
                   help="download + parse PDFs for extraction (slower)")
    args = p.parse_args()
    run(args.term, location=args.location, use_llm=args.llm,
        seed_query=args.seed_query,
        year_from=args.year_from, max_results=args.max_results,
        peer_reviewed_only=args.peer_reviewed_only, use_fulltext=args.fulltext)


if __name__ == "__main__":
    main()
