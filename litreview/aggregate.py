"""Tier-1/2 aggregation: join paper counts (OpenAlex) with value medians (ESVD).

This produces deliverable #1: one table per ecosystem service (and per
ecosystem type) showing, side by side,
  * how many papers exist globally vs in North America (OpenAlex counts), and
  * the median transferable value, Int$/ha/yr, globally vs in North America
    (ESVD, robust median + IQR).

Counts and values come from two independent sources, so the table reports them
as parallel columns rather than implying one was derived from the other. The
"global" scope is unfiltered; "North America" is US/CA/MX (OpenAlex ISO-2 for
counts, ESVD ISO-3 for values).
"""

from __future__ import annotations

import os
import time
from typing import Optional

import pandas as pd

from .config import (
    SETTINGS,
    ECOSYSTEM_SERVICE_KEYWORDS,
    ECOSYSTEM_KEYWORDS,
    NORTH_AMERICA_COUNTRY_CODES,
)
from .sources import openalex, esvd


# Below this many distinct studies, a median Int$/ha/yr is too unstable to
# report — the cell shows the study count but blanks the value (see _value_cells).
MIN_STUDIES_FOR_MEDIAN = 5

# Count-precision: OpenAlex `.search` is relevance-based, so multi-word terms
# over-match. We quote every term for exact-phrase matching. A few terms still
# collide with unrelated literature even when quoted (generic English or short
# tokens), so they get a domain anchor ANDed in. Anchored rows are flagged in
# the output `count_query` column so the table stays auditable.
COUNT_OVERRIDES: dict[str, str] = {
    "REV": '"REV" AND "ecosystem services"',
    "supporting services": '"supporting services" AND ecosystem',
    "regulating services": '"regulating services" AND ecosystem',
    "provisioning services": '"provisioning services" AND ecosystem',
}


def count_query(term: str) -> str:
    """Precise OpenAlex search string for a keyword (quoted, anchored if noisy)."""
    if term in COUNT_OVERRIDES:
        return COUNT_OVERRIDES[term]
    return f'"{term}"'


def _iqr(stats: dict) -> str:
    if not stats.get("n"):
        return ""
    return f"{stats['q1']:.0f}-{stats['q3']:.0f}"


def _value_cells(db: Optional[esvd.ESVD], facet: str, key: str,
                 country_codes: Optional[list[str]]) -> dict:
    """ESVD median/IQR/study-count for one service-or-ecosystem cell."""
    if db is None:
        return {"median": None, "iqr": "", "n_studies": None, "n_values": None}
    kw = {"service": key} if facet == "service" else {"ecosystem": key}
    d = db.filter(country_codes=country_codes, **kw)
    s = esvd.ESVD.summarize_values(d)
    n_studies = s.get("n_studies")
    # Suppress medians backed by too few studies (keep the count visible).
    stable = bool(n_studies and n_studies >= MIN_STUDIES_FOR_MEDIAN)
    return {
        "median": s.get("median") if stable else None,
        "iqr": _iqr(s) if stable else "",
        "n_studies": n_studies,
        "n_values": s.get("n"),
    }


def _facet_table(
    facet: str,
    terms: list[str],
    db: Optional[esvd.ESVD],
    *,
    year_from: Optional[int] = 2000,
    sleep: float = 0.1,
) -> pd.DataFrame:
    """Build the joined counts+values table for a list of terms."""
    na_oa = NORTH_AMERICA_COUNTRY_CODES                 # ISO-2 for OpenAlex
    na_esvd = esvd.north_america_country_codes()        # ISO-3 for ESVD
    rows = []
    for term in terms:
        query = count_query(term)
        g_papers = openalex.total_count(search=query, year_from=year_from)
        time.sleep(sleep)
        na_papers = openalex.total_count(
            search=query, year_from=year_from, country_codes=na_oa)
        time.sleep(sleep)
        g_val = _value_cells(db, facet, term, None)
        na_val = _value_cells(db, facet, term, na_esvd)
        rows.append({
            facet: term,
            "count_query": query,
            "papers_global": g_papers,
            "papers_north_america": na_papers,
            "na_share": round(na_papers / g_papers, 3) if g_papers else None,
            "value_median_global": g_val["median"],
            "value_iqr_global": g_val["iqr"],
            "esvd_studies_global": g_val["n_studies"],
            "value_median_north_america": na_val["median"],
            "value_iqr_north_america": na_val["iqr"],
            "esvd_studies_north_america": na_val["n_studies"],
        })
    return pd.DataFrame(rows)


def service_table(db: Optional[esvd.ESVD] = None, *, year_from: int = 2000,
                  sleep: float = 0.1) -> pd.DataFrame:
    return _facet_table("service", ECOSYSTEM_SERVICE_KEYWORDS, db,
                        year_from=year_from, sleep=sleep)


def ecosystem_table(db: Optional[esvd.ESVD] = None, *, year_from: int = 2000,
                    sleep: float = 0.1) -> pd.DataFrame:
    return _facet_table("ecosystem", ECOSYSTEM_KEYWORDS, db,
                        year_from=year_from, sleep=sleep)


def service_by_ecosystem_counts(
    *, year_from: int = 2000, region: str = "global", sleep: float = 0.05
) -> pd.DataFrame:
    """Cross-tab of paper counts: ecosystem service (rows) x ecosystem (cols).

    region: "global" or "north_america". Values are OpenAlex paper counts for
    the conjunction of both terms (service AND ecosystem in title/abstract).
    """
    cc = NORTH_AMERICA_COUNTRY_CODES if region == "north_america" else None
    matrix = {}
    for svc in ECOSYSTEM_SERVICE_KEYWORDS:
        row = {}
        for eco in ECOSYSTEM_KEYWORDS:
            row[eco] = openalex.total_count(
                search=f'{count_query(svc)} AND "{eco}"',
                year_from=year_from, country_codes=cc)
            time.sleep(sleep)
        matrix[svc] = row
    return pd.DataFrame(matrix).T  # services as rows


def run(
    esvd_path: Optional[str] = None,
    *,
    year_from: int = 2000,
    with_crosstab: bool = False,
    out_dir: Optional[str] = None,
    do_print: bool = True,
) -> dict[str, pd.DataFrame]:
    db = esvd.load(esvd_path) if (esvd_path or SETTINGS.esvd_csv) else None
    if db is None and do_print:
        print("WARN: no ESVD CSV set; value columns will be empty.\n")

    tables = {
        "service_x_region": service_table(db, year_from=year_from),
        "ecosystem_x_region": ecosystem_table(db, year_from=year_from),
    }
    if with_crosstab:
        tables["service_by_ecosystem_counts_global"] = \
            service_by_ecosystem_counts(year_from=year_from, region="global")

    if do_print:
        pd.set_option("display.max_columns", None, "display.width", 200)
        print(f"Paper counts: OpenAlex (>= {year_from}). "
              f"Values: ESVD median Int$/ha/yr (IQR).")
        print("\n=== Ecosystem service x region ===")
        print(tables["service_x_region"].to_string(index=False))
        print("\n=== Ecosystem type x region ===")
        print(tables["ecosystem_x_region"].to_string(index=False))

    out_dir = out_dir or SETTINGS.output_dir
    os.makedirs(out_dir, exist_ok=True)
    for name, df in tables.items():
        df.to_csv(os.path.join(out_dir, f"aggregate_{name}.csv"), index=False)
    if do_print:
        print(f"\nWrote {len(tables)} CSVs to {out_dir}/aggregate_*.csv")
    return tables


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(
        description="Join OpenAlex paper counts with ESVD value medians into "
                    "service x region and ecosystem x region tables.")
    p.add_argument("--esvd", default=None, help="ESVD CSV (default: $ESVD_CSV)")
    p.add_argument("--year-from", type=int, default=2000)
    p.add_argument("--crosstab", action="store_true",
                   help="also build the service x ecosystem count matrix (slow)")
    p.add_argument("--out", default=None, help="output dir (default: outputs)")
    args = p.parse_args()
    run(args.esvd, year_from=args.year_from, with_crosstab=args.crosstab,
        out_dir=args.out)


if __name__ == "__main__":
    main()
