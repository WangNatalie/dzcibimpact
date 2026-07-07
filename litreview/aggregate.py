"""Tier-1/2 aggregation: join paper counts (OpenAlex) with value medians (ESVD).

This produces deliverable #1: one table per ecosystem service (and per
ecosystem type) showing, side by side,
  * how many papers exist globally vs in North America (OpenAlex counts), and
  * the median transferable value, Int$/ha/yr, globally vs in North America
    (ESVD, robust median + IQR).

Counts and values come from two independent sources, so the table reports them
as parallel columns rather than implying one was derived from the other. Each
metric is reported at three nested scopes: "global" (unfiltered), "North
America" (US/CA/MX), and "Canada" (CA only).
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
    CANADA_COUNTRY_CODES,
)
from .sources import openalex, esvd, ebsco, wos


# Count sources for the aggregate cross-tabs, in output order. Each entry is
# (name, module exposing total_count(search=, year_from=, country_codes=),
#  supports_country). EBSCO has no author-country facet, so its per-region
# (North America / Canada) cells are left blank rather than faked from a
# keyword proxy; OpenAlex and WoS do support it. New count sources plug in
# here. `available_count_sources()` filters to the ones whose credentials are
# actually configured.
COUNT_SOURCES: list[tuple[str, object, bool]] = [
    ("openalex", openalex, True),
    ("ebsco", ebsco, False),
    ("wos", wos, True),
]


def available_count_sources() -> list[tuple[str, object, bool]]:
    """COUNT_SOURCES minus any whose credentials aren't configured."""
    out = []
    for name, mod, supports_country in COUNT_SOURCES:
        if hasattr(mod, "is_configured") and not mod.is_configured():
            continue
        out.append((name, mod, supports_country))
    return out


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
    source: object = openalex,
    supports_country: bool = True,
    year_from: Optional[int] = None,
    sleep: float = 0.1,
) -> pd.DataFrame:
    """Build the joined counts+values table for a list of terms.

    Paper counts come from `source` (any module exposing total_count); the
    ESVD value columns are source-independent. When `supports_country` is
    False the per-region count cells are left blank (the source can't filter by
    author country), so the schema stays identical across sources.
    """
    na_oa = NORTH_AMERICA_COUNTRY_CODES                 # ISO-2 for OpenAlex
    na_esvd = esvd.north_america_country_codes()        # ISO-3 for ESVD
    ca_oa = CANADA_COUNTRY_CODES                        # ISO-2 for OpenAlex
    ca_esvd = esvd.canada_country_codes()               # ISO-3 for ESVD
    rows = []
    for term in terms:
        query = count_query(term)
        g_papers = source.total_count(search=query, year_from=year_from)
        time.sleep(sleep)
        if supports_country:
            na_papers = source.total_count(
                search=query, year_from=year_from, country_codes=na_oa)
            time.sleep(sleep)
            ca_papers = source.total_count(
                search=query, year_from=year_from, country_codes=ca_oa)
            time.sleep(sleep)
        else:
            na_papers = ca_papers = None
        g_val = _value_cells(db, facet, term, None)
        na_val = _value_cells(db, facet, term, na_esvd)
        ca_val = _value_cells(db, facet, term, ca_esvd)
        rows.append({
            facet: term,
            "count_query": query,
            "papers_global": g_papers,
            "papers_north_america": na_papers,
            "papers_canada": ca_papers,
            "na_share": (round(na_papers / g_papers, 3)
                         if g_papers and na_papers is not None else None),
            "ca_share": (round(ca_papers / g_papers, 3)
                         if g_papers and ca_papers is not None else None),
            "value_median_global": g_val["median"],
            "value_iqr_global": g_val["iqr"],
            "esvd_studies_global": g_val["n_studies"],
            "value_median_north_america": na_val["median"],
            "value_iqr_north_america": na_val["iqr"],
            "esvd_studies_north_america": na_val["n_studies"],
            "value_median_canada": ca_val["median"],
            "value_iqr_canada": ca_val["iqr"],
            "esvd_studies_canada": ca_val["n_studies"],
        })
    return pd.DataFrame(rows)


def service_table(db: Optional[esvd.ESVD] = None, *, source: object = openalex,
                  supports_country: bool = True, year_from: Optional[int] = None,
                  sleep: float = 0.1) -> pd.DataFrame:
    return _facet_table("service", ECOSYSTEM_SERVICE_KEYWORDS, db,
                        source=source, supports_country=supports_country,
                        year_from=year_from, sleep=sleep)


def ecosystem_table(db: Optional[esvd.ESVD] = None, *, source: object = openalex,
                    supports_country: bool = True, year_from: Optional[int] = None,
                    sleep: float = 0.1) -> pd.DataFrame:
    return _facet_table("ecosystem", ECOSYSTEM_KEYWORDS, db,
                        source=source, supports_country=supports_country,
                        year_from=year_from, sleep=sleep)


def service_by_ecosystem_counts(
    *, source: object = openalex, supports_country: bool = True,
    year_from: Optional[int] = None, region: str = "global", sleep: float = 0.05
) -> pd.DataFrame:
    """Cross-tab of paper counts: ecosystem service (rows) x ecosystem (cols).

    region: "global" or "north_america". Values are `source` paper counts for
    the conjunction of both terms (service AND ecosystem in title/abstract).
    A source without country support ignores a non-global region.
    """
    cc = (NORTH_AMERICA_COUNTRY_CODES
          if region == "north_america" and supports_country else None)
    matrix = {}
    for svc in ECOSYSTEM_SERVICE_KEYWORDS:
        row = {}
        for eco in ECOSYSTEM_KEYWORDS:
            row[eco] = source.total_count(
                search=f'{count_query(svc)} AND "{eco}"',
                year_from=year_from, country_codes=cc)
            time.sleep(sleep)
        matrix[svc] = row
    return pd.DataFrame(matrix).T  # services as rows


def run(
    esvd_path: Optional[str] = None,
    *,
    year_from: Optional[int] = None,
    with_crosstab: bool = False,
    out_dir: Optional[str] = None,
    do_print: bool = True,
) -> dict[str, pd.DataFrame]:
    db = esvd.load(esvd_path) if (esvd_path or SETTINGS.esvd_csv) else None
    if db is None and do_print:
        print("WARN: no ESVD CSV set; value columns will be empty.\n")

    base_dir = out_dir or SETTINGS.output_dir
    sources = available_count_sources()
    if do_print:
        print(f"Count sources: {', '.join(n for n, _, _ in sources)}. "
              f"Values: ESVD median Int$/ha/yr (IQR).")

    # Each count source gets its own subdirectory so the tables stay comparable
    # rather than merged (EBSCO and OpenAlex index different corpora and can't
    # be summed). The returned dict is keyed "<source>/<table>".
    tables: dict[str, pd.DataFrame] = {}
    for source_name, mod, supports_country in sources:
        src_tables = {
            "service_x_region": service_table(
                db, source=mod, supports_country=supports_country,
                year_from=year_from),
            "ecosystem_x_region": ecosystem_table(
                db, source=mod, supports_country=supports_country,
                year_from=year_from),
        }
        if with_crosstab:
            src_tables["service_by_ecosystem_counts_global"] = \
                service_by_ecosystem_counts(
                    source=mod, supports_country=supports_country,
                    year_from=year_from, region="global")

        if do_print:
            pd.set_option("display.max_columns", None, "display.width", 200)
            print(f"\n### Source: {source_name} (>= {year_from}) ###")
            print("=== Ecosystem service x region ===")
            print(src_tables["service_x_region"].to_string(index=False))
            print("\n=== Ecosystem type x region ===")
            print(src_tables["ecosystem_x_region"].to_string(index=False))

        src_dir = os.path.join(base_dir, "aggregate", source_name)
        os.makedirs(src_dir, exist_ok=True)
        for name, df in src_tables.items():
            df.to_csv(os.path.join(src_dir, f"aggregate_{name}.csv"),
                      index=False, encoding="utf-8-sig")
            tables[f"{source_name}/{name}"] = df
        if do_print:
            print(f"Wrote {len(src_tables)} CSVs to {src_dir}/")
    return tables


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(
        description="Join bibliographic paper counts with ESVD value medians "
                    "into service x region and ecosystem x region tables, one "
                    "set of files per configured count source (OpenAlex, EBSCO).")
    p.add_argument("--esvd", default=None, help="ESVD CSV (default: $ESVD_CSV)")
    p.add_argument("--year-from", type=int, default=None,
                   help="publication year floor (default: no year filter)")
    p.add_argument("--crosstab", action="store_true",
                   help="also build the service x ecosystem count matrix (slow)")
    p.add_argument("--out", default=None, help="output dir (default: outputs)")
    args = p.parse_args()
    run(args.esvd, year_from=args.year_from, with_crosstab=args.crosstab,
        out_dir=args.out)


if __name__ == "__main__":
    main()
