"""Provincial analysis: service/ecosystem x Canadian-province count matrices.

The provincial counterpart to aggregate.py. Where aggregate.py reports each
keyword at global / North America / Canada scopes, this breaks the Canadian
literature down by province and territory, as paper-count matrices:

  rows    = our ecosystem-service (or ecosystem) keywords
  columns = the 13 provinces / territories (config.CANADIAN_PROVINCES)
  cells   = OpenAlex paper counts

How a "province" is defined (OpenAlex has no sub-national filter): the province
name appears in the title/abstract AND the work has a Canadian author
institution (the country scope, default CA, removes false hits like "Ontario,
California"). The query template is printed so the table stays auditable.

Counts only — no ESVD value columns. Unlike aggregate.py, there is no value
side here: ESVD's Canadian coverage is too thin to split by province (only ~80
valued records carry a study location, and no province has more than ~2 distinct
studies — far below the threshold for a defensible median). For Canada-level
dollar values use aggregate.py / esvd_summary.py instead.
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
    CANADA_COUNTRY_CODES,
    CANADIAN_PROVINCES,
)
from .sources import openalex
from .aggregate import count_query


def _count_matrix(
    facet: str,
    terms: list[str],
    *,
    year_from: Optional[int],
    country_codes: Optional[list[str]],
    sleep: float,
) -> pd.DataFrame:
    """OpenAlex paper counts: keyword (rows) x province (cols)."""
    matrix: dict[str, dict[str, int]] = {}
    for term in terms:
        row: dict[str, int] = {}
        for prov, frags in CANADIAN_PROVINCES.items():
            query = f'{count_query(term)} AND "{frags[0]}"'
            row[prov] = openalex.total_count(
                search=query, year_from=year_from, country_codes=country_codes)
            time.sleep(sleep)
        matrix[term] = row
    df = pd.DataFrame(matrix).T.reindex(columns=list(CANADIAN_PROVINCES))
    df.index.name = facet
    return df


def service_by_province(
    *, year_from: Optional[int] = None,
    country_codes: Optional[list[str]] = None, sleep: float = 0.1,
) -> pd.DataFrame:
    cc = CANADA_COUNTRY_CODES if country_codes is None else (country_codes or None)
    return _count_matrix("service", ECOSYSTEM_SERVICE_KEYWORDS,
                         year_from=year_from, country_codes=cc, sleep=sleep)


def ecosystem_by_province(
    *, year_from: Optional[int] = None,
    country_codes: Optional[list[str]] = None, sleep: float = 0.1,
) -> pd.DataFrame:
    cc = CANADA_COUNTRY_CODES if country_codes is None else (country_codes or None)
    return _count_matrix("ecosystem", ECOSYSTEM_KEYWORDS,
                         year_from=year_from, country_codes=cc, sleep=sleep)


def run(
    *,
    year_from: Optional[int] = None,
    country_codes: Optional[list[str]] = None,
    out_dir: Optional[str] = None,
    sleep: float = 0.1,
    do_print: bool = True,
) -> dict[str, pd.DataFrame]:
    """Build the province count matrices and write them to outputs/provincial/."""
    cc = CANADA_COUNTRY_CODES if country_codes is None else (country_codes or None)
    tables = {
        "service_x_province": _count_matrix(
            "service", ECOSYSTEM_SERVICE_KEYWORDS,
            year_from=year_from, country_codes=cc, sleep=sleep),
        "ecosystem_x_province": _count_matrix(
            "ecosystem", ECOSYSTEM_KEYWORDS,
            year_from=year_from, country_codes=cc, sleep=sleep),
    }

    if do_print:
        pd.set_option("display.max_columns", None, "display.width", 240)
        scope = "any country" if cc is None else "/".join(cc)
        print(f"OpenAlex paper counts (>= {year_from}); province = name in "
              f"title/abstract, author institution in [{scope}].")
        print("\n=== Ecosystem service x province (paper counts) ===")
        print(tables["service_x_province"].to_string())
        print("\n=== Ecosystem type x province (paper counts) ===")
        print(tables["ecosystem_x_province"].to_string())

    out_dir = os.path.join(out_dir or SETTINGS.output_dir, "provincial")
    os.makedirs(out_dir, exist_ok=True)
    for name, df in tables.items():
        df.to_csv(os.path.join(out_dir, f"provincial_{name}.csv"),
                  index=True, encoding="utf-8-sig")
    if do_print:
        print(f"\nWrote {len(tables)} CSVs to {out_dir}/")
    return tables


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(
        description="Service/ecosystem x Canadian-province paper-count matrices "
                    "(OpenAlex).")
    p.add_argument("--year-from", type=int, default=None,
                   help="publication year floor (default: no year filter)")
    p.add_argument("--any-country", action="store_true",
                   help="don't restrict to Canadian author institutions "
                        "(province text match only — noisier)")
    p.add_argument("--out", default=None, help="output dir (default: outputs)")
    args = p.parse_args()
    run(year_from=args.year_from,
        country_codes=[] if args.any_country else None,
        out_dir=args.out)


if __name__ == "__main__":
    main()
