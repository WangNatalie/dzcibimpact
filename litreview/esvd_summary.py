"""Standalone ESVD value summary.

Prints (and optionally writes) robust median/IQR summaries of ESVD's
`Int$ Per Hectare Per Year` by ecosystem service and by ecosystem type, for
global and North American (US/CA/MX) scopes. This is the self-contained version
of the tier-1/2 value tables — it needs only the ESVD CSV, no web APIs.

Usage:
    python -m litreview.esvd_summary
    python -m litreview.esvd_summary --esvd path/to/esvd.csv --reviewed-only
    ESVD_CSV=... python -m litreview.esvd_summary --out litreview/outputs
"""

from __future__ import annotations

import argparse
import os
from typing import Optional

import pandas as pd

from .config import SETTINGS
from .sources import esvd

_DISPLAY_COLS = ["n", "n_studies", "median", "q1", "q3", "trimmed_mean_10pct"]


def _print_table(title: str, df: pd.DataFrame, index_col: str) -> None:
    print(f"\n=== {title} ===")
    if df.empty:
        print("  (no records)")
        return
    cols = [index_col] + [c for c in _DISPLAY_COLS if c in df.columns]
    print(df[cols].to_string(index=False))


def build(
    db: esvd.ESVD, *, reviewed_only: bool = False
) -> dict[str, pd.DataFrame]:
    """Return the four summary tables as DataFrames."""
    na = esvd.north_america_country_codes()
    return {
        "service_global": db.summary_by("service", reviewed_only=reviewed_only),
        "service_north_america": db.summary_by(
            "service", country_codes=na, reviewed_only=reviewed_only
        ),
        "ecosystem_global": db.summary_by("ecosystem", reviewed_only=reviewed_only),
        "ecosystem_north_america": db.summary_by(
            "ecosystem", country_codes=na, reviewed_only=reviewed_only
        ),
    }


def run(
    esvd_path: Optional[str] = None,
    *,
    reviewed_only: bool = False,
    out_dir: Optional[str] = None,
    do_print: bool = True,
) -> dict[str, pd.DataFrame]:
    db = esvd.load(esvd_path)
    tables = build(db, reviewed_only=reviewed_only)

    if do_print:
        scope = " (peer-reviewed records only)" if reviewed_only else ""
        print(f"ESVD loaded: {len(db.df):,} value records"
              f" ({int(db.df['StudyId'].nunique()):,} studies){scope}")
        print("Value unit: Int$ (PPP) per hectare per year — medians, not means")
        _print_table("GLOBAL — by ecosystem service",
                     tables["service_global"], "service")
        _print_table("NORTH AMERICA (US/CA/MX) — by ecosystem service",
                     tables["service_north_america"], "service")
        _print_table("GLOBAL — by ecosystem type",
                     tables["ecosystem_global"], "ecosystem")
        _print_table("NORTH AMERICA (US/CA/MX) — by ecosystem type",
                     tables["ecosystem_north_america"], "ecosystem")

    out_dir = out_dir or SETTINGS.output_dir
    os.makedirs(out_dir, exist_ok=True)
    for name, df in tables.items():
        path = os.path.join(out_dir, f"esvd_summary_{name}.csv")
        df.to_csv(path, index=False)
    if do_print:
        print(f"\nWrote 4 CSVs to {out_dir}/esvd_summary_*.csv")
    return tables


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--esvd", default=None,
                   help="path to ESVD CSV (default: $ESVD_CSV)")
    p.add_argument("--reviewed-only", action="store_true",
                   help="restrict to records flagged Reviewed in ESVD")
    p.add_argument("--out", default=None,
                   help="output dir for CSVs (default: litreview/outputs)")
    args = p.parse_args()
    run(args.esvd, reviewed_only=args.reviewed_only, out_dir=args.out)


if __name__ == "__main__":
    main()
