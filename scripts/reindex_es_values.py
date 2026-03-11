"""
reindex_es_values.py
--------------------
Upload ecosystem-service lookup tables to Supabase.

Run this to update SOLRIS and water filtration ecosystem service values in Supabase. 
Tables in Supabase required to run ecosystem service valuation of an area by processor.py.

Usage:
    python reindex_es_values.py
    python reindex_es_values.py --solris-csv path/to/solris_lookup.csv --water-csv  path/to/water_filtration_lookup.csv
"""

import argparse
import os
import sys

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

SOLRIS_REQUIRED_COLUMNS = [
    "solris_code", "solris_class", "biocapacity_category",
    "biocapacity_conversion_factor", "lulc_category",
    "agc_tc_ha", "bgc_tc_ha", "soc_tc_ha", "deoc_tc_ha",
    "naturalness", "description",
]

WATER_REQUIRED_COLUMNS = ["wetland_type", "value"]


def _supabase_engine():
    url = os.getenv("SUPABASE_URL")
    if not url:
        sys.exit("Error: SUPABASE_URL is not set in .env")
    conn_str = url.replace("postgres://", "postgresql://", 1)
    return create_engine(conn_str)


def _drop_and_upload(engine, table_name: str, df: pd.DataFrame) -> None:
    """Cascade drop table if it exists then upload df as a fresh table."""
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
    df.to_sql(table_name, engine, if_exists="append", index=False)


def upload_solris_lookup(csv_path: str) -> None:
    df = pd.read_csv(csv_path)
    missing = [c for c in SOLRIS_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        sys.exit(f"Error: solris CSV is missing columns: {missing}")

    df = df.dropna(subset=["solris_code"])

    engine = _supabase_engine()
    try:
        _drop_and_upload(engine, "solris_lookup", df)
        print(f"  solris_lookup: {len(df)} rows uploaded from '{csv_path}'")
    finally:
        engine.dispose()


def upload_water_filtration_lookup(csv_path: str) -> None:
    df = pd.read_csv(csv_path)
    missing = [c for c in WATER_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        sys.exit(f"Error: water filtration CSV is missing columns: {missing}")

    # Normalise to the column names the processor expects
    df = df.rename(columns={"wetland_type": "solris_class", "value": "wf_value_per_ha"})

    engine = _supabase_engine()
    try:
        _drop_and_upload(engine, "water_filtration_lookup", df)
        print(f"  water_filtration_lookup: {len(df)} rows uploaded from '{csv_path}'")
    finally:
        engine.dispose()


def main():
    parser = argparse.ArgumentParser(
        description="Upload ecosystem-service lookup CSVs to Supabase"
    )
    parser.add_argument(
        "--solris-csv",
        default="data/solris_lookup.csv",
        help="SOLRIS classification lookup CSV  [default: %(default)s]",
    )
    parser.add_argument(
        "--water-csv",
        default="data/water_filtration_lookup.csv",
        help="Water filtration lookup CSV  [default: %(default)s]",
    )
    args = parser.parse_args()

    print("Uploading lookup tables to Supabase...")
    upload_solris_lookup(args.solris_csv)
    upload_water_filtration_lookup(args.water_csv)
    print("Done.")


if __name__ == "__main__":
    main()
