"""
database_helpers.py
-------------------
Utilities for syncing data between local CSVs, Supabase, and local GeoPackages.

Subcommands:
    reindex   Upload solris_lookup.csv and water_filtration_lookup.csv to Supabase.
              Run this after editing either CSV to keep Supabase in sync.

    export    Pull a Supabase table to a local GeoPackage via ogr2ogr.

Usage:
    python database_helpers.py reindex
    python database_helpers.py reindex --solris-csv data/solris_lookup.csv --water-csv data/water_filtration_lookup.csv

    python database_helpers.py export --table dzcib_projects_solris
    python database_helpers.py export --table dzcib_projects_solris --output GIS/projects.gpkg
"""

import argparse
import os
import subprocess
import sys

import pandas as pd
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine, text

load_dotenv(find_dotenv())

SOLRIS_REQUIRED_COLUMNS = [
    "solris_code", "solris_class", "biocapacity_category",
    "biocapacity_conversion_factor", "lulc_category",
    "agc_tc_ha", "bgc_tc_ha", "soc_tc_ha", "deoc_tc_ha",
    "naturalness", "description",
]

WATER_REQUIRED_COLUMNS = ["wetland_type", "value"]


# ── Shared helpers ─────────────────────────────────────────────────────────────

def supabase_engine():
    url = os.getenv("SUPABASE_URL")
    if not url:
        sys.exit("Error: SUPABASE_URL is not set in .env")
    conn_str = url.replace("postgres://", "postgresql://", 1)
    return create_engine(conn_str)


def _drop_and_upload(engine, table_name: str, df: pd.DataFrame) -> None:
    """Cascade drop table if it exists, then upload df as a fresh table."""
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
    df.to_sql(table_name, engine, if_exists="append", index=False)


def load_es_lookup(engine) -> dict:
    """Load and merge ES lookup tables from Supabase into a per-SOLRIS-code dict.

    Returns {solris_code: {column: value}} with two derived fields added:
      - total_c_per_ha  (agc + bgc + soc + deoc per hectare)
      - wf_value_per_ha (water filtration value, 0 for non-wetland classes)
    """
    solris_df = pd.read_sql("SELECT * FROM solris_lookup", engine)
    solris_df = solris_df.dropna(subset=["solris_code"])
    solris_df["solris_code"] = solris_df["solris_code"].astype(int)

    for col in ("agc_tc_ha", "bgc_tc_ha", "soc_tc_ha", "deoc_tc_ha"):
        solris_df[col] = pd.to_numeric(solris_df[col], errors="coerce").fillna(0)

    solris_df["total_c_per_ha"] = (
        solris_df["agc_tc_ha"]
        + solris_df["bgc_tc_ha"]
        + solris_df["soc_tc_ha"]
        + solris_df["deoc_tc_ha"]
    )

    wf_df = pd.read_sql("SELECT * FROM water_filtration_lookup", engine)
    solris_df = solris_df.merge(wf_df[["solris_class", "wf_value_per_ha"]], on="solris_class", how="left")
    solris_df["wf_value_per_ha"] = solris_df["wf_value_per_ha"].fillna(0)

    def _coerce(val):
        if pd.isna(val):
            return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            return val

    return {
        int(row["solris_code"]): {
            col: _coerce(row[col])
            for col in solris_df.columns
            if col != "solris_code"
        }
        for _, row in solris_df.iterrows()
    }


def upload_gpkg_to_supabase(
    gpkg_path: str,
    layer_name: str,
    table_name: str,
    geometry_type: str | None = None,
) -> None:
    """Upload a GeoPackage layer to Supabase via ogr2ogr.

    Args:
        gpkg_path:     Path to the source GeoPackage.
        layer_name:    Layer within the GeoPackage to upload.
        table_name:    Destination Supabase table name (without schema prefix).
        geometry_type: Optional geometry type override passed to ogr2ogr -nlt
                       (e.g. "MULTIPOLYGON"). Omit for non-spatial or auto-detected layers.
    """
    supabase_url = os.getenv("SUPABASE_URL")
    if not supabase_url:
        print("  Skipping upload: SUPABASE_URL is not set in .env.")
        return

    separator = "&" if "?" in supabase_url else "?"
    pg_conn = f"PG:{supabase_url}{separator}options=-c%20statement_timeout%3D0"

    cmd = [
        "ogr2ogr",
        "-f", "PostgreSQL",
        pg_conn,
        gpkg_path,
        layer_name,
        "-nln", f"public.{table_name}",
        "-lco", "GEOMETRY_NAME=geom",
        "-lco", "FID=id",
        "-unsetFid",
        "-overwrite",
        "-progress",
    ]
    if geometry_type:
        cmd += ["-nlt", geometry_type]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"  Uploaded to Supabase table '{table_name}' successfully.")
    else:
        print(f"  ogr2ogr upload failed (exit {result.returncode}):")
        if result.stderr:
            print(result.stderr)


# ── reindex ────────────────────────────────────────────────────────────────────

def upload_solris_lookup(csv_path: str) -> None:
    df = pd.read_csv(csv_path)
    missing = [c for c in SOLRIS_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        sys.exit(f"Error: solris CSV is missing columns: {missing}")
    df = df.dropna(subset=["solris_code"])
    engine = supabase_engine()
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
    df = df.rename(columns={"wetland_type": "solris_class", "value": "wf_value_per_ha"})
    engine = supabase_engine()
    try:
        _drop_and_upload(engine, "water_filtration_lookup", df)
        print(f"  water_filtration_lookup: {len(df)} rows uploaded from '{csv_path}'")
    finally:
        engine.dispose()


def cmd_reindex(args):
    print("Uploading lookup tables to Supabase...")
    upload_solris_lookup(args.solris_csv)
    upload_water_filtration_lookup(args.water_csv)
    print("Done.")


# ── export ─────────────────────────────────────────────────────────────────────

def cmd_export(args):
    supabase_url = os.getenv("SUPABASE_URL")
    if not supabase_url:
        sys.exit("Error: SUPABASE_URL is not set in .env")

    output = args.output or f"{args.table}.gpkg"
    separator = "&" if "?" in supabase_url else "?"
    pg_conn = f"PG:{supabase_url}{separator}options=-c%20statement_timeout%3D0"

    cmd = [
        "ogr2ogr",
        "-f", "GPKG",
        output,
        pg_conn,
        args.table,
        "-progress",
    ]

    print(f"Pulling '{args.table}' from Supabase → {output} ...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Done. Written to {output}")
    else:
        print(f"ogr2ogr failed (exit {result.returncode}):")
        if result.stderr:
            print(result.stderr)
        sys.exit(1)


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Supabase database helpers: upload lookup tables or export tables to GeoPackage."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # reindex subcommand
    p_reindex = sub.add_parser(
        "reindex",
        help="Upload solris_lookup.csv and water_filtration_lookup.csv to Supabase.",
    )
    p_reindex.add_argument(
        "--solris-csv",
        default="data/solris_lookup.csv",
        help="SOLRIS classification lookup CSV  [default: %(default)s]",
    )
    p_reindex.add_argument(
        "--water-csv",
        default="data/water_filtration_lookup.csv",
        help="Water filtration lookup CSV  [default: %(default)s]",
    )

    # export subcommand
    p_export = sub.add_parser(
        "export",
        help="Pull a Supabase table to a local GeoPackage.",
    )
    p_export.add_argument(
        "--table",
        required=True,
        help="Name of the Supabase table to export.",
    )
    p_export.add_argument(
        "--output",
        default=None,
        help="Output GeoPackage path  [default: {table}.gpkg]",
    )

    args = parser.parse_args()
    {"reindex": cmd_reindex, "export": cmd_export}[args.command](args)


if __name__ == "__main__":
    main()
