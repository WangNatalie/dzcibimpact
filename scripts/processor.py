import os
import pandas as pd
import logging
import argparse

from ecosystem_services import discover_processors
from lookup_support import load_lookup_tables, supabase_engine
from runtime_support import ensure_parent_dir, load_project_dotenv, resolve_repo_path

load_project_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Supabase connection ────────────────────────────────────────────────────────

def _supabase_engine():
    return supabase_engine(required=True)


# ── Output helpers ─────────────────────────────────────────────────────────────

def _write_es_outputs(df, proc, study_area, folder, csv_cols, report_label):
    """Write per-ES CSV and report .txt to data/{study_area}/{folder}/."""
    out_dir = resolve_repo_path(os.path.join("data", study_area, folder))
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / f"{folder}_results.csv"
    report_path = out_dir / f"{folder}_report.txt"

    df[csv_cols].to_csv(csv_path, index=False)
    logger.info(f"Wrote {csv_path}")

    report = proc.generate_report(study_area, results_df=df)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    logger.info(f"Wrote {report_path}")
    print(report)


# ── Build combined results table ───────────────────────────────────────────────

def build_combined_results(area_df, solris_df, wf_df, study_area):
    """
    Run all ecosystem-service processors against area_df in memory, write per-ES
    CSVs and report .txts to their folders, then merge everything into a single
    combined DataFrame (one row per solris_code) for the final report.

    Processors are auto-discovered from the ecosystem_services directory — add a
    new *Processor class there and it will be included automatically.
    """
    combined = area_df[["solris_code", "area_hectares"]].copy()
    combined = combined.merge(
        solris_df[["solris_code", "solris_class"]], on="solris_code", how="left"
    )

    for ProcessorClass in discover_processors():
        logger.info(f"Running {ProcessorClass.FOLDER_NAME}...")
        proc = ProcessorClass()
        df = proc.process(area_df, solris_df, wf_df)
        _write_es_outputs(
            df, proc, study_area,
            ProcessorClass.FOLDER_NAME, ProcessorClass.CSV_COLS,
            ProcessorClass.FOLDER_NAME,
        )
        combined = combined.merge(
            df[["solris_code"] + ProcessorClass.MERGE_COLS],
            on="solris_code",
            how="left",
        )

    return combined


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Calculate ecosystem service values and write a combined results table"
    )
    parser.add_argument(
        "--source-table",
        default="carolinian_zone_classified",
        help="Supabase table produced by classify_area.py  [default: %(default)s]",
    )
    parser.add_argument("--study-area", default="carolinian_zone")
    args = parser.parse_args()

    engine = _supabase_engine()
    try:
        logger.info("Loading lookup tables from Supabase...")
        solris_df, wf_df, lookup_source = load_lookup_tables(
            prefer_supabase=True,
            require_supabase=True,
        )
        logger.info("Loaded lookup tables from %s.", lookup_source)

        logger.info(f"Loading area data from Supabase table '{args.source_table}'...")
        area_df = pd.read_sql(
            f"""
            SELECT solris_code,
                   SUM(area_ha) AS area_hectares
            FROM "{args.source_table}"
            WHERE solris_code IS NOT NULL
              AND solris_code != 0
            GROUP BY solris_code
            ORDER BY solris_code
            """,
            engine,
        )
        logger.info(f"Loaded {len(area_df)} SOLRIS codes.")
    finally:
        engine.dispose()

    combined = build_combined_results(area_df, solris_df, wf_df, args.study_area)

    study_area = args.study_area
    table_name = f"ecosystem_services_results_{study_area}"
    output_csv = ensure_parent_dir(
        os.path.join("data", study_area, "ecosystem_services_report.csv")
    )

    # ── Upload to Supabase ─────────────────────────────────────────────────────
    upload_engine = _supabase_engine()
    try:
        combined.to_sql(table_name, upload_engine, if_exists="replace", index=False)
        logger.info(f"Uploaded {len(combined)} rows to Supabase table '{table_name}'")
    finally:
        upload_engine.dispose()

    # ── Export CSV ─────────────────────────────────────────────────────────────
    combined.to_csv(output_csv, index=False)
    logger.info(f"Report written to {output_csv}")


if __name__ == "__main__":
    main()
