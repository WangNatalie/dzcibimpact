import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import argparse

from dotenv import load_dotenv
from database_setup import setup_database
from ecosystem_services import discover_processors

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Shared base ───────────────────────────────────────────────────────────────

class CIBImpactProcessor:
    """
    Handles database connection, schema creation, and lookup syncing.
    Runs all ecosystem-service processors and merges their output into a
    single combined results table.
    """

    def __init__(self, db_config):
        self.db_config = db_config
        self.engine = self._create_db_connection()

    def _create_db_connection(self):
        try:
            connection_string = (
                f"postgresql://{self.db_config['user']}:"
                f"{self.db_config['password']}@"
                f"{self.db_config['host']}:"
                f"{self.db_config['port']}/"
                f"{self.db_config['database']}"
            )
            engine = create_engine(connection_string)
            logger.info("Database connection established")
            return engine
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def create_database_schema(self):
        """Create the solris_lookup table if it does not already exist."""
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS solris_lookup (
                    solris_code                   INTEGER PRIMARY KEY,
                    solris_class                  TEXT          NOT NULL,
                    biocapacity_category          TEXT          NOT NULL,
                    biocapacity_conversion_factor DECIMAL(4,2)  NOT NULL,
                    lulc_category                 TEXT          NOT NULL,
                    agc_tc_ha                     DECIMAL(8,4)  NOT NULL,
                    bgc_tc_ha                     DECIMAL(8,4)  NOT NULL,
                    soc_tc_ha                     DECIMAL(8,4)  NOT NULL,
                    deoc_tc_ha                    DECIMAL(8,4)  NOT NULL,
                    naturalness                   DECIMAL(4,2)  NOT NULL,
                    description                   TEXT
                );
            """))
            conn.commit()
        logger.info("Database schema ready")

    def load_area_data(self, table_name):
        """
        Query the dissolved classified table uploaded to Supabase by classify_area.py.
        Returns a DataFrame with columns: solris_code (int), area_hectares (float).
        Requires SUPABASE_URL to be set in .env.
        """
        supabase_url = os.getenv("SUPABASE_URL")
        if not supabase_url:
            raise RuntimeError("SUPABASE_URL is not set in .env")

        conn_str = supabase_url.replace("postgres://", "postgresql://", 1)
        supabase_engine = create_engine(conn_str)

        area_df = pd.read_sql(
            f"""
            SELECT solris_code,
                   SUM(area_ha) AS area_hectares
            FROM "{table_name}"
            WHERE solris_code IS NOT NULL
              AND solris_code != 0
            GROUP BY solris_code
            ORDER BY solris_code
            """,
            supabase_engine,
        )
        supabase_engine.dispose()
        logger.info(f"Loaded {len(area_df)} SOLRIS codes from Supabase table '{table_name}'")
        return area_df

    def upload_to_supabase(self, df, table_name):
        """Upload a results DataFrame to Supabase, replacing the table if it exists."""
        supabase_url = os.getenv("SUPABASE_URL")
        if not supabase_url:
            logger.warning("SUPABASE_URL not set — skipping Supabase upload.")
            return

        conn_str = supabase_url.replace("postgres://", "postgresql://", 1)
        supabase_engine = create_engine(conn_str)
        try:
            df.to_sql(table_name, supabase_engine, if_exists="replace", index=False)
            logger.info(f"Uploaded {len(df)} rows to Supabase table '{table_name}'")
        finally:
            supabase_engine.dispose()

    def sync_lookup_tables_from_supabase(self):
        """
        Pull solris_lookup and water_filtration_lookup from Supabase and write
        them into the local database so ecosystem-service processors can query
        them via the local engine.

        Requires SUPABASE_URL to be set in .env.
        Run reindex_es_values.py first to populate the Supabase tables from CSV.
        """
        supabase_url = os.getenv("SUPABASE_URL")
        if not supabase_url:
            raise RuntimeError("SUPABASE_URL is not set in .env")

        conn_str = supabase_url.replace("postgres://", "postgresql://", 1)
        supabase_engine = create_engine(conn_str)

        try:
            solris_df = pd.read_sql("SELECT * FROM solris_lookup", supabase_engine)
            water_df  = pd.read_sql("SELECT * FROM water_filtration_lookup", supabase_engine)
        finally:
            supabase_engine.dispose()

        solris_df.to_sql("solris_lookup",           self.engine, if_exists="replace", index=False)
        water_df.to_sql( "water_filtration_lookup", self.engine, if_exists="replace", index=False)
        logger.info(f"Synced {len(solris_df)} solris_lookup rows and {len(water_df)} water_filtration_lookup rows from Supabase")


# ── Build combined results table ──────────────────────────────────────────────

def _write_es_outputs(df, proc, study_area, folder, csv_cols, report_label):
    """Write per-ES CSV and report .txt to data/{folder}/."""
    out_dir = os.path.join("data", study_area, folder)
    os.makedirs(out_dir, exist_ok=True)

    csv_path    = os.path.join(out_dir, f"{folder}_results.csv")
    report_path = os.path.join(out_dir, f"{folder}_report.txt")

    df[csv_cols].to_csv(csv_path, index=False)
    logger.info(f"Wrote {csv_path}")

    report = proc.generate_report(study_area, results_df=df)
    with open(report_path, "w") as f:
        f.write(report)
    logger.info(f"Wrote {report_path}")
    print(report)


def build_combined_results(area_df, engine, study_area):
    """
    Run all ecosystem-service processors against area_df, write per-ES
    CSVs and report .txts to their folders, then merge everything into a single
    combined DataFrame (one row per solris_code) for the final report.

    Processors are auto-discovered from the ecosystem_services directory — add a
    new *Processor class there and it will be included automatically.
    """
    combined = area_df[["solris_code", "area_hectares"]].copy()
    solris_class_df = pd.read_sql(
        "SELECT solris_code, solris_class FROM solris_lookup", engine
    )
    combined = combined.merge(solris_class_df, on="solris_code", how="left")

    for ProcessorClass in discover_processors():
        logger.info(f"Running {ProcessorClass.FOLDER_NAME}...")
        proc = ProcessorClass(engine)
        df = proc.process(area_df)
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


# ── Main ──────────────────────────────────────────────────────────────────────

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

    db_config = {
        "host":     "localhost",
        "port":     5432,
        "database": "carolinian_zone",
        "user":     "nataliewang",
        "password": "dzcibimpact",
    }

    logger.info("Checking database...")
    if not setup_database(db_config, create_user=False):
        logger.error("Failed to set up database.")
        return

    base = CIBImpactProcessor(db_config)
    base.create_database_schema()

    # Sync lookup tables from Supabase into local DB
    base.sync_lookup_tables_from_supabase()

    # Load aggregated area per SOLRIS code from Supabase
    area_df = base.load_area_data(args.source_table)

    # Run all processors, write per-ES outputs, and build combined table
    combined = build_combined_results(area_df, base.engine, args.study_area)

    study_area  = args.study_area
    table_name  = f"ecosystem_services_results_{study_area}"
    output_csv  = os.path.join("data", study_area, "ecosystem_services_report.csv")

    # ── Save to local DB ──────────────────────────────────────────────────────
    combined.to_sql("ecosystem_services_results", base.engine,
                    if_exists="replace", index=False)
    logger.info(f"Saved {len(combined)} rows to local DB table 'ecosystem_services_results'")

    # ── Upload to Supabase ────────────────────────────────────────────────────
    base.upload_to_supabase(combined, table_name)

    # ── Export CSV ────────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    combined.to_csv(output_csv, index=False)
    logger.info(f"Report written to {output_csv}")


if __name__ == "__main__":
    main()
