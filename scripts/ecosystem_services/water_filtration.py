import pandas as pd
import logging

logger = logging.getLogger(__name__)


class WaterFiltrationProcessor:
    FOLDER_NAME = "water_filtration"
    CSV_COLS = [
        "solris_code", "solris_class", "area_hectares",
        "wf_value_per_ha", "total_wf_value", "wf_pct",
    ]
    MERGE_COLS = ["wf_value_per_ha", "total_wf_value", "wf_pct"]
    CHANGE_FIELDS = ["change_wf_value_cad"]

    @staticmethod
    def compute_change(area_ha: float, old_vals: dict, new_vals: dict) -> dict:
        return {
            "change_wf_value_cad": area_ha * (
                new_vals.get("wf_value_per_ha", 0) - old_vals.get("wf_value_per_ha", 0)
            )
        }

    def __init__(self, engine):
        self.engine = engine

    def process(self, area_df):
        """Map wetland water filtration values to SOLRIS classes from a pre-aggregated area DataFrame.

        Args:
            area_df: DataFrame with columns solris_code (int), area_hectares (float)

        Reads solris_lookup and water_filtration_lookup from the local database
        (synced from Supabase by processor.py at startup via sync_lookup_tables_from_supabase).
        """
        lookup_df = pd.read_sql(
            "SELECT solris_code, solris_class FROM solris_lookup", self.engine
        )
        merged = area_df.merge(lookup_df, on="solris_code", how="left")

        wf_df = pd.read_sql(
            "SELECT solris_class, wf_value_per_ha FROM water_filtration_lookup", self.engine
        )

        merged = merged.merge(wf_df, on="solris_class", how="left")
        merged["wf_value_per_ha"] = merged["wf_value_per_ha"].fillna(0)
        merged["total_wf_value"] = (merged["area_hectares"] * merged["wf_value_per_ha"]).round(4)

        total_wf = merged["total_wf_value"].sum()
        merged["wf_pct"] = (
            (merged["total_wf_value"] / total_wf * 100).fillna(0) if total_wf != 0 else 0
        )

        return merged

    def save_to_database(self, results_df):
        """Write water filtration results to the database."""
        cols = [
            "solris_code", "solris_class", "area_hectares",
            "wf_value_per_ha", "total_wf_value", "wf_pct",
        ]
        results_df = results_df.copy()
        results_df["area_hectares"] = results_df["area_hectares"].round(4)
        results_df[cols].to_sql(
            "water_filtration_results", self.engine, if_exists="append", index=False
        )
        logger.info("Water filtration results saved to database.")

    def generate_report(self, study_area_name, results_df=None):
        """Generate a plain-text water filtration summary report.

        Args:
            study_area_name: label for the report header
            results_df: DataFrame from process() — if None, reads from the local DB
        """
        if results_df is None:
            results_df = pd.read_sql(
                """
                SELECT solris_class,
                       SUM(area_hectares)   AS total_area_hectares,
                       AVG(wf_value_per_ha) AS wf_value_per_ha,
                       SUM(total_wf_value)  AS total_wf_value
                FROM water_filtration_results
                GROUP BY solris_class
                ORDER BY total_wf_value DESC
                """,
                self.engine,
            )
        else:
            results_df = (
                results_df.groupby("solris_class", as_index=False)
                .agg(total_area_hectares=("area_hectares",    "sum"),
                     wf_value_per_ha    =("wf_value_per_ha", "mean"),
                     total_wf_value     =("total_wf_value",  "sum"))
                .sort_values("total_wf_value", ascending=False)
            )

        total_area = results_df["total_area_hectares"].sum() if not results_df.empty else 0
        total_wf = results_df["total_wf_value"].sum() if not results_df.empty else 0

        report = (
            f"\n        WATER FILTRATION ANALYSIS REPORT\n"
            f"        Study Area: {study_area_name}\n"
            f"        Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"        {'='*60}\n        SUMMARY BY SOLRIS CLASS\n        {'='*60}\n\n"
        )

        for _, row in results_df.iterrows():
            pct_area = (row["total_area_hectares"] / total_area * 100) if total_area else 0
            pct_wf = (row["total_wf_value"] / total_wf * 100) if total_wf else 0
            report += (
                f"        {row['solris_class']}:\n"
                f"        Area: {row['total_area_hectares']:,.2f} hectares ({pct_area:.1f}% of total)\n"
                f"        WF Value($)/ha: {row['wf_value_per_ha']:,.2f}\n"
                f"        Total WF Value($): {row['total_wf_value']:,.2f} ({pct_wf:.1f}% of total)\n\n"
            )

        report += (
            f"        {'='*60}\n        TOTALS\n        {'='*60}\n"
            f"        Total Area: {total_area:,.2f} hectares\n"
            f"        Total Water Filtration Value ($ millions CAD): {total_wf/1e6:,.6f}\n"
        )

        return report

    def export_to_csv(self, output_path):
        """Export water filtration results from the database to a CSV file."""
        results_df = pd.read_sql(
            """
            SELECT solris_class, solris_code, area_hectares,
                   wf_value_per_ha, total_wf_value, wf_pct
            FROM water_filtration_results
            ORDER BY total_wf_value DESC
            """,
            self.engine,
        )
        if "solris_code" in results_df.columns:
            results_df["solris_code"] = results_df["solris_code"].astype("Int64")
        results_df.to_csv(output_path, index=False)
        logger.info(f"Water filtration results exported to CSV: {output_path}")
        return results_df
