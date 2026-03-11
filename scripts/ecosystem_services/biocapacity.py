import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BiocapacityProcessor:
    def __init__(self, engine):
        self.engine = engine

    def process(self, area_df):
        """Calculate biocapacity per SOLRIS class from a pre-aggregated area DataFrame.

        Args:
            area_df: DataFrame with columns solris_code (int), area_hectares (float)
        """
        lookup_df = pd.read_sql("SELECT * FROM solris_lookup", self.engine)
        merged = area_df.merge(lookup_df, on="solris_code", how="left")

        missing_codes = merged[merged["solris_class"].isna()]["solris_code"].unique()
        if len(missing_codes) > 0:
            logger.warning(f"Missing lookup entries for SOLRIS codes: {missing_codes}")

        # Biocapacity (gha) = Area (ha) × Conversion Ratio (gha/ha)
        merged["biocapacity_gha"] = merged["area_hectares"] * merged["biocapacity_conversion_factor"]

        total_biocapacity = merged["biocapacity_gha"].sum()
        merged["percentage_of_total"] = merged["biocapacity_gha"] / total_biocapacity * 100

        return merged

    def save_to_database(self, results_df):
        """Write biocapacity results to the database."""
        cols = [
            "solris_code", "solris_class", "biocapacity_category",
            "area_hectares", "biocapacity_conversion_factor",
            "biocapacity_gha", "percentage_of_total",
        ]
        results_df = results_df.copy()
        results_df["area_hectares"] = results_df["area_hectares"].round(4)
        results_df[cols].to_sql("biocapacity_results", self.engine, if_exists="append", index=False)
        logger.info("Biocapacity results saved to database.")

    def generate_report(self, study_area_name, results_df=None):
        """Generate a plain-text biocapacity summary report.

        Args:
            study_area_name: label for the report header
            results_df: DataFrame from process() — if None, reads from the local DB
        """
        if results_df is None:
            results_df = pd.read_sql(
                """
                SELECT solris_class,
                       SUM(area_hectares)    AS total_area_hectares,
                       SUM(biocapacity_gha) AS total_biocapacity_gha
                FROM biocapacity_results
                GROUP BY solris_class
                ORDER BY total_biocapacity_gha DESC
                """,
                self.engine,
            )
        else:
            results_df = (
                results_df.groupby("solris_class", as_index=False)
                .agg(total_area_hectares=("area_hectares", "sum"),
                     total_biocapacity_gha=("biocapacity_gha", "sum"))
                .sort_values("total_biocapacity_gha", ascending=False)
            )

        total_biocapacity = results_df["total_biocapacity_gha"].sum()
        total_area = results_df["total_area_hectares"].sum()
        results_df["percentage_of_total"] = results_df["total_biocapacity_gha"] / total_biocapacity * 100

        report = (
            f"\n        BIOCAPACITY ANALYSIS REPORT\n"
            f"        Study Area: {study_area_name}\n"
            f"        Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"        {'='*50}\n        SUMMARY BY SOLRIS CLASS\n        {'='*50}\n\n"
        )

        for _, row in results_df.iterrows():
            report += (
                f"        {row['solris_class']}:\n"
                f"        Area: {row['total_area_hectares']:,.2f} hectares "
                f"({row['total_area_hectares']/total_area*100:.1f}% of total)\n"
                f"        Biocapacity: {row['total_biocapacity_gha']:,.2f} global hectares "
                f"({row['percentage_of_total']:.1f}% of total)\n\n"
            )

        report += (
            f"        {'='*50}\n        TOTALS\n        {'='*50}\n"
            f"        Total Area: {total_area:,.2f} hectares\n"
            f"        Total Biocapacity: {total_biocapacity:,.2f} global hectares\n"
            f"        Biocapacity per Hectare: {total_biocapacity/total_area:.3f} gha/ha\n"
        )

        return report

    def export_to_csv(self, output_path):
        """Export biocapacity results from the database to a CSV file."""
        results_df = pd.read_sql(
            """
            SELECT solris_class, solris_code, biocapacity_category,
                   area_hectares, biocapacity_conversion_factor,
                   biocapacity_gha, percentage_of_total
            FROM biocapacity_results
            ORDER BY biocapacity_gha DESC
            """,
            self.engine,
        )
        if "solris_code" in results_df.columns:
            results_df["solris_code"] = results_df["solris_code"].astype("Int64")
        results_df.to_csv(output_path, index=False)
        logger.info(f"Biocapacity results exported to CSV: {output_path}")
        return results_df
