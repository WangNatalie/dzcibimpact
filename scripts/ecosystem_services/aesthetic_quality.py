import pandas as pd
import logging

logger = logging.getLogger(__name__)

_NATURALNESS_WEIGHT = 0.67
_RARITY_WEIGHT = 0.33

# Rarity bins: percentage-of-total area → score (5 = rarest, 1 = most common)
_RARITY_BINS   = [-float("inf"), 1, 5, 15, 30, float("inf")]
_RARITY_LABELS = [5, 4, 3, 2, 1]


class AestheticQualityProcessor:
    FOLDER_NAME = "aesthetic_quality"
    CSV_COLS = [
        "solris_code", "solris_class", "area_hectares",
        "naturalness_score", "rarity_score", "aesthetic_quality_score",
    ]
    MERGE_COLS = ["naturalness_score", "rarity_score", "aesthetic_quality_score"]
    CHANGE_FIELDS = ["change_aesthetic_score"]

    @staticmethod
    def compute_change(area_ha: float, old_vals: dict, new_vals: dict) -> dict:
        return {
            "change_aesthetic_score": (
                new_vals.get("naturalness", 0) - old_vals.get("naturalness", 0)
            ) * 0.67
        }

    def __init__(self, engine):
        self.engine = engine

    def process(self, area_df):
        """Calculate aesthetic quality scores per SOLRIS class from a pre-aggregated area DataFrame.

        Args:
            area_df: DataFrame with columns solris_code (int), area_hectares (float)
        """
        lookup_df = pd.read_sql(
            "SELECT solris_code, solris_class, naturalness FROM solris_lookup", self.engine
        )
        lookup_df = lookup_df.rename(columns={"naturalness": "naturalness_score"})

        merged = area_df.merge(lookup_df, on="solris_code", how="left")
        merged.dropna(subset=["solris_class"], inplace=True)

        total_study_area = merged["area_hectares"].sum()
        merged["percentage_of_total"] = merged["area_hectares"] / total_study_area * 100

        # Rarity: binned by how rare each class is as a share of the total area
        merged["rarity_score"] = pd.cut(
            merged["percentage_of_total"],
            bins=_RARITY_BINS,
            labels=_RARITY_LABELS,
            right=True,
        ).astype(int)

        merged["aesthetic_quality_score"] = (
            merged["naturalness_score"] * _NATURALNESS_WEIGHT
            + merged["rarity_score"] * _RARITY_WEIGHT
        )

        return merged

    def save_to_database(self, results_df):
        """Write aesthetic quality results to the database."""
        cols = [
            "solris_code", "solris_class", "area_hectares",
            "naturalness_score", "rarity_score", "aesthetic_quality_score",
        ]
        results_df = results_df.copy()
        results_df["area_hectares"] = results_df["area_hectares"].round(4)
        results_df[cols].to_sql(
            "aesthetic_quality_results", self.engine, if_exists="append", index=False
        )
        logger.info("Aesthetic quality results saved to database.")

    def generate_report(self, study_area_name, results_df=None):
        """Generate a plain-text aesthetic quality summary report.

        Args:
            study_area_name: label for the report header
            results_df: DataFrame from process() — if None, reads from the local DB
        """
        if results_df is None:
            results_df = pd.read_sql(
                """
                SELECT solris_class, area_hectares,
                       naturalness_score, rarity_score, aesthetic_quality_score
                FROM aesthetic_quality_results
                ORDER BY aesthetic_quality_score DESC
                """,
                self.engine,
            )
        else:
            results_df = (
                results_df[["solris_class", "area_hectares",
                             "naturalness_score", "rarity_score", "aesthetic_quality_score"]]
                .sort_values("aesthetic_quality_score", ascending=False)
            )

        total_area = results_df["area_hectares"].sum()
        weighted_avg = (
            (results_df["aesthetic_quality_score"] * results_df["area_hectares"]).sum()
            / total_area
            if total_area > 0
            else 0
        )

        report = (
            f"\n        AESTHETIC QUALITY ANALYSIS REPORT\n"
            f"        Study Area: {study_area_name}\n"
            f"        Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"        {'='*60}\n        SUMMARY BY SOLRIS CLASS\n        {'='*60}\n\n"
        )

        for _, row in results_df.iterrows():
            area_pct = (row["area_hectares"] / total_area * 100) if total_area > 0 else 0
            report += (
                f"        {row['solris_class']}:\n"
                f"        Aesthetic Score: {row['aesthetic_quality_score']:.2f}\n"
                f"          - Area: {row['area_hectares']:,.2f} hectares ({area_pct:.1f}% of total)\n"
                f"          - Naturalness Score: {row['naturalness_score']:.2f}\n"
                f"          - Rarity Score: {row['rarity_score']} (5=rarest, 1=most common)\n\n"
            )

        report += (
            f"        {'='*60}\n        TOTALS\n        {'='*60}\n"
            f"        Total Area: {total_area:,.2f} hectares\n"
            f"        Area-Weighted Average Aesthetic Score: {weighted_avg:.2f}\n"
        )

        return report

    def export_to_csv(self, output_path):
        """Export aesthetic quality results from the database to a CSV file."""
        results_df = pd.read_sql(
            """
            SELECT solris_class, solris_code, area_hectares,
                   naturalness_score, rarity_score, aesthetic_quality_score
            FROM aesthetic_quality_results
            ORDER BY aesthetic_quality_score DESC
            """,
            self.engine,
        )
        if "solris_code" in results_df.columns:
            results_df["solris_code"] = results_df["solris_code"].astype("Int64")
        results_df.to_csv(output_path, index=False)
        logger.info(f"Aesthetic quality results exported to CSV: {output_path}")
        return results_df
