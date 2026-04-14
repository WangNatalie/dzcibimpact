import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BiocapacityProcessor:
    FOLDER_NAME = "biocapacity"
    CSV_COLS = [
        "solris_code", "solris_class", "biocapacity_category",
        "area_hectares", "biocapacity_gha", "biocapacity_pct",
    ]
    MERGE_COLS = ["biocapacity_gha", "biocapacity_pct"]
    CHANGE_FIELDS = ["change_biocapacity_gha"]

    @staticmethod
    def compute_change(area_ha: float, old_vals: dict, new_vals: dict, **kwargs) -> dict:
        return {
            "change_biocapacity_gha": area_ha * (
                new_vals.get("biocapacity_conversion_factor", 0)
                - old_vals.get("biocapacity_conversion_factor", 0)
            )
        }

    def process(self, area_df, solris_df, wf_df=None):
        merged = area_df.merge(solris_df, on="solris_code", how="left")

        missing_codes = merged[merged["solris_class"].isna()]["solris_code"].unique()
        if len(missing_codes) > 0:
            logger.warning(f"Missing lookup entries for SOLRIS codes: {missing_codes}")

        merged["biocapacity_gha"] = (
            merged["area_hectares"] * merged["biocapacity_conversion_factor"]
        )

        total_biocapacity = merged["biocapacity_gha"].sum()
        merged["biocapacity_pct"] = (
            merged["biocapacity_gha"] / total_biocapacity * 100
            if total_biocapacity != 0 else 0
        )

        return merged

    def generate_report(self, study_area_name, results_df):
        results_df = (
            results_df.groupby("solris_class", as_index=False)
            .agg(total_area_hectares  =("area_hectares",     "sum"),
                 total_biocapacity_gha=("biocapacity_gha",   "sum"))
            .sort_values("total_biocapacity_gha", ascending=False)
        )

        total_biocapacity = results_df["total_biocapacity_gha"].sum()
        total_area = results_df["total_area_hectares"].sum()
        results_df["biocapacity_pct"] = (
            results_df["total_biocapacity_gha"] / total_biocapacity * 100
        )

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
                f"({row['biocapacity_pct']:.1f}% of total)\n\n"
            )

        report += (
            f"        {'='*50}\n        TOTALS\n        {'='*50}\n"
            f"        Total Area: {total_area:,.2f} hectares\n"
            f"        Total Biocapacity: {total_biocapacity:,.2f} global hectares\n"
            f"        Biocapacity per Hectare: {total_biocapacity/total_area:.3f} gha/ha\n"
        if total_area > 0 else
        f"        Biocapacity per Hectare: N/A\n"
        )

        return report
