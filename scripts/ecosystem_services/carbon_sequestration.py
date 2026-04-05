import pandas as pd
import logging

logger = logging.getLogger(__name__)

_SCC_BASE_YEAR_VALUE = 252  # $/tC in 2021


class CarbonSequestrationProcessor:
    FOLDER_NAME = "carbon_sequestration"
    CSV_COLS = [
        "solris_code", "solris_class", "area_hectares",
        "agc_tc_ha", "bgc_tc_ha", "soc_tc_ha", "deoc_tc_ha",
        "total_carbon_tc", "ssc", "carbon_pct",
    ]
    MERGE_COLS = ["total_carbon_tc", "ssc_million_cad", "carbon_pct"]
    CHANGE_FIELDS = ["change_carbon_tc", "change_ssc_cad"]

    @staticmethod
    def compute_change(area_ha: float, old_vals: dict, new_vals: dict, **kwargs) -> dict:
        change_c = area_ha * (
            new_vals.get("total_c_per_ha", 0) - old_vals.get("total_c_per_ha", 0)
        )
        return {"change_carbon_tc": change_c, "change_ssc_cad": change_c * _SCC_BASE_YEAR_VALUE}

    def process(self, area_df, solris_df, wf_df=None):
        cols = ["solris_code", "solris_class", "agc_tc_ha", "bgc_tc_ha", "soc_tc_ha", "deoc_tc_ha"]
        lookup_df = solris_df[[c for c in cols if c in solris_df.columns]]
        merged = area_df.merge(lookup_df, on="solris_code", how="left")

        missing_codes = merged[merged["solris_class"].isna()]["solris_code"].unique()
        if len(missing_codes) > 0:
            logger.warning(f"Missing lookup entries for SOLRIS codes: {missing_codes}")

        merged["total_carbon_tc"] = (
            merged["agc_tc_ha"].fillna(0)
            + merged["bgc_tc_ha"].fillna(0)
            + merged["soc_tc_ha"].fillna(0)
            + merged["deoc_tc_ha"].fillna(0)
        ) * merged["area_hectares"]

        merged["ssc"] = merged["total_carbon_tc"] * _SCC_BASE_YEAR_VALUE
        merged["ssc_million_cad"] = (merged["ssc"] / 1_000_000).round(4)

        total_carbon = merged["total_carbon_tc"].sum()
        merged["carbon_pct"] = (
            merged["total_carbon_tc"] / total_carbon * 100 if total_carbon != 0 else 0
        )

        return merged

    def generate_report(self, study_area_name, results_df):
        df = results_df.copy()
        df["ssc_density"] = (df["ssc_million_cad"] / df["area_hectares"]).replace(
            [float("inf"), -float("inf")], 0
        ).fillna(0)

        agg = (
            df.groupby("solris_class", as_index=False)
            .agg(total_area_hectares=("area_hectares",   "sum"),
                 total_carbon_tc    =("total_carbon_tc", "sum"),
                 avg_agc_tc_ha      =("agc_tc_ha",      "mean"),
                 avg_bgc_tc_ha      =("bgc_tc_ha",      "mean"),
                 avg_soc_tc_ha      =("soc_tc_ha",      "mean"),
                 avg_deoc_tc_ha     =("deoc_tc_ha",     "mean"),
                 total_ssc          =("ssc_million_cad", "sum"),
                 total_ssc_density  =("ssc_density",    "sum"))
            .sort_values("total_carbon_tc", ascending=False)
        )

        total_carbon = agg["total_carbon_tc"].sum()
        total_area   = agg["total_area_hectares"].sum()
        total_ssc    = agg["total_ssc"].sum()
        agg["carbon_pct"] = agg["total_carbon_tc"] / total_carbon * 100

        report = (
            f"\n        CARBON SEQUESTRATION ANALYSIS REPORT\n"
            f"        Study Area: {study_area_name}\n"
            f"        Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"        {'='*60}\n        SUMMARY BY SOLRIS CLASS\n        {'='*60}\n\n"
        )

        for _, row in agg.iterrows():
            report += (
                f"        {row['solris_class']}:\n"
                f"        Area: {row['total_area_hectares']:,.2f} hectares "
                f"({row['total_area_hectares']/total_area*100:.1f}% of total)\n"
                f"        Total Carbon: {row['total_carbon_tc']:,.2f} tonnes C "
                f"({row['carbon_pct']:.1f}% of total)\n"
                f"        Carbon Density: {row['total_carbon_tc']/row['total_area_hectares']:.2f} tC/ha\n"
                f"        Breakdown per hectare:\n"
                f"          - AGC:  {row['avg_agc_tc_ha']:.2f} tC/ha\n"
                f"          - BGC:  {row['avg_bgc_tc_ha']:.2f} tC/ha\n"
                f"          - SOC:  {row['avg_soc_tc_ha']:.2f} tC/ha\n"
                f"          - DeOC: {row['avg_deoc_tc_ha']:.2f} tC/ha\n"
                f"          - SSC:  ${1_000_000 * row['total_ssc_density']:.2f} $CAD/ha\n"
                f"        Total SSC: ${row['total_ssc']:,.6f} million CAD\n\n"
            )

        report += (
            f"        {'='*60}\n        TOTALS\n        {'='*60}\n"
            f"        Total Area: {total_area:,.2f} hectares\n"
            f"        Total Carbon Sequestration: {total_carbon:,.2f} tonnes C\n"
            f"        Average Carbon Density: {total_carbon/total_area:.2f} tC/ha\n"
            f"        Total SSC: ${total_ssc:,.2f} million CAD\n"
        )

        return report
