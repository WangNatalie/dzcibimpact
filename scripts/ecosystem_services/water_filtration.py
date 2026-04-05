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
    def compute_change(area_ha: float, old_vals: dict, new_vals: dict, **kwargs) -> dict:
        return {
            "change_wf_value_cad": area_ha * (
                new_vals.get("wf_value_per_ha", 0) - old_vals.get("wf_value_per_ha", 0)
            )
        }

    def process(self, area_df, solris_df, wf_df=None):
        merged = area_df.merge(
            solris_df[["solris_code", "solris_class"]], on="solris_code", how="left"
        )

        if wf_df is not None:
            wf = wf_df[["solris_class", "wf_value_per_ha"]]
            merged = merged.merge(wf, on="solris_class", how="left")
        else:
            merged["wf_value_per_ha"] = 0.0

        merged["wf_value_per_ha"] = merged["wf_value_per_ha"].fillna(0)
        merged["total_wf_value"] = (
            merged["area_hectares"] * merged["wf_value_per_ha"]
        ).round(4)

        total_wf = merged["total_wf_value"].sum()
        merged["wf_pct"] = (
            (merged["total_wf_value"] / total_wf * 100).fillna(0) if total_wf != 0 else 0
        )

        return merged

    def generate_report(self, study_area_name, results_df):
        agg = (
            results_df.groupby("solris_class", as_index=False)
            .agg(total_area_hectares=("area_hectares",    "sum"),
                 wf_value_per_ha    =("wf_value_per_ha", "mean"),
                 total_wf_value     =("total_wf_value",  "sum"))
            .sort_values("total_wf_value", ascending=False)
        )

        total_area = agg["total_area_hectares"].sum() if not agg.empty else 0
        total_wf   = agg["total_wf_value"].sum()      if not agg.empty else 0

        report = (
            f"\n        WATER FILTRATION ANALYSIS REPORT\n"
            f"        Study Area: {study_area_name}\n"
            f"        Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"        {'='*60}\n        SUMMARY BY SOLRIS CLASS\n        {'='*60}\n\n"
        )

        for _, row in agg.iterrows():
            pct_area = (row["total_area_hectares"] / total_area * 100) if total_area else 0
            pct_wf   = (row["total_wf_value"]      / total_wf   * 100) if total_wf   else 0
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
