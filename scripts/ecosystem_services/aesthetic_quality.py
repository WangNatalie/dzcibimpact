import bisect
import pandas as pd
import logging

logger = logging.getLogger(__name__)

_NATURALNESS_WEIGHT = 0.67
_RARITY_WEIGHT = 0.33

# Rarity bins: percentage-of-total area → score (5 = rarest, 1 = most common)
_RARITY_BINS        = [-float("inf"), 1, 5, 15, 30, float("inf")]
_RARITY_LABELS      = [5, 4, 3, 2, 1]
_RARITY_BREAKPOINTS = [1, 5, 15, 30]


def _pct_to_rarity(pct: float) -> int:
    """Convert a percentage-of-total-area value to a rarity score (5=rarest, 1=most common)."""
    return _RARITY_LABELS[bisect.bisect_right(_RARITY_BREAKPOINTS, pct)]


_no_context_warned = False


def _rarity_from_areas(code: int, areas: dict) -> int:
    """Return the rarity score for a SOLRIS code given a {code: area_ha} landscape dict."""
    total = sum(v for v in areas.values() if v > 0)
    if total == 0:
        return 1
    return _pct_to_rarity(areas.get(code, 0.0) / total * 100)


def landscape_aq(lookup: dict, context_areas: dict) -> float:
    """Compute the area-weighted average aesthetic quality score for a landscape.

    Args:
        lookup:        ES lookup dict keyed by solris_code → {naturalness: float, ...}
        context_areas: {solris_code: area_ha} composition of the landscape
    """
    total_area = sum(context_areas.values())
    if total_area == 0:
        return 0.0
    weighted_sum = 0.0
    for code, area_ha in context_areas.items():
        naturalness = lookup.get(code, {}).get("naturalness", 0.0)
        rarity = _rarity_from_areas(code, context_areas)
        weighted_sum += (naturalness * _NATURALNESS_WEIGHT + rarity * _RARITY_WEIGHT) * area_ha
    return weighted_sum / total_area


class AestheticQualityProcessor:
    FOLDER_NAME = "aesthetic_quality"
    CSV_COLS = [
        "solris_code", "solris_class", "area_hectares",
        "naturalness_score", "rarity_score", "aesthetic_quality_score",
    ]
    MERGE_COLS = ["naturalness_score", "rarity_score", "aesthetic_quality_score"]
    CHANGE_FIELDS = ["change_aesthetic_score"]

    @staticmethod
    def compute_change(
        area_ha: float,
        old_vals: dict,
        new_vals: dict,
        context_areas: dict | None = None,
        old_code: int | None = None,
        new_code: int | None = None,
        **kwargs,
    ) -> dict:
        old_nat = old_vals.get("naturalness", 0)
        new_nat = new_vals.get("naturalness", 0)

        if context_areas is not None and old_code is not None and new_code is not None:
            old_rarity = _rarity_from_areas(old_code, context_areas)
            # Shift area_ha from old_code to new_code to get new rarity scores
            adjusted = dict(context_areas)
            adjusted[old_code] = max(0.0, adjusted.get(old_code, 0.0) - area_ha)
            adjusted[new_code] = adjusted.get(new_code, 0.0) + area_ha
            new_rarity = _rarity_from_areas(new_code, adjusted)
            old_aq = old_nat * _NATURALNESS_WEIGHT + old_rarity * _RARITY_WEIGHT
            new_aq = new_nat * _NATURALNESS_WEIGHT + new_rarity * _RARITY_WEIGHT
        else:
            global _no_context_warned
            if not _no_context_warned:
                logger.warning(
                    "No context_areas provided — rarity component omitted from aesthetic quality change. "
                    "Pass --boundary-geojson (site_calculator) or --geojson (potential_calculator) for full scoring."
                )
                _no_context_warned = True
            old_aq = old_nat * _NATURALNESS_WEIGHT
            new_aq = new_nat * _NATURALNESS_WEIGHT

        return {"change_aesthetic_score": new_aq - old_aq}

    def process(self, area_df, solris_df, wf_df=None):
        lookup_df = solris_df[["solris_code", "solris_class", "naturalness"]].rename(
            columns={"naturalness": "naturalness_score"}
        )
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

    def generate_report(self, study_area_name, results_df):
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
