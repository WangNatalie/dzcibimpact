import pandas as pd
import matplotlib.pyplot as plt
import logging

logger = logging.getLogger(__name__)

# Present value base year for SSC calculation
_SCC_BASE_YEAR_VALUE = 252  # $/tC in 2021


class CarbonSequestrationProcessor:
    def __init__(self, engine):
        self.engine = engine

    def process(self, area_df):
        """Calculate carbon stocks and social cost of carbon from a pre-aggregated area DataFrame.

        Args:
            area_df: DataFrame with columns solris_code (int), area_hectares (float)
        """
        lookup_df = pd.read_sql(
            "SELECT solris_code, solris_class, agc_tc_ha, bgc_tc_ha, soc_tc_ha, deoc_tc_ha FROM solris_lookup",
            self.engine,
        )
        merged = area_df.merge(lookup_df, on="solris_code", how="left")

        missing_codes = merged[merged["solris_class"].isna()]["solris_code"].unique()
        if len(missing_codes) > 0:
            logger.warning(f"Missing lookup entries for SOLRIS codes: {missing_codes}")

        # Total Carbon (tC) = (agc + bgc + soc + deoc) × Area (ha)
        merged["total_carbon_tc"] = (
            merged["agc_tc_ha"].fillna(0)
            + merged["bgc_tc_ha"].fillna(0)
            + merged["soc_tc_ha"].fillna(0)
            + merged["deoc_tc_ha"].fillna(0)
        ) * merged["area_hectares"]

        # Social cost of carbon using 2021 PV ($252/tC)
        merged["ssc"] = merged["total_carbon_tc"] * _SCC_BASE_YEAR_VALUE

        total_carbon = merged["total_carbon_tc"].sum()
        merged["percentage_of_total"] = (
            merged["total_carbon_tc"] / total_carbon * 100 if total_carbon != 0 else 0
        )

        return merged

    def save_to_database(self, results_df):
        """Write carbon sequestration results to the database."""
        cols = [
            "solris_code", "solris_class", "area_hectares",
            "agc_tc_ha", "bgc_tc_ha", "soc_tc_ha", "deoc_tc_ha",
            "total_carbon_tc", "ssc", "percentage_of_total",
        ]
        results_df = results_df.copy()
        results_df["area_hectares"] = results_df["area_hectares"].round(4)
        # Store SSC in millions of dollars
        results_df["ssc"] = (results_df["ssc"] / 1_000_000).round(4)
        results_df["ssc_density"] = (
            (results_df["ssc"] / results_df["area_hectares"])
            .replace([float("inf"), -float("inf")], 0)
            .fillna(0)
            .round(6)
        )
        results_df[cols + ["ssc_density"]].to_sql(
            "carbon_sequestration_results", self.engine, if_exists="append", index=False
        )
        logger.info("Carbon sequestration results saved to database.")

    def generate_report(self, study_area_name, results_df=None):
        """Generate a plain-text carbon sequestration summary report.

        Args:
            study_area_name: label for the report header
            results_df: DataFrame from process() — if None, reads from the local DB
        """
        if results_df is None:
            results_df = pd.read_sql(
                """
                SELECT solris_class,
                       SUM(area_hectares)    AS total_area_hectares,
                       SUM(total_carbon_tc)  AS total_carbon_tc,
                       AVG(agc_tc_ha)        AS avg_agc_tc_ha,
                       AVG(bgc_tc_ha)        AS avg_bgc_tc_ha,
                       AVG(soc_tc_ha)        AS avg_soc_tc_ha,
                       AVG(deoc_tc_ha)       AS avg_deoc_tc_ha,
                       SUM(ssc)              AS total_ssc,
                       SUM(ssc_density)      AS total_ssc_density
                FROM carbon_sequestration_results
                GROUP BY solris_class
                ORDER BY total_carbon_tc DESC
                """,
                self.engine,
            )
        else:
            df = results_df.copy()
            df["ssc_millions"] = df["ssc"] / 1_000_000
            df["ssc_density"]  = (df["ssc_millions"] / df["area_hectares"]).replace(
                [float("inf"), -float("inf")], 0
            ).fillna(0)
            results_df = (
                df.groupby("solris_class", as_index=False)
                .agg(total_area_hectares=("area_hectares",  "sum"),
                     total_carbon_tc     =("total_carbon_tc","sum"),
                     avg_agc_tc_ha       =("agc_tc_ha",     "mean"),
                     avg_bgc_tc_ha       =("bgc_tc_ha",     "mean"),
                     avg_soc_tc_ha       =("soc_tc_ha",     "mean"),
                     avg_deoc_tc_ha      =("deoc_tc_ha",    "mean"),
                     total_ssc           =("ssc_millions",  "sum"),
                     total_ssc_density   =("ssc_density",   "sum"))
                .sort_values("total_carbon_tc", ascending=False)
            )

        total_carbon = results_df["total_carbon_tc"].sum()
        total_area = results_df["total_area_hectares"].sum()
        total_ssc = results_df["total_ssc"].sum()
        results_df["percentage_of_total"] = results_df["total_carbon_tc"] / total_carbon * 100

        report = (
            f"\n        CARBON SEQUESTRATION ANALYSIS REPORT\n"
            f"        Study Area: {study_area_name}\n"
            f"        Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"        {'='*60}\n        SUMMARY BY SOLRIS CLASS\n        {'='*60}\n\n"
        )

        for _, row in results_df.iterrows():
            report += (
                f"        {row['solris_class']}:\n"
                f"        Area: {row['total_area_hectares']:,.2f} hectares "
                f"({row['total_area_hectares']/total_area*100:.1f}% of total)\n"
                f"        Total Carbon: {row['total_carbon_tc']:,.2f} tonnes C "
                f"({row['percentage_of_total']:.1f}% of total)\n"
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

    def plot_discounted_social_cost(
        self,
        study_area_name,
        scc_csv_path="carbon_sequestration/annual-scc.csv",
        start_year=2020,
        end_year=2080,
        discount_rate=0.02,
        save_path=None,
    ):
        """
        Plot the present value (2021) of total SSC from start_year to end_year
        at a given discount rate, using annual SCC values from a CSV file.
        """
        result = pd.read_sql(
            "SELECT SUM(ssc) AS total_ssc_millions FROM carbon_sequestration_results",
            self.engine,
        )
        total_ssc_millions = result["total_ssc_millions"].iloc[0] if not result.empty else 0

        scc_df = pd.read_csv(scc_csv_path)
        scc_df["SCC"] = scc_df["SCC"].str.replace("$", "").str.replace(",", "").astype(float)
        scc_by_year = dict(zip(scc_df["Year"], scc_df["SCC"]))

        scaling_factor = total_ssc_millions / _SCC_BASE_YEAR_VALUE

        years = list(range(start_year, end_year + 1))
        discounted_ssc = []
        for year in years:
            base = scc_by_year.get(year) or scc_by_year[max(y for y in scc_by_year if y <= year)]
            discounted_ssc.append(
                (scaling_factor * base) / ((1 + discount_rate) ** (year - start_year))
            )

        plt.figure(figsize=(10, 6))
        plt.plot(years, discounted_ssc, marker="o")
        plt.title(
            f"Present Value (2021) of Discounted Social Cost of Carbon ({study_area_name})\n"
            f"{start_year}–{end_year} at {discount_rate*100:.1f}% Discount Rate"
        )
        plt.xlabel("Year")
        plt.ylabel("Present Value (2021) of Social Cost (million $)")
        plt.grid(True)
        if save_path:
            plt.savefig(save_path, bbox_inches="tight")

    def export_to_csv(self, output_path):
        """Export carbon sequestration results from the database to a CSV file."""
        results_df = pd.read_sql(
            """
            SELECT solris_class, solris_code, area_hectares,
                   agc_tc_ha, bgc_tc_ha, soc_tc_ha, deoc_tc_ha,
                   total_carbon_tc, ssc, ssc_density, percentage_of_total
            FROM carbon_sequestration_results
            ORDER BY total_carbon_tc DESC
            """,
            self.engine,
        )
        if "solris_code" in results_df.columns:
            results_df["solris_code"] = results_df["solris_code"].astype("Int64")
        results_df.to_csv(output_path, index=False)
        logger.info(f"Carbon sequestration results exported to CSV: {output_path}")
        return results_df
