"""ESVD loader — the curated value source for tier-1/2 dollar averages.

The Ecosystem Services Valuation Database (ESVD 2.0) ships ~12k valuation
records already normalized to `Int$ Per Hectare Per Year` (PPP international
dollars per hectare per year), which is what makes cross-study averaging
defensible. This module:

  * loads the CSV,
  * crosswalks ESVD's TEEB service categories, biomes, and method codes onto
    THIS project's keyword vocabularies (config.py), and
  * provides robust summaries (median / IQR, not mean) because the value
    distribution is extremely heavy-tailed (median ~$296 vs mean ~$0.8B).

The crosswalks below are editable judgement calls — adjust them rather than
trusting them blindly. References: TEEB classification; ESVD codebook.
"""

from __future__ import annotations

import re
from typing import Optional

import pandas as pd

from ..config import SETTINGS

VALUE_COL = "Int$ Per Hectare Per Year"

# ESVD stores ISO-3 country codes; OpenAlex uses ISO-2. This is the ESVD-side
# North America set (US + Canada + Mexico) matching the OpenAlex US/CA/MX.
NORTH_AMERICA_ESVD_CODES = ["USA", "CAN", "MEX"]

# --- crosswalk: our ecosystem-service keyword -> ESVD standardized ES_1 cats ---
# ES_1 is the cleanest service field (23 TEEB categories, exhaustively listed
# below). Several project keywords are coarser than TEEB, so each maps to one
# or more classes. The four MEA service categories aggregate many ES_1 classes.
#   ES_1 universe: Aesthetic information, Air quality regulation, Biological
#   control, Climate regulation, Erosion prevention, Existence/bequest values,
#   Food, Genetic resources, Information for cognitive development, Inspiration
#   for culture/art/design, Maintenance of genetic diversity, Maintenance of
#   life cycles, Maintenance of soil fertility, Medicinal resources, Moderation
#   of extreme events, Opportunities for recreation and tourism, Ornamental
#   resources, Pollination, Raw materials, Regulation of water flows, Spiritual
#   experience, Waste treatment, Water.
SERVICE_CROSSWALK: dict[str, list[str]] = {
    # individual services
    "carbon sequestration": ["Climate regulation"],
    "carbon storage": ["Climate regulation"],
    "climate regulation": ["Climate regulation"],
    "water purification": ["Waste treatment", "Air quality regulation"],
    "flood mitigation": ["Moderation of extreme events", "Regulation of water flows"],
    "pollination": ["Pollination"],
    "erosion control": ["Erosion prevention"],
    "pest control": ["Biological control"],
    "nutrient cycling": ["Maintenance of soil fertility"],
    "seed dispersal": ["Maintenance of life cycles"],
    "recreation": ["Opportunities for recreation and tourism"],
    "food security": ["Food"],
    "biodiversity": [
        "Existence, bequest values",
        "Maintenance of genetic diversity",
        "Maintenance of life cycles",
        "Genetic resources",
        "Biological control",
    ],
    # MEA / CICES top-level service classes
    "provisioning services": [
        "Food", "Raw materials", "Water", "Genetic resources",
        "Medicinal resources", "Ornamental resources",
    ],
    "regulating services": [
        "Climate regulation", "Air quality regulation", "Moderation of extreme events",
        "Regulation of water flows", "Waste treatment", "Erosion prevention",
        "Pollination", "Biological control",
    ],
    "cultural ecosystem services": [
        "Aesthetic information", "Opportunities for recreation and tourism",
        "Inspiration for culture, art and design",
        "Information for cognitive development", "Spiritual experience",
    ],
    "supporting services": [
        "Maintenance of soil fertility", "Maintenance of life cycles",
        "Maintenance of genetic diversity",
    ],
    "biocapacity": ["Food", "Raw materials"],
    # broad catch-alls: empty list = no service filter (matches all records)
    "ecosystem services": [],
    "natural capital": [],
    "natural assets": [],
}

# --- crosswalk: our ecosystem keyword -> fragments searched across ESVD's
# biome + ecozone + ecosystem text. Substring match, case-insensitive. Marine/
# coastal/mangrove/seagrass/coral live at the ECOSYSTEM level, not the biome
# level, which is why filter() searches the combined column (see __init__).
BIOME_CROSSWALK: dict[str, list[str]] = {
    "wetlands": ["wetland", "marsh", "swamp", "peat", "fen", "bog"],
    "forest": ["forest", "woodland"],
    "boreal forest": ["cold climate evergreen forest", "boreal"],
    "grasslands": ["grassland", "rangeland", "prairie"],
    "savanna": ["savanna"],
    "urban green space": ["urban green", "urban"],
    "freshwater": ["rivers and lakes", "river", "lake", "freshwater"],
    "coastal": ["coastal", "estuar", "shoreline"],
    "marine": ["marine", "ocean"],
    "mangrove": ["mangrove"],
    "seagrass": ["seagrass"],
    "coral reef": ["coral", "reef"],
    "agricultural land": ["agricultur", "cropland", "intensive land use", "farmland"],
}

# --- ESVD valuation-method codes -> (readable name, our method keyword) ---
METHOD_CODES: dict[str, tuple[str, str]] = {
    "MP": ("Market Prices", "market price"),
    "CV": ("Contingent Valuation", "stated preference"),
    "CE": ("Choice Experiment", "stated preference"),
    "TC": ("Travel Cost", "travel cost"),
    "HP": ("Hedonic Pricing", "hedonic pricing"),
    "RC": ("Replacement Cost", "replacement cost"),
    "DC": ("Damage Cost Avoided", "avoided cost"),
    "AC": ("Avoided Cost", "avoided cost"),
    "OC": ("Opportunity Cost", "avoided cost"),
    "PF": ("Production Function", "residual value"),
    "FI": ("Factor Income / Net Factor Income", "residual value"),
    "VT": ("Value (Benefit) Transfer", "benefits transfer"),
    "RT": ("Benefit Transfer", "benefits transfer"),
    "SC": ("Substitute Cost", "replacement cost"),
    "AB": ("Averting Behaviour", "averting behavior"),
    "GV": ("Group Valuation", "stated preference"),
    "PES": ("Payments for Ecosystem Services", "market price"),
    "OT": ("Other", ""),
    "GP": ("Group Valuation", "stated preference"),
}


class ESVD:
    def __init__(self, path: Optional[str] = None):
        self.path = path or SETTINGS.esvd_csv
        if not self.path:
            raise ValueError("No ESVD CSV path set (ESVD_CSV env or pass path=).")
        df = pd.read_csv(self.path, low_memory=False)
        df.columns = [c.strip().strip('"').lstrip("﻿") for c in df.columns]
        df[VALUE_COL] = pd.to_numeric(df[VALUE_COL], errors="coerce")
        # Combined ecosystem search field: biome + ecozone + ecosystem text, so
        # ecosystem filters can match terms that ESVD records at any of those
        # levels (e.g. mangrove/seagrass/coral are ecosystem-level, not biome).
        eco_cols = [c for c in ("ESVD2.0_Biome", "ESVD2.0_Ecozones",
                                "ESVD2.0_Ecosystems", "Ecosystem Text")
                    if c in df.columns]
        df["_eco_search"] = (
            df[eco_cols].fillna("").agg(" | ".join, axis=1).str.lower()
        )
        self.df = df

    # ------------------------------------------------------------------ #
    def filter(
        self,
        *,
        continent: Optional[str] = None,
        country_codes: Optional[list[str]] = None,
        bbox: Optional[dict] = None,
        service: Optional[str] = None,
        ecosystem: Optional[str] = None,
        require_value: bool = True,
        reviewed_only: bool = False,
    ) -> pd.DataFrame:
        """Return the subset matching the given facets (project vocabulary)."""
        d = self.df
        if require_value:
            d = d[d[VALUE_COL].notna()]
        if reviewed_only and "Reviewed" in d.columns:
            d = d[d["Reviewed"].astype(str).str.strip().str.lower().isin(
                {"yes", "y", "true", "1", "reviewed"})]
        if continent:
            d = d[d["Continent"] == continent]
        if country_codes:
            codes = set(country_codes)
            cc = d["Country_Codes"].fillna("").astype(str)
            d = d[cc.apply(lambda s: any(c in codes for c in s.replace(";", ",").split(",")))]
        if bbox:
            d = d[
                d["Latitude"].between(bbox["lat_min"], bbox["lat_max"])
                & d["Longitude"].between(bbox["lon_min"], bbox["lon_max"])
            ]
        if service is not None:
            cats = SERVICE_CROSSWALK.get(service, [service])
            if cats:  # empty list = match-all catch-all term
                d = d[d["ES_1"].isin(cats)]
        if ecosystem is not None:
            frags = BIOME_CROSSWALK.get(ecosystem, [ecosystem])
            pat = "|".join(re.escape(f.lower()) for f in frags)
            d = d[d["_eco_search"].str.contains(pat, regex=True)]
        return d

    # ------------------------------------------------------------------ #
    @staticmethod
    def summarize_values(d: pd.DataFrame) -> dict:
        """Robust summary of Int$/ha/yr. Median + IQR (mean is meaningless here)."""
        v = d[VALUE_COL].dropna()
        if v.empty:
            return {"n": 0}
        return {
            "n": int(v.size),
            "n_studies": int(d["StudyId"].nunique()) if "StudyId" in d else None,
            "median": round(float(v.median()), 2),
            "q1": round(float(v.quantile(0.25)), 2),
            "q3": round(float(v.quantile(0.75)), 2),
            "trimmed_mean_10pct": round(
                float(v[(v >= v.quantile(0.05)) & (v <= v.quantile(0.95))].mean()), 2
            ),
            "min": round(float(v.min()), 2),
            "max": round(float(v.max()), 2),
        }

    def summary_by(
        self,
        group: str = "service",
        *,
        continent: Optional[str] = None,
        country_codes: Optional[list[str]] = None,
        reviewed_only: bool = False,
    ) -> pd.DataFrame:
        """Cross-tab of robust value stats by 'service' or 'ecosystem'.

        Rows are this project's keyword categories (not raw ESVD categories),
        so the table lines up with the screening counts from OpenAlex.
        """
        keys = (
            list(SERVICE_CROSSWALK) if group == "service" else list(BIOME_CROSSWALK)
        )
        rows = []
        for key in keys:
            kwargs = {"continent": continent, "country_codes": country_codes,
                      "reviewed_only": reviewed_only}
            d = self.filter(**({"service": key} if group == "service"
                               else {"ecosystem": key}), **kwargs)
            stats = self.summarize_values(d)
            if stats.get("n"):
                rows.append({group: key, **stats})
        return pd.DataFrame(rows)


def load(path: Optional[str] = None) -> ESVD:
    return ESVD(path)


def north_america_country_codes() -> list[str]:
    return NORTH_AMERICA_ESVD_CODES
