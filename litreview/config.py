"""Configuration for the ecosystem-services literature review pipeline.

Everything here is data, not logic: keyword sets, region definitions, the
ecosystem -> SOLRIS mapping, and run settings. The pipeline modules read from
this so the search criteria live in one auditable place.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

# Load .env (if present) before Settings reads the environment, so credentials
# in litreview/.env, the repo root, or scripts/.env are picked up automatically.
try:
    from dotenv import load_dotenv

    _here = os.path.dirname(__file__)
    _root = os.path.dirname(_here)
    for _env in (os.path.join(_here, ".env"),
                 os.path.join(_root, ".env"),
                 os.path.join(_root, "scripts", ".env")):
        if os.path.exists(_env):
            load_dotenv(_env, override=False)
except Exception:
    pass

# --------------------------------------------------------------------------
# Keyword sets (from the project's Literature Criterion document)
# --------------------------------------------------------------------------
# Each list is a controlled vocabulary. The screening step tags a paper with
# every term whose pattern appears in its title/abstract. Synonyms that should
# collapse to one canonical tag are grouped via ALIASES below.

ECOSYSTEM_SERVICE_KEYWORDS = [
    "ecosystem services",
    "natural assets",
    "natural capital",
    "biocapacity",
    "biodiversity",
    "carbon sequestration",
    "carbon storage",
    "water purification",
    "flood mitigation",
    "pollination",
    "nature-based solutions",
    "REV",  # Restored Ecosystem Value (kept as-is from criteria doc)
    # MEA service categories (the four classes most papers organize around)
    "provisioning services",
    "regulating services",
    "cultural ecosystem services",
    "supporting services",
    # individual services prevalent in the corpus
    "climate regulation",
    "erosion control",
    "pest control",
    "nutrient cycling",
    "seed dispersal",
    "recreation",
    "food security",
]

METHOD_KEYWORDS = [
    "benefits transfer",
    "WTP",
    "willingness-to-pay",
    "hedonic pricing",
    "replacement cost",
    "avoided cost",
    "restoration cost",
    "market price",
    "travel cost",
    "stated preference",
    "residual value",
    "averting behavior",
    "simulated exchange value",
    "GIS mapping",
    "InVEST",
    "machine learning",
    "LULC",
    "sensitivity analysis",
    "payments for ecosystem services",
    "economic valuation",
    "ecosystem service value",
    "remote sensing",
]

ECOSYSTEM_KEYWORDS = [
    "wetlands",
    "grasslands",
    "boreal forest",
    "savanna",
    "urban green space",
    "forest",
    # freshwater + marine/coastal biomes (global meta-analysis scope; not all
    # have a SOLRIS analogue, see ECOSYSTEM_TO_SOLRIS)
    "freshwater",
    "coastal",
    "marine",
    "mangrove",
    "seagrass",
    "coral reef",
    "agricultural land",
]

# Synonyms / spelling variants -> canonical term used in the keyword lists.
# Matching is case-insensitive; add variants here rather than bloating the
# canonical vocabularies above.
ALIASES: dict[str, list[str]] = {
    "willingness-to-pay": ["willingness to pay", "WTP"],
    "WTP": ["willingness to pay", "willingness-to-pay"],
    "benefits transfer": ["benefit transfer", "value transfer"],
    "nature-based solutions": ["nature based solutions", "NbS"],
    "carbon sequestration": ["carbon uptake", "c sequestration"],
    "carbon storage": ["carbon stock", "carbon pool", "soil carbon",
                       "soil organic carbon"],
    "water purification": ["water quality", "water filtration"],
    "pollination": ["pollinator", "pollination services"],
    "payments for ecosystem services": ["PES", "payment for ecosystem services"],
    "economic valuation": ["economic value", "monetary valuation"],
    "ecosystem service value": ["ESV", "ecosystem service values",
                                "ecosystem services value"],
    "LULC": ["land use land cover", "land-use/land-cover", "land use/land cover"],
    "InVEST": ["integrated valuation of ecosystem services and tradeoffs"],
    "wetlands": ["wetland", "marsh", "swamp", "fen", "bog", "peatland"],
    "grasslands": ["grassland", "tallgrass prairie", "prairie", "meadow"],
    "forest": ["forests", "woodland", "treed"],
    "urban green space": ["urban greenspace", "urban green-space", "green space"],
    "freshwater": ["lake", "lakes", "river", "rivers", "stream", "streams",
                   "watershed", "aquatic ecosystem", "aquatic"],
    "coastal": ["coastal ecosystem", "coastal wetland", "estuary", "estuarine"],
    "marine": ["marine ecosystem", "ocean", "oceanic", "sea"],
    "mangrove": ["mangroves", "mangrove forest"],
    "seagrass": ["seagrasses", "seagrass meadow"],
    "coral reef": ["coral reefs", "coral", "reef", "reefs"],
    "agricultural land": ["agricultural landscape", "cropland", "agroecosystem",
                          "farmland", "cultivated land", "agriculture"],
}

# --------------------------------------------------------------------------
# Regions (the three tiers)
# --------------------------------------------------------------------------
# North America is defined by author-institution country in OpenAlex.
# The Carolinian filter is applied on top of NA results using location text
# and lat/long (see geography.py / transferability.py).

NORTH_AMERICA_COUNTRY_CODES = ["US", "CA", "MX"]
CANADA_COUNTRY_CODES = ["CA"]

# Rough bounding box for the Carolinian zone / southern Ontario + Great Lakes
# basin, used as a coarse geographic pre-filter before criteria scoring.
CAROLINIAN_BBOX = {  # (lat/long degrees)
    "lat_min": 41.0,
    "lat_max": 45.5,
    "lon_min": -84.0,
    "lon_max": -76.0,
}
# Broader "eastern North America / Great Lakes" net for transferability scoring.
GREAT_LAKES_EASTERN_NA_BBOX = {
    "lat_min": 38.0,
    "lat_max": 49.0,
    "lon_min": -93.0,
    "lon_max": -70.0,
}

# --------------------------------------------------------------------------
# Ecosystem -> SOLRIS land class mapping
# --------------------------------------------------------------------------
# Reused at tier 3 to fill the "Overlap with SOLRIS Land Class(es)" column.
# Loaded lazily from the existing GIS lookup so we keep one source of truth.
SOLRIS_LOOKUP_CSV = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "data", "solris_lookup.csv"
)

# Coarse map from our ecosystem keyword -> SOLRIS class name fragments.
ECOSYSTEM_TO_SOLRIS = {
    "wetlands": ["Swamp", "Marsh", "Fen", "Bog", "Open Water"],
    "forest": ["Forest", "Treed", "Woodland", "Plantation"],
    "grasslands": ["Tallgrass", "Prairie", "Meadow", "Open Field"],
    "savanna": ["Savannah", "Tallgrass Savannah"],
    "urban green space": ["Built-Up", "Open Field"],
    "boreal forest": ["Coniferous Forest", "Mixed Forest"],
    "freshwater": ["Open Water"],
    "agricultural land": ["Tilled", "Undifferentiated"],
    # marine, coastal, mangrove, seagrass, coral reef have no SOLRIS analogue
    # (Ontario terrestrial land cover) and intentionally map to nothing.
}

# --------------------------------------------------------------------------
# Value normalization
# --------------------------------------------------------------------------
BASE_CURRENCY = "USD"   # target currency for all aggregated $/ha/yr figures
BASE_YEAR = 2020        # target year for inflation adjustment


@dataclass
class Settings:
    """Runtime settings; secrets pulled from the environment."""

    # Polite-pool contact for OpenAlex / Unpaywall (improves rate limits).
    contact_email: str = field(
        default_factory=lambda: os.getenv("LITREVIEW_EMAIL", "")
    )
    semantic_scholar_api_key: str = field(
        default_factory=lambda: os.getenv("SEMANTIC_SCHOLAR_API_KEY", "")
    )
    # Azure OpenAI (the user's key) for classification / extraction.
    azure_openai_endpoint: str = field(
        default_factory=lambda: os.getenv("AZURE_OPENAI_ENDPOINT", "")
    )
    azure_openai_api_key: str = field(
        default_factory=lambda: os.getenv("AZURE_OPENAI_API_KEY", "")
    )
    # In Azure, the "deployment" is the name you gave the model when you
    # deployed it in the portal. It often equals the model name, so we fall
    # back to AZURE_OPENAI_MODEL (e.g. "gpt-4.1") when DEPLOYMENT isn't set.
    azure_openai_deployment: str = field(
        default_factory=lambda: (
            os.getenv("AZURE_OPENAI_DEPLOYMENT")
            or os.getenv("AZURE_OPENAI_MODEL", "")
        )
    )
    azure_openai_api_version: str = field(
        default_factory=lambda: os.getenv(
            "AZURE_OPENAI_API_VERSION", "2025-01-01-preview"
        )
    )
    # Path to an ESVD / TEEB valuation-database export (CSV).
    esvd_csv: str = field(
        default_factory=lambda: os.getenv("ESVD_CSV", "")
    )
    request_timeout: int = 30
    output_dir: str = os.path.join(os.path.dirname(__file__), "outputs")


SETTINGS = Settings()
