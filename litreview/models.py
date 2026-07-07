"""Normalized data model shared across sources and tiers.

A `Paper` is the common record every source client maps into, so dedup,
screening, scoring, and aggregation never touch source-specific JSON shapes.
The field names are chosen to map cleanly onto the target output table
(`research/Ecosystem Services Mapping(Data).csv`).
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Optional


@dataclass
class Paper:
    # --- identity ---
    source_db: str                      # "openalex" | "semantic_scholar" | "esvd" | "ebsco" | "wos"
    source_id: str                      # native id within that source
    doi: Optional[str] = None           # normalized lowercase, no prefix
    title: str = ""
    abstract: str = ""
    year: Optional[int] = None

    # Every source_db that contributed to this record after cross-source dedup in dedup.py
    contributing_sources: list[str] = field(default_factory=list)

    # --- venue / provenance ---
    venue: str = ""                     # journal/source display name
    venue_type: str = ""                # journal | repository | conference | book | report
    publication_type: str = ""          # article | review | preprint | dataset | report
    issn: list[str] = field(default_factory=list)

    # --- people / geography ---
    authors: list[str] = field(default_factory=list)
    author_countries: list[str] = field(default_factory=list)  # ISO-2 codes
    institutions: list[str] = field(default_factory=list)
    location_text: str = ""             # study-site text if extracted
    lat: Optional[float] = None
    lon: Optional[float] = None

    # --- access / impact ---
    open_access: bool = False
    oa_pdf_url: Optional[str] = None    # direct full-text PDF when available
    citation_count: Optional[int] = None
    concepts: list[str] = field(default_factory=list)  # source topic tags

    # --- peer-review status (peer_review.py fills these) ---
    is_peer_reviewed: Optional[bool] = None
    grey_literature: bool = False
    peer_review_reason: str = ""

    # --- screening tags (screening.py fills these) ---
    ecosystem_services: list[str] = field(default_factory=list)
    methods: list[str] = field(default_factory=list)
    ecosystems: list[str] = field(default_factory=list)

    # --- tier classification ---
    region: str = ""                    # "global" | "north_america" | "carolinian"

    # raw source payload, kept for audit / re-extraction
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def dedup_key(self) -> str:
        """Stable key for cross-source dedup: DOI if present, else norm title."""
        if self.doi:
            return f"doi:{self.doi}"
        return "title:" + "".join(c for c in self.title.lower() if c.isalnum())

    @property
    def source_label(self) -> str:
        """Human-readable provenance, e.g. 'OpenAlex + Semantic Scholar'."""
        names = {"openalex": "OpenAlex",
                 "semantic_scholar": "Semantic Scholar",
                 "esvd": "ESVD",
                 "ebsco": "EBSCO",
                 "wos": "Web of Science"}
        out: list[str] = []
        for s in (self.contributing_sources or [self.source_db]):
            label = names.get(s, s)
            if label and label not in out:
                out.append(label)
        return " + ".join(out)

    def to_row(self) -> dict[str, Any]:
        """Flatten list fields for CSV export."""
        d = asdict(self)
        d.pop("raw", None)
        for k, v in list(d.items()):
            if isinstance(v, list):
                d[k] = "; ".join(str(x) for x in v)
        return d


@dataclass
class TableRow:
    """One row of the target output table (tier-3 in-depth review)."""

    source: str = ""                         # Source (title or citation)
    location: str = ""                       # Location
    lat: Optional[float] = None
    lon: Optional[float] = None
    ecosystem: str = ""                      # Ecosystem
    solris_overlap: str = ""                 # Overlap with SOLRIS Land Class(es)
    ecosystem_service: str = ""              # Ecosystem Service
    value_measurement_scale: str = ""        # Value/Measurement Scale
    method: str = ""                         # Method
    limitations_uncertainty: str = ""        # Limitations/Uncertainty
    link: str = ""                           # Link
    key_findings: str = ""                   # Key Findings/Implications
    utility_transferability: str = ""        # Utility/Transferability

    # provenance kept out of the user-facing table but useful internally
    data_source: str = ""                    # "OpenAlex" / "OpenAlex + Semantic Scholar"
    peer_reviewed: Optional[bool] = None
    transferability_score: Optional[float] = None

    # Column order matching research/Ecosystem Services Mapping(Data).csv
    CSV_COLUMNS = [
        ("source", "Source"),
        ("location", "Location"),
        ("lat", "Lat"),
        ("lon", "Long"),
        ("ecosystem", "Ecosystem"),
        ("solris_overlap", "Overlap with SOLRIS Land Class(es)"),
        ("ecosystem_service", "Ecosystem Service"),
        ("value_measurement_scale", "Value/Measurement Scale"),
        ("method", "Method"),
        ("limitations_uncertainty", "Limitations/Uncertainty"),
        ("link", "Link"),
        ("key_findings", "Key Findings/Implications"),
        ("utility_transferability", "Utility/Transferability"),
    ]

    def to_csv_dict(self) -> dict[str, Any]:
        return {label: getattr(self, attr) for attr, label in self.CSV_COLUMNS}
