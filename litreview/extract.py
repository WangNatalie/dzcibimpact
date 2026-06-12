"""Tier-3 deep extraction: turn a selected paper into a target-table row.

Fills the columns of `research/Ecosystem Services Mapping(Data).csv` using the
Azure OpenAI model over the paper's full text (or abstract fallback). The
geographic/SOLRIS/transferability scaffolding is computed deterministically;
the model is asked only for what requires reading the paper (values, methods,
limitations, findings, applicability), and is instructed to never invent
numbers that aren't in the text.
"""

from __future__ import annotations

from typing import Optional

from .config import ECOSYSTEM_TO_SOLRIS, SOLRIS_LOOKUP_CSV
from .models import Paper, TableRow
from . import llm_azure, transferability
from .fulltext import fetch_fulltext

_SYSTEM = (
    "You are a research assistant compiling an ecosystem-services valuation "
    "literature review for the Carolinian Zone of southern Ontario, Canada. "
    "Extract information ONLY from the provided paper text. Never invent "
    "numeric values, units, or study locations; if a field is not stated, "
    "return an empty string. Be concise and quantitative. Respond with a "
    "single JSON object using exactly the requested keys."
)

_KEYS = [
    "location", "lat", "lon", "ecosystem", "ecosystem_service",
    "value_measurement_scale", "method", "limitations_uncertainty",
    "key_findings", "utility_transferability",
]


def solris_overlap(paper: Paper) -> str:
    """Deterministic SOLRIS land-class overlap from the paper's ecosystem tags."""
    classes: list[str] = []
    for eco in paper.ecosystems:
        for cls in ECOSYSTEM_TO_SOLRIS.get(eco, []):
            if cls not in classes:
                classes.append(cls)
    return ", ".join(classes)


def _build_user_prompt(paper: Paper, text: str, text_source: str) -> str:
    return (
        f"PAPER METADATA\n"
        f"Title: {paper.title}\n"
        f"Authors: {', '.join(paper.authors[:8])}\n"
        f"Year: {paper.year}\n"
        f"Venue: {paper.venue}\n"
        f"DOI: {paper.doi or ''}\n"
        f"Detected ecosystem services: {', '.join(paper.ecosystem_services) or 'none'}\n"
        f"Detected ecosystems: {', '.join(paper.ecosystems) or 'none'}\n"
        f"Detected methods: {', '.join(paper.methods) or 'none'}\n\n"
        f"PAPER TEXT (source: {text_source}):\n{text}\n\n"
        "Return JSON with these keys (empty string if not stated in the text):\n"
        "  location: study site location (place names)\n"
        "  lat: site latitude as a number, or null\n"
        "  lon: site longitude as a number, or null\n"
        "  ecosystem: ecosystem/habitat type studied\n"
        "  ecosystem_service: ecosystem service(s) valued\n"
        "  value_measurement_scale: the reported value(s) WITH units and scale "
        "(e.g. '243 g C m-2 yr-1' or '$464 USD/acre/year'); copy verbatim\n"
        "  method: valuation/measurement method used\n"
        "  limitations_uncertainty: stated limitations, uncertainty, caveats\n"
        "  key_findings: 1-3 key findings/implications, as terse bullet phrases\n"
        "  utility_transferability: how usable these data are for ecosystem "
        "valuation in the Carolinian Zone / southern Ontario, and why\n"
    )


def extract_row(
    paper: Paper,
    *,
    use_fulltext: bool = True,
    max_chars: int = 60000,
) -> TableRow:
    """Extract one TableRow for a paper (must be screened first for tags)."""
    if use_fulltext:
        text, source = fetch_fulltext(paper, max_chars=max_chars)
    else:
        text, source = paper.abstract or "", "abstract"

    tscore = transferability.score(paper)

    data: dict = {}
    if text.strip() and llm_azure.is_configured():
        data = llm_azure.complete_json(
            _SYSTEM, _build_user_prompt(paper, text, source), max_tokens=1600
        )

    def g(key: str) -> str:
        v = data.get(key, "")
        return "" if v is None else str(v).strip()

    link = (f"https://doi.org/{paper.doi}" if paper.doi
            else (paper.oa_pdf_url or ""))

    row = TableRow(
        source=paper.title,
        location=g("location") or paper.location_text,
        lat=data.get("lat") if isinstance(data.get("lat"), (int, float)) else paper.lat,
        lon=data.get("lon") if isinstance(data.get("lon"), (int, float)) else paper.lon,
        ecosystem=g("ecosystem") or ", ".join(paper.ecosystems),
        solris_overlap=solris_overlap(paper),
        ecosystem_service=g("ecosystem_service") or ", ".join(paper.ecosystem_services),
        value_measurement_scale=g("value_measurement_scale"),
        method=g("method") or ", ".join(paper.methods),
        limitations_uncertainty=g("limitations_uncertainty"),
        link=link,
        key_findings=g("key_findings"),
        utility_transferability=(
            g("utility_transferability")
            or f"[auto] {tscore['rationale']}"
        ),
        peer_reviewed=paper.is_peer_reviewed,
        transferability_score=tscore["transferability_score"],
    )
    return row


def extract_rows(papers: list[Paper], **kwargs) -> list[TableRow]:
    return [extract_row(p, **kwargs) for p in papers]
