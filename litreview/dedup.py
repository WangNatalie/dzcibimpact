"""Cross-source deduplication and record merging.

OpenAlex is the authoritative backbone; Semantic Scholar records merge into a
matching OpenAlex record (by DOI, else normalized title) to add abstract text
and OA PDF links without ever creating a second counted paper.
"""

from __future__ import annotations

from .models import Paper

# Which source wins when both have a value for a scalar field.
_PRIORITY = {"openalex": 2, "semantic_scholar": 1, "esvd": 0}


def _merge(primary: Paper, other: Paper) -> Paper:
    """Fill blanks on `primary` from `other`; prefer S2 for abstract/PDF."""
    if not primary.abstract and other.abstract:
        primary.abstract = other.abstract
    if not primary.oa_pdf_url and other.oa_pdf_url:
        primary.oa_pdf_url = other.oa_pdf_url
    if not primary.open_access and other.open_access:
        primary.open_access = True
    if primary.citation_count is None and other.citation_count is not None:
        primary.citation_count = other.citation_count
    for attr in ("venue", "publication_type", "venue_type", "location_text"):
        if not getattr(primary, attr) and getattr(other, attr):
            setattr(primary, attr, getattr(other, attr))
    primary.raw[f"merged_{other.source_db}"] = other.raw
    return primary


def deduplicate(papers: list[Paper]) -> list[Paper]:
    """Collapse duplicates across sources into one record per dedup_key."""
    by_key: dict[str, Paper] = {}
    for p in papers:
        key = p.dedup_key
        if key not in by_key:
            by_key[key] = p
            continue
        existing = by_key[key]
        if _PRIORITY.get(p.source_db, 0) > _PRIORITY.get(existing.source_db, 0):
            by_key[key] = _merge(p, existing)
        else:
            by_key[key] = _merge(existing, p)
    return list(by_key.values())
