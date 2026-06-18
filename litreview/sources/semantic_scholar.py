"""Semantic Scholar client — enrichment layer for tiers 2 and 3.

Used for what OpenAlex does less well:
  - clean abstract text + AI TLDR summaries,
  - direct open-access PDF URLs (`openAccessPdf`) for full-text extraction,
  - relevance-ranked search and recommendations for citation snowballing.

Records are keyed by DOI so they merge into the OpenAlex backbone in dedup.py
and never contribute to prevalence counts (which stay OpenAlex-only).

Docs: https://api.semanticscholar.org/api-docs/graph
"""

from __future__ import annotations

import time
from typing import Iterator, Optional

import requests

from ..config import SETTINGS
from ..models import Paper

BULK_SEARCH_URL = "https://api.semanticscholar.org/graph/v1/paper/search/bulk"
RECOMMEND_URL = "https://api.semanticscholar.org/recommendations/v1/papers/forpaper"

FIELDS = [
    "title", "abstract", "year", "externalIds", "venue",
    "publicationTypes", "publicationVenue", "openAccessPdf",
    "isOpenAccess", "citationCount", "fieldsOfStudy", "tldr", "authors",
]


def _headers() -> dict[str, str]:
    h = {"User-Agent": "litreview/0.1"}
    if SETTINGS.semantic_scholar_api_key:
        h["x-api-key"] = SETTINGS.semantic_scholar_api_key
    return h


def to_s2_query(query: str) -> str:
    """Translate an OpenAlex-style search string to S2 bulk-search syntax.

    OpenAlex joins clauses with ` AND `; the S2 bulk endpoint treats a space
    between terms as AND, so the only rewrite needed is dropping the keyword.
    Quoted phrases work in both. Used so callers can pass one query to both.
    """
    return query.replace(" AND ", " ").strip()


def _map_paper(p: dict) -> Paper:
    ext = p.get("externalIds") or {}
    doi = ext.get("DOI")
    if doi:
        doi = doi.lower()
    venue_obj = p.get("publicationVenue") or {}
    oa = p.get("openAccessPdf") or {}
    pub_types = p.get("publicationTypes") or []
    abstract = p.get("abstract") or ""
    if not abstract and p.get("tldr"):
        abstract = (p["tldr"] or {}).get("text", "") or ""
    return Paper(
        source_db="semantic_scholar",
        source_id=p.get("paperId", ""),
        doi=doi,
        title=p.get("title") or "",
        abstract=abstract,
        year=p.get("year"),
        venue=p.get("venue") or venue_obj.get("name", "") or "",
        venue_type=(venue_obj.get("type") or ""),
        publication_type=(pub_types[0].lower() if pub_types else ""),
        issn=([venue_obj["issn"]] if venue_obj.get("issn") else []),
        authors=[a.get("name", "") for a in (p.get("authors") or [])],
        open_access=bool(p.get("isOpenAccess")),
        oa_pdf_url=oa.get("url"),
        citation_count=p.get("citationCount"),
        concepts=p.get("fieldsOfStudy") or [],
        raw=p,
    )


def search(
    *,
    query: str,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    max_results: int = 500,
    sleep: float = 1.0,
) -> Iterator[Paper]:
    """Relevance-ranked bulk search. Token-paginated, up to ~1000/page.

    `query` may be an OpenAlex-style string (` AND `-joined); it is normalized
    to S2 syntax via to_s2_query so the same query works against both sources.
    """
    params: dict[str, str] = {"query": to_s2_query(query),
                              "fields": ",".join(FIELDS)}
    if year_from or year_to:
        lo = year_from or 1900
        hi = year_to or 2100
        params["year"] = f"{lo}-{hi}"
    token: Optional[str] = None
    fetched = 0
    while fetched < max_results:
        if token:
            params["token"] = token
        r = requests.get(BULK_SEARCH_URL, params=params, headers=_headers(),
                         timeout=SETTINGS.request_timeout)
        if r.status_code == 429:
            time.sleep(5)
            continue
        r.raise_for_status()
        data = r.json()
        for p in data.get("data", []) or []:
            yield _map_paper(p)
            fetched += 1
            if fetched >= max_results:
                return
        token = data.get("token")
        if not token:
            return
        time.sleep(sleep)


def recommendations(paper_id_or_doi: str, limit: int = 50) -> Iterator[Paper]:
    """Snowball: papers similar to a seed (by S2 id or DOI:xxx)."""
    url = f"{RECOMMEND_URL}/{paper_id_or_doi}"
    r = requests.get(url, params={"fields": ",".join(FIELDS), "limit": str(limit)},
                     headers=_headers(), timeout=SETTINGS.request_timeout)
    r.raise_for_status()
    for p in r.json().get("recommendedPapers", []) or []:
        yield _map_paper(p)
