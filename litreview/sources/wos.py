"""Web of Science client — Clarivate WoS Starter API.

Adds Web of Science as a counting + enumeration source for the **aggregate** and
**tier3** steps only (never keyword discovery). Same small surface the pipeline
needs from a source:

  * ``total_count(search=...)`` — hit count for a query (the aggregate columns),
  * ``search(query=...)``       — record enumeration mapped to ``Paper`` (tier3).

Unlike EBSCO, WoS supports an author **country** facet (the ``CU=`` field), so
the per-region (North America / Canada) count columns *are* available here —
``supports_country`` is True in the aggregate. ISO-2 codes are mapped to the
country names WoS expects via ``_COUNTRY_NAMES``.

Access: needs a Clarivate **WoS API key** (``X-ApiKey`` header), provisioned by
your institution. This targets the **Starter** API (JSON, widely licensed); note
its records are metadata-only — **no abstracts** — so tier-3 screening for WoS
matches on title + keywords rather than abstract text. When the key is absent the
source reports ``is_configured() == False`` and every caller skips it gracefully
(same pattern as Semantic Scholar / EBSCO / Azure).

Docs: https://developer.clarivate.com/apis/wos-starter
"""

from __future__ import annotations

import time
from typing import Iterator, Optional

import requests

from ..config import SETTINGS
from ..models import Paper

# The `documents` search endpoint lives under the configured base (default
# Starter). Override via WOS_API_BASE if you have a different tier/host.
DOCUMENTS_PATH = "/documents"

# ISO-2 (OpenAlex/pipeline) -> the country name WoS indexes in the CU field.
# Only the codes the pipeline actually uses are mapped; unmapped codes are
# dropped from the CU clause.
_COUNTRY_NAMES = {"US": "USA", "CA": "Canada", "MX": "Mexico"}


def is_configured() -> bool:
    """True when a WoS API key is present in the environment."""
    return bool(SETTINGS.wos_api_key)


def _headers() -> dict[str, str]:
    return {"Accept": "application/json", "X-ApiKey": SETTINGS.wos_api_key}


def to_wos_query(query: str) -> str:
    """Wrap an OpenAlex-style search string as a WoS Topic (``TS``) clause.

    The pipeline passes one query to every source. OpenAlex strings join clauses
    with ` AND ` and quote phrases (e.g. ``"REV" AND "ecosystem services"``);
    WoS ``TS=(...)`` accepts that boolean text verbatim and searches title +
    abstract + author keywords + keywords-plus, matching OpenAlex's
    title-and-abstract search closely enough for comparable counts.
    """
    return f"TS=({query.strip()})"


def _country_clause(country_codes: Optional[list[str]]) -> str:
    names = [ _COUNTRY_NAMES[c] for c in (country_codes or [])
              if c in _COUNTRY_NAMES ]
    if not names:
        return ""
    return "CU=(" + " OR ".join(f'"{n}"' for n in names) + ")"


def _build_q(
    *,
    search: str,
    year_from: Optional[int],
    year_to: Optional[int],
    country_codes: Optional[list[str]],
) -> str:
    """Assemble the full WoS query string (TS + optional CU + PY clauses)."""
    clauses = [to_wos_query(search)]
    cu = _country_clause(country_codes)
    if cu:
        clauses.append(cu)
    if year_from is not None or year_to is not None:
        lo = year_from or 1900
        hi = year_to or 2100
        clauses.append(f"PY=({lo}-{hi})")
    return " AND ".join(clauses)


def _get(params: dict[str, str]) -> dict:
    """GET the documents endpoint with basic 429 backoff."""
    url = SETTINGS.wos_api_base.rstrip("/") + DOCUMENTS_PATH
    for _ in range(4):
        r = requests.get(url, params=params, headers=_headers(),
                         timeout=SETTINGS.request_timeout)
        if r.status_code == 429:
            time.sleep(5)
            continue
        r.raise_for_status()
        return r.json()
    return {}


def total_count(
    *,
    search: str,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    country_codes: Optional[list[str]] = None,
) -> int:
    """Total WoS hits for a query (the aggregate count cell).

    Honors ``country_codes`` via the WoS CU field. Returns 0 when WoS is not
    configured (callers gate on ``is_configured()`` first).
    """
    if not is_configured():
        return 0
    params = {
        "q": _build_q(search=search, year_from=year_from, year_to=year_to,
                      country_codes=country_codes),
        "db": "WOS",
        "limit": "1",
        "page": "1",
    }
    data = _get(params)
    return int((data.get("metadata") or {}).get("total") or 0)


def _map_hit(h: dict) -> Paper:
    """Map one WoS Starter ``documents`` hit into a ``Paper``.

    Starter records are metadata-only (no abstract). Field paths follow the
    Starter document schema; every access is defensive since optional blocks
    (identifiers, names, citations) may be absent.
    """
    source = h.get("source") or {}
    idents = h.get("identifiers") or {}
    names = h.get("names") or {}
    kw = h.get("keywords") or {}
    links = h.get("links") or {}

    doi = (idents.get("doi") or "").lower() or None
    issns = [v for v in (idents.get("issn"), idents.get("eissn")) if v]

    authors = [a.get("displayName") or a.get("wosStandard") or ""
               for a in (names.get("authors") or [])]
    authors = [a for a in authors if a]

    # Citation count: prefer the WOS-database tally when several are present.
    citation_count = None
    for c in h.get("citations") or []:
        if c.get("count") is not None:
            citation_count = c["count"]
            if (c.get("db") or "").upper() == "WOS":
                break

    types = h.get("types") or h.get("sourceTypes") or []
    publication_type = (types[0].lower() if types else "")

    concepts = list(kw.get("authorKeywords") or [])[:8]

    record_link = links.get("record")
    if isinstance(record_link, list):
        record_link = record_link[0] if record_link else None

    return Paper(
        source_db="wos",
        source_id=h.get("uid", "") or "",
        doi=doi,
        title=h.get("title") or "",
        abstract="",                 # Starter API returns no abstract
        year=source.get("publishYear"),
        venue=source.get("sourceTitle") or "",
        venue_type="",
        publication_type=publication_type,
        issn=issns,
        authors=authors,
        open_access=False,
        oa_pdf_url=None,
        citation_count=citation_count,
        concepts=concepts,
        raw=h,
    )


def search(
    *,
    query: str,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    country_codes: Optional[list[str]] = None,
    max_results: int = 500,
    per_page: int = 50,
    sleep: float = 0.2,
) -> Iterator[Paper]:
    """Yield ``Paper`` records for a query, page-paginated.

    Accepts the same OpenAlex-style ``query`` the other sources take (see
    ``to_wos_query``) plus an optional ``country_codes`` constraint. Yields
    nothing when WoS is not configured so callers can iterate unconditionally.
    """
    if not is_configured():
        return
    q = _build_q(search=query, year_from=year_from, year_to=year_to,
                 country_codes=country_codes)
    page = 1
    fetched = 0
    while fetched < max_results:
        params = {"q": q, "db": "WOS",
                  "limit": str(min(per_page, 50)), "page": str(page)}
        data = _get(params)
        hits = data.get("hits") or []
        if not hits:
            return
        for h in hits:
            yield _map_hit(h)
            fetched += 1
            if fetched >= max_results:
                return
        total = int((data.get("metadata") or {}).get("total") or 0)
        if fetched >= total:
            return
        page += 1
        if sleep:
            time.sleep(sleep)
