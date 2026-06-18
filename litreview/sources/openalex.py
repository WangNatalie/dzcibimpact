"""OpenAlex client — the counting / enumeration backbone for every tier.

Two jobs:
  1. faceted_count(): cheap prevalence counts via the `group_by` API without
     downloading any papers. This is what powers the tier 1/2 cross-tabs
     (papers per ecosystem service x method x ecosystem x region).
  2. search(): cursor-paginated enumeration of full records, mapped to Paper.

OpenAlex is free and keyless; passing a contact email joins the faster
"polite pool". Docs: https://docs.openalex.org/
"""

from __future__ import annotations

import time
from typing import Iterator, Optional
from urllib.parse import quote

import requests

from ..config import SETTINGS, NORTH_AMERICA_COUNTRY_CODES
from ..models import Paper

BASE_URL = "https://api.openalex.org/works"
TOPICS_URL = "https://api.openalex.org/topics"


def _headers() -> dict[str, str]:
    ua = "litreview/0.1 (ecosystem-services review)"
    if SETTINGS.contact_email:
        ua += f" mailto:{SETTINGS.contact_email}"
    return {"User-Agent": ua}


def _reconstruct_abstract(inverted_index: Optional[dict]) -> str:
    """OpenAlex ships abstracts as a word -> [positions] inverted index."""
    if not inverted_index:
        return ""
    positions: list[tuple[int, str]] = []
    for word, idxs in inverted_index.items():
        for i in idxs:
            positions.append((i, word))
    positions.sort()
    return " ".join(word for _, word in positions)


def _build_filter(
    *,
    search: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    country_codes: Optional[list[str]] = None,
    extra: Optional[dict[str, str]] = None,
) -> dict[str, str]:
    """Assemble the OpenAlex `filter` clauses.

    `search` is applied via `title_and_abstract.search` so we match the
    controlled vocabulary in abstracts, not just titles.
    """
    clauses: list[str] = []
    if search:
        clauses.append(f"title_and_abstract.search:{search}")
    if year_from is not None:
        clauses.append(f"from_publication_date:{year_from}-01-01")
    if year_to is not None:
        clauses.append(f"to_publication_date:{year_to}-12-31")
    if country_codes:
        clauses.append("institutions.country_code:" + "|".join(country_codes))
    if extra:
        for k, v in extra.items():
            clauses.append(f"{k}:{v}")
    return {"filter": ",".join(clauses)} if clauses else {}


def faceted_count(
    *,
    search: str,
    group_by: str = "type",
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    country_codes: Optional[list[str]] = None,
) -> dict[str, int]:
    """Return {group_value: count} for a query without downloading papers.

    Example: faceted_count(search="carbon sequestration wetland",
                           group_by="institutions.country_code")
    """
    params = _build_filter(
        search=search,
        year_from=year_from,
        year_to=year_to,
        country_codes=country_codes,
    )
    params["group_by"] = group_by
    params["per_page"] = "200"
    r = requests.get(BASE_URL, params=params, headers=_headers(),
                     timeout=SETTINGS.request_timeout)
    r.raise_for_status()
    data = r.json()
    return {g["key"]: g["count"] for g in data.get("group_by", [])}


def faceted_groups(
    *,
    search: str,
    group_by: str,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    country_codes: Optional[list[str]] = None,
) -> list[dict]:
    """Like faceted_count but keeps display names: [{key, name, count}, ...].

    Needed for concept/keyword discovery where the group key is an opaque id.
    """
    params = _build_filter(
        search=search,
        year_from=year_from,
        year_to=year_to,
        country_codes=country_codes,
    )
    params["group_by"] = group_by
    params["per_page"] = "200"
    r = requests.get(BASE_URL, params=params, headers=_headers(),
                     timeout=SETTINGS.request_timeout)
    r.raise_for_status()
    out = []
    for g in r.json().get("group_by", []):
        out.append({
            "key": g["key"],
            "name": g.get("key_display_name", g["key"]),
            "count": g["count"],
        })
    return out


def resolve_topic_id(name: str) -> Optional[dict]:
    """Resolve a Topic display name to its OpenAlex id (e.g. 'T10168').

    keywords_topics.csv stores Topic *display names*; works can only be
    filtered by Topic *id*, so this looks the name up via the topics endpoint
    and returns the best (most-cited) match as {id, display_name, works_count},
    or None if nothing matches. No LLM involved — exact/relevance match only.
    """
    params = {"filter": f"display_name.search:{name}",
              "sort": "works_count:desc", "per_page": "5"}
    r = requests.get(TOPICS_URL, params=params, headers=_headers(),
                     timeout=SETTINGS.request_timeout)
    r.raise_for_status()
    results = r.json().get("results", [])
    if not results:
        return None
    # Prefer a case-insensitive exact name match; else the top relevance hit.
    exact = next((t for t in results
                  if t.get("display_name", "").lower() == name.lower()), None)
    t = exact or results[0]
    return {
        "id": t["id"].rsplit("/", 1)[-1],   # short id, e.g. 'T10168'
        "display_name": t.get("display_name", ""),
        "works_count": t.get("works_count", 0),
    }


def total_count(
    *,
    search: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    country_codes: Optional[list[str]] = None,
    extra: Optional[dict[str, str]] = None,
) -> int:
    """Total number of works matching a query (the denominator)."""
    params = _build_filter(
        search=search,
        year_from=year_from,
        year_to=year_to,
        country_codes=country_codes,
        extra=extra,
    )
    params["per_page"] = "1"
    r = requests.get(BASE_URL, params=params, headers=_headers(),
                     timeout=SETTINGS.request_timeout)
    r.raise_for_status()
    return r.json().get("meta", {}).get("count", 0)


def _map_work(w: dict) -> Paper:
    venue = ((w.get("primary_location") or {}).get("source") or {}) or {}
    countries: list[str] = []
    institutions: list[str] = []
    authors: list[str] = []
    for au in w.get("authorships", []):
        if au.get("author", {}).get("display_name"):
            authors.append(au["author"]["display_name"])
        for inst in au.get("institutions", []):
            if inst.get("country_code"):
                countries.append(inst["country_code"])
            if inst.get("display_name"):
                institutions.append(inst["display_name"])
    best_oa = w.get("best_oa_location") or {}
    doi = w.get("doi")
    if doi:
        doi = doi.lower().replace("https://doi.org/", "")
    return Paper(
        source_db="openalex",
        source_id=w.get("id", "").rsplit("/", 1)[-1],
        doi=doi,
        title=w.get("title") or "",
        abstract=_reconstruct_abstract(w.get("abstract_inverted_index")),
        year=w.get("publication_year"),
        venue=venue.get("display_name", "") or "",
        venue_type=venue.get("type", "") or "",
        publication_type=w.get("type", "") or "",
        issn=venue.get("issn") or [],
        authors=authors,
        author_countries=sorted(set(countries)),
        institutions=sorted(set(institutions)),
        open_access=bool((w.get("open_access") or {}).get("is_oa")),
        oa_pdf_url=best_oa.get("pdf_url"),
        citation_count=w.get("cited_by_count"),
        concepts=[t["display_name"] for t in w.get("topics", [])[:8]],
        raw=w,
    )


def search(
    *,
    search: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    country_codes: Optional[list[str]] = None,
    extra: Optional[dict[str, str]] = None,
    max_results: int = 500,
    per_page: int = 200,
    sleep: float = 0.0,
) -> Iterator[Paper]:
    """Yield Paper records for a query, cursor-paginated.

    `max_results` caps the pull; raise it for full enumeration runs. `extra`
    passes additional filter clauses (e.g. {'primary_topic.id': 'T10168'}) so
    callers can enumerate a Topic rather than a free-text search.
    """
    params = _build_filter(
        search=search,
        year_from=year_from,
        year_to=year_to,
        country_codes=country_codes,
        extra=extra,
    )
    params["per_page"] = str(min(per_page, 200))
    cursor = "*"
    fetched = 0
    while cursor and fetched < max_results:
        params["cursor"] = cursor
        r = requests.get(BASE_URL, params=params, headers=_headers(),
                         timeout=SETTINGS.request_timeout)
        r.raise_for_status()
        data = r.json()
        for w in data.get("results", []):
            yield _map_work(w)
            fetched += 1
            if fetched >= max_results:
                break
        cursor = data.get("meta", {}).get("next_cursor")
        if sleep:
            time.sleep(sleep)


def north_america_filter() -> list[str]:
    return NORTH_AMERICA_COUNTRY_CODES
