"""EBSCO client — EDS (EBSCO Discovery Service) REST API.

Adds EBSCO as a counting + enumeration source for the **aggregate** and
**tier3** steps only (never keyword discovery). It mirrors the small surface the
pipeline needs from a source:

  * ``total_count(search=...)`` — hit count for a query (the aggregate columns),
  * ``search(query=...)``       — record enumeration mapped to ``Paper`` (tier3).

Unlike OpenAlex, EDS has **no author-institution country facet**, so region
filtering (North America / Canada) is not available here — ``total_count``
accepts ``country_codes`` for interface parity but ignores it. Callers that need
per-region counts should keep using OpenAlex for those cells.

Access: EDS is not the public EBSCOhost website — it needs an **EDS API profile**
(UserId / Password / Profile) provisioned by your institution's EBSCO admin or
librarian. Institutional web access to EBSCOhost does *not* by itself grant API
access. When credentials are absent the source reports ``is_configured() ==
False`` and every caller skips it gracefully (same pattern as Semantic Scholar /
Azure).

Auth flow (per EDS docs):
  1. POST /authservice/rest/uidauth {UserId, Password}      -> AuthToken (~30 min)
  2. POST /edsapi/rest/CreateSession?profile=..&guest=n     -> SessionToken
  3. GET  /edsapi/rest/Search  (headers: x-authenticationToken, x-sessionToken)

Docs: https://connect.ebsco.com/s/article/EBSCO-Discovery-Service-API-Reference
"""

from __future__ import annotations

import re
import threading
import time
from typing import Iterator, Optional

import requests

from ..config import SETTINGS
from ..models import Paper

AUTH_URL = "https://eds-api.ebscohost.com/authservice/rest/uidauth"
CREATE_SESSION_URL = "https://eds-api.ebscohost.com/edsapi/rest/CreateSession"
SEARCH_URL = "https://eds-api.ebscohost.com/edsapi/rest/Search"

# Header shape reused for every JSON request.
_JSON_HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}

# EDS PubTypeId -> our normalized publication_type (best-effort; unknown falls
# through as the raw lowercased id).
_PUB_TYPE_MAP = {
    "academicjournal": "article",
    "journal": "article",
    "periodical": "article",
    "magazine": "article",
    "book": "book",
    "ebook": "book",
    "conference": "conference",
    "dissertation": "dissertation",
    "report": "report",
}

_TAG_RE = re.compile(r"<[^>]+>")


def is_configured() -> bool:
    """True when EDS API credentials are present in the environment."""
    return bool(SETTINGS.ebsco_user_id and SETTINGS.ebsco_password
                and SETTINGS.ebsco_profile)


# --------------------------------------------------------------------------
# Auth / session token management (cached, thread-safe, auto-refreshed)
# --------------------------------------------------------------------------
class _Session:
    """Holds and lazily refreshes the EDS auth + session tokens.

    Tokens are cached on the instance and refreshed on demand or when the API
    signals expiry (see ``_is_expired``). One shared instance is enough; a lock
    guards concurrent refreshes.
    """

    def __init__(self) -> None:
        self._auth_token: Optional[str] = None
        self._session_token: Optional[str] = None
        self._lock = threading.Lock()

    def _authenticate(self) -> None:
        if not is_configured():
            raise EbscoNotConfigured(
                "EBSCO EDS API not configured. Set EBSCO_USER_ID, "
                "EBSCO_PASSWORD, and EBSCO_PROFILE (see litreview/.env.example)."
            )
        r = requests.post(
            AUTH_URL,
            json={"UserId": SETTINGS.ebsco_user_id,
                  "Password": SETTINGS.ebsco_password},
            headers=_JSON_HEADERS,
            timeout=SETTINGS.request_timeout,
        )
        r.raise_for_status()
        self._auth_token = r.json().get("AuthToken")
        if not self._auth_token:
            raise RuntimeError(f"EDS auth returned no AuthToken: {r.text[:200]!r}")

    def _create_session(self) -> None:
        if not self._auth_token:
            self._authenticate()
        r = requests.post(
            CREATE_SESSION_URL,
            params={"profile": SETTINGS.ebsco_profile, "guest": "n"},
            headers={**_JSON_HEADERS, "x-authenticationToken": self._auth_token},
            timeout=SETTINGS.request_timeout,
        )
        r.raise_for_status()
        self._session_token = r.json().get("SessionToken")
        if not self._session_token:
            raise RuntimeError(
                f"EDS CreateSession returned no SessionToken: {r.text[:200]!r}")

    def ensure(self) -> tuple[str, str]:
        """Return (auth_token, session_token), authenticating if needed."""
        with self._lock:
            if not self._auth_token:
                self._authenticate()
            if not self._session_token:
                self._create_session()
            return self._auth_token, self._session_token  # type: ignore[return-value]

    def refresh(self) -> tuple[str, str]:
        """Force a full re-auth (used after an expiry error)."""
        with self._lock:
            self._auth_token = None
            self._session_token = None
            self._authenticate()
            self._create_session()
            return self._auth_token, self._session_token  # type: ignore[return-value]


class EbscoNotConfigured(RuntimeError):
    pass


_SESSION = _Session()

# EDS error codes that mean "token expired / invalid" -> re-auth and retry once.
_EXPIRY_ERROR_CODES = {"104", "106", "108", "109"}


def _is_expired(payload: dict) -> bool:
    code = str(payload.get("ErrorNumber") or payload.get("ErrorCode") or "")
    return code in _EXPIRY_ERROR_CODES


def to_eds_query(query: str) -> str:
    """Translate an OpenAlex-style search string to an EDS query term.

    The pipeline passes one query to every source. OpenAlex strings join
    clauses with ` AND ` and quote phrases (e.g. ``"ecosystem services" AND
    "carolinian"``) — EDS accepts the same boolean text verbatim as the term of
    a guided ``query`` parameter, so the only thing to strip is any leading
    boolean the caller may have embedded. The ``AND,`` prefix (field-code slot)
    is added by the caller when building request params.
    """
    return query.strip()


def _search_params(
    *,
    query: str,
    year_from: Optional[int],
    year_to: Optional[int],
    per_page: int,
    page: int,
    view: str,
) -> dict[str, str]:
    params: dict[str, str] = {
        # `query-1=AND,<text>`: first (and only) query clause, ANDed.
        "query-1": f"AND,{to_eds_query(query)}",
        "resultsperpage": str(min(per_page, 100)),
        "pagenumber": str(page),
        "view": view,
        "sort": "relevance",
    }
    # Publication-date limiter DT1 takes a YYYY-MM/YYYY-MM range. Open-ended
    # bounds are filled with wide sentinels so a one-sided filter still works.
    if year_from is not None or year_to is not None:
        lo = f"{year_from or 1500}-01"
        hi = f"{year_to or 2100}-12"
        params["limiter"] = f"DT1:{lo}/{hi}"
    return params


def _get_search(params: dict[str, str]) -> dict:
    """GET /Search with one automatic re-auth+retry on token expiry."""
    auth_token, session_token = _SESSION.ensure()
    for attempt in range(2):
        r = requests.get(
            SEARCH_URL,
            params=params,
            headers={"Accept": "application/json",
                     "x-authenticationToken": auth_token,
                     "x-sessionToken": session_token},
            timeout=SETTINGS.request_timeout,
        )
        if r.status_code == 429:
            time.sleep(5)
            continue
        # EDS reports auth/session expiry as 200-with-error or 401; refresh once.
        try:
            data = r.json()
        except ValueError:
            r.raise_for_status()
            raise
        if (r.status_code == 401 or _is_expired(data)) and attempt == 0:
            auth_token, session_token = _SESSION.refresh()
            continue
        r.raise_for_status()
        return data
    return {}


def total_count(
    *,
    search: str,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    country_codes: Optional[list[str]] = None,  # accepted for parity; unused
) -> int:
    """Total EDS hits for a query (the aggregate count cell).

    ``country_codes`` is accepted so callers can pass the same kwargs they pass
    to OpenAlex, but EDS has no author-country facet, so it is ignored. Returns
    0 when EBSCO is not configured (callers gate on ``is_configured()`` first).
    """
    if not is_configured():
        return 0
    params = _search_params(
        query=search, year_from=year_from, year_to=year_to,
        per_page=1, page=1, view="brief",
    )
    data = _get_search(params)
    stats = (data.get("SearchResult") or {}).get("Statistics") or {}
    return int(stats.get("TotalHits") or 0)


def _dig(d: dict, *path):
    """Safe nested lookup; returns None on any missing/invalid step."""
    cur = d
    for key in path:
        if isinstance(cur, list):
            cur = cur[key] if isinstance(key, int) and 0 <= key < len(cur) else None
        elif isinstance(cur, dict):
            cur = cur.get(key)
        else:
            return None
        if cur is None:
            return None
    return cur


def _strip_html(text: str) -> str:
    return _TAG_RE.sub("", text or "").strip()


def _first_year(dates: Optional[list]) -> Optional[int]:
    for d in dates or []:
        y = d.get("Y")
        if y and str(y).isdigit():
            return int(y)
    return None


def _map_record(rec: dict) -> Paper:
    """Map one EDS ``SearchResult.Data.Records[i]`` into a ``Paper``.

    EDS records are deeply nested and vary by database/profile, so every access
    is defensive. The paths below follow the documented BibRecord schema
    (BibEntity for the item, BibRelationships for venue + authors).
    """
    header = rec.get("Header") or {}
    bib = _dig(rec, "RecordInfo", "BibRecord") or {}
    bib_entity = bib.get("BibEntity") or {}
    rels = bib.get("BibRelationships") or {}

    # Title (prefer the "main" title, else the first available).
    titles = bib_entity.get("Titles") or []
    title = ""
    for t in titles:
        if (t.get("Type") or "").lower() == "main":
            title = t.get("TitleFull") or ""
            break
    if not title and titles:
        title = titles[0].get("TitleFull") or ""

    # Identifiers on the item (DOI).
    doi = None
    for ident in bib_entity.get("Identifiers") or []:
        if (ident.get("Type") or "").lower().startswith("doi"):
            doi = (ident.get("Value") or "").lower() or None
            break

    # Venue + ISSNs live on the IsPartOf relationship (the containing journal).
    part_of = (rels.get("IsPartOfRelationships") or [{}])[0] or {}
    part_entity = part_of.get("BibEntity") or {}
    venue = _dig(part_entity, "Titles", 0, "TitleFull") or ""
    issns: list[str] = []
    for ident in part_entity.get("Identifiers") or []:
        if (ident.get("Type") or "").lower().startswith("issn"):
            v = ident.get("Value")
            if v:
                issns.append(v)

    # Year: item dates first, else the containing-issue dates.
    year = _first_year(bib_entity.get("Dates")) or _first_year(
        part_entity.get("Dates"))

    # Authors from contributor relationships.
    authors: list[str] = []
    for c in rels.get("HasContributorRelationships") or []:
        name = _dig(c, "PersonEntity", "Name", "NameFull")
        if name:
            authors.append(name)

    # Abstract + a full-text/detail link come through as `Items` in detail view.
    abstract = ""
    for item in rec.get("Items") or []:
        if (item.get("Name") or "").lower() == "abstract":
            abstract = _strip_html(item.get("Data") or "")
            break

    pub_type_id = (header.get("PubTypeId") or "").lower()
    publication_type = _PUB_TYPE_MAP.get(pub_type_id, pub_type_id)

    db_id = header.get("DbId") or ""
    an = header.get("An") or ""
    return Paper(
        source_db="ebsco",
        source_id=f"{db_id}:{an}" if db_id or an else "",
        doi=doi,
        title=title,
        abstract=abstract,
        year=year,
        venue=venue,
        venue_type=(header.get("PubType") or ""),
        publication_type=publication_type,
        issn=issns,
        authors=authors,
        open_access=False,           # EDS does not expose a reliable OA flag here
        oa_pdf_url=None,
        citation_count=None,
        concepts=[s.get("SubjectFull", "") for s in
                  (bib_entity.get("Subjects") or []) if s.get("SubjectFull")][:8],
        raw=rec,
    )


def search(
    *,
    query: str,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    max_results: int = 500,
    per_page: int = 100,
    sleep: float = 0.2,
) -> Iterator[Paper]:
    """Yield ``Paper`` records for a query, page-paginated.

    Accepts the same OpenAlex-style ``query`` the other sources take (see
    ``to_eds_query``). Yields nothing when EBSCO is not configured so callers
    can iterate unconditionally.
    """
    if not is_configured():
        return
    page = 1
    fetched = 0
    while fetched < max_results:
        params = _search_params(
            query=query, year_from=year_from, year_to=year_to,
            per_page=per_page, page=page, view="detailed",
        )
        data = _get_search(params)
        result = data.get("SearchResult") or {}
        records = _dig(result, "Data", "Records") or []
        if not records:
            return
        for rec in records:
            yield _map_record(rec)
            fetched += 1
            if fetched >= max_results:
                return
        total = int(_dig(result, "Statistics", "TotalHits") or 0)
        if fetched >= total:
            return
        page += 1
        if sleep:
            time.sleep(sleep)
