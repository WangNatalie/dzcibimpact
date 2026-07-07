"""Elsevier ScienceDirect client — the text layer for CPA content analysis.

OpenAlex is the counting/identity backbone, but CPA is an Elsevier journal and
Elsevier restricts abstract-text redistribution to third-party indexes
(OpenAlex and Semantic Scholar both come back nearly empty for CPA). The
publisher's own Article Retrieval API, however, returns clean abstracts and —
where the key is entitled — full body text. That is what makes the EAR-style
content analysis (accounting sub-area, issue specificity, actor, orientation)
possible.

Requires ELSEVIER_API_KEY in the environment (see .env). Records are keyed by
DOI and cached to disk so the ~2k-article journal sweep is a one-time cost.

Docs: https://dev.elsevier.com/documentation/ArticleRetrievalAPI.wadl
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Optional

import requests

from ..config import SETTINGS

ARTICLE_URL = "https://api.elsevier.com/content/article/doi/{doi}"

# view=FULL returns body text + abstract but 400s on very old / non-entitled
# articles; META_ABS always returns at least the abstract + metadata.
_VIEWS = ("FULL", "META_ABS")

_CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                          ".cache", "elsevier")


@dataclass
class Article:
    doi: str
    status: int                 # HTTP status of the successful (or last) call
    view: str = ""              # which view produced the payload
    title: str = ""
    abstract: str = ""
    fulltext: str = ""          # body text ("" when only META_ABS was available)
    open_access: Optional[bool] = None
    cover_date: str = ""

    @property
    def has_fulltext(self) -> bool:
        return len(self.fulltext.strip()) > 500

    @property
    def best_text(self) -> str:
        """Full text when we have it, else the abstract."""
        return self.fulltext if self.has_fulltext else self.abstract


def _headers() -> dict[str, str]:
    key = os.getenv("ELSEVIER_API_KEY", "")
    if not key:
        raise RuntimeError(
            "ELSEVIER_API_KEY not set. Add it to litreview/.env "
            "(a ScienceDirect / Article Retrieval API key)."
        )
    return {"X-ELS-APIKey": key, "Accept": "application/json"}


def _cache_path(doi: str, want_fulltext: bool) -> str:
    # Cache is view-aware: a body-text ("full") fetch and an abstract-only
    # ("abs") fetch of the same DOI are stored separately, so a cheap screening
    # pass never masks a later full-text request for the same article.
    safe = doi.lower().replace("/", "_").replace(":", "_")
    suffix = "full" if want_fulltext else "abs"
    return os.path.join(_CACHE_DIR, f"{safe}.{suffix}.json")


def _parse(doi: str, view: str, payload: dict) -> Article:
    resp = payload.get("full-text-retrieval-response", {}) or {}
    core = resp.get("coredata", {}) or {}
    ot = resp.get("originalText")
    fulltext = ot if isinstance(ot, str) else ""
    oa = core.get("openaccess")
    return Article(
        doi=doi,
        status=200,
        view=view,
        title=(core.get("dc:title") or "").strip(),
        abstract=(core.get("dc:description") or "").strip(),
        fulltext=fulltext,
        open_access=(str(oa) in ("1", "true", "True")) if oa is not None else None,
        cover_date=core.get("prism:coverDate", "") or "",
    )


def fetch_article(
    doi: str,
    *,
    want_fulltext: bool = True,
    use_cache: bool = True,
    max_retries: int = 4,
    sleep: float = 0.35,
) -> Article:
    """Fetch one CPA article by DOI, trying FULL then falling back to META_ABS.

    On a persistent miss (404 / not in ScienceDirect) returns an Article with
    the failing status and empty text, so callers can record coverage rather
    than crash on the odd missing DOI.
    """
    doi = str(doi or "").lower().replace("https://doi.org/", "")
    if not doi:
        return Article(doi="", status=0)
    cache = _cache_path(doi, want_fulltext)
    if use_cache and os.path.exists(cache):
        with open(cache, encoding="utf-8") as fh:
            data = json.load(fh)
        return Article(**data)

    views = _VIEWS if want_fulltext else _VIEWS[1:]
    last_status = 0
    result: Optional[Article] = None
    for view in views:
        for attempt in range(max_retries):
            try:
                r = requests.get(
                    ARTICLE_URL.format(doi=doi),
                    headers=_headers(),
                    params={"view": view},
                    timeout=SETTINGS.request_timeout,
                )
            except requests.RequestException:
                time.sleep(sleep * (attempt + 2))
                continue
            last_status = r.status_code
            if r.status_code == 429:               # rate limited: back off
                time.sleep(2 + attempt * 2)
                continue
            if r.status_code == 200:
                result = _parse(doi, view, r.json())
                break
            break  # 400/404/403 for this view -> try the next (cheaper) view
        if result is not None:
            break
        time.sleep(sleep)

    if result is None:
        result = Article(doi=doi, status=last_status)

    if use_cache:
        os.makedirs(_CACHE_DIR, exist_ok=True)
        with open(cache, "w", encoding="utf-8") as fh:
            json.dump(result.__dict__, fh)
    time.sleep(sleep)
    return result
