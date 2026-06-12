"""Full-text retrieval for tier-3 deep extraction.

Tier 3 needs more than the abstract to fill the value/method/limitations
columns, so we fetch the open-access PDF when one is available (OpenAlex
`best_oa_location` or Semantic Scholar `openAccessPdf`) and extract its text.
When no OA PDF exists we fall back to the abstract — extraction still runs, just
with less to work from. Nothing here scrapes paywalled content.
"""

from __future__ import annotations

import io
from typing import Optional

import requests

from .config import SETTINGS
from .models import Paper

_MAX_PDF_BYTES = 25 * 1024 * 1024  # skip absurdly large PDFs


def _download(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, headers={"User-Agent": "litreview/0.1"},
                         timeout=SETTINGS.request_timeout, stream=True)
        r.raise_for_status()
        ctype = r.headers.get("Content-Type", "")
        if "pdf" not in ctype.lower() and not url.lower().endswith(".pdf"):
            # not obviously a PDF; bail rather than parse HTML
            if "pdf" not in ctype.lower():
                return None
        data = r.content
        if len(data) > _MAX_PDF_BYTES:
            return None
        return data
    except Exception:
        return None


def _pdf_to_text(data: bytes, max_pages: int = 40) -> str:
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(data))
        parts = []
        for page in reader.pages[:max_pages]:
            parts.append(page.extract_text() or "")
        return "\n".join(parts)
    except Exception:
        return ""


def fetch_fulltext(paper: Paper, *, max_chars: int = 60000) -> tuple[str, str]:
    """Return (text, source) where source is 'pdf' or 'abstract'.

    Tries the paper's OA PDF URL first; on any failure falls back to the
    abstract so downstream extraction always has something to work with.
    """
    if paper.oa_pdf_url:
        data = _download(paper.oa_pdf_url)
        if data:
            text = _pdf_to_text(data)
            if len(text.strip()) > 500:  # got real body text
                return text[:max_chars], "pdf"
    return paper.abstract or "", "abstract"
