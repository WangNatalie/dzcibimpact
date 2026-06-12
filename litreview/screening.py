"""Keyword screening: tag each paper against the three controlled vocabularies.

This is the cheap, deterministic backbone of tiers 1 and 2 — no LLM needed.
A paper is tagged with every canonical term whose word-boundary pattern (or any
of its aliases) appears in the title+abstract. Counts and cross-tabs in
aggregate.py are built directly from these tags.
"""

from __future__ import annotations

import re
from functools import lru_cache

from .config import (
    ECOSYSTEM_SERVICE_KEYWORDS,
    METHOD_KEYWORDS,
    ECOSYSTEM_KEYWORDS,
    ALIASES,
)
from .models import Paper


@lru_cache(maxsize=None)
def _pattern_for(term: str) -> re.Pattern:
    """Compile a case-insensitive, word-boundary pattern for a term + aliases."""
    variants = [term] + ALIASES.get(term, [])
    # Escape, then allow flexible whitespace/hyphen between tokens.
    parts = []
    for v in variants:
        toks = re.split(r"[\s\-]+", v.strip())
        esc = r"[\s\-]+".join(re.escape(t) for t in toks)
        parts.append(esc)
    body = "|".join(parts)
    return re.compile(rf"(?<![A-Za-z]){body}(?![A-Za-z])", re.IGNORECASE)


def _match_terms(text: str, vocab: list[str]) -> list[str]:
    if not text:
        return []
    return [term for term in vocab if _pattern_for(term).search(text)]


def screen(paper: Paper) -> Paper:
    """Populate ecosystem_services / methods / ecosystems tags in place."""
    text = f"{paper.title}. {paper.abstract}"
    paper.ecosystem_services = _match_terms(text, ECOSYSTEM_SERVICE_KEYWORDS)
    paper.methods = _match_terms(text, METHOD_KEYWORDS)
    paper.ecosystems = _match_terms(text, ECOSYSTEM_KEYWORDS)
    return paper


def is_relevant(paper: Paper, require_ecosystem: bool = False) -> bool:
    """Minimum bar: at least one ecosystem-service tag (the project's anchor).

    Set require_ecosystem=True to also demand an ecosystem-type tag (tighter
    precision for tier 2/3).
    """
    if not paper.ecosystem_services:
        return False
    if require_ecosystem and not paper.ecosystems:
        return False
    return True
