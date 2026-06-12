"""Peer-review vs grey-literature classification.

Neither OpenAlex nor Semantic Scholar exposes a clean "peer reviewed" boolean,
so we infer it from venue type, publication type, and DOI/ISSN presence. The
goal matches the criteria doc: mark non-peer-reviewed sources so the grey-lit
search is auditable, not to be perfect. Edge cases get is_peer_reviewed=None.
"""

from __future__ import annotations

from .models import Paper

# Hosts / venues that signal preprints or grey literature.
PREPRINT_VENUES = {
    "arxiv", "biorxiv", "medrxiv", "ssrn", "preprints",
    "research square", "researchsquare", "osf", "zenodo",
    "techrxiv", "authorea",
}
GREY_TYPES = {
    "preprint", "report", "dataset", "dissertation", "thesis",
    "book", "book-chapter", "monograph", "posted-content",
    "working paper", "other",
}
PEER_TYPES = {"article", "journal-article", "review", "journalarticle"}


def classify(paper: Paper) -> Paper:
    """Set is_peer_reviewed / grey_literature / peer_review_reason in place."""
    venue = (paper.venue or "").lower()
    vtype = (paper.venue_type or "").lower()
    ptype = (paper.publication_type or "").lower()

    # 1. Explicit preprint/repository signals -> grey.
    if vtype in {"repository", "preprint"} or any(p in venue for p in PREPRINT_VENUES):
        paper.is_peer_reviewed = False
        paper.grey_literature = True
        paper.peer_review_reason = f"preprint/repository venue ({paper.venue or vtype})"
        return paper

    # 2. Grey publication types.
    if ptype in GREY_TYPES:
        paper.is_peer_reviewed = False
        paper.grey_literature = True
        paper.peer_review_reason = f"grey publication type ({ptype})"
        return paper

    # 3. Journal article in a journal venue with an ISSN -> peer reviewed.
    if ptype in PEER_TYPES or vtype == "journal":
        if paper.issn or vtype == "journal":
            paper.is_peer_reviewed = True
            paper.grey_literature = False
            paper.peer_review_reason = "journal article with ISSN"
            return paper

    # 4. Has a registered DOI in a journal-ish type but missing ISSN -> likely peer.
    if paper.doi and ptype in PEER_TYPES:
        paper.is_peer_reviewed = True
        paper.grey_literature = False
        paper.peer_review_reason = "journal-type DOI (ISSN unconfirmed)"
        return paper

    # 5. Unknown.
    paper.is_peer_reviewed = None
    paper.grey_literature = not bool(paper.doi)
    paper.peer_review_reason = "indeterminate; manual check advised"
    return paper
