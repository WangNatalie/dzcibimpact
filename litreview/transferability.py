"""Score a paper's transferability to the Carolinian Zone / southern Ontario.

Implements the project's criteria document as a weighted score so tier-3
candidate selection is explicit and auditable. The geographically/structurally
computable criteria are scored by rule here; the criteria that require reading
the paper (data applicability, reproducibility, key quantitative findings) are
filled during LLM extraction (extract.py) and can be folded back in.

Criteria & weights:
  location proximity        0.30   author country + region mentions in text
  ecosystem similarity      0.25   overlap with Carolinian-relevant ecosystems
  ecosystem-service match   0.15   at least one target service (screening)
  peer-reviewed             0.15   peer_review.classify result
  method reproducibility    0.15   GIS-/data-replicable method keywords present
"""

from __future__ import annotations

import re

from .models import Paper

# Ecosystems that actually occur in the Carolinian Zone (terrestrial + inland).
# Marine/coastal/mangrove/seagrass/coral/boreal are NOT relevant here.
CAROLINIAN_ECOSYSTEMS = {
    "wetlands", "forest", "grasslands", "savanna", "freshwater",
    "agricultural land", "urban green space",
}

# Region mentions, tiered by proximity to the Carolinian Zone.
_REGION_TIERS: list[tuple[float, list[str]]] = [
    (1.00, ["carolinian", "southern ontario", "southwestern ontario",
            "south-western ontario", "golden horseshoe", "niagara",
            "lake erie", "lake ontario", "long point", "norfolk county"]),
    (0.80, ["ontario", "great lakes", "quebec", "st. lawrence", "saint lawrence",
            "michigan", "ohio", "new york", "pennsylvania", "wisconsin",
            "minnesota", "indiana", "illinois", "vermont", "new england",
            "northeastern united states", "northeastern u.s", "midwest",
            "appalachian", "temperate deciduous"]),
    (0.55, ["canada", "canadian", "united states", "u.s.", "usa", "north america"]),
]

# Methods that imply a GIS-/public-data-replicable workflow (reproducibility).
_REPRODUCIBLE_METHODS = {
    "GIS mapping", "InVEST", "LULC", "remote sensing", "machine learning",
    "benefits transfer", "sensitivity analysis", "market price",
    "replacement cost", "avoided cost", "ecosystem service value",
}


def _region_score(paper: Paper) -> tuple[float, str]:
    text = f"{paper.title} {paper.abstract} {paper.location_text}".lower()
    for score, terms in _REGION_TIERS:
        for t in terms:
            if re.search(rf"(?<![a-z]){re.escape(t)}(?![a-z])", text):
                return score, f"mentions '{t}'"
    # fall back to author institution country
    if "CA" in paper.author_countries:
        return 0.55, "Canadian author institution"
    if "US" in paper.author_countries:
        return 0.50, "US author institution"
    return 0.15, "no regional signal"


def _ecosystem_score(paper: Paper) -> tuple[float, str]:
    if not paper.ecosystems:
        return 0.20, "no ecosystem tag"
    overlap = [e for e in paper.ecosystems if e in CAROLINIAN_ECOSYSTEMS]
    if not overlap:
        return 0.25, f"non-Carolinian ecosystems ({', '.join(paper.ecosystems)})"
    frac = len(overlap) / len(paper.ecosystems)
    return 0.6 + 0.4 * frac, f"Carolinian-relevant: {', '.join(overlap)}"


def _method_score(paper: Paper) -> tuple[float, str]:
    repro = [m for m in paper.methods if m in _REPRODUCIBLE_METHODS]
    if repro:
        return 1.0, f"replicable method(s): {', '.join(repro)}"
    if paper.methods:
        return 0.5, f"method(s) present: {', '.join(paper.methods)}"
    return 0.2, "no method keyword detected"


WEIGHTS = {
    "location": 0.30,
    "ecosystem": 0.25,
    "service": 0.15,
    "peer_reviewed": 0.15,
    "method": 0.15,
}


def score(paper: Paper) -> dict:
    """Return per-criterion subscores, weighted total, and a rationale string."""
    loc, loc_why = _region_score(paper)
    eco, eco_why = _ecosystem_score(paper)
    svc = 1.0 if paper.ecosystem_services else 0.0
    pr = 1.0 if paper.is_peer_reviewed else (0.5 if paper.is_peer_reviewed is None else 0.2)
    meth, meth_why = _method_score(paper)

    subs = {"location": loc, "ecosystem": eco, "service": svc,
            "peer_reviewed": pr, "method": meth}
    total = round(sum(WEIGHTS[k] * v for k, v in subs.items()), 3)

    rationale = (
        f"location {loc:.2f} ({loc_why}); ecosystem {eco:.2f} ({eco_why}); "
        f"service {svc:.0f} ({', '.join(paper.ecosystem_services) or 'none'}); "
        f"peer-reviewed {pr:.1f}; method {meth:.2f} ({meth_why})"
    )
    return {
        "transferability_score": total,
        "subscores": subs,
        "rationale": rationale,
    }


def rank(papers: list[Paper]) -> list[tuple[Paper, dict]]:
    """Score and sort papers best-first."""
    scored = [(p, score(p)) for p in papers]
    scored.sort(key=lambda x: x[1]["transferability_score"], reverse=True)
    return scored
