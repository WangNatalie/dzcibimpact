"""CPA environmental-accounting review — a Bebbington/Larrinaga-style reflection.

Replicates Bebbington, Laine, Larrinaga & Michelon (2023), "Environmental
Accounting in the European Accounting Review: A Reflection" (EAR 32(5)), but
for *Critical Perspectives on Accounting* (CPA).

Pipeline (each step writes a CSV to outputs/cpa/ and is resumable):

    enumerate  OpenAlex -> every CPA research article (id, doi, year, cites,
               issue) = the master list + the per-year denominator
    screen     keyword prefilter (OpenAlex title/abstract search) -> LLM screen
               of the hits for genuine env-accounting relevance, + a random
               sample of the misses to estimate recall
    code       Elsevier full text -> LLM codes each corpus paper on the four EAR
               axes (sub-area, issue specificity, actor, orientation)
    cite       citation analysis: where env papers place among all CPA outputs
    counts     per-year frequency + proportion of the confirmed corpus
    deliver    Table-1 corpus CSV, plots, findings.md, reproducibility appendix

Run `python -m litreview.cpa_review <step>` (or `all`). OpenAlex + Elsevier +
Azure OpenAI; see .env. Backbone = OpenAlex; text layer = Elsevier
ScienceDirect (CPA is Elsevier, so third-party indexes have no abstracts).
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import random
from typing import Iterable, Optional

import pandas as pd

from . import llm_azure
from .config import (
    CPA_ACTORS, CPA_ENV_KEYWORDS, CPA_ISSUE_SPECIFICITY, CPA_ORIENTATION,
    CPA_SOURCE_ID, CPA_SUBAREAS, SETTINGS,
)
from .sources import elsevier, openalex

OUT_DIR = os.path.join(SETTINGS.output_dir, "cpa")
ARTICLES_CSV = os.path.join(OUT_DIR, "cpa_articles.csv")
SCREEN_CSV = os.path.join(OUT_DIR, "cpa_screen.csv")
RECALL_CSV = os.path.join(OUT_DIR, "cpa_recall_sample.csv")
CODED_CSV = os.path.join(OUT_DIR, "cpa_corpus_coded.csv")
COUNTS_CSV = os.path.join(OUT_DIR, "cpa_yearly_counts.csv")
BINS_CSV = os.path.join(OUT_DIR, "cpa_5yr_bins.csv")
CITES_CSV = os.path.join(OUT_DIR, "cpa_citation_analysis.csv")
TABLE1_CSV = os.path.join(OUT_DIR, "cpa_table1_corpus.csv")
MANIFEST_JSON = os.path.join(OUT_DIR, "run_manifest.json")

RECALL_SAMPLE_N = 120       # non-candidates to LLM-screen for recall estimation
FULLTEXT_CHARS = 24000      # cap body text fed to the coder (keeps tokens sane)


def _ensure_dir() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def _env_search_string() -> str:
    """Quoted-phrase OR string for OpenAlex title_and_abstract.search."""
    return " OR ".join(f'"{k}"' for k in CPA_ENV_KEYWORDS)


def _author_short(authors: list[str]) -> str:
    """'Smith', 'Smith & Jones', or 'Smith et al.' from surnames."""
    surs = [a.split()[-1] for a in authors if a.strip()]
    if not surs:
        return ""
    if len(surs) == 1:
        return surs[0]
    if len(surs) == 2:
        return f"{surs[0]} & {surs[1]}"
    return f"{surs[0]} et al."


# --------------------------------------------------------------------------
# Step 1: enumerate the journal (master list + denominator + citations)
# --------------------------------------------------------------------------
def enumerate_journal(refresh: bool = False) -> pd.DataFrame:
    """Every CPA research article from OpenAlex, one row each."""
    _ensure_dir()
    if os.path.exists(ARTICLES_CSV) and not refresh:
        df = pd.read_csv(ARTICLES_CSV)
        # empty DOIs / author strings come back as NaN from CSV; normalize.
        for col in ("doi", "authors", "author_short", "title"):
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)
        return df

    rows = []
    for p in openalex.search(
        extra={"primary_location.source.id": CPA_SOURCE_ID, "type": "article"},
        max_results=10000, per_page=200, sleep=0.1,
    ):
        biblio = (p.raw or {}).get("biblio", {}) or {}
        rows.append({
            "openalex_id": p.source_id,
            "doi": p.doi or "",
            "year": p.year,
            "title": p.title,
            "authors": "; ".join(p.authors),
            "author_short": _author_short(p.authors),
            "cited_by_count": p.citation_count or 0,
            "volume": biblio.get("volume") or "",
            "issue": biblio.get("issue") or "",
            "first_page": biblio.get("first_page") or "",
            "has_abstract_openalex": bool(p.abstract),
        })
    df = pd.DataFrame(rows).sort_values(["year", "openalex_id"]).reset_index(drop=True)
    df.to_csv(ARTICLES_CSV, index=False)
    print(f"[enumerate] {len(df)} CPA articles -> {ARTICLES_CSV}")
    return df


def _keyword_candidate_dois() -> set[str]:
    """DOIs of CPA articles whose title/abstract match the env vocabulary.

    Server-side OpenAlex search = the cheap prefilter. Abstract coverage is
    thin for CPA (Elsevier), so this leans on titles; the recall sample in
    `screen` bounds what that misses.
    """
    dois: set[str] = set()
    no_doi = 0
    for p in openalex.search(
        search=_env_search_string(),
        extra={"primary_location.source.id": CPA_SOURCE_ID, "type": "article"},
        max_results=10000, per_page=200, sleep=0.1,
    ):
        if p.doi:
            dois.add(p.doi)
        else:
            no_doi += 1
    print(f"[screen] keyword prefilter matched {len(dois)} DOIs "
          f"({no_doi} hits had no DOI)")
    return dois


# --------------------------------------------------------------------------
# Step 2: screen — LLM confirms genuine environmental-accounting papers
# --------------------------------------------------------------------------
_SCREEN_SYS = (
    "You are screening articles from the accounting journal Critical "
    "Perspectives on Accounting to decide whether each is an ENVIRONMENTAL "
    "ACCOUNTING paper, using the broad-but-bounded definition from Bebbington, "
    "Laine, Larrinaga & Michelon (2023). Environmental accounting = accounting, "
    "reporting, disclosure, auditing/assurance, measurement or management-"
    "accounting work that substantively addresses the natural environment or "
    "an organization's environmental impacts. INCLUDE social-and-environmental "
    "accounting, sustainability/CSR reporting, carbon/climate/biodiversity/"
    "water/pollution accounting, integrated reporting and ESG WHEN the natural "
    "environment is a substantive part of the paper. EXCLUDE papers that are "
    "purely about labour, gender, tax, audit quality, financial-only topics, "
    "or that mention 'sustainability'/'social responsibility' only in passing "
    "with no environmental content. Judge the paper's substance, not keywords. "
    'Respond as JSON: {"is_env": true|false, "confidence": 0.0-1.0, '
    '"rationale": "<=25 words"}.'
)


def _screen_one(title: str, text: str) -> dict:
    user = f"TITLE: {title}\n\nABSTRACT/TEXT:\n{text[:6000]}"
    try:
        r = llm_azure.complete_json(_SCREEN_SYS, user, max_tokens=200)
        return {
            "is_env": bool(r.get("is_env")),
            "confidence": float(r.get("confidence", 0) or 0),
            "rationale": str(r.get("rationale", ""))[:300],
        }
    except Exception as e:  # keep the sweep alive on the odd bad reply
        return {"is_env": False, "confidence": 0.0, "rationale": f"ERROR: {e}"}


def _text_for(doi: str, title: str, want_fulltext: bool = False) -> str:
    """Elsevier abstract (or full text) for screening/coding; title fallback."""
    if not doi:
        return title
    art = elsevier.fetch_article(doi, want_fulltext=want_fulltext)
    txt = art.best_text if want_fulltext else art.abstract
    return txt or art.abstract or title


def screen(refresh: bool = False) -> pd.DataFrame:
    """Prefilter + LLM screen -> confirmed corpus, plus a recall sample."""
    _ensure_dir()
    if not llm_azure.is_configured():
        raise SystemExit("Azure OpenAI not configured (needed for screening).")
    articles = enumerate_journal()
    candidate_dois = _keyword_candidate_dois()
    articles["is_candidate"] = articles["doi"].isin(candidate_dois)

    # --- LLM-screen every keyword candidate (resumable) ---
    done: dict[str, dict] = {}
    if os.path.exists(SCREEN_CSV) and not refresh:
        done = {r["doi"]: r for r in pd.read_csv(SCREEN_CSV).to_dict("records")}

    cand = articles[articles["is_candidate"]]
    rows = []
    for i, a in enumerate(cand.to_dict("records"), 1):
        doi = a["doi"]
        if doi in done:
            rows.append(done[doi]); continue
        text = _text_for(doi, a["title"], want_fulltext=False)
        res = _screen_one(a["title"], text)
        rows.append({"doi": doi, "openalex_id": a["openalex_id"], "year": a["year"],
                     "title": a["title"], **res})
        if i % 20 == 0:
            print(f"[screen] {i}/{len(cand)} candidates screened")
            pd.DataFrame(rows).to_csv(SCREEN_CSV, index=False)
    screen_df = pd.DataFrame(rows)
    screen_df.to_csv(SCREEN_CSV, index=False)
    n_env = int(screen_df["is_env"].sum())
    print(f"[screen] confirmed env corpus: {n_env}/{len(screen_df)} candidates "
          f"(precision={n_env/max(1,len(screen_df)):.0%})")

    # --- recall sample: LLM-screen a random draw of NON-candidates ---
    _recall_sample(articles)
    return screen_df


def _recall_sample(articles: pd.DataFrame, refresh: bool = False) -> pd.DataFrame:
    if os.path.exists(RECALL_CSV) and not refresh:
        return pd.read_csv(RECALL_CSV)
    non = articles[(~articles["is_candidate"]) & articles["doi"].astype(str).str.len().gt(0)]
    n = min(RECALL_SAMPLE_N, len(non))
    sample = non.sample(n=n, random_state=42)
    rows = []
    for i, a in enumerate(sample.to_dict("records"), 1):
        text = _text_for(a["doi"], a["title"], want_fulltext=False)
        res = _screen_one(a["title"], text)
        rows.append({"doi": a["doi"], "year": a["year"], "title": a["title"], **res})
        if i % 20 == 0:
            print(f"[recall] {i}/{n} non-candidates screened")
    rdf = pd.DataFrame(rows)
    rdf.to_csv(RECALL_CSV, index=False)
    missed = int(rdf["is_env"].sum())
    print(f"[recall] {missed}/{n} sampled non-candidates were actually env "
          f"-> est. keyword-prefilter miss rate {missed/max(1,n):.0%}")
    return rdf


# --------------------------------------------------------------------------
# Step 3: code the corpus on the four EAR axes (LLM over full text)
# --------------------------------------------------------------------------
_CODE_SYS = (
    "You are coding an ENVIRONMENTAL ACCOUNTING paper from Critical Perspectives "
    "on Accounting along four axes from Bebbington, Laine, Larrinaga & Michelon "
    "(2023). Read the provided text and return JSON with EXACTLY these keys:\n"
    f'1. "subarea": one of {CPA_SUBAREAS} — the paper\'s accounting focus '
    "(financial reporting; non-financial reporting incl. CSR/sustainability/ESG "
    "disclosure; management accounting for internal decisions; audit/assurance; "
    "measurement/valuation of environmental performance; other = editorial, "
    "viewpoint, conceptual, book review).\n"
    f'2. "issue_specificity": one of {CPA_ISSUE_SPECIFICITY} — "specific issue" '
    "if it centres on a particular biophysical issue (e.g. climate, carbon, "
    'water, biodiversity, emissions), "umbrella construct" if it uses broad '
    "framings (CSR, sustainability, social/environmental reporting, ESG).\n"
    '3. "issue_name": the specific environmental issue if applicable, else "".\n'
    f'4. "actor": one of {CPA_ACTORS} — whose behaviour/role the paper centres '
    'on; "none/multiple" if unspecified or several.\n'
    f'5. "orientation": one of {CPA_ORIENTATION} — the paper\'s (often implicit) '
    "view of who environmental accounting is FOR: society/accountability vs. "
    "capital-market participants/investors, or both/ambiguous.\n"
    '6. "rationale": <=30 words justifying subarea + orientation.\n'
    "Choose exactly one value per categorical axis."
)


def _code_one(title: str, text: str) -> dict:
    user = f"TITLE: {title}\n\nTEXT:\n{text[:FULLTEXT_CHARS]}"
    try:
        r = llm_azure.complete_json(_CODE_SYS, user, max_tokens=350)
        return {
            "subarea": _norm(r.get("subarea"), CPA_SUBAREAS, "other"),
            "issue_specificity": _norm(r.get("issue_specificity"),
                                       CPA_ISSUE_SPECIFICITY, "umbrella construct"),
            "issue_name": str(r.get("issue_name", ""))[:60],
            "actor": _norm(r.get("actor"), CPA_ACTORS, "none/multiple"),
            "orientation": _norm(r.get("orientation"), CPA_ORIENTATION,
                                 "both/ambiguous"),
            "code_rationale": str(r.get("rationale", ""))[:300],
        }
    except Exception as e:
        return {"subarea": "", "issue_specificity": "", "issue_name": "",
                "actor": "", "orientation": "", "code_rationale": f"ERROR: {e}"}


def _norm(val, allowed: list[str], default: str) -> str:
    if not val:
        return default
    v = str(val).strip().lower()
    for a in allowed:
        if v == a.lower():
            return a
    for a in allowed:                       # loose contains-match
        if a.lower() in v or v in a.lower():
            return a
    return default


def code(refresh: bool = False) -> pd.DataFrame:
    """Fetch full text + LLM-code every confirmed-env paper."""
    _ensure_dir()
    if not os.path.exists(SCREEN_CSV):
        screen()
    screen_df = pd.read_csv(SCREEN_CSV)
    corpus = screen_df[screen_df["is_env"] == True].copy()  # noqa: E712
    articles = enumerate_journal().set_index("doi")

    done: dict[str, dict] = {}
    if os.path.exists(CODED_CSV) and not refresh:
        done = {r["doi"]: r for r in pd.read_csv(CODED_CSV).to_dict("records")}

    rows = []
    for i, a in enumerate(corpus.to_dict("records"), 1):
        doi = a["doi"]
        if doi in done:
            rows.append(done[doi]); continue
        text = _text_for(doi, a["title"], want_fulltext=True)
        art = elsevier.fetch_article(doi, want_fulltext=True) if doi else None
        codes = _code_one(a["title"], text)
        base = articles.loc[doi] if doi in articles.index else {}
        rows.append({
            "doi": doi, "openalex_id": a["openalex_id"], "year": a["year"],
            "title": a["title"],
            "cited_by_count": int(base.get("cited_by_count", 0)) if len(base) else 0,
            "volume": base.get("volume", "") if len(base) else "",
            "issue": base.get("issue", "") if len(base) else "",
            "text_source": ("fulltext" if (art and art.has_fulltext) else "abstract"),
            **codes,
        })
        if i % 10 == 0:
            print(f"[code] {i}/{len(corpus)} coded")
            pd.DataFrame(rows).to_csv(CODED_CSV, index=False)
    coded = pd.DataFrame(rows).sort_values(["year", "title"]).reset_index(drop=True)
    coded.to_csv(CODED_CSV, index=False)
    print(f"[code] coded {len(coded)} corpus papers -> {CODED_CSV}")
    return coded


# --------------------------------------------------------------------------
# Step 4: counts — per-year frequency + proportion of the confirmed corpus
# --------------------------------------------------------------------------
def counts() -> pd.DataFrame:
    _ensure_dir()
    articles = enumerate_journal()
    coded = pd.read_csv(CODED_CSV) if os.path.exists(CODED_CSV) else pd.DataFrame(
        columns=["year"])
    totals = articles.groupby("year").size().rename("n_total")
    env = coded.groupby("year").size().rename("n_env") if len(coded) else pd.Series(
        dtype=int, name="n_env")
    df = pd.concat([totals, env], axis=1).fillna(0).astype(int).reset_index()
    df["proportion"] = df["n_env"] / df["n_total"].where(df["n_total"] > 0)
    df.to_csv(COUNTS_CSV, index=False)

    # 5-year bins (a la Hopper & Bui), labelled by their start year
    b = df.copy()
    b["bin"] = (b["year"] // 5) * 5
    binned = b.groupby("bin").agg(n_env=("n_env", "sum"),
                                  n_total=("n_total", "sum")).reset_index()
    binned["proportion"] = binned["n_env"] / binned["n_total"].where(
        binned["n_total"] > 0)
    binned["bin_label"] = binned["bin"].astype(str) + "–" + (binned["bin"] + 4).astype(str)
    binned.to_csv(BINS_CSV, index=False)
    print(f"[counts] {int(df['n_env'].sum())} env of {int(df['n_total'].sum())} "
          f"articles -> {COUNTS_CSV}, {BINS_CSV}")
    return df


# --------------------------------------------------------------------------
# Step 5: citation analysis — where env papers place among all CPA outputs
# --------------------------------------------------------------------------
def cite() -> dict:
    _ensure_dir()
    articles = enumerate_journal().copy()
    coded = pd.read_csv(CODED_CSV) if os.path.exists(CODED_CSV) else pd.DataFrame()
    env_dois = set(coded["doi"]) if len(coded) else set()
    articles["is_env"] = articles["doi"].isin(env_dois)
    articles = articles.sort_values("cited_by_count", ascending=False).reset_index(drop=True)
    articles["cite_rank"] = articles.index + 1

    n_env = int(articles["is_env"].sum())
    n_total = len(articles)
    top50 = articles.head(50)
    top100 = articles.head(100)
    stats = {
        "n_env": n_env,
        "n_total": n_total,
        "env_share_overall": round(n_env / max(1, n_total), 4),
        "env_in_top50": int(top50["is_env"].sum()),
        "env_in_top100": int(top100["is_env"].sum()),
        "env_share_top50": round(int(top50["is_env"].sum()) / 50, 4),
        "median_cites_env": float(articles[articles["is_env"]]["cited_by_count"].median()
                                  ) if n_env else 0.0,
        "median_cites_nonenv": float(
            articles[~articles["is_env"]]["cited_by_count"].median()),
    }
    # within-issue: for each issue containing >=1 env paper, how does its
    # best-ranked env paper place? (a la the EAR "first or second most cited").
    within = []
    for (vol, iss), grp in articles.groupby(["volume", "issue"]):
        if not grp["is_env"].any() or str(iss) in ("", "nan") or len(grp) < 2:
            continue
        grp2 = grp.sort_values("cited_by_count", ascending=False).reset_index(drop=True)
        best = grp2.index[grp2["is_env"]].min() + 1   # rank of top env paper
        within.append({"volume": vol, "issue": iss, "issue_size": len(grp2),
                       "best_env_rank": int(best)})
    wdf = pd.DataFrame(within)
    stats["issues_with_env"] = int(len(wdf))
    stats["env_top1_in_issue"] = int((wdf["best_env_rank"] == 1).sum()) if len(wdf) else 0
    stats["env_top2_in_issue"] = int((wdf["best_env_rank"] <= 2).sum()) if len(wdf) else 0

    articles.to_csv(CITES_CSV, index=False)
    with open(os.path.join(OUT_DIR, "cpa_citation_stats.json"), "w") as fh:
        json.dump(stats, fh, indent=2)
    print(f"[cite] env papers: {stats['env_in_top50']} of top-50 most-cited; "
          f"top-cited in issue for {stats['env_top1_in_issue']}/{stats['issues_with_env']} issues")
    return stats


# --------------------------------------------------------------------------
# Step 6: deliverables — Table 1, plots, findings, reproducibility manifest
# --------------------------------------------------------------------------
def deliver() -> None:
    _ensure_dir()
    from . import cpa_report  # local import; plotting + prose live there
    cpa_report.build_all()


def _write_manifest() -> None:
    manifest = {
        "generated": _dt.datetime.now().isoformat(timespec="seconds"),
        "journal": "Critical Perspectives on Accounting",
        "openalex_source_id": CPA_SOURCE_ID,
        "openalex_filters": f"primary_location.source.id:{CPA_SOURCE_ID},type:article",
        "env_search_string": _env_search_string(),
        "n_keywords": len(CPA_ENV_KEYWORDS),
        "llm_model": SETTINGS.azure_openai_deployment,
        "text_source": "Elsevier ScienceDirect Article Retrieval API "
                       "(view=FULL, fallback META_ABS)",
        "recall_sample_n": RECALL_SAMPLE_N,
        "coding_axes": {
            "subarea": CPA_SUBAREAS,
            "issue_specificity": CPA_ISSUE_SPECIFICITY,
            "actor": CPA_ACTORS,
            "orientation": CPA_ORIENTATION,
        },
    }
    with open(MANIFEST_JSON, "w") as fh:
        json.dump(manifest, fh, indent=2)


STEPS = {
    "enumerate": lambda: enumerate_journal(refresh=True),
    "screen": screen,
    "code": code,
    "counts": counts,
    "cite": cite,
    "deliver": deliver,
}


def main(argv: Optional[list[str]] = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("step", choices=list(STEPS) + ["all"], nargs="?", default="all")
    args = ap.parse_args(argv)
    _ensure_dir()
    _write_manifest()
    if args.step == "all":
        enumerate_journal(refresh=True)
        screen(); code(); counts(); cite(); deliver()
    else:
        STEPS[args.step]()


if __name__ == "__main__":
    main()
