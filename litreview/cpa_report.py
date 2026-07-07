"""Deliverables for the CPA environmental-accounting review.

Renders the analysis CSVs from cpa_review into the EAR-paper-style outputs:
a Table-1 corpus listing, publication-frequency and proportion charts, a
sub-area / orientation breakdown, a society x sub-area map (their Figure 1),
and a findings note that leads with the answers.
"""

from __future__ import annotations

import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from .config import CPA_ORIENTATION, CPA_SUBAREAS  # noqa: E402
from .cpa_review import (  # noqa: E402
    BINS_CSV, CODED_CSV, COUNTS_CSV, MANIFEST_JSON, OUT_DIR, RECALL_CSV,
    SCREEN_CSV, TABLE1_CSV,
)

FIG_DIR = os.path.join(OUT_DIR, "figures")
FINDINGS_MD = os.path.join(OUT_DIR, "findings.md")

INK = "#1a1a1a"
ACCENT = "#2f6f4e"      # env / society
ACCENT2 = "#b5651d"     # capital markets
GRID = "#d9d9d9"


def _style(ax):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.tick_params(colors=INK, labelsize=9)
    ax.yaxis.grid(True, color=GRID, lw=0.6)
    ax.set_axisbelow(True)


# --------------------------------------------------------------------------
def build_table1() -> pd.DataFrame:
    coded = pd.read_csv(CODED_CSV)
    articles = pd.read_csv(os.path.join(OUT_DIR, "cpa_articles.csv"))
    a = articles.set_index("doi")[["author_short"]]
    coded = coded.merge(a, on="doi", how="left")
    coded["author_short"] = coded["author_short"].fillna("")
    t1 = coded.sort_values(["year", "author_short"])[[
        "author_short", "year", "title", "subarea", "issue_specificity",
        "issue_name", "actor", "orientation", "cited_by_count",
    ]].rename(columns={
        "author_short": "Authors", "year": "Year", "title": "Title",
        "subarea": "Accounting sub-area", "issue_specificity": "Issue specificity",
        "issue_name": "Issue", "actor": "Actor", "orientation": "Orientation",
        "cited_by_count": "Citations",
    })
    t1.to_csv(TABLE1_CSV, index=False)
    return coded


# --------------------------------------------------------------------------
def fig_frequency(counts: pd.DataFrame):
    os.makedirs(FIG_DIR, exist_ok=True)
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.bar(counts["year"], counts["n_env"], color=ACCENT, width=0.8)
    _style(ax)
    ax.set_title("Environmental-accounting papers in CPA, per year", loc="left",
                 fontsize=12, color=INK, fontweight="bold")
    ax.set_ylabel("papers")
    fig.tight_layout(); fig.savefig(os.path.join(FIG_DIR, "frequency.png"), dpi=150)
    plt.close(fig)


def fig_proportion(counts: pd.DataFrame, bins: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.plot(counts["year"], counts["proportion"] * 100, color=GRID, lw=1.2,
            marker="o", ms=3, label="annual")
    # 5-year bin step overlay
    for _, r in bins.iterrows():
        ax.hlines(r["proportion"] * 100, r["bin"], r["bin"] + 4, color=ACCENT,
                  lw=3, zorder=3)
    ax.plot([], [], color=ACCENT, lw=3, label="5-year mean")
    _style(ax)
    ax.set_title("Environmental papers as a share of all CPA articles", loc="left",
                 fontsize=12, color=INK, fontweight="bold")
    ax.set_ylabel("% of CPA articles"); ax.legend(frameon=False, fontsize=9)
    fig.tight_layout(); fig.savefig(os.path.join(FIG_DIR, "proportion.png"), dpi=150)
    plt.close(fig)


def fig_subarea(coded: pd.DataFrame):
    counts = coded["subarea"].value_counts().reindex(CPA_SUBAREAS).fillna(0)
    fig, ax = plt.subplots(figsize=(7, 3.4))
    ax.barh(counts.index[::-1], counts.values[::-1], color=ACCENT)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.tick_params(labelsize=9)
    ax.set_title("CPA environmental papers by accounting sub-area", loc="left",
                 fontsize=12, fontweight="bold")
    ax.set_xlabel("papers")
    fig.tight_layout(); fig.savefig(os.path.join(FIG_DIR, "subarea.png"), dpi=150)
    plt.close(fig)


def fig_orientation_over_time(coded: pd.DataFrame):
    c = coded.copy()
    c["bin"] = (c["year"] // 5) * 5
    piv = c.pivot_table(index="bin", columns="orientation", values="doi",
                        aggfunc="count", fill_value=0).reindex(columns=CPA_ORIENTATION,
                                                               fill_value=0)
    fig, ax = plt.subplots(figsize=(9, 3.6))
    colors = {"for society": ACCENT, "for capital markets": ACCENT2,
              "both/ambiguous": "#888888"}
    bottom = None
    for col in CPA_ORIENTATION:
        vals = piv[col].values
        ax.bar(piv.index, vals, bottom=bottom, width=4, label=col,
               color=colors[col], align="edge")
        bottom = vals if bottom is None else bottom + vals
    _style(ax)
    ax.set_title("Orientation of CPA environmental papers over time (5-yr bins)",
                 loc="left", fontsize=12, fontweight="bold")
    ax.set_ylabel("papers"); ax.legend(frameon=False, fontsize=8, ncol=3)
    fig.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, "orientation_over_time.png"), dpi=150)
    plt.close(fig)


def fig_society_map(coded: pd.DataFrame):
    """Society-vs-capital-markets x accounting sub-area count grid (their Fig 1)."""
    piv = coded.pivot_table(index="subarea", columns="orientation", values="doi",
                            aggfunc="count", fill_value=0)
    piv = piv.reindex(index=CPA_SUBAREAS, columns=CPA_ORIENTATION, fill_value=0)
    fig, ax = plt.subplots(figsize=(7, 4))
    im = ax.imshow(piv.values, cmap="Greens", aspect="auto")
    ax.set_xticks(range(len(CPA_ORIENTATION))); ax.set_xticklabels(CPA_ORIENTATION, fontsize=9)
    ax.set_yticks(range(len(CPA_SUBAREAS))); ax.set_yticklabels(CPA_SUBAREAS, fontsize=9)
    for i in range(piv.shape[0]):
        for j in range(piv.shape[1]):
            v = int(piv.values[i, j])
            ax.text(j, i, v, ha="center", va="center",
                    color="white" if v > piv.values.max() / 2 else INK, fontsize=10)
    ax.set_title("CPA env papers: sub-area x orientation", loc="left",
                 fontsize=12, fontweight="bold")
    fig.colorbar(im, ax=ax, shrink=0.7, label="papers")
    fig.tight_layout(); fig.savefig(os.path.join(FIG_DIR, "society_map.png"), dpi=150)
    plt.close(fig)


# --------------------------------------------------------------------------
def _trend_words(counts: pd.DataFrame) -> tuple[str, str]:
    """Crude absolute + proportional trend descriptors (early vs late thirds)."""
    c = counts[counts["n_total"] > 0].sort_values("year")
    if len(c) < 6:
        return "insufficient data", "insufficient data"
    k = len(c) // 3
    early, late = c.head(k), c.tail(k)
    abs_e, abs_l = early["n_env"].mean(), late["n_env"].mean()
    pr_e, pr_l = early["proportion"].mean(), late["proportion"].mean()
    absw = ("risen" if abs_l > abs_e * 1.1 else
            "fallen" if abs_l < abs_e * 0.9 else "held roughly flat")
    prw = ("risen" if pr_l > pr_e * 1.1 else
           "fallen" if pr_l < pr_e * 0.9 else "held roughly flat")
    return (f"{absw} (early mean {abs_e:.1f}/yr -> recent {abs_l:.1f}/yr)",
            f"{prw} (early {pr_e*100:.1f}% -> recent {pr_l*100:.1f}%)")


def build_findings() -> None:
    counts = pd.read_csv(COUNTS_CSV)
    bins = pd.read_csv(BINS_CSV)
    coded = pd.read_csv(CODED_CSV)
    screen = pd.read_csv(SCREEN_CSV)
    recall = pd.read_csv(RECALL_CSV) if os.path.exists(RECALL_CSV) else pd.DataFrame()
    cite = json.load(open(os.path.join(OUT_DIR, "cpa_citation_stats.json")))
    manifest = json.load(open(MANIFEST_JSON))

    n_env = len(coded)
    n_total = int(counts["n_total"].sum())
    n_cand = len(screen)
    precision = int(screen["is_env"].sum()) / max(1, n_cand)
    miss_rate = (recall["is_env"].sum() / len(recall)) if len(recall) else float("nan")
    est_missed = round(miss_rate * (n_total - n_cand)) if len(recall) else 0
    n_full = int((coded["text_source"] == "fulltext").sum())
    n_abs = n_env - n_full
    abs_trend, prop_trend = _trend_words(counts)
    sub = coded["subarea"].value_counts()
    sub_substantive = sub.drop("other", errors="ignore")   # excl. editorials etc.
    n_other = int(sub.get("other", 0))
    spec = coded["issue_specificity"].value_counts()
    orient = coded["orientation"].value_counts()

    def top(vc, n=6):
        return ", ".join(f"{k} ({v})" for k, v in vc.head(n).items())

    md = f"""# Environmental accounting in *Critical Perspectives on Accounting*: a reflection

*Generated {manifest['generated']}. Replicates Bebbington, Laine, Larrinaga &
Michelon (2023, EAR 32(5)) for CPA. Backbone: OpenAlex (source
{manifest['openalex_source_id']}). Text: Elsevier ScienceDirect. Coding:
{manifest['llm_model']} with human-verifiable rationales.*

## Headline answers

1. **Absolute trend.** The number of environmental-accounting papers CPA
   publishes per year has **{abs_trend.split(' (')[0]}** — {abs_trend.split('(',1)[1][:-1]}.
2. **Proportional trend.** As a share of all CPA articles, environmental work has
   **{prop_trend.split(' (')[0]}** — {prop_trend.split('(',1)[1][:-1]}.
3. **What kind of environmental accounting.** Among research articles the corpus
   concentrates in **{sub_substantive.index[0]}** ({int(sub_substantive.iloc[0])}
   papers), with {n_other} editorials/viewpoints/conceptual pieces ("other").
   Most papers use **{spec.index[0]}** framings ({int(spec.iloc[0])} of {n_env});
   orientation is overwhelmingly **{orient.index[0]}** ({int(orient.iloc[0])} of
   {n_env}) — as expected for a critical journal.
4. **Citation standing.** {cite['env_in_top50']} of CPA's 50 most-cited articles
   are environmental papers (env are {cite['env_share_overall']*100:.1f}% of all
   output); the environmental paper is the top-cited in its issue for
   {cite['env_top1_in_issue']} of {cite['issues_with_env']} issues that contain one.

## Corpus

- **{n_env}** environmental-accounting papers identified across **{n_total}** CPA
  research articles ({manifest['journal']}), {int(counts['year'].min())}–{int(counts['year'].max())}.
- Overall share: **{n_env/max(1,n_total)*100:.1f}%** of CPA output.
- See `cpa_table1_corpus.csv` for the full list with codes and citations.

## Content analysis (four axes)

**Accounting sub-area** — {top(sub, 8)}.

**Issue specificity** — {top(spec)}. (Papers coded "specific issue" name a
particular biophysical concern; "umbrella construct" papers use CSR /
sustainability / social-and-environmental framings.)

**Orientation (who environmental accounting is *for*)** — {top(orient, 3)}. This
is the society-vs-capital-markets axis; see `figures/society_map.png` for the
sub-area × orientation map and `figures/orientation_over_time.png` for its drift.

**Actor** — {top(coded['actor'].value_counts())}. (As in the EAR paper, this axis
is often weakly discriminating: many papers specify no single actor.)

## Citation analysis

- Environmental papers in top-50 most-cited: **{cite['env_in_top50']}**
  (top-100: {cite['env_in_top100']}).
- Median citations: environmental **{cite['median_cites_env']:.0f}** vs.
  non-environmental **{cite['median_cites_nonenv']:.0f}**.
- Within issues containing an environmental paper (n={cite['issues_with_env']}):
  top-cited in **{cite['env_top1_in_issue']}**, top-two in **{cite['env_top2_in_issue']}**.

## Method, validation, and caveats

- **Corpus identification.** Inclusive keyword prefilter on OpenAlex
  (title+abstract, {manifest['n_keywords']} phrases) → LLM relevance screen of
  every hit. Screen **precision ≈ {precision*100:.0f}%** ({int(screen['is_env'].sum())}
  of {len(screen)} candidates confirmed environmental).
- **Recall.** A random sample of {len(recall) if len(recall) else 0}
  non-candidate articles was LLM-screened; **≈{miss_rate*100:.0f}%** were
  actually environmental (keyword-prefilter recall ≈ {100-miss_rate*100:.0f}%).
  Extrapolated over the {n_total - n_cand} non-candidates, ≈**{est_missed}**
  environmental papers are likely missed, so the {n_env}-paper corpus is best
  read as a **lower bound** (~{n_env + est_missed} true). The missed papers
  (`cpa_recall_sample.csv`) tend to be ones where the environmental angle is
  implicit in the title.
- **Text.** CPA is Elsevier, and OpenAlex / Semantic Scholar carry almost no CPA
  abstracts — so the text layer is Elsevier's own ScienceDirect API, which
  returns a clean **abstract for every paper** (back to 1990) and **full body
  text only for open-access articles**. Coding therefore ran on full text for
  **{n_full}** papers and on the abstract for **{n_abs}** (per-paper flag in
  `cpa_corpus_coded.csv`). Coding is LLM-assisted with a rationale on every code
  — spot-check before citing figures in the manuscript.
- **Venue, not stance.** This is a content classification of CPA's environmental
  corpus; it does not code critical vs. positivist stance.

## Reproducibility

- OpenAlex filter: `{manifest['openalex_filters']}`
- Env search string and coding schemes: `run_manifest.json`.
- Pull date: {manifest['generated']}.
"""
    with open(FINDINGS_MD, "w", encoding="utf-8") as fh:
        fh.write(md)
    print(f"[deliver] findings -> {FINDINGS_MD}")


def build_all() -> None:
    os.makedirs(FIG_DIR, exist_ok=True)
    coded = build_table1()
    counts = pd.read_csv(COUNTS_CSV)
    bins = pd.read_csv(BINS_CSV)
    fig_frequency(counts)
    fig_proportion(counts, bins)
    fig_subarea(coded)
    fig_orientation_over_time(coded)
    fig_society_map(coded)
    build_findings()
    print(f"[deliver] figures -> {FIG_DIR}")
