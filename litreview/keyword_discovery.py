"""Data-driven keyword discovery.

Before committing to the hand-curated vocabularies in config.py, run a broad
"ecosystem service" search and surface the terms the literature actually uses.
Two complementary views:

  1. discover_topics(): OpenAlex's Topic classification for the corpus, faceted
     with counts. Topics replaced the deprecated Concepts taxonomy; they carry
     an inline topic -> subfield -> field -> domain hierarchy, so `granularity`
     picks how broad the grouping is (topic = specific, subfield/field = broad)
     without a separate lookup call.
  2. discover_ngrams(): TF-IDF-ranked 1-3 grams over a sample of abstracts.
     TF-IDF demotes both ubiquitous phrases ("ecosystem services", in ~86% of
     docs) and generic academic filler, surfacing distinctive domain terms.

Use the merged top list to sanity-check / extend ECOSYSTEM_SERVICE_KEYWORDS,
METHOD_KEYWORDS, and ECOSYSTEM_KEYWORDS.
"""

from __future__ import annotations

import csv
import math
import os
import re
from collections import Counter
from typing import Optional

from .config import SETTINGS
from .sources import openalex

# Generic academic / function words that carry no domain signal. TF-IDF already
# demotes ubiquitous terms, but pre-filtering keeps the n-gram space clean.
_STOPWORDS = {
    # function words
    "the", "and", "for", "are", "was", "were", "this", "that", "these", "those",
    "with", "from", "have", "has", "had", "not", "but", "can", "may", "such",
    "which", "their", "they", "them", "its", "our", "also", "more", "most",
    "than", "then", "thus", "here", "there", "into", "over", "under", "between",
    "however", "therefore", "within", "across", "both", "each", "all", "one",
    "two", "three", "we", "be", "is", "in", "of", "to", "on", "as", "by", "an",
    "at", "or", "a", "it", "i", "ii", "iii", "iv", "et", "al", "per", "via",
    "while", "where", "how", "will", "would", "could", "should", "must", "many",
    "much", "some", "any", "other", "others", "only", "very", "well", "due",
    "first", "second", "new", "non", "yet", "less", "least", "about", "above",
    # generic research filler
    "study", "studies", "paper", "papers", "results", "result", "using", "used",
    "use", "uses", "based", "approach", "approaches", "method", "methods",
    "data", "analysis", "found", "show", "shows", "shown", "high", "low",
    "different", "several", "various", "important", "provide", "provides",
    "provided", "providing", "research", "article", "review", "fig", "table",
    "abstract", "however", "increase", "increases", "increased", "increasing",
    "include", "includes", "including", "included", "find", "finding",
    "findings", "consider", "considered", "present", "presents", "presented",
    "given", "general", "specific", "potential", "level", "levels", "case",
    "number", "order", "process", "system", "systems", "model", "models",
}
_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z\-]+")


# --------------------------------------------------------------------------
# 1. Topic facets
# --------------------------------------------------------------------------
# Topic hierarchy, broad -> specific: domain -> field -> subfield -> topic.
# Grouping at a coarser level is how we drop broad disciplines, replacing the
# old concept level>=N filter.
_GRANULARITY_GROUP_BY = {
    "topic": "primary_topic.id",
    "subfield": "primary_topic.subfield.id",
    "field": "primary_topic.field.id",
    "domain": "primary_topic.domain.id",
}


def discover_topics(
    seed_query: str = "ecosystem service",
    *,
    year_from: Optional[int] = None,
    country_codes: Optional[list[str]] = None,
    top_n: int = 500,
    granularity: str = "topic",
) -> list[dict]:
    """Top OpenAlex Topics for the seed corpus, with counts.

    `granularity` is one of topic/subfield/field/domain; coarser values group
    broad disciplines together instead of surfacing specific topics.
    Returns [{term, count, share}].
    """
    group_by = _GRANULARITY_GROUP_BY.get(granularity)
    if group_by is None:
        raise ValueError(
            f"granularity must be one of {sorted(_GRANULARITY_GROUP_BY)}, "
            f"got {granularity!r}"
        )
    total = openalex.total_count(
        search=seed_query, year_from=year_from, country_codes=country_codes
    )
    groups = openalex.faceted_groups(
        search=seed_query,
        group_by=group_by,
        year_from=year_from,
        country_codes=country_codes,
    )
    out = [
        {
            "term": g["name"],
            "count": g["count"],
            "share": round(g["count"] / total, 4) if total else 0.0,
        }
        for g in groups
    ]
    out.sort(key=lambda r: r["count"], reverse=True)
    return out[:top_n]


# --------------------------------------------------------------------------
# 2. Abstract n-grams (TF-IDF ranked)
# --------------------------------------------------------------------------
def _doc_ngrams(abstract: str, lo: int, hi: int) -> list[str]:
    words = [w.lower() for w in _TOKEN_RE.findall(abstract)]
    grams: list[str] = []
    for n in range(lo, hi + 1):
        for i in range(len(words) - n + 1):
            window = words[i:i + n]
            # drop n-grams that start/end on a stopword or are all-short noise
            if window[0] in _STOPWORDS or window[-1] in _STOPWORDS:
                continue
            if all(len(w) <= 2 for w in window):
                continue
            grams.append(" ".join(window))
    return grams


def discover_ngrams(
    seed_query: str = "ecosystem service",
    *,
    sample: int = 2000,
    year_from: Optional[int] = None,
    country_codes: Optional[list[str]] = None,
    top_n: int = 500,
    ngram_range: tuple[int, int] = (1, 3),
    min_doc_freq: int = 4,
) -> list[dict]:
    """TF-IDF-ranked abstract n-grams over a sample of the seed corpus.

    Score = sublinear_tf(corpus_count) * smoothed_idf(doc_freq), so terms that
    appear in almost every abstract (low idf) or barely at all (low tf) sink,
    and distinctive mid-frequency domain phrases rise.
    """
    lo, hi = ngram_range
    corpus_tf: Counter[str] = Counter()
    doc_freq: Counter[str] = Counter()
    n_docs = 0
    for paper in openalex.search(
        search=seed_query,
        year_from=year_from,
        country_codes=country_codes,
        max_results=sample,
    ):
        if not paper.abstract:
            continue
        n_docs += 1
        grams = _doc_ngrams(paper.abstract, lo, hi)
        corpus_tf.update(grams)
        for g in set(grams):
            doc_freq[g] += 1

    scored = []
    for term, tf in corpus_tf.items():
        df = doc_freq[term]
        if df < min_doc_freq:
            continue
        sublinear_tf = 1.0 + math.log(tf)
        idf = math.log((n_docs + 1) / (df + 1)) + 1.0
        # mild bonus for multi-word phrases (domain terms are usually phrases)
        phrase_bonus = 1.0 + 0.15 * (term.count(" "))
        scored.append({
            "term": term,
            "tfidf": round(sublinear_tf * idf * phrase_bonus, 3),
            "count": tf,
            "doc_freq": df,
            "doc_share": round(df / n_docs, 4) if n_docs else 0.0,
        })
    scored.sort(key=lambda r: r["tfidf"], reverse=True)
    return scored[:top_n]


# --------------------------------------------------------------------------
# Output + orchestration
# --------------------------------------------------------------------------
def write_csv(rows: list[dict], path: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        return path
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    return path


def run(
    seed_query: str = "ecosystem service",
    *,
    sample: int = 2000,
    year_from: Optional[int] = None,
    top_n: int = 500,
    granularity: str = "topic",
) -> dict[str, list[dict]]:
    """Run both discoveries, write CSVs to outputs/, return both."""
    topics = discover_topics(
        seed_query, year_from=year_from, top_n=min(top_n, 500),
        granularity=granularity,
    )
    ngrams = discover_ngrams(
        seed_query, sample=sample, year_from=year_from, top_n=top_n
    )
    kw_dir = os.path.join(SETTINGS.output_dir, "keywords")
    write_csv(topics, os.path.join(kw_dir, "keywords_topics.csv"))
    write_csv(ngrams, os.path.join(kw_dir, "keywords_ngrams.csv"))
    return {"topics": topics, "ngrams": ngrams}


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Discover keywords from the literature via OpenAlex "
        "(topic facets + TF-IDF abstract n-grams). Writes "
        "keywords_topics.csv and keywords_ngrams.csv to the outputs dir."
    )
    parser.add_argument(
        "--seed-query", default="ecosystem service",
        help="broad search seeding the corpus (default: %(default)s)",
    )
    parser.add_argument(
        "--sample", type=int, default=2000,
        help="abstracts to sample for the n-gram pass (default: %(default)s)",
    )
    parser.add_argument(
        "--year-from", type=int, default=None,
        help="publication year floor (default: no year filter)",
    )
    parser.add_argument(
        "--top-n", type=int, default=500,
        help="terms to keep per list; concepts cap at 200 (default: %(default)s)",
    )
    parser.add_argument(
        "--granularity", choices=["topic", "subfield", "field", "domain"],
        default="topic",
        help="OpenAlex topic grouping level; coarser = broader (default: "
        "%(default)s)",
    )
    args = parser.parse_args()

    results = run(
        args.seed_query,
        sample=args.sample,
        year_from=args.year_from,
        top_n=args.top_n,
        granularity=args.granularity,
    )
    kw_dir = os.path.join(SETTINGS.output_dir, "keywords")
    print(f"{len(results['topics'])} topics  -> "
          f"{os.path.join(kw_dir, 'keywords_topics.csv')}")
    print(f"{len(results['ngrams'])} n-grams  -> "
          f"{os.path.join(kw_dir, 'keywords_ngrams.csv')}")


if __name__ == "__main__":
    main()
