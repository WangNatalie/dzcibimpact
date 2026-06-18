# litreview — ecosystem-services valuation literature review pipeline

A research pipeline that surveys the ecosystem-services valuation literature and
distills it into review tables, scoped for the **Carolinian Zone of southern
Ontario, Canada**. It is organized as a three-tier funnel from broad prevalence
counts down to a deep, per-paper extraction table.

```
tier 1  global         prevalence counts (OpenAlex) + value medians (ESVD)
tier 2  north america  same metrics, narrowed to US/CA/MX (and Canada-only)
tier 3  carolinian     full-text, per-paper extraction into the target table
```

**OpenAlex** is the counting/enumeration backbone. **Semantic Scholar** is a
record-level *enrichment* layer used only in the deep-extraction stages (it adds
clean abstracts, AI TLDRs, and open-access PDF links). **ESVD** (the Ecosystem
Services Valuation Database) supplies the dollar values. See
[Data sources](#data-sources) for why each is used where.

---

## Quick start

```bash
# from the repo root, using the project venv
cp litreview/.env.example litreview/.env     # fill in keys (see Configuration)

./venv/bin/python -m litreview.esvd_summary  # ESVD value tables   (no network)
./venv/bin/python -m litreview.aggregate     # tier-1/2 counts+values (network)
./venv/bin/python -m litreview.tier3         # tier-3 table        (network + Azure)
./venv/bin/python -m litreview.report        # render outputs/ -> reports/
```

Every runnable module writes machine-readable CSVs to `litreview/outputs/`.
`report` then renders those into human-readable CSVs in `litreview/reports/`
(English column headers, tidied prose). **Run `report` last**, after whichever
generators you ran.

---

## Configuration

Copy `.env.example` to `.env` (in `litreview/`, the repo root, or `scripts/`)
and fill in what you need. Loaded automatically via `python-dotenv`
(`config.py`).

| Variable | Used by | Required for |
|----------|---------|--------------|
| `LITREVIEW_EMAIL` | OpenAlex / Unpaywall polite pool | recommended (better rate limits) |
| `ESVD_CSV` | ESVD loader | aggregate, esvd_summary (the value columns) |
| `AZURE_OPENAI_ENDPOINT` / `_API_KEY` / `_MODEL` | LLM extraction | tier-3 value/method/findings cells |
| `SEMANTIC_SCHOLAR_API_KEY` | Semantic Scholar | optional (raises S2 rate limits) |

Search vocabularies, region bounding boxes, the ecosystem→SOLRIS map, and
scope country codes all live in **`config.py`** as data — edit there, not in the
pipeline modules.

---

## Stages (runnable modules)

Each of these has a CLI (`python -m litreview.<module>`). Pass `-h` for flags.

### `aggregate` — tier-1/2 prevalence + value tables
Joins OpenAlex paper **counts** with ESVD value **medians**, side by side, one
row per keyword. Each metric is reported at three nested scopes: **global**,
**North America** (US/CA/MX), and **Canada** (CA only). Counts are OpenAlex-only
(count APIs can't be deduplicated against another index). Output →
`outputs/aggregate/`: `aggregate_service_x_region.csv`,
`aggregate_ecosystem_x_region.csv`, and with `--crosstab` a service×ecosystem
matrix.

### `esvd_summary` — standalone ESVD value tables
The value half of tier 1/2 with **no web APIs** — just the ESVD CSV. Robust
median/IQR of Int$/ha/yr by service and by ecosystem, for global / North America
/ Canada scopes. Output → `outputs/esvd/` (6 CSVs:
`esvd_summary_{service,ecosystem}_{global,north_america,canada}.csv`).

### `tier3` — Carolinian deep-extraction table
The flagship deliverable. Gathers regional candidates from **OpenAlex + Semantic
Scholar**, deduplicates, screens, ranks by transferability, and deep-extracts
the top N into the target table (full text + Azure OpenAI). Output →
`outputs/filtered/tier3_carolinian_table.csv`, matching the columns of
`research/Ecosystem Services Mapping(Data).csv` plus provenance columns. See
[Tier-3 run flow](#tier-3-run-flow).

### `topic_table` — tier-3 table for any OpenAlex Topic
Generalizes `tier3` from the hard-wired "ecosystem services + Carolinian" search
to *any* Topic term from `keywords_topics.csv`. Selects by OpenAlex Topic id,
with a `--location` profile (`global` / `north_america` / `carolinian`) and an
optional `--llm` flag (default is a no-LLM heuristic extractor). Requires
`--term`. Output → `outputs/filtered/topic_table_<term>_<location>.csv` (and
writes its own cleaned copy straight to `reports/filtered/`).

### `keyword_discovery` — data-driven vocabulary discovery
Surfaces the terms the literature actually uses, to sanity-check / extend the
vocabularies in `config.py`. Two views: OpenAlex **Topic facets** and TF-IDF
**abstract n-grams**. OpenAlex-only. Output → `outputs/keywords/`:
`keywords_topics.csv`, `keywords_ngrams.csv`.

### `report` — human-readable rendering
Walks `outputs/` and mirrors it into `reports/`, preserving the subdirectory
layout: snake_case headers → English phrases, and for the per-paper tables under
`filtered/`, strips the inline numeric subscores from the rationale prose and
drops the raw score column. Pure presentation — values are otherwise copied
verbatim.

---

## Supporting modules (libraries, not run directly)

| Module | Role |
|--------|------|
| `config.py` | All tunable data: keyword vocabularies, region boxes, country codes, ecosystem→SOLRIS map, `Settings` (env). |
| `models.py` | `Paper` (normalized cross-source record) and `TableRow` (one output-table row). |
| `sources/openalex.py` | OpenAlex client: faceted/total counts and cursor-paginated record search. |
| `sources/semantic_scholar.py` | Semantic Scholar client: bulk search + recommendations; enrichment only. |
| `sources/esvd.py` | ESVD loader + TEEB crosswalks; robust value summaries (median/IQR). |
| `dedup.py` | Cross-source dedup by DOI/title; merges S2 into the OpenAlex backbone, tracks `contributing_sources`. |
| `screening.py` | Tags each paper against the three controlled vocabularies (deterministic). |
| `peer_review.py` | Peer-reviewed vs grey-literature classification. |
| `transferability.py` | Weighted, auditable score of a paper's transferability to the Carolinian Zone. |
| `fulltext.py` | Full-text retrieval (OA PDF → text), with abstract fallback. |
| `extract.py` | Turns a selected `Paper` into a `TableRow` (deterministic scaffolding + Azure LLM). |
| `llm_azure.py` | Thin Azure OpenAI wrapper for classification/extraction. |

---

## Tier-3 run flow

```
gather_candidates                     select                     extract → write
─────────────────                     ──────                     ───────────────
OpenAlex search ─┐
                 ├─► dedup ─► screen ─► peer_review ─► transferability ─► top-N ─► fulltext + LLM ─► TableRow ─► CSV
Semantic Schol. ─┘   (DOI/    (keyword   (grey-lit       (weighted        rank      (extract.py)
                      title)   tags)      flag)           score)
```

1. **Gather** — query both sources per region term; `dedup.deduplicate` merges
   duplicates (OpenAlex is the backbone, S2 fills missing abstract / OA PDF /
   citations). The surviving record's `contributing_sources` becomes the
   **Data Source(s)** column.
2. **Screen** — `screening.screen` tags ecosystem services / methods /
   ecosystems; non-relevant papers are dropped.
3. **Classify** — `peer_review.classify` flags peer-reviewed vs grey literature.
4. **Rank** — `transferability.score` (location, ecosystem, service,
   peer-review, method) selects the top N above `--min-score`.
5. **Extract** — `fulltext.fetch_fulltext` gets the PDF text (abstract
   fallback), Azure OpenAI fills the value/method/limitations/findings cells;
   geography/SOLRIS/transferability are computed deterministically.

Steps 1–4 need no LLM. Step 5 uses Azure OpenAI if configured, else writes
deterministic scaffolding only (so the selection pipeline is still usable).

---

## Data sources

| Source | Role | Why / where |
|--------|------|-------------|
| **OpenAlex** | counting + enumeration backbone | free, keyless, faceted counts without downloading; powers every stage. |
| **Semantic Scholar** | record-level enrichment | clean abstracts + AI TLDRs + OA PDF links + extra recall. Used **only** in `tier3` / `topic_table`, never in counts — count APIs can't be deduped and the indexes overlap heavily (a sum would near-double-count). |
| **ESVD** | dollar values | ~12k valuation records normalized to Int$/ha/yr; the value columns in `aggregate` and `esvd_summary`. |

---

## Outputs

- `outputs/` — machine-readable CSVs written by the generators.
- `reports/` — human-readable renderings produced by `report` (English headers).

Both are organized into one subdirectory per stage, and `reports/` mirrors the
`outputs/` layout exactly:

```
outputs/ (and reports/)
├── aggregate/   aggregate_*            ← aggregate
├── esvd/        esvd_summary_*         ← esvd_summary
├── keywords/    keywords_*             ← keyword_discovery
└── filtered/    tier3_*, topic_table_* ← tier3, topic_table  (per-paper tables)
```

Re-running a generator overwrites its own outputs; run `report` afterward to
refresh the reports.
