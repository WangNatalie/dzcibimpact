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
clean abstracts, AI TLDRs, and open-access PDF links). **EBSCO** (EDS API) and
**Web of Science** (Clarivate API) are optional additional bibliographic sources
for the `aggregate` and `tier3` steps — their results are kept in **separate
per-source files** rather than merged, since different indexes can't be summed or
cross-deduped for counts. **ESVD** (the Ecosystem Services Valuation Database)
supplies the dollar values. See [Data sources](#data-sources) for why each is
used where.

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
| `EBSCO_USER_ID` / `EBSCO_PASSWORD` / `EBSCO_PROFILE` | EBSCO EDS API | optional; enables the EBSCO source in `aggregate` + `tier3`. Needs an EDS API profile from your library (not the EBSCOhost website login). Unset ⇒ EBSCO is skipped. |
| `WOS_API_KEY` (+ optional `WOS_API_BASE`) | Web of Science API | optional; enables the WoS source in `aggregate` + `tier3`. Needs a Clarivate WoS API key. Unset ⇒ WoS is skipped. |

Search vocabularies, region bounding boxes, the ecosystem→SOLRIS map, and
scope country codes all live in **`config.py`** as data — edit there, not in the
pipeline modules.

---

## Stages (runnable modules)

Each of these has a CLI (`python -m litreview.<module>`). Pass `-h` for flags.

### `aggregate` — tier-1/2 prevalence + value tables
Joins bibliographic paper **counts** with ESVD value **medians**, side by side,
one row per keyword. Each metric is reported at three nested scopes: **global**,
**North America** (US/CA/MX), and **Canada** (CA only). Counts run **once per
configured count source** (OpenAlex always; EBSCO and Web of Science when
credentials are set) into a per-source subdir — sources are never summed. Output
→ `outputs/aggregate/<source>/`: `aggregate_service_x_region.csv`,
`aggregate_ecosystem_x_region.csv`, and with `--crosstab` a service×ecosystem
matrix. OpenAlex and WoS support the author-country facet (all three region
columns); EBSCO (EDS) does not, so its North America / Canada count columns are
left blank (global counts only). The ESVD value columns are source-independent
and appear in every source's file.

### `provincial` — service/ecosystem × province matrices
The provincial counterpart to `aggregate`. Breaks the Canadian literature into
**OpenAlex paper-count matrices** with keywords as rows and the 13 provinces /
territories as columns (one for services, one for ecosystems). A "province" =
its name in the title/abstract with a Canadian author institution (default;
`--any-country` relaxes the institution scope). Counts only — ESVD's Canadian
coverage is too thin to split values by province (≤2 studies each). Output →
`outputs/provincial/`: `provincial_service_x_province.csv`,
`provincial_ecosystem_x_province.csv`.

### `esvd_summary` — standalone ESVD value tables
The value half of tier 1/2 with **no web APIs** — just the ESVD CSV. Robust
median/IQR of Int$/ha/yr by service and by ecosystem, for global / North America
/ Canada scopes. Output → `outputs/esvd/` (6 CSVs:
`esvd_summary_{service,ecosystem}_{global,north_america,canada}.csv`).

### `tier3` — Carolinian deep-extraction table
The flagship deliverable. Runs **independently per primary source** (OpenAlex
always; EBSCO and Web of Science when configured), each producing its own table:
gather regional candidates, deduplicate, screen, rank by transferability, and
deep-extract the top N (full text + Azure OpenAI). The OpenAlex run also folds in
**Semantic Scholar** as an enrichment layer (merged via dedup, never its own
table); the EBSCO and WoS runs query their own index alone (WoS constrained to
North America via the CU field). Output →
`outputs/filtered/<source>/tier3_carolinian_table.csv`, matching the columns of
`research/Ecosystem Services Mapping(Data).csv` plus provenance columns. Use
`--sources` to pick which sources run (default: all configured). See
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

### `cpa_review` — CPA environmental-accounting reflection

A self-contained sub-project (independent of the ecosystem-services tiers): a
Bebbington/Laine/Larrinaga/Michelon (2023, *EAR* 32(5))-style reflection on the
environmental-accounting corpus of **Critical Perspectives on Accounting**.
Tracks publication frequency and proportion over time, content-analyses each
paper along four axes (accounting sub-area, issue specificity, actor, and
society-vs-capital-markets orientation), and runs a citation analysis.

Resumable steps: `python -m litreview.cpa_review <enumerate|screen|code|counts|cite|deliver|all>`.

- **Backbone = OpenAlex** (source `S66510378`): enumerates every CPA article, the
  per-year denominator, and citation counts.
- **Text = Elsevier ScienceDirect** (`ELSEVIER_API_KEY`): CPA is Elsevier, so
  OpenAlex/Semantic Scholar carry almost no CPA abstracts. Elsevier returns an
  abstract for every paper (back to 1990) and full body text for OA articles.
- **Coding = Azure OpenAI**: keyword prefilter → LLM relevance screen (with a
  recall sample) → LLM content-coding, every code carrying a rationale.

Output → `outputs/cpa/`: `cpa_table1_corpus.csv` (corpus + codes),
`cpa_yearly_counts.csv` / `cpa_5yr_bins.csv`, `cpa_citation_analysis.csv`,
`figures/`, `findings.md`, and `run_manifest.json` (reproducibility). Modules:
`cpa_review.py` (pipeline), `cpa_report.py` (tables/plots/prose),
`sources/elsevier.py` (text layer).

---

## Supporting modules (libraries, not run directly)

| Module | Role |
|--------|------|
| `config.py` | All tunable data: keyword vocabularies, region boxes, country codes, ecosystem→SOLRIS map, `Settings` (env). |
| `models.py` | `Paper` (normalized cross-source record) and `TableRow` (one output-table row). |
| `sources/openalex.py` | OpenAlex client: faceted/total counts and cursor-paginated record search. |
| `sources/semantic_scholar.py` | Semantic Scholar client: bulk search + recommendations; enrichment only. |
| `sources/ebsco.py` | EBSCO EDS API client: token auth, total counts, record search; used in `aggregate` + `tier3` when configured. |
| `sources/wos.py` | Web of Science Starter API client: total counts (with country facet) + record search; used in `aggregate` + `tier3` when configured. |
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

1. **Gather** — for each primary source (OpenAlex, plus EBSCO and Web of Science
   if configured), query per region term; `dedup.deduplicate` collapses
   duplicates. In the OpenAlex run, S2 is folded in (OpenAlex is the backbone, S2
   fills missing abstract / OA PDF / citations); the EBSCO and WoS runs stand
   alone. Each source produces its own table under `filtered/<source>/`. The
   surviving record's `contributing_sources` becomes the **Data Source(s)**
   column.
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
| **EBSCO (EDS API)** | optional bibliographic source | counts + candidate records in `aggregate` + `tier3`, kept in separate per-source files (never merged/summed with OpenAlex). Not used for keyword discovery. Needs an EDS API profile from your library; no author-country facet (so no per-region counts). |
| **Web of Science (Clarivate API)** | optional bibliographic source | counts + candidate records in `aggregate` + `tier3`, in separate per-source files. Not used for keyword discovery. Needs a WoS API key; supports the author-country facet (full per-region counts). Starter API records are metadata-only (no abstracts), so WoS tier-3 screening matches on title + keywords. |
| **ESVD** | dollar values | ~12k valuation records normalized to Int$/ha/yr; the value columns in `aggregate` and `esvd_summary`. |

---

## Outputs

- `outputs/` — machine-readable CSVs written by the generators.
- `reports/` — human-readable renderings produced by `report` (English headers).

Both are organized into one subdirectory per stage, and `reports/` mirrors the
`outputs/` layout exactly. The multi-source stages (`aggregate`, `tier3`) nest a
further **per-source** subdir so each source's results stay separate:

```
outputs/ (and reports/)
├── aggregate/
│   ├── openalex/  aggregate_*           ← aggregate (OpenAlex counts)
│   ├── ebsco/     aggregate_*           ← aggregate (EBSCO counts, if configured)
│   └── wos/       aggregate_*           ← aggregate (Web of Science, if configured)
├── provincial/    provincial_*          ← provincial
├── esvd/          esvd_summary_*        ← esvd_summary
├── keywords/      keywords_*            ← keyword_discovery
└── filtered/
    ├── openalex/  tier3_*               ← tier3 (OpenAlex + S2)
    ├── ebsco/     tier3_*               ← tier3 (EBSCO, if configured)
    ├── wos/       tier3_*               ← tier3 (Web of Science, if configured)
    └── topic_table_*                    ← topic_table (per-paper tables)
```

Re-running a generator overwrites its own outputs; run `report` afterward to
refresh the reports.
