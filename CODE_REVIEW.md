## Code Review Findings

### 1. No single working directory satisfies both the README and the scripts

- `README.md` instructs users to create `.env` inside `scripts/` and run `cd scripts` before `python database_helpers.py reindex` (`README.md:19`, `README.md:30`).
- The scripts load dotenv from the current working directory with bare `load_dotenv()` (`scripts/classify_area.py:39`, `scripts/processor.py:10`, `scripts/database_helpers.py:29`, `scripts/potential_calculator.py:45`, `scripts/site_calculator.py:39`).
- At the same time, the default input/output paths are rooted at `data/...` and `GIS/...` (`scripts/classify_area.py:55`, `scripts/classify_area.py:60`, `scripts/processor.py:30`, `scripts/processor.py:123`, `scripts/potential_calculator.py:54`, `scripts/potential_calculator.py:55`, `scripts/site_calculator.py:46`, `scripts/site_calculator.py:47`, `scripts/site_calculator.py:76`).

Impact:
- Running from the repo root makes the `data/` and `GIS/` defaults work, but a `.env` placed in `scripts/` is not discovered.
- Running from `scripts/` makes `.env` discovery work, but the default `data/...` and `GIS/...` paths fail because those directories live one level up.
- The current docs and defaults therefore force users into a broken setup depending on which instruction they follow.

### 2. Landscape aesthetic quality "after" values are not correct once multiple changes are applied

- `AestheticQualityProcessor.compute_change()` computes rarity for one local area transfer at a time by adjusting a copy of `context_areas` for just that single change (`scripts/ecosystem_services/aesthetic_quality.py:72` to `scripts/ecosystem_services/aesthetic_quality.py:80`).
- `potential_calculator.py` then sums those per-feature deltas into `aq_weighted_change` and derives a single landscape-level `aq_after` from that sum (`scripts/potential_calculator.py:400`, `scripts/potential_calculator.py:428` to `scripts/potential_calculator.py:445`, `scripts/potential_calculator.py:561` to `scripts/potential_calculator.py:568`).
- `site_calculator.py` does the same thing across all sites and habitat types (`scripts/site_calculator.py:322`, `scripts/site_calculator.py:353` to `scripts/site_calculator.py:367`, `scripts/site_calculator.py:382` to `scripts/site_calculator.py:389`).

Impact:
- The printed landscape-level `Before`/`After`/`Delta` values are only valid for a single isolated change.
- As soon as multiple polygons or sites are changed, rarity should be recomputed from the fully updated landscape composition, not from independent one-off adjustments that are later summed.
- The per-feature output fields may still be usable, but the aggregate landscape summary is overstated or understated depending on change mix.

### 3. `site_calculator.py` leaves stale values behind on reruns

- The script updates the GeoPackage in place and only writes `solris_code` if sampling succeeds (`scripts/site_calculator.py:337` to `scripts/site_calculator.py:339`).
- It skips land-change columns whose field value is `None` (`scripts/site_calculator.py:347` to `scripts/site_calculator.py:350`).
- It only writes result fields that appear in `totals` for the current run (`scripts/site_calculator.py:365` to `scripts/site_calculator.py:370`).

Impact:
- If a feature had output values from an earlier run and later its geometry samples outside the raster, its previous `solris_code` remains.
- If a land-change input is cleared to null, the corresponding `change_*` outputs are not reset and the old values remain in the GeoPackage.
- Because the file is modified in place, rerunning the script can silently preserve stale results instead of reflecting current source data.

## Verification

- `python -m compileall scripts`
- Confirmed path mismatch from `scripts/` vs repo root with `Test-Path` checks against the default `data/...` and `GIS/...` locations.

## Higher-Level Design Concerns

### 4. The pipeline has two competing sources of truth for lookup data

- `processor.py` reads `solris_lookup` and `water_filtration_lookup` from Supabase (`scripts/processor.py:97` to `scripts/processor.py:100`).
- `potential_calculator.py` and `site_calculator.py` read the same conceptual lookup data from local CSV files (`scripts/potential_calculator.py:54` to `scripts/potential_calculator.py:55`, `scripts/potential_calculator.py:504`, `scripts/site_calculator.py:46` to `scripts/site_calculator.py:47`, `scripts/site_calculator.py:261`).

Impact:
- Results can diverge across scripts even when they are supposed to represent the same model, depending on whether the local CSVs and Supabase tables are perfectly synchronized.
- That makes the system harder to reason about and weakens reproducibility for a paper or report workflow.

### 5. Site-level impact calculations assume one baseline land-cover class per site

- The README describes the project site layer as points with restoration acreage columns for forest, wetland, and tallgrass prairie (`README.md:45`, `README.md:66` to `README.md:69`).
- `site_calculator.py` samples a single SOLRIS value for each feature (`scripts/site_calculator.py:337`) and then uses that one sampled `old_vals` baseline for every restoration acreage field on the feature (`scripts/site_calculator.py:341`, `scripts/site_calculator.py:347` to `scripts/site_calculator.py:358`).

Impact:
- This is a strong modeling assumption: all restoration acres on a site are treated as if they currently occupy the same starting land-cover class.
- If a site is heterogeneous, or if the acreage fields refer to different subareas, the reported ES deltas are only approximate and may systematically misstate site impacts.

### 6. "Carbon sequestration" is implemented as a stock-difference model, not a time-based sequestration model

- The carbon change calculation is based on the difference between total carbon pools per hectare for the old and new land-cover classes (`scripts/ecosystem_services/carbon_sequestration.py:20` to `scripts/ecosystem_services/carbon_sequestration.py:24`).
- The lookup value being compared is `total_c_per_ha`, derived from the sum of AGC, BGC, SOC, and dead organic carbon pools (`scripts/potential_calculator.py:118` to `scripts/potential_calculator.py:123`, `scripts/site_calculator.py:103` to `scripts/site_calculator.py:108`).

Impact:
- The implementation estimates an immediate carbon stock difference, not a sequestration trajectory over time.
- If the paper or documentation presents this output as annual sequestration, restoration-rate-adjusted uptake, or time-phased climate benefit, that would overstate what the code is actually computing.
