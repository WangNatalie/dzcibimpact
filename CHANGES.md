# Implementation Changelog

Changes implemented in response to the external code review (`CODE_REVIEW.md`) and a subsequent independent review of the full repository.

---

## External Code Review (`CODE_REVIEW.md`)

### Issue 1 — No single working directory satisfies both the README and the scripts

**Problem:** Scripts used bare `load_dotenv()`, which only searches the current working directory for `.env`. Default file paths (`GIS/...`, `data/...`) are rooted at the repo root, so scripts had to be run from there — but that meant a `.env` placed in `scripts/` was never found, and vice versa.

**Fix:**
- Replaced `load_dotenv()` with `load_dotenv(find_dotenv())` in all five scripts (`classify_area.py`, `potential_calculator.py`, `site_calculator.py`, `processor.py`, `database_helpers.py`). `find_dotenv()` walks up the directory tree to find `.env` regardless of working directory.
- Updated `README.md`: removed `cd scripts` instruction, moved `.env` location to repo root, changed all example commands to `python scripts/<script>.py ...` run from the repo root.

---

### Issue 2 — Landscape aesthetic quality "after" values incorrect when multiple changes are applied

**Problem:** Both `potential_calculator.py` and `site_calculator.py` summed per-feature AQ deltas and derived a post-change landscape score from that sum. Because rarity is a non-linear function of landscape composition, computing it independently per feature and summing the results gives a different answer than computing it once from the fully updated landscape composition.

**Fix:**
- **`potential_calculator.py`:** `write_impact_gpkg` now accumulates a `transitions` dict (`{old_code: total_area_ha}`) and returns it. In `main()`, `context_areas_after` is built by subtracting all transitions from old codes and adding the total to the new code. `landscape_aq` is called once on `context_areas_before` and once on the fully-updated `context_areas_after`.
- **`site_calculator.py`:** Same approach using a `net_delta` dict (`{code: net_area_ha_change}`) accumulated across all features and all land change types. `landscape_aq` is called once before and once after applying all changes.

---

### Issue 3 — `site_calculator.py` leaves stale values behind on reruns

**Problem:** The script modifies the GeoPackage in-place. If a feature's input data changes between runs (e.g. a land change area is cleared, or SOLRIS sampling fails), the previous output values remain in the file because only fields with new computed values were written.

**Fix:** Added a null-out block at the start of each feature's processing loop that explicitly sets `solris_code` and all processor `CHANGE_FIELDS` to `None` before any computation. If computation is skipped (no geometry, no SOLRIS sample), the fields remain null rather than retaining stale values.

---

### Issue 4 — Two competing sources of truth for lookup data

**Problem:** `processor.py` read ecosystem service lookups from Supabase, while `potential_calculator.py` and `site_calculator.py` read the same data from local CSV files. Results could diverge across scripts if the CSVs and Supabase were out of sync.

**Fix:** Supabase is the single runtime source of truth.
- Added `supabase_engine()` (previously private) and `load_es_lookup(engine) -> dict` to `database_helpers.py`. `load_es_lookup` queries `solris_lookup` and `water_filtration_lookup`, computes `total_c_per_ha`, merges `wf_value_per_ha`, and returns a `{solris_code: {col: value}}` dict.
- Removed local `load_es_lookup` implementations and all `_SOLRIS_LOOKUP_CSV` / `_WF_LOOKUP_CSV` constants from `potential_calculator.py` and `site_calculator.py`.
- Removed the private `_supabase_engine()` from `processor.py`; all scripts now import `supabase_engine` from `database_helpers`.

---

### Issue 5 — Single-class baseline assumption undocumented

**Problem:** `site_calculator.py` samples one SOLRIS pixel per feature and uses that single class as the baseline for all restoration acreage columns. Sites that span multiple land cover classes will have approximate ES deltas, but this was not communicated to users.

**Fix:** Added a modeling assumption note to the `site_calculator.py` module docstring explaining that a single pixel is sampled and used as the baseline for all acreage columns on that feature. Added a corresponding callout to `README.md`.

---

## Independent Code Review

### Finding 1 — Missing null checks after `gdal.Open` in `create_masked_solris`

**Problem:** `potential_calculator.py:create_masked_solris` called `gdal.Open` on both the SOLRIS and change rasters but did not check for `None` returns before reading arrays, risking an unhandled `AttributeError`.

**Fix:** Added explicit null checks with `sys.exit` error messages after both `gdal.Open` calls.

---

### Finding 2 — Docstring listed wrong column name for wetland change

**Problem:** The `site_calculator.py` module docstring listed `land_change_wetland_acres` but the actual column in `LAND_CHANGE_TYPES` is `land_change_wetlands_acres` (with an 's').

**Fix:** Corrected the docstring to match the actual column name.

---

### Finding 3 — `compute_solris_areas`, `sample_solris`, and `clip_raster_to_geojson` duplicated across scripts

**Problem:** `potential_calculator.py`, `site_calculator.py`, and `classify_area.py` each contained their own copies of `compute_solris_areas`, `sample_solris`, and/or the raster clipping logic. Any bug fix or improvement had to be applied in three places.

**Fix:** Created `scripts/gis_helpers.py` with the single canonical implementations of:
- `clip_raster_to_geojson(tif, geojson, output, nodata=0)`
- `compute_solris_areas(solris_tif, geojson=None) -> dict`
- `sample_solris(x, y, src_srs, solris_ds, solris_srs) -> int | None`

Added `upload_gpkg_to_supabase(gpkg_path, layer_name, table_name, geometry_type=None)` to `database_helpers.py` as the single canonical ogr2ogr upload helper.

Removed all local copies from the three scripts and updated call sites:
- `clip_to_geojson(...)` → `clip_raster_to_geojson(..., nodata=255)` in `potential_calculator.py`
- `step_clip(...)` → `clip_raster_to_geojson(...)` in `classify_area.py`
- `upload_to_supabase(...)` / `step_upload_to_supabase(...)` → `upload_gpkg_to_supabase(...)` in all three scripts

---

### Finding 4 — Silent zero-baseline when SOLRIS sampling fails

**Problem:** When `sample_solris` returned `None`, `old_vals` was set to `{}`. All processors then treated the baseline as zero (zero carbon, zero biocapacity, etc.), computing deltas as if restoring from bare nothing. This could significantly overstate ES benefits with no indication to the user.

**Fix:** When `sample_solris` returns `None`, the script now skips the ES delta loop entirely for that feature (all change fields remain null, consistent with the stale-value fix). A single summary warning is printed after the loop if any features had land change area data but no SOLRIS sample, using a `skipped_no_solris` counter accumulated during iteration.

---

### Finding 5 — No-op column rename in `processor.py`

**Problem:** `processor.py` renamed `water_filtration_lookup` columns (`wetland_type` → `solris_class`, `value` → `wf_value_per_ha`) after querying Supabase, but the `database_helpers.py reindex` command already stores those canonical column names in the table. The rename was a no-op.

**Fix:** Removed the redundant `.rename()` call from `processor.py`.

---

### Finding 6 — Pixel coordinate truncation in `sample_solris`

**Problem:** Pixel coordinates were computed with `int()` (truncation toward zero), which introduces up to one full pixel of systematic offset. For a 10 m SOLRIS raster, this could place the sample point up to 10 m away from the true location.

**Fix:** Changed `int()` to `round()` for both `px` and `py` in `sample_solris` (in `gis_helpers.py`).

---

### Finding 7 — Division by zero in `generate_report`

**Problem:** `carbon_stock.py` and `biocapacity.py` divided by `total_area` to compute a per-hectare density in `generate_report`. If no features had valid area (e.g. an empty input), this raised a `ZeroDivisionError`.

**Fix:** Added a guard in both files: if `total_area > 0`, print the computed density; otherwise print `N/A`.

---

## Other Changes

### "Carbon sequestration" renamed to "carbon stock" throughout

The carbon ES was renamed from `sequestration` to `stock` to accurately reflect the stock-difference model being implemented. Changes:
- `scripts/ecosystem_services/carbon_sequestration.py` → `carbon_stock.py`
- All field name prefixes updated from `sequestration_` to `stock_`
- `GIS/carbon_sequestration_style.qml` → `GIS/carbon_stock_style.qml`, with internal field references updated to match
- `README.md` updated to reference `carbon_stock.py`
