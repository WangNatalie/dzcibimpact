# DZCIB Impact — Data Processing

Scripts and inputs for processing spatial land cover data into ecosystem service indicators for the DZCIB project. Outputs are written to Supabase.

---

## Setup

### 1. Create a Supabase project

Create a free project at [supabase.com](https://supabase.com). Once created, go to **Project Settings → Database** and copy the connection string (direct connection, not pooler). It will look like:

```
postgres://postgres:[password]@[host]:5432/postgres
```

### 2. Configure environment variables

Create a `.env` file in the repository root with the **URL of your Supabase database**:

```
SUPABASE_URL=postgres://postgres:[password]@[host]:5432/postgres
```

### 3. Upload lookup tables to Supabase

Before running any processing scripts, upload the ecosystem service lookup tables:

```bash
python scripts/database_helpers.py reindex
```

This uploads `data/solris_lookup.csv` and `data/water_filtration_lookup.csv` to Supabase. Re-run any time either CSV is edited.

### 4. Data files

The following GIS data files are required in the `GIS/` directory:


| File                                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                           |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GIS/SOLRIS_Version_3_0/`                     | SOLRIS 3.0 land cover raster (Lambert projection). The foundational input for all processing — each pixel is classified into one of ~30 land cover types. **Download from ArcGIS Online:** [https://www.arcgis.com/home/item.html?id=0279f65b82314121b5b5ec93d76bc6ba](https://www.arcgis.com/home/item.html?id=0279f65b82314121b5b5ec93d76bc6ba)                                                                                     |
| `GIS/carolinian_zone.geojson`                 | Study area boundary for the Carolinian Zone. Used to clip SOLRIS to the region of interest before computing ecosystem service values.                                                                                                                                                                                                                                                                                                 |
| `GIS/dataverse_files/Area_of_opportunity.tif` | Land cover change raster. Non-zero pixels mark areas identified as candidates for forest restoration. Used as input to `potential_calculator.py` to estimate the ecosystem service impact of restoring those areas to forest. **Download from Harvard Dataverse:** [https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/5D3SZI](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/5D3SZI) |
| `GIS/DZCIB_Projects_SOLRIS.gpkg`              | Point layer of DZCIB project sites. Each point represents a project location with columns for area (in acres) proposed to be restored to forest, wetland, or tallgrass prairie. Used as input to `site_calculator.py`.                                                                                                                                                                                                                |


### 5. Workflow

Run the scripts in this order:

1. **Classify your study area** — clip SOLRIS to a boundary and upload the land cover summary to Supabase:
  ```bash
   python scripts/classify_area.py --geojson GIS/carolinian_zone.geojson --table carolinian_zone_classified
  ```
2. **Calculate ecosystem service values** — compute ES values for each land cover class in the study area:
  ```bash
   python scripts/processor.py --source-table carolinian_zone_classified --study-area carolinian_zone
  ```
3. **Calculate potential ES impact of a land cover change** — given a raster marking a change area and a target land cover type, compute per-polygon ES deltas:
  ```bash
   python scripts/potential_calculator.py --change-tif GIS/dataverse_files/Area_of_opportunity.tif --new-solris-code 90 --geojson GIS/carolinian_zone.geojson
  ```
4. **Calculate ES deltas for project sites** — for each point in a project site layer, compute ES change values from the existing land cover to the restored habitat types:
  ```bash
   python scripts/site_calculator.py --geodatabase GIS/DZCIB_Projects_SOLRIS.gpkg --boundary-geojson GIS/carolinian_zone.geojson
  ```

### 6. Adding a new ecosystem service

To add a new ecosystem service, create a new Python file in `scripts/ecosystem_services/` with a class named `*Processor` (e.g. `FloodMitigationProcessor`) that implements:

- `process(area_df, solris_df, wf_df=None)` — compute ES values for a study area
- `compute_change(area_ha, old_vals, new_vals, context_areas, old_code, new_code, **kwargs)` — compute ES delta for a single land cover change
- `FOLDER_NAME`, `CSV_COLS`, `MERGE_COLS`, `CHANGE_FIELDS` class attributes

The new processor will be picked up automatically by `processor.py`, `potential_calculator.py`, and `site_calculator.py` — no changes to those scripts required.

---

## Folder Structure

```
scripts/
├── classify_area.py                # Clips SOLRIS raster to a study area boundary, polygonizes, and uploads to Supabase
├── processor.py                    # Computes ecosystem service valuations for a study area
├── potential_calculator.py         # Computes potential ES change for a land cover change raster
├── site_calculator.py              # Computes ES change per project site from restoration area inputs
├── database_helpers.py             # Upload lookup tables to Supabase (reindex) or export a table to GeoPackage (export)
├── ecosystem_services/
│   ├── __init__.py                 # Auto-discovery of *Processor classes
│   ├── biocapacity.py
│   ├── carbon_stock.py
│   ├── water_filtration.py
│   └── aesthetic_quality.py
data/
├── solris_lookup.csv               # Master SOLRIS classification + per-ha ES values
├── water_filtration_lookup.csv     # Wetland type → water filtration value ($/ha)
└── output/                         # CSV and report outputs from processor.py  
    └── {study_area}/
        ├── biocapacity/
        ├── carbon_stock/
        ├── water_filtration/
        ├── aesthetic_quality/
        └── ecosystem_services_report.csv
GIS/
├── SOLRIS_Version_3_0/             # SOLRIS 3.0 raster (Lambert)         [external download]
├── dataverse_files/                # Harvard Dataverse raster inputs     [external download]
├── carolinian_zone.geojson         # Study area boundary
├── DZCIB_Projects_SOLRIS.gpkg      # Project site point layer
└── output/                         # GeoPackage outputs                  
```

---

## Scripts

### `classify_area.py` — Study-Area Land Cover Classification

Clips the SOLRIS 3.0 raster to a study area boundary GeoJSON, polygonizes it, dissolves polygons by SOLRIS code with area in hectares, and uploads the result to Supabase. Run this first before `processor.py`.

```bash
python classify_area.py \
    [--tif         GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif] \
    [--geojson     GIS/carolinian_zone.geojson] \
    [--output-gpkg GIS/output/carolinian_zone_classified.gpkg] \
    [--table       carolinian_zone_classified]
```


| Argument        | Description             | Default                                                 |
| --------------- | ----------------------- | ------------------------------------------------------- |
| `--tif`         | SOLRIS 3.0 raster       | `GIS/SOLRIS_Version_3_0/Solris_Version_3_0_LAMBERT.tif` |
| `--geojson`     | Study area boundary     | `GIS/carolinian_zone.geojson`                           |
| `--output-gpkg` | Local GeoPackage output | `GIS/output/carolinian_zone_classified.gpkg`            |
| `--table`       | Supabase table name     | `carolinian_zone_classified`                            |


**Supabase output table:** `{table}` — one row per SOLRIS code with `solris_code`, geometry, and `area_ha`.

---

### `processor.py` — Study-Area ES Valuation Pipeline

Runs all ecosystem service processors against the area-level SOLRIS land class summary table of a study area, computes the value of each SOLRIS class for each ecosystem service, writes individual ecosystem service-level reports, and uploads a combined results table to Supabase. Lookup tables and the area land class summary table are fetched from Supabase at startup.

```bash
python processor.py \
    --source-table carolinian_zone_classified \
    --study-area   carolinian_zone
```


| Argument         | Description                                                                     | Default                      |
| ---------------- | ------------------------------------------------------------------------------- | ---------------------------- |
| `--source-table` | Supabase table with the area-level land summary (created by `classify_area.py`) | `carolinian_zone_classified` |
| `--study-area`   | Name of the study area, used to label output file names and Supabase table name | `carolinian_zone`            |


**Supabase output table:** `ecosystem_services_results_{study_area}`

**Local outputs** (written to `data/output/{study_area}/`):

- `biocapacity/biocapacity_results.csv` + text report
- `carbon_stock/carbon_stock_results.csv` + text report
- `water_filtration/water_filtration_results.csv` + text report
- `aesthetic_quality/aesthetic_quality_results.csv` + text report
- `ecosystem_services_report.csv` — combined summary table (aggregated by SOLRIS code)

---

### `potential_calculator.py` — Land Cover Change Raster Pipeline

Intersects a land-cover-change raster with SOLRIS to identify the existing land cover classification of the change area, then computes the per-feature delta for each ecosystem service value from the existing SOLRIS class to a target SOLRIS class. Outputs a GeoPackage and uploads it to Supabase.

```bash
python potential_calculator.py \
    --change-tif      GIS/dataverse_files/Area_of_opportunity.tif \
    --new-solris-code 90 \
    [--geojson        GIS/carolinian_zone.geojson] \
    [--solris-tif     GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif] \
    [--output-gpkg    GIS/output/land_cover_change_impact.gpkg] \
    [--table          land_cover_change_impact]
```


| Argument            | Description                                                                                                                 | Default                                    |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| `--change-tif`      | Raster (.tif) where non-zero pixels mark the land change area                                                               | required                                   |
| `--new-solris-code` | Target SOLRIS code after land cover change                                                                                  | required                                   |
| `--geojson`         | Boundary to clip the change raster before processing. If omitted, the full SOLRIS raster extent is used for rarity context. | —                                          |
| `--solris-tif`      | SOLRIS 3.0 raster                                                                                                           | `GIS/SOLRIS_Version_3_0/...`               |
| `--output-gpkg`     | Output GeoPackage path                                                                                                      | `GIS/output/land_cover_change_impact.gpkg` |
| `--table`           | Supabase table name                                                                                                         | `land_cover_change_impact`                 |


**Output GeoPackage fields** (one polygon per contiguous SOLRIS patch in the change area):


| Field                    | Description                              |
| ------------------------ | ---------------------------------------- |
| `old_solris_code`        | Existing SOLRIS class                    |
| `new_solris_code`        | Target SOLRIS class                      |
| `area_ha`                | Polygon area (ha)                        |
| `change_carbon_tc`       | Change in carbon stock (tC)              |
| `change_ssc_cad`         | Change in social cost of carbon ($ CAD)  |
| `change_biocapacity_gha` | Change in biocapacity (gha)              |
| `change_wf_value_cad`    | Change in water filtration value ($ CAD) |
| `change_aesthetic_score` | Change in aesthetic quality score        |


Always prints landscape-level area-weighted aesthetic quality before and after all changes.

---

### `site_calculator.py` — Per-Project-Site ES Delta Calculator

For each point feature in an input GeoPackage, samples the SOLRIS raster to assign a status-quo `solris_code` to the location, then computes ecosystem service change values from the site's existing land cover to the target land cover for each restoration habitat type (forest/wetland/prairie). Modifies the GeoPackage in-place and uploads results to Supabase.

Land change columns are read in **acres** and converted to hectares internally. Target SOLRIS codes:


| Column                                | Target SOLRIS code | Land cover        |
| ------------------------------------- | ------------------ | ----------------- |
| `land_change_forest_acres`            | 90                 | Mixed forest      |
| `land_change_wetlands_acres`          | 160                | Wetland           |
| `land_change_tallgrass_prairie_acres` | 81                 | Tallgrass prairie |


ES change fields are **aggregated across all habitat types** per site (summed, except aesthetic quality which is area-weighted averaged).

```bash
python site_calculator.py \
    --geodatabase       GIS/DZCIB_Projects_SOLRIS.gpkg \
    [--boundary-geojson GIS/carolinian_zone.geojson] \
    [--layer            layer_name] \
    [--solris-tif       GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif] \
    [--supabase-table   dzcib_projects_solris]
```


| Argument             | Description                                                                                                    | Default                      |
| -------------------- | -------------------------------------------------------------------------------------------------------------- | ---------------------------- |
| `--geodatabase`      | Input/output GeoPackage                                                                                        | required                     |
| `--boundary-geojson` | Boundary for landscape SOLRIS composition (rarity context). If omitted, the full SOLRIS raster extent is used. | —                            |
| `--layer`            | Layer name within the GeoPackage                                                                               | first layer                  |
| `--solris-tif`       | SOLRIS 3.0 raster                                                                                              | `GIS/SOLRIS_Version_3_0/...` |
| `--supabase-table`   | Supabase table to upload results to                                                                            | layer name                   |


**Added/updated fields per site:**


| Field                    | Description                                             |
| ------------------------ | ------------------------------------------------------- |
| `solris_code`            | SOLRIS code sampled at the site point                   |
| `change_carbon_tc`       | Total change in carbon stock (tC)                       |
| `change_ssc_cad`         | Total change in social cost of carbon ($ CAD)           |
| `change_biocapacity_gha` | Total change in biocapacity (gha)                       |
| `change_wf_value_cad`    | Total change in water filtration value ($ CAD)          |
| `change_aesthetic_score` | Area-weighted average change in aesthetic quality score |


Always prints landscape-level area-weighted aesthetic quality before and after all changes.

> **Modeling assumption:** Due to limitations in project boundary availbility, the existing land cover is sampled from the SOLRIS raster at the point's location and applied as the approximate baseline land class for the entire site. 

---

### `database_helpers.py` — Supabase Utilities

Two subcommands for syncing data with Supabase.

`**reindex`** — upload lookup CSVs to Supabase. Run after editing either CSV:

```bash
python database_helpers.py reindex
python database_helpers.py reindex \
    --solris-csv data/solris_lookup.csv \
    --water-csv  data/water_filtration_lookup.csv
```

`**export**` — pull a Supabase table to a local GeoPackage:

```bash
python database_helpers.py export --table dzcib_projects_solris
python database_helpers.py export --table dzcib_projects_solris --output GIS/projects.gpkg
```

---

### `ecosystem_services/` — Ecosystem Service Processor Classes

Each processor class exposes:

- `process(area_df, solris_df, wf_df=None)` — compute ES values for a study area (used by `processor.py`)
- `compute_change(area_ha, old_vals, new_vals, context_areas, old_code, new_code, **kwargs)` — return a dict of ES deltas for a single land cover change (used by `potential_calculator.py` and `site_calculator.py`)
- `FOLDER_NAME`, `CSV_COLS`, `MERGE_COLS`, `CHANGE_FIELDS` — class-level metadata

New processors placed in this directory are picked up automatically by all three pipelines — no changes to any other script required.

---

## Lookup CSVs

### `data/solris_lookup.csv`

Master classification table. Each row is one SOLRIS code.


| Column                          | Description                                              |
| ------------------------------- | -------------------------------------------------------- |
| `solris_code`                   | SOLRIS land cover code                                   |
| `solris_class`                  | Land cover name                                          |
| `biocapacity_category`          | Biocapacity land use classification                      |
| `biocapacity_conversion_factor` | gha/ha conversion factor                                 |
| `lulc_category`                 | Simplified land use/land cover category                  |
| `agc_tc_ha`                     | Above-ground carbon (tC/ha)                              |
| `bgc_tc_ha`                     | Below-ground carbon (tC/ha)                              |
| `soc_tc_ha`                     | Soil organic carbon (tC/ha)                              |
| `deoc_tc_ha`                    | Dead organic carbon (tC/ha)                              |
| `naturalness`                   | Naturalness index (0–1 scale, used in aesthetic quality) |
| `description`                   | Narrative description                                    |


### `data/water_filtration_lookup.csv`


| Column         | Description                                  |
| -------------- | -------------------------------------------- |
| `wetland_type` | Matches `solris_class` from SOLRIS lookup    |
| `value`        | Per-hectare water filtration benefit ($ CAD) |


