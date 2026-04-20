# DZCIB Impact - Data Processing

Scripts and inputs for processing spatial land cover data into ecosystem service indicators for the DZCIB project. Outputs are written to Supabase and to local files under `data/`.

## Installation

These setup steps were validated on Windows PowerShell with Python 3.10.4. Run the commands below from the repository root.

### Operating system

- Validated on Windows PowerShell.
- The scripts are standard Python/GDAL code and should also work on macOS or Linux if the same dependencies are installed, but the commands below use Windows-style virtual environment activation.

### Programming language

- Python 3.10 or newer is required.
- Python 3.10 is the minimum because the scripts use modern type-union syntax such as `str | None`.

### Software dependencies

1. Install Python 3.10+.
2. Install GDAL so that both the Python bindings and the `ogr2ogr` command-line tool are available.
   - On Windows, the most reliable options are a QGIS install or OSGeo4W.
   - `ogr2ogr` must be on your `PATH`.
3. Create and activate a virtual environment.
4. Install the Python packages listed in `scripts/requirements.txt`.

Example setup from the repository root:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r scripts/requirements.txt
```

You can verify the non-Python GDAL dependency with:

```powershell
ogr2ogr --version
```

### Non-standard hardware or external resources

The code does not require special hardware such as a GPU, but it does require external data/services:

- A Supabase project and database connection string.
- The SOLRIS 3.0 raster downloaded from the Ontario Ministry of Natural Resources.
- A study-area boundary GeoJSON.
- For `potential_calculator.py`, a land-cover-change raster that marks the area to change.
- Enough local disk space and RAM to work with the SOLRIS raster and derived GeoPackages.

### Typical install time on a current computer

- About 10-20 minutes if Python and GDAL are already installed.
- About 30-60+ minutes if you still need to install QGIS/OSGeo4W and download the GIS inputs.

## Setup

### 1. Create a Supabase project

Create a free project at [supabase.com](https://supabase.com). Once created, go to Project Settings -> Database and copy the direct connection string. It looks like:

```text
postgres://postgres:[password]@[host]:5432/postgres
```

### 2. Configure environment variables

Create a `.env` file in the repository root:

```text
SUPABASE_URL=postgres://postgres:[password]@[host]:5432/postgres
```

The scripts also load `scripts/.env` for backward compatibility, but the repository-root `.env` is the default location documented here.

### 3. Upload lookup tables to Supabase

Before running the processing scripts, upload the lookup CSVs:

```powershell
python scripts/database_helpers.py reindex
```

Re-run this any time `data/solris_lookup.csv` or `data/water_filtration_lookup.csv` changes.

## Repository data

### Included in this repository

| Path | Purpose |
| --- | --- |
| `data/solris_lookup.csv` | Master SOLRIS classification table and per-hectare model inputs. |
| `data/water_filtration_lookup.csv` | Water filtration lookup table keyed by SOLRIS class. |
| `GIS/carolinian_zone.geojson` | Example study-area boundary. |
| `GIS/DZCIB_Projects_SOLRIS.gpkg` | Example project-site GeoPackage for `site_calculator.py`. |
| `GIS/SOLRIS_Version_3_0/` | Reference PDFs about SOLRIS 3.0. |

### You must supply separately

| Required path | Notes |
| --- | --- |
| `GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif` | The main SOLRIS raster used by the scripts. The repository does not include the raster itself. |
| A change raster such as `path/to/change_area.tif` | Required by `potential_calculator.py`. The repository does not currently include a sample change raster. |

## Workflow

Run the scripts in this order.

### 1. Classify a study area

Clip the SOLRIS raster to a boundary, polygonize it, dissolve by SOLRIS code, and upload the result to Supabase.

```powershell
python scripts/classify_area.py `
    --geojson GIS/carolinian_zone.geojson `
    --table carolinian_zone_classified
```

Arguments:

| Argument | Description | Default |
| --- | --- | --- |
| `--tif` | SOLRIS raster | `GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif` |
| `--geojson` | Study-area boundary | `GIS/carolinian_zone.geojson` |
| `--output-gpkg` | Output GeoPackage | `GIS/carolinian_zone_classified.gpkg` |
| `--table` | Supabase table name | `carolinian_zone_classified` |

Supabase output: one row per SOLRIS code with `solris_code`, geometry, and `area_ha`.

### 2. Calculate ecosystem service values for the classified area

```powershell
python scripts/processor.py `
    --source-table carolinian_zone_classified `
    --study-area carolinian_zone
```

Arguments:

| Argument | Description | Default |
| --- | --- | --- |
| `--source-table` | Supabase table created by `classify_area.py` | `carolinian_zone_classified` |
| `--study-area` | Name used in output file and table names | `carolinian_zone` |

Outputs:

- Supabase table: `ecosystem_services_results_{study_area}`
- Local folder: `data/{study_area}/`

### 3. Calculate the impact of a land-cover-change raster

This script now prefers lookup tables from Supabase and falls back to the local CSV files if Supabase is not configured.

```powershell
python scripts/potential_calculator.py `
    --change-tif path/to/change_area.tif `
    --new-solris-code 90 `
    --geojson GIS/carolinian_zone.geojson
```

Arguments:

| Argument | Description | Default |
| --- | --- | --- |
| `--change-tif` | Raster whose non-zero pixels define the change area | required |
| `--new-solris-code` | Target SOLRIS code after the change | required |
| `--geojson` | Optional boundary used for clipping and rarity context | none |
| `--solris-tif` | SOLRIS raster | `GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif` |
| `--output-gpkg` | Output GeoPackage | `GIS/land_cover_change_impact.gpkg` |
| `--table` | Supabase table name | `land_cover_change_impact` |

Output fields include:

- `old_solris_code`
- `new_solris_code`
- `area_ha`
- `change_carbon_tc`
- `change_ssc_cad`
- `change_biocapacity_gha`
- `change_wf_value_cad`
- `change_aesthetic_score`

The landscape-level aesthetic-quality summary is recomputed from the fully updated landscape composition after all changes are applied.

### 4. Calculate ecosystem service deltas for project sites

This script modifies the GeoPackage in place, clears old output values on rerun, and then writes the current results.

```powershell
python scripts/site_calculator.py `
    --geodatabase GIS/DZCIB_Projects_SOLRIS.gpkg `
    --boundary-geojson GIS/carolinian_zone.geojson
```

Arguments:

| Argument | Description | Default |
| --- | --- | --- |
| `--geodatabase` | Input/output GeoPackage | required |
| `--boundary-geojson` | Optional boundary used for rarity context | none |
| `--layer` | Layer name inside the GeoPackage | first layer |
| `--solris-tif` | SOLRIS raster | `GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif` |
| `--supabase-table` | Supabase upload table name | layer name |

Restoration acreage columns are expected in acres:

| Column | Target SOLRIS code | Land cover |
| --- | --- | --- |
| `land_change_forest_acres` | 90 | Mixed forest |
| `land_change_wetlands_acres` | 160 | Wetland |
| `land_change_tallgrass_prairie_acres` | 81 | Tallgrass prairie |

Updated fields written per site:

- `solris_code`
- `change_carbon_tc`
- `change_ssc_cad`
- `change_biocapacity_gha`
- `change_wf_value_cad`
- `change_aesthetic_score`

If a feature cannot be sampled against the SOLRIS raster, the script leaves the derived output fields blank for that feature instead of preserving stale values from an earlier run.

## Database helpers

### Reindex lookup tables

```powershell
python scripts/database_helpers.py reindex
python scripts/database_helpers.py reindex `
    --solris-csv data/solris_lookup.csv `
    --water-csv data/water_filtration_lookup.csv
```

### Export a Supabase table to GeoPackage

```powershell
python scripts/database_helpers.py export --table dzcib_projects_solris
python scripts/database_helpers.py export `
    --table dzcib_projects_solris `
    --output GIS/projects.gpkg
```

## Adding a new ecosystem service

Create a new Python module under `scripts/ecosystem_services/` with a class whose name ends in `Processor`. The class must define:

- `process(area_df, solris_df, wf_df=None)`
- `compute_change(area_ha, old_vals, new_vals, context_areas, old_code, new_code, **kwargs)`
- `FOLDER_NAME`
- `CSV_COLS`
- `MERGE_COLS`
- `CHANGE_FIELDS`

The processors are auto-discovered by `processor.py`, `potential_calculator.py`, and `site_calculator.py`.

## Folder structure

```text
scripts/
  classify_area.py
  database_helpers.py
  processor.py
  potential_calculator.py
  site_calculator.py
  lookup_support.py
  runtime_support.py
  ecosystem_services/
data/
  solris_lookup.csv
  water_filtration_lookup.csv
GIS/
  carolinian_zone.geojson
  DZCIB_Projects_SOLRIS.gpkg
  SOLRIS_Version_3_0/
```
