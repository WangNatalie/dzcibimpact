#!/usr/bin/env python3
"""
Land Cover Classification Processing Pipeline
Uses GDAL to apply SOLRIS land cover classification to a study area.
Requires a .tif raster layer of SOLRIS 3.0 and a GeoJSON mask layer of the study area.

Steps:
    1. Clip raster to GeoJSON mask                    (gdal.Warp)
    2. Polygonize clipped raster to /vsimem/          (gdal.Polygonize, in-memory)
       with field "solris_code"
    3. Dissolve by solris_code + compute area_ha      (gdal.VectorTranslate → GPKG)
       using SQLite ST_Union GROUP BY
    4. Upload GeoPackage to Supabase / PostGIS        (ogr2ogr)

Requirements:
    pip install gdal python-dotenv

    ogr2ogr must be on PATH (ships with GDAL / QGIS).

Usage:
    python classify_area.py
    python classify_area.py --tif path/to/raster.tif --geojson path/to/mask.geojson
    python classify_area.py --output-gpkg GIS/out.gpkg --table my_table

Environment variables (.env):
    SUPABASE_URL   PostgreSQL connection string, e.g.
                   postgres://user:password@host:port/dbname
"""

import os
import sys
import argparse
import subprocess
import tempfile

from osgeo import gdal, ogr, osr
from runtime_support import ensure_parent_dir, load_project_dotenv, resolve_repo_path

load_project_dotenv()

gdal.UseExceptions()
ogr.UseExceptions()

_OUTPUT_LAYER = "solris_classified"


# ── Argument parsing ──────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Clip, polygonize, dissolve, and upload a SOLRIS raster."
    )
    parser.add_argument(
        "--tif",
        default="GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif",
        help="Input SOLRIS raster layer (.tif)  [default: %(default)s]",
    )
    parser.add_argument(
        "--geojson",
        default="GIS/carolinian_zone.geojson",
        help="Study area mask layer (.geojson)  [default: %(default)s]",
    )
    parser.add_argument(
        "--output-gpkg",
        default="GIS/carolinian_zone_classified.gpkg",
        help="Save to geopackage path  [default: %(default)s]",
    )
    parser.add_argument(
        "--table",
        default="carolinian_zone_classified",
        help="Supabase table name  [default: %(default)s]",
    )
    return parser.parse_args()


# ── Step 1: Clip SOLRIS land classification layer to the study area boundary ─

def step_clip(tif_path: str, geojson_path: str, output_path: str) -> None:
    """Clip the SOLRIS raster layer to the GeoJSON of the study area boundary using gdal.Warp."""
    warp_options = gdal.WarpOptions(
        cutlineDSName=geojson_path,
        cropToCutline=True,
        dstNodata=0,
        format="GTiff",
        creationOptions=["COMPRESS=LZW", "TILED=YES"],
        multithread=True,
        warpOptions=["NUM_THREADS=ALL_CPUS"],
    )
    ds = gdal.Warp(output_path, tif_path, options=warp_options)
    if ds is None:
        sys.exit("Error: gdal.Warp (clip) returned None — check inputs.")
    ds.FlushCache()
    ds = None


# ── Steps 2 + 3: Polygonize → dissolve (in-memory, no intermediate file) ─────

_VSIMEM_POLYGONIZED = "/vsimem/polygonized.gpkg"

def step_polygonize_and_dissolve(clipped_tif: str, output_gpkg: str) -> None:
    """
    1. Polygonize the clipped raster into individual polygons stored in GDAL's
       virtual memory filesystem (/vsimem/) — no intermediate file on disk.
    2. Dissolve by solris_code using gdal.VectorTranslate with a SQLite
       ST_Union GROUP BY query, writing one MultiPolygon + area_ha per class
       directly to the output GeoPackage (~30 rows for SOLRIS 3.0).

    Requires GDAL built with SpatiaLite support (standard in QGIS distributions).
    """
    # ── Polygonize to /vsimem/ ────────────────────────────────────────────────
    src_ds = gdal.Open(clipped_tif, gdal.GA_ReadOnly)
    if src_ds is None:
        sys.exit(f"Error: could not open clipped raster: {clipped_tif}")
    src_band = src_ds.GetRasterBand(1)

    drv = ogr.GetDriverByName("GPKG")
    if gdal.VSIStatL(_VSIMEM_POLYGONIZED):    # clear any previous run
        gdal.Unlink(_VSIMEM_POLYGONIZED)
    mem_ds = drv.CreateDataSource(_VSIMEM_POLYGONIZED)

    srs = osr.SpatialReference()
    srs.ImportFromWkt(src_ds.GetProjection())

    mem_layer = mem_ds.CreateLayer(
        _OUTPUT_LAYER,
        srs=srs,
        geom_type=ogr.wkbPolygon,
        options=["GEOMETRY_NAME=geom"],
    )
    mem_layer.CreateField(ogr.FieldDefn("solris_code", ogr.OFTInteger))
    field_idx = mem_layer.GetLayerDefn().GetFieldIndex("solris_code")

    err = gdal.Polygonize(
        src_band, src_band.GetMaskBand(), mem_layer, field_idx,
        [], callback=gdal.TermProgress_nocb,
    )
    if err != gdal.CE_None:
        sys.exit("Error: gdal.Polygonize failed.")

    mem_ds.FlushCache()
    mem_ds = None
    src_ds = None

    # ── Dissolve in-memory using SQLite ST_Union GROUP BY ─────────────────────
    if os.path.exists(output_gpkg):
        ogr.GetDriverByName("GPKG").DeleteDataSource(output_gpkg)

    result = gdal.VectorTranslate(
        output_gpkg,
        _VSIMEM_POLYGONIZED,
        format="GPKG",
        SQLStatement=f"""
            SELECT solris_code,
                   ST_Union(geom)                AS geom,
                   SUM(ST_Area(geom)) / 10000.0  AS area_ha
            FROM   {_OUTPUT_LAYER}
            WHERE  solris_code IS NOT NULL
              AND  solris_code != 0
            GROUP  BY solris_code
        """,
        SQLDialect="SQLite",
        layerName=_OUTPUT_LAYER,
        geometryType="MULTIPOLYGON",
    )
    if result is None:
        sys.exit("Error: gdal.VectorTranslate (dissolve) failed.")
    result.FlushCache()
    result = None

    gdal.Unlink(_VSIMEM_POLYGONIZED)           # free virtual memory


# ── Step 4: Upload to Supabase ────────────────────────────────────────────────

def step_upload_to_supabase(gpkg_path: str, table_name: str) -> None:
    """Upload the dissolved classified data layer as a GeoPackage to Supabase via ogr2ogr."""
    supabase_url = os.getenv("SUPABASE_URL")
    if not supabase_url:
        print("  Skipping upload: SUPABASE_URL is not set in .env.")
        return

    # Append statement_timeout=0 so the session has no timeout limit.
    # Supabase's default timeout will cancel large COPY/INSERT operations.
    separator = "&" if "?" in supabase_url else "?"
    pg_conn = f"PG:{supabase_url}{separator}options=-c%20statement_timeout%3D0"

    cmd = [
        "ogr2ogr",
        "-f", "PostgreSQL",
        pg_conn,
        gpkg_path,
        _OUTPUT_LAYER,
        "-nln", table_name,
        "-nlt", "MULTIPOLYGON",
        # Let Postgres assign its own FIDs; reusing the source FID causes insert errors
        "-unsetFid",
        "-overwrite",
        "-progress",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"  Uploaded to Supabase table '{table_name}' successfully.")
    else:
        print(f"  ogr2ogr upload failed (exit {result.returncode}):")
        if result.stderr:
            print(result.stderr)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    tif_path = resolve_repo_path(args.tif)
    geojson_path = resolve_repo_path(args.geojson)
    output_gpkg = ensure_parent_dir(args.output_gpkg)

    if not tif_path.exists():
        sys.exit(f"Error: TIF not found: {tif_path}")
    if not geojson_path.exists():
        sys.exit(f"Error: GeoJSON not found: {geojson_path}")

    with tempfile.TemporaryDirectory() as tmpdir:

        print("\nStep 1: Clip raster by mask layer (gdal.Warp)")
        clipped_tif = os.path.join(tmpdir, "clipped.tif")
        step_clip(str(tif_path), str(geojson_path), clipped_tif)
        print(f"  → {clipped_tif}")

        print("\nSteps 2+3: Polygonize → dissolve by solris_code + area_ha (/vsimem/)")
        step_polygonize_and_dissolve(clipped_tif, str(output_gpkg))
        print(f"  → {output_gpkg}")

    print("\nStep 4: Upload to Supabase")
    step_upload_to_supabase(str(output_gpkg), args.table)

    print("\nDone.")


if __name__ == "__main__":
    main()
