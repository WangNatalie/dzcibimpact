#!/usr/bin/env python3
"""
Land Cover Classification Processing Pipeline
Uses GDAL to apply SOLRIS land cover classification to a study area.
Requires a .tif raster layer of SOLRIS 3.0 and a GeoJSON mask layer of the study area.

Steps:
    1. Clip raster to GeoJSON mask               (gdal.Warp)
    2. Polygonize clipped raster to multipart     (gdal.Polygonize → GPKG)
       polygons, field "solris_code"
    3. Upload GeoPackage to Supabase / PostGIS    (ogr2ogr)

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

from dotenv import load_dotenv
from osgeo import gdal, ogr, osr

load_dotenv()

gdal.UseExceptions()
ogr.UseExceptions()

_OUTPUT_LAYER = "solris_classified"


# ── Argument parsing ──────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Clip, polygonize, and upload a SOLRIS raster."
    )
    parser.add_argument(
        "--tif",
        default="GIS/SOLRIS_Version_3_0/Solris_Version_3_0_LAMBERT.tif",
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


# ── Step 1: Clip SOLRIS land classification layer to the study area boundary ────

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


# ── Step 2: Convert features into polygons for data analysis ─────────────────

def step_polygonize(clipped_tif: str, output_gpkg: str) -> None:
    """
    Convert the raster layer to a vector polygon layer using gdal.Polygonize, 
    storing the SOLRIS code for each feature in the 'solris_code' field. 
    """
    src_ds = gdal.Open(clipped_tif, gdal.GA_ReadOnly)
    if src_ds is None:
        sys.exit(f"Error: could not open clipped raster: {clipped_tif}")
    src_band = src_ds.GetRasterBand(1)

    drv = ogr.GetDriverByName("GPKG")
    if os.path.exists(output_gpkg):
        drv.DeleteDataSource(output_gpkg)
    dst_ds = drv.CreateDataSource(output_gpkg)

    srs = osr.SpatialReference()
    srs.ImportFromWkt(src_ds.GetProjection())

    dst_layer = dst_ds.CreateLayer(
        _OUTPUT_LAYER,
        srs=srs,
        geom_type=ogr.wkbPolygon,
        options=["GEOMETRY_NAME=geom"],
    )
    dst_layer.CreateField(ogr.FieldDefn("solris_code", ogr.OFTInteger))
    field_idx = dst_layer.GetLayerDefn().GetFieldIndex("solris_code")

    mask_band = src_band.GetMaskBand()

    err = gdal.Polygonize(
        src_band, mask_band, dst_layer, field_idx,
        [], callback=gdal.TermProgress_nocb,
    )
    if err != gdal.CE_None:
        sys.exit("Error: gdal.Polygonize failed.")

    dst_ds.FlushCache()
    dst_ds = None
    src_ds = None


# ── Step 3: Upload to Supabase ────────────────────────────────────────────────

def step_upload_to_supabase(gpkg_path: str, table_name: str) -> None:
    """Upload the land classified data layer as a GeoPackage to Supabase via ogr2ogr."""
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

    if not os.path.exists(args.tif):
        sys.exit(f"Error: TIF not found: {args.tif}")
    if not os.path.exists(args.geojson):
        sys.exit(f"Error: GeoJSON not found: {args.geojson}")

    os.makedirs(os.path.dirname(os.path.abspath(args.output_gpkg)), exist_ok=True)

    with tempfile.TemporaryDirectory() as tmpdir:

        print("\nStep 1: Clip raster by mask layer (gdal.Warp)")
        clipped_tif = os.path.join(tmpdir, "clipped.tif")
        step_clip(args.tif, args.geojson, clipped_tif)
        print(f"  → {clipped_tif}")

        print("\nStep 2: Polygonize raster → multipart vector (gdal.Polygonize)")
        step_polygonize(clipped_tif, args.output_gpkg)
        print(f"  → {args.output_gpkg}")

    print("\nStep 3: Upload to Supabase")
    step_upload_to_supabase(args.output_gpkg, args.table)

    print("\nDone.")


if __name__ == "__main__":
    main()
