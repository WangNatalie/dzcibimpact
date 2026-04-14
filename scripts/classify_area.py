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
import tempfile

from dotenv import load_dotenv, find_dotenv
from osgeo import gdal, ogr, osr
from gis_helpers import clip_raster_to_geojson
from database_helpers import upload_gpkg_to_supabase

load_dotenv(find_dotenv())

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
        clip_raster_to_geojson(args.tif, args.geojson, clipped_tif)
        print(f"  → {clipped_tif}")

        print("\nSteps 2+3: Polygonize → dissolve by solris_code + area_ha (/vsimem/)")
        step_polygonize_and_dissolve(clipped_tif, args.output_gpkg)
        print(f"  → {args.output_gpkg}")

    print("\nStep 4: Upload to Supabase")
    upload_gpkg_to_supabase(args.output_gpkg, _OUTPUT_LAYER, args.table, geometry_type="MULTIPOLYGON")

    print("\nDone.")


if __name__ == "__main__":
    main()
