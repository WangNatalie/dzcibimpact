#!/usr/bin/env python3
"""
Land cover change impact analysis pipeline.

Intersects a land-cover-change raster with the SOLRIS raster, polygonizes the
affected area, computes per-feature ecosystem-service deltas, writes a
GeoPackage, and optionally uploads it to Supabase.
"""

import argparse
import os
import subprocess
import sys
import tempfile

import numpy as np
from osgeo import gdal, ogr, osr

from ecosystem_services import discover_processors
from ecosystem_services.aesthetic_quality import apply_land_cover_changes, landscape_aq
from lookup_support import load_lookup_dict
from runtime_support import (
    ensure_parent_dir,
    load_project_dotenv,
    resolve_optional_repo_path,
    resolve_repo_path,
)

load_project_dotenv()

gdal.UseExceptions()
ogr.UseExceptions()

_OUTPUT_LAYER = "land_cover_change_impact"
_VSIMEM_MASKED = "/vsimem/masked_solris.tif"
_VSIMEM_POLYGONIZED = "/vsimem/polygonized.gpkg"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compute per-feature ecosystem service deltas for a land cover change layer."
    )
    parser.add_argument(
        "--change-tif",
        required=True,
        help="Raster marking areas of land cover change (non-zero pixels = change area).",
    )
    parser.add_argument(
        "--new-solris-code",
        required=True,
        type=int,
        help="SOLRIS code representing the new land cover class.",
    )
    parser.add_argument(
        "--solris-tif",
        default="GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif",
        help="SOLRIS raster [default: %(default)s]",
    )
    parser.add_argument(
        "--geojson",
        default=None,
        help="Optional GeoJSON boundary used to clip the change raster.",
    )
    parser.add_argument(
        "--output-gpkg",
        default="GIS/land_cover_change_impact.gpkg",
        help="Output GeoPackage path [default: %(default)s]",
    )
    parser.add_argument(
        "--table",
        default="land_cover_change_impact",
        help="Supabase table name [default: %(default)s]",
    )
    return parser.parse_args()


def es_values(lookup: dict, code: int) -> dict:
    return lookup.get(code, {})


def compute_solris_areas(solris_tif: str, geojson: str | None = None) -> dict:
    """Return {solris_code: area_ha} for all SOLRIS classes."""
    nodata_val = None
    if geojson is not None:
        with tempfile.TemporaryDirectory() as tmpdir:
            clipped = os.path.join(tmpdir, "solris_boundary.tif")
            opts = gdal.WarpOptions(
                cutlineDSName=geojson,
                cropToCutline=True,
                dstNodata=0,
                format="GTiff",
                resampleAlg=gdal.GRA_NearestNeighbour,
                multithread=True,
            )
            ds = gdal.Warp(clipped, solris_tif, options=opts)
            if ds is None:
                return {}
            arr = ds.GetRasterBand(1).ReadAsArray()
            gt = ds.GetGeoTransform()
            ds = None
    else:
        ds = gdal.Open(solris_tif, gdal.GA_ReadOnly)
        if ds is None:
            return {}
        band = ds.GetRasterBand(1)
        arr = band.ReadAsArray()
        gt = ds.GetGeoTransform()
        nodata_val = band.GetNoDataValue()
        ds = None

    pixel_area_ha = abs(gt[1] * gt[5]) / 10_000.0
    areas = {}
    for code in np.unique(arr):
        if code == 0:
            continue
        if nodata_val is not None and code == int(nodata_val):
            continue
        areas[int(code)] = int(np.sum(arr == code)) * pixel_area_ha
    return areas


def clip_to_geojson(change_tif: str, geojson: str, output: str) -> None:
    opts = gdal.WarpOptions(
        cutlineDSName=geojson,
        cropToCutline=True,
        dstNodata=255,
        format="GTiff",
        creationOptions=["COMPRESS=LZW", "TILED=YES"],
        multithread=True,
        warpOptions=["NUM_THREADS=ALL_CPUS"],
    )
    ds = gdal.Warp(output, change_tif, options=opts)
    if ds is None:
        sys.exit("Error: gdal.Warp (clip) returned None; check inputs.")
    ds.FlushCache()
    ds = None


def warp_solris_to_match(solris_tif: str, reference_tif: str, output: str) -> None:
    ref_ds = gdal.Open(reference_tif, gdal.GA_ReadOnly)
    if ref_ds is None:
        sys.exit(f"Error: cannot open reference raster: {reference_tif}")

    gt = ref_ds.GetGeoTransform()
    x_min = gt[0]
    y_max = gt[3]
    x_max = x_min + gt[1] * ref_ds.RasterXSize
    y_min = y_max + gt[5] * ref_ds.RasterYSize
    x_res = abs(gt[1])
    y_res = abs(gt[5])
    proj = ref_ds.GetProjection()
    ref_ds = None

    opts = gdal.WarpOptions(
        outputBounds=(x_min, y_min, x_max, y_max),
        xRes=x_res,
        yRes=y_res,
        dstSRS=proj,
        resampleAlg=gdal.GRA_NearestNeighbour,
        dstNodata=0,
        format="GTiff",
        multithread=True,
        warpOptions=["NUM_THREADS=ALL_CPUS"],
    )
    ds = gdal.Warp(output, solris_tif, options=opts)
    if ds is None:
        sys.exit("Error: gdal.Warp (SOLRIS resample) returned None; check inputs.")
    ds.FlushCache()
    ds = None


def create_masked_solris(solris_path: str, change_path: str) -> None:
    """Write a masked SOLRIS raster to /vsimem/."""
    solris_ds = gdal.Open(solris_path, gdal.GA_ReadOnly)
    change_ds = gdal.Open(change_path, gdal.GA_ReadOnly)

    solris_arr = solris_ds.GetRasterBand(1).ReadAsArray()
    change_arr = change_ds.GetRasterBand(1).ReadAsArray()
    change_nodata = change_ds.GetRasterBand(1).GetNoDataValue()

    if change_nodata is not None:
        mask = (change_arr != 0) & (change_arr != change_nodata)
    else:
        mask = change_arr != 0

    masked = np.where(mask, solris_arr, 0).astype(np.int32)

    drv = gdal.GetDriverByName("GTiff")
    if gdal.VSIStatL(_VSIMEM_MASKED):
        gdal.Unlink(_VSIMEM_MASKED)

    out_ds = drv.Create(
        _VSIMEM_MASKED,
        solris_ds.RasterXSize,
        solris_ds.RasterYSize,
        1,
        gdal.GDT_Int32,
    )
    out_ds.SetGeoTransform(solris_ds.GetGeoTransform())
    out_ds.SetProjection(solris_ds.GetProjection())
    out_band = out_ds.GetRasterBand(1)
    out_band.SetNoDataValue(0)
    out_band.WriteArray(masked)
    out_ds.FlushCache()
    out_ds = None
    solris_ds = None
    change_ds = None


def polygonize_masked(srs_wkt: str) -> ogr.DataSource:
    src_ds = gdal.Open(_VSIMEM_MASKED, gdal.GA_ReadOnly)
    if src_ds is None:
        sys.exit("Error: cannot open masked SOLRIS raster in /vsimem/")
    src_band = src_ds.GetRasterBand(1)

    drv = ogr.GetDriverByName("GPKG")
    if gdal.VSIStatL(_VSIMEM_POLYGONIZED):
        gdal.Unlink(_VSIMEM_POLYGONIZED)

    mem_ds = drv.CreateDataSource(_VSIMEM_POLYGONIZED)
    srs = osr.SpatialReference()
    srs.ImportFromWkt(srs_wkt)

    layer = mem_ds.CreateLayer(
        _OUTPUT_LAYER,
        srs=srs,
        geom_type=ogr.wkbPolygon,
        options=["GEOMETRY_NAME=geom"],
    )
    layer.CreateField(ogr.FieldDefn("old_solris_code", ogr.OFTInteger))
    field_idx = layer.GetLayerDefn().GetFieldIndex("old_solris_code")

    err = gdal.Polygonize(
        src_band,
        src_band.GetMaskBand(),
        layer,
        field_idx,
        [],
        callback=gdal.TermProgress_nocb,
    )
    if err != gdal.CE_None:
        sys.exit("Error: gdal.Polygonize failed.")

    mem_ds.FlushCache()
    src_ds = None
    return mem_ds


def _add_field(layer_defn, out_layer, name, field_type):
    if layer_defn.GetFieldIndex(name) == -1:
        out_layer.CreateField(ogr.FieldDefn(name, field_type))


def write_impact_gpkg(
    poly_ds: ogr.DataSource,
    output_gpkg: str,
    new_code: int,
    lookup: dict,
    srs_wkt: str,
    context_areas: dict | None = None,
) -> list[tuple[int, int, float]]:
    processors = discover_processors()
    new_vals = es_values(lookup, new_code)

    src_layer = poly_ds.GetLayer(_OUTPUT_LAYER)

    if os.path.exists(output_gpkg):
        ogr.GetDriverByName("GPKG").DeleteDataSource(output_gpkg)

    out_drv = ogr.GetDriverByName("GPKG")
    out_ds = out_drv.CreateDataSource(output_gpkg)
    srs = osr.SpatialReference()
    srs.ImportFromWkt(srs_wkt)

    out_layer = out_ds.CreateLayer(
        _OUTPUT_LAYER,
        srs=srs,
        geom_type=ogr.wkbMultiPolygon,
        options=["GEOMETRY_NAME=geom"],
    )

    defn = out_layer.GetLayerDefn()
    for name, ftype in (
        ("old_solris_code", ogr.OFTInteger),
        ("new_solris_code", ogr.OFTInteger),
        ("area_ha", ogr.OFTReal),
    ):
        _add_field(defn, out_layer, name, ftype)

    for cls in processors:
        for field_name in cls.CHANGE_FIELDS:
            _add_field(defn, out_layer, field_name, ogr.OFTReal)

    transitions = []
    out_layer.StartTransaction()
    src_layer.ResetReading()

    for feat in src_layer:
        old_code = feat.GetField("old_solris_code")
        if old_code is None or old_code == 0:
            continue

        geom = feat.GetGeometryRef()
        if geom is None:
            continue

        if geom.GetGeometryType() == ogr.wkbPolygon:
            multi = ogr.Geometry(ogr.wkbMultiPolygon)
            multi.AddGeometry(geom)
            geom = multi

        area_ha = geom.GetArea() / 10000.0
        old_vals = es_values(lookup, old_code)

        out_feat = ogr.Feature(out_layer.GetLayerDefn())
        out_feat.SetGeometry(geom)
        out_feat.SetField("old_solris_code", old_code)
        out_feat.SetField("new_solris_code", new_code)
        out_feat.SetField("area_ha", round(area_ha, 6))

        for cls in processors:
            for field_name, value in cls.compute_change(
                area_ha,
                old_vals,
                new_vals,
                context_areas=context_areas,
                old_code=old_code,
                new_code=new_code,
            ).items():
                out_feat.SetField(field_name, round(value, 6))

        out_layer.CreateFeature(out_feat)
        transitions.append((old_code, new_code, area_ha))

    out_layer.CommitTransaction()
    out_ds.FlushCache()
    out_ds = None

    return transitions


def upload_to_supabase(gpkg_path: str, table_name: str) -> None:
    supabase_url = os.getenv("SUPABASE_URL")
    if not supabase_url:
        print("  Skipping upload: SUPABASE_URL is not set in .env.")
        return

    separator = "&" if "?" in supabase_url else "?"
    pg_conn = f"PG:{supabase_url}{separator}options=-c%20statement_timeout%3D0"

    cmd = [
        "ogr2ogr",
        "-f",
        "PostgreSQL",
        pg_conn,
        gpkg_path,
        _OUTPUT_LAYER,
        "-nln",
        f"public.{table_name}",
        "-nlt",
        "MULTIPOLYGON",
        "-lco",
        "GEOMETRY_NAME=geom",
        "-lco",
        "FID=id",
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


def main():
    args = parse_args()
    change_tif = resolve_repo_path(args.change_tif)
    solris_tif = resolve_repo_path(args.solris_tif)
    geojson = resolve_optional_repo_path(args.geojson)
    output_gpkg = ensure_parent_dir(args.output_gpkg)

    if not change_tif.exists():
        sys.exit(f"Error: change TIF not found: {change_tif}")
    if not solris_tif.exists():
        sys.exit(f"Error: SOLRIS TIF not found: {solris_tif}")
    if geojson and not geojson.exists():
        sys.exit(f"Error: GeoJSON not found: {geojson}")

    print("\nLoading ecosystem service lookup tables...")
    lookup, lookup_source = load_lookup_dict(prefer_supabase=True, require_supabase=False)
    if args.new_solris_code not in lookup:
        sys.exit(
            f"Error: new SOLRIS code {args.new_solris_code} not found in lookup. "
            f"Available codes: {sorted(lookup.keys())}"
        )
    print(f"  Loaded {len(lookup)} SOLRIS classes from {lookup_source}.")

    if geojson:
        print(f"\nComputing SOLRIS composition within boundary: {geojson}")
        context_areas = compute_solris_areas(str(solris_tif), str(geojson))
    else:
        print("\nNo boundary provided - using full SOLRIS raster for rarity context...")
        context_areas = compute_solris_areas(str(solris_tif))
    print(f"  Found {len(context_areas)} SOLRIS classes.")

    with tempfile.TemporaryDirectory() as tmpdir:
        working_change_tif = str(change_tif)

        if geojson:
            print(f"\nStep 1b: Clipping change raster to GeoJSON: {geojson}")
            clipped_change = os.path.join(tmpdir, "change_clipped.tif")
            clip_to_geojson(working_change_tif, str(geojson), clipped_change)
            working_change_tif = clipped_change
            print(f"  -> {clipped_change}")

        print("\nStep 2a: Warping SOLRIS to match change raster...")
        resampled_solris = os.path.join(tmpdir, "solris_resampled.tif")
        warp_solris_to_match(str(solris_tif), working_change_tif, resampled_solris)
        print(f"  -> {resampled_solris}")

        print("\nStep 2b: Masking SOLRIS to change area pixels...")
        create_masked_solris(resampled_solris, working_change_tif)
        print(f"  -> {_VSIMEM_MASKED}")

        masked_ds = gdal.Open(_VSIMEM_MASKED, gdal.GA_ReadOnly)
        srs_wkt = masked_ds.GetProjection()
        masked_ds = None

        print("\nStep 2c: Polygonizing masked SOLRIS raster...")
        poly_ds = polygonize_masked(srs_wkt)
        src_count = poly_ds.GetLayer(_OUTPUT_LAYER).GetFeatureCount()
        print(f"  -> {src_count} polygons (before filtering zero/null codes)")

    print("\nStep 3+4: Computing ES delta fields and writing GeoPackage...")
    transitions = write_impact_gpkg(
        poly_ds,
        str(output_gpkg),
        args.new_solris_code,
        lookup,
        srs_wkt,
        context_areas=context_areas,
    )
    poly_ds = None

    if gdal.VSIStatL(_VSIMEM_MASKED):
        gdal.Unlink(_VSIMEM_MASKED)
    if gdal.VSIStatL(_VSIMEM_POLYGONIZED):
        gdal.Unlink(_VSIMEM_POLYGONIZED)

    print(f"  -> {output_gpkg}")

    aq_before = landscape_aq(lookup, context_areas)
    aq_after = landscape_aq(lookup, apply_land_cover_changes(context_areas, transitions))
    area_label = "boundary" if geojson else "full SOLRIS extent"
    print(f"\nLandscape aesthetic quality ({area_label}):")
    print(f"  Before: {aq_before:.3f}")
    print(f"  After:  {aq_after:.3f}")
    print(f"  Delta:  {aq_after - aq_before:+.3f}")

    print("\nStep 5: Uploading to Supabase...")
    upload_to_supabase(str(output_gpkg), args.table)

    print("\nDone.")


if __name__ == "__main__":
    main()
