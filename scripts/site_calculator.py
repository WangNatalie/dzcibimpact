#!/usr/bin/env python3
"""
Site-level ecosystem service change calculator.

Samples a baseline SOLRIS class for each project feature, computes aggregated
ecosystem-service deltas for the requested restoration acreage fields, updates
the GeoPackage in place, and optionally uploads the result to Supabase.
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
from runtime_support import load_project_dotenv, resolve_optional_repo_path, resolve_repo_path

load_project_dotenv()

gdal.UseExceptions()
ogr.UseExceptions()

_ACRES_TO_HA = 0.404686

LAND_CHANGE_TYPES = [
    ("land_change_forest_acres", 90),
    ("land_change_wetlands_acres", 160),
    ("land_change_tallgrass_prairie_acres", 81),
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compute per-site ecosystem service deltas from land change area columns."
    )
    parser.add_argument(
        "--geodatabase",
        required=True,
        help="Path to the input GeoPackage with project site features.",
    )
    parser.add_argument(
        "--layer",
        default=None,
        help="Layer name within the GeoPackage. Defaults to the first layer.",
    )
    parser.add_argument(
        "--solris-tif",
        default="GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif",
        help="SOLRIS raster [default: %(default)s]",
    )
    parser.add_argument(
        "--boundary-geojson",
        default=None,
        help="Optional GeoJSON boundary used for rarity context.",
    )
    parser.add_argument(
        "--supabase-table",
        default=None,
        help="Supabase table name to upload results to. Defaults to the layer name.",
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


def sample_solris(
    x: float,
    y: float,
    src_srs: osr.SpatialReference,
    solris_ds: gdal.Dataset,
    solris_srs: osr.SpatialReference,
) -> int | None:
    if not src_srs.IsSame(solris_srs):
        transform = osr.CoordinateTransformation(src_srs, solris_srs)
        x, y, _ = transform.TransformPoint(x, y)

    gt = solris_ds.GetGeoTransform()
    px = int((x - gt[0]) / gt[1])
    py = int((y - gt[3]) / gt[5])

    if px < 0 or py < 0 or px >= solris_ds.RasterXSize or py >= solris_ds.RasterYSize:
        return None

    value = int(solris_ds.GetRasterBand(1).ReadAsArray(px, py, 1, 1)[0][0])
    return value if value != 0 else None


def _ensure_field(layer: ogr.Layer, name: str, field_type: int) -> None:
    if layer.GetLayerDefn().GetFieldIndex(name) == -1:
        layer.CreateField(ogr.FieldDefn(name, field_type))


def _unset_field(feature: ogr.Feature, name: str) -> None:
    field_idx = feature.GetFieldIndex(name)
    if field_idx != -1:
        feature.UnsetField(field_idx)


def upload_to_supabase(gpkg_path: str, layer_name: str, table_name: str) -> None:
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
        layer_name,
        "-nln",
        f"public.{table_name}",
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
    geodatabase = resolve_repo_path(args.geodatabase)
    solris_tif = resolve_repo_path(args.solris_tif)
    boundary_geojson = resolve_optional_repo_path(args.boundary_geojson)

    if not geodatabase.exists():
        sys.exit(f"Error: geodatabase not found: {geodatabase}")
    if not solris_tif.exists():
        sys.exit(f"Error: SOLRIS TIF not found: {solris_tif}")
    if boundary_geojson and not boundary_geojson.exists():
        sys.exit(f"Error: boundary GeoJSON not found: {boundary_geojson}")

    print("Loading ecosystem service lookup tables...")
    lookup, lookup_source = load_lookup_dict(prefer_supabase=True, require_supabase=False)
    print(f"  Loaded {len(lookup)} SOLRIS classes from {lookup_source}.")

    for _, new_code in LAND_CHANGE_TYPES:
        if new_code not in lookup:
            sys.exit(
                f"Error: target SOLRIS code {new_code} not found in lookup. "
                f"Available codes: {sorted(lookup.keys())}"
            )

    processors = discover_processors()
    output_fields = []
    for cls in processors:
        for field_name in cls.CHANGE_FIELDS:
            if field_name not in output_fields:
                output_fields.append(field_name)

    if boundary_geojson:
        print(f"\nComputing SOLRIS composition within boundary: {boundary_geojson}")
        context_areas = compute_solris_areas(str(solris_tif), str(boundary_geojson))
    else:
        print("\nNo boundary provided - using full SOLRIS raster for rarity context...")
        context_areas = compute_solris_areas(str(solris_tif))
    print(f"  Found {len(context_areas)} SOLRIS classes.")

    change_configs = [
        (col, new_code, es_values(lookup, new_code))
        for col, new_code in LAND_CHANGE_TYPES
    ]

    print("Opening SOLRIS raster...")
    solris_ds = gdal.Open(str(solris_tif), gdal.GA_ReadOnly)
    if solris_ds is None:
        sys.exit(f"Error: cannot open SOLRIS TIF: {solris_tif}")
    solris_srs = osr.SpatialReference()
    solris_srs.ImportFromWkt(solris_ds.GetProjection())
    solris_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    print(f"Opening geodatabase: {geodatabase}")
    gdb_ds = ogr.Open(str(geodatabase), 1)
    if gdb_ds is None:
        sys.exit(f"Error: cannot open geodatabase: {geodatabase}")

    layer = gdb_ds.GetLayerByName(args.layer) if args.layer else gdb_ds.GetLayer(0)
    if layer is None:
        sys.exit(f"Error: layer '{args.layer}' not found in geodatabase.")
    print(f"  Layer: {layer.GetName()}  ({layer.GetFeatureCount()} features)")

    layer_srs = layer.GetSpatialRef()
    if layer_srs is None:
        sys.exit("Error: point layer has no spatial reference defined.")
    layer_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    _ensure_field(layer, "solris_code", ogr.OFTInteger)
    for field_name in output_fields:
        _ensure_field(layer, field_name, ogr.OFTReal)

    layer.ResetReading()
    updated = 0
    skipped_no_geometry = 0
    skipped_unsampled = 0
    transitions = []

    for feat in layer:
        _unset_field(feat, "solris_code")
        for field_name in output_fields:
            _unset_field(feat, field_name)

        geom = feat.GetGeometryRef()
        if geom is None:
            layer.SetFeature(feat)
            skipped_no_geometry += 1
            continue

        geom_type = geom.GetGeometryType() & ~0x80000000
        if geom_type == ogr.wkbPoint:
            x, y = geom.GetX(), geom.GetY()
        else:
            centroid = geom.Centroid()
            x, y = centroid.GetX(), centroid.GetY()

        solris_code = sample_solris(x, y, layer_srs, solris_ds, solris_srs)
        if solris_code is None:
            layer.SetFeature(feat)
            skipped_unsampled += 1
            updated += 1
            continue

        feat.SetField("solris_code", solris_code)
        old_vals = es_values(lookup, solris_code)

        totals = {}
        aq_weighted_sum = 0.0
        aq_total_area = 0.0

        for col, new_code_val, new_vals in change_configs:
            raw = feat.GetField(col)
            if raw is None:
                continue

            area_ha = float(raw) * _ACRES_TO_HA
            if area_ha <= 0:
                continue

            transitions.append((solris_code, new_code_val, area_ha))
            for cls in processors:
                for field_name, value in cls.compute_change(
                    area_ha,
                    old_vals,
                    new_vals,
                    context_areas=context_areas,
                    old_code=solris_code,
                    new_code=new_code_val,
                ).items():
                    if field_name == "change_aesthetic_score":
                        aq_weighted_sum += value * area_ha
                        aq_total_area += area_ha
                    else:
                        totals[field_name] = totals.get(field_name, 0.0) + value

        if aq_total_area > 0:
            totals["change_aesthetic_score"] = aq_weighted_sum / aq_total_area

        for field_name, value in totals.items():
            feat.SetField(field_name, round(value, 6))

        layer.SetFeature(feat)
        updated += 1

    layer_name = layer.GetName()
    gdb_ds.FlushCache()
    gdb_ds = None
    solris_ds = None

    print(
        f"\nDone. Updated {updated} features, skipped {skipped_no_geometry} with no geometry, "
        f"and left {skipped_unsampled} unsampled features blank."
    )

    aq_before = landscape_aq(lookup, context_areas)
    aq_after = landscape_aq(lookup, apply_land_cover_changes(context_areas, transitions))
    area_label = str(boundary_geojson) if boundary_geojson else "full SOLRIS extent"
    print(f"\nLandscape aesthetic quality ({area_label}):")
    print(f"  Before: {aq_before:.3f}")
    print(f"  After:  {aq_after:.3f}")
    print(f"  Delta:  {aq_after - aq_before:+.3f}")

    table_name = args.supabase_table or layer_name
    print(f"\nUploading to Supabase table '{table_name}'...")
    upload_to_supabase(str(geodatabase), layer_name, table_name)


if __name__ == "__main__":
    main()
