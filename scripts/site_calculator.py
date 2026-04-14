#!/usr/bin/env python3
"""
Site-level Ecosystem Service Change Calculator

For each point in an input geodatabase:
  1. Samples the SOLRIS raster to assign a solris_code field
  2. For each land change column (forest, wetland, tallgrass prairie),
     converts acres to hectares and computes per-ES delta values using
     the same lookup and processor infrastructure as potential_calculator.py

Target SOLRIS codes for each change type:
  - land_change_forest_acres            → 90
  - land_change_wetlands_acres          → 160
  - land_change_tallgrass_prairie_acres → 81

Output fields are prefixed by land change type, e.g. forest_change_carbon_tc.
The geodatabase is modified in-place.

Modeling assumption: a single SOLRIS pixel is sampled at the point location and
used as the baseline land cover class for all restoration acreage columns on that
feature. Sites that span multiple SOLRIS classes will have approximate ES deltas.

Usage:
    python site_calculator.py \
        --geodatabase  GIS/DZCIB_Project_Data.gpkg \
        [--layer       project_sites] \
        [--solris-tif  GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif]
"""

import os
import sys
import argparse

from dotenv import load_dotenv, find_dotenv
from osgeo import gdal, ogr, osr
from gis_helpers import compute_solris_areas, sample_solris
from database_helpers import supabase_engine, load_es_lookup, upload_gpkg_to_supabase
from ecosystem_services import discover_processors
from ecosystem_services.aesthetic_quality import landscape_aq

load_dotenv(find_dotenv())

gdal.UseExceptions()
ogr.UseExceptions()

# ── Constants ─────────────────────────────────────────────────────────────────

_ACRES_TO_HA       = 0.404686

# (input column, target SOLRIS code)
LAND_CHANGE_TYPES = [
    ("land_change_forest_acres",            90),
    ("land_change_wetlands_acres",          160),
    ("land_change_tallgrass_prairie_acres", 81),
]


# ── CLI ───────────────────────────────────────────────────────────────────────

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
        help="Layer name within the geodatabase. Defaults to the first layer.",
    )
    parser.add_argument(
        "--solris-tif",
        default="GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif",
        help="SOLRIS 3.0 raster [default: GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif]",
    )
    parser.add_argument(
        "--boundary-geojson",
        default=None,
        help="GeoJSON boundary used to compute landscape-level SOLRIS composition for rarity scoring. "
             "If omitted, the full SOLRIS raster is used.",
    )
    parser.add_argument(
        "--supabase-table",
        default=None,
        help="Supabase table name to upload results to. Defaults to the layer name.",
    )
    return parser.parse_args()


def es_values(lookup: dict, code: int) -> dict:
    return lookup.get(code, {})


# ── Field helpers ─────────────────────────────────────────────────────────────

def _ensure_field(layer: ogr.Layer, name: str, field_type: int) -> None:
    if layer.GetLayerDefn().GetFieldIndex(name) == -1:
        layer.CreateField(ogr.FieldDefn(name, field_type))


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    if not os.path.exists(args.geodatabase):
        sys.exit(f"Error: geodatabase not found: {args.geodatabase}")
    if not os.path.exists(args.solris_tif):
        sys.exit(f"Error: SOLRIS TIF not found: {args.solris_tif}")
    print("Loading ecosystem service lookup tables from Supabase...")
    engine = supabase_engine()
    try:
        lookup = load_es_lookup(engine)
    finally:
        engine.dispose()
    print(f"  Loaded {len(lookup)} SOLRIS classes.")

    for _, new_code in LAND_CHANGE_TYPES:
        if new_code not in lookup:
            sys.exit(
                f"Error: target SOLRIS code {new_code} not found in lookup. "
                f"Available codes: {sorted(lookup.keys())}"
            )

    processors = discover_processors()

    if args.boundary_geojson:
        if not os.path.exists(args.boundary_geojson):
            sys.exit(f"Error: boundary GeoJSON not found: {args.boundary_geojson}")
        print(f"\nComputing SOLRIS composition within boundary: {args.boundary_geojson}")
        context_areas = compute_solris_areas(args.solris_tif, args.boundary_geojson)
    else:
        print("\nNo boundary provided — using full SOLRIS raster for rarity context...")
        context_areas = compute_solris_areas(args.solris_tif)
    print(f"  Found {len(context_areas)} SOLRIS classes.")

    # Pre-fetch new_vals for each land change type
    change_configs = [
        (col, new_code, es_values(lookup, new_code))
        for col, new_code in LAND_CHANGE_TYPES
    ]

    print("Opening SOLRIS raster...")
    solris_ds = gdal.Open(args.solris_tif, gdal.GA_ReadOnly)
    if solris_ds is None:
        sys.exit(f"Error: cannot open SOLRIS TIF: {args.solris_tif}")
    solris_srs = osr.SpatialReference()
    solris_srs.ImportFromWkt(solris_ds.GetProjection())
    solris_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    print(f"Opening geodatabase: {args.geodatabase}")
    gdb_ds = ogr.Open(args.geodatabase, 1)  # 1 = update
    if gdb_ds is None:
        sys.exit(f"Error: cannot open geodatabase: {args.geodatabase}")

    layer = gdb_ds.GetLayerByName(args.layer) if args.layer else gdb_ds.GetLayer(0)
    if layer is None:
        sys.exit(f"Error: layer '{args.layer}' not found in geodatabase.")
    print(f"  Layer: {layer.GetName()}  ({layer.GetFeatureCount()} features)")

    layer_srs = layer.GetSpatialRef()
    if layer_srs is None:
        sys.exit("Error: point layer has no spatial reference defined.")
    layer_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    # Add output fields
    _ensure_field(layer, "solris_code", ogr.OFTInteger)
    for cls in processors:
        for field_name in cls.CHANGE_FIELDS:
            _ensure_field(layer, field_name, ogr.OFTReal)

    # Process features
    layer.ResetReading()
    updated = 0
    skipped = 0
    skipped_no_solris = 0
    net_delta = {}  # {solris_code: net area_ha change} across all features and change types

    for feat in layer:
        geom = feat.GetGeometryRef()
        if geom is None:
            skipped += 1
            continue

        geom_type = geom.GetGeometryType() & ~0x80000000  # strip Z/M flags
        if geom_type == ogr.wkbPoint:
            x, y = geom.GetX(), geom.GetY()
        else:
            centroid = geom.Centroid()
            x, y = centroid.GetX(), centroid.GetY()

        # Clear all output fields before recomputing so stale values from
        # previous runs don't persist if inputs change or sampling fails.
        feat.SetField("solris_code", None)
        for cls in processors:
            for field_name in cls.CHANGE_FIELDS:
                feat.SetField(field_name, None)

        solris_code = sample_solris(x, y, layer_srs, solris_ds, solris_srs)
        if solris_code is not None:
            feat.SetField("solris_code", solris_code)
        else:
            has_change = any(feat.GetField(col) is not None for col, _, _ in change_configs)
            if has_change:
                skipped_no_solris += 1
            layer.SetFeature(feat)
            updated += 1
            continue

        old_vals = es_values(lookup, solris_code)

        totals = {}
        aq_weighted_sum = 0.0
        aq_total_area = 0.0

        for col, new_code_val, new_vals in change_configs:
            raw = feat.GetField(col)
            if raw is None:
                continue
            area_ha = float(raw) * _ACRES_TO_HA
            if solris_code is not None:
                net_delta[solris_code] = net_delta.get(solris_code, 0.0) - area_ha
            net_delta[new_code_val] = net_delta.get(new_code_val, 0.0) + area_ha
            for cls in processors:
                for field_name, value in cls.compute_change(
                    area_ha, old_vals, new_vals,
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

    if skipped_no_solris:
        print(f"  Warning: {skipped_no_solris} feature(s) had land change areas but no SOLRIS sample — ES deltas left null.")
    print(f"\nDone. Updated {updated} features, skipped {skipped} (no geometry).")

    context_areas_after = dict(context_areas)
    for code, delta in net_delta.items():
        context_areas_after[code] = max(0.0, context_areas_after.get(code, 0.0) + delta)

    area_label = args.boundary_geojson or "full SOLRIS extent"
    aq_before = landscape_aq(lookup, context_areas)
    aq_after = landscape_aq(lookup, context_areas_after)
    print(f"\nLandscape aesthetic quality ({area_label}):")
    print(f"  Before: {aq_before:.3f}")
    print(f"  After:  {aq_after:.3f}")
    print(f"  Delta:  {aq_after - aq_before:+.3f}")

    table_name = args.supabase_table or layer_name
    print(f"\nUploading to Supabase table '{table_name}'...")
    upload_gpkg_to_supabase(args.geodatabase, layer_name, table_name)


if __name__ == "__main__":
    main()
