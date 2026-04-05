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
  - land_change_wetland_acres           → 160
  - land_change_tallgrass_prairie_acres → 81

Output fields are prefixed by land change type, e.g. forest_change_carbon_tc.
The geodatabase is modified in-place.

Usage:
    python site_calculator.py \
        --geodatabase  GIS/DZCIB_Project_Data.gpkg \
        [--layer       project_sites] \
        [--solris-tif  GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif]
"""

import os
import sys
import argparse
import subprocess
import tempfile

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from osgeo import gdal, ogr, osr
from ecosystem_services import discover_processors
from ecosystem_services.aesthetic_quality import landscape_aq

load_dotenv()

gdal.UseExceptions()
ogr.UseExceptions()

# ── Constants ─────────────────────────────────────────────────────────────────

_SOLRIS_LOOKUP_CSV = "data/solris_lookup.csv"
_WF_LOOKUP_CSV     = "data/water_filtration_lookup.csv"
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


# ── Lookup tables (mirrors potential_calculator.py) ───────────────────────────

def load_es_lookup(solris_csv: str, wf_csv: str) -> dict:
    solris_df = pd.read_csv(solris_csv)
    solris_df = solris_df.dropna(subset=["solris_code"])
    solris_df["solris_code"] = solris_df["solris_code"].astype(int)

    for col in ("agc_tc_ha", "bgc_tc_ha", "soc_tc_ha", "deoc_tc_ha"):
        solris_df[col] = pd.to_numeric(solris_df[col], errors="coerce").fillna(0)

    solris_df["total_c_per_ha"] = (
        solris_df["agc_tc_ha"]
        + solris_df["bgc_tc_ha"]
        + solris_df["soc_tc_ha"]
        + solris_df["deoc_tc_ha"]
    )

    wf_df = pd.read_csv(wf_csv).rename(
        columns={"wetland_type": "solris_class", "value": "wf_value_per_ha"}
    )
    solris_df = solris_df.merge(wf_df, on="solris_class", how="left")
    solris_df["wf_value_per_ha"] = solris_df["wf_value_per_ha"].fillna(0)

    def _coerce(val):
        if pd.isna(val):
            return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            return val

    lookup = {}
    for _, row in solris_df.iterrows():
        code = int(row["solris_code"])
        lookup[code] = {
            col: _coerce(row[col])
            for col in solris_df.columns
            if col != "solris_code"
        }
    return lookup


def es_values(lookup: dict, code: int) -> dict:
    return lookup.get(code, {})

# ── SOLRIS area composition ───────────────────────────────────────────────────

def compute_solris_areas(solris_tif: str, geojson: str | None = None) -> dict:
    """Return {solris_code: area_ha} for all SOLRIS classes.

    If geojson is provided, clips to that boundary first.
    Otherwise reads the entire raster (used for rarity context when no boundary is given).
    """
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


# ── SOLRIS sampling ───────────────────────────────────────────────────────────

def sample_solris(x: float, y: float, src_srs: osr.SpatialReference,
                  solris_ds: gdal.Dataset, solris_srs: osr.SpatialReference) -> int | None:
    """Return the SOLRIS code at (x, y), reprojecting from src_srs if needed."""
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


# ── Field helpers ─────────────────────────────────────────────────────────────

def _ensure_field(layer: ogr.Layer, name: str, field_type: int) -> None:
    if layer.GetLayerDefn().GetFieldIndex(name) == -1:
        layer.CreateField(ogr.FieldDefn(name, field_type))


# ── Supabase upload ─────────────────────────────────��─────────────────────────

def upload_to_supabase(gpkg_path: str, layer_name: str, table_name: str) -> None:
    supabase_url = os.getenv("SUPABASE_URL")
    if not supabase_url:
        print("  Skipping upload: SUPABASE_URL is not set in .env.")
        return

    separator = "&" if "?" in supabase_url else "?"
    pg_conn = f"PG:{supabase_url}{separator}options=-c%20statement_timeout%3D0"

    cmd = [
        "ogr2ogr",
        "-f", "PostgreSQL",
        pg_conn,
        gpkg_path,
        layer_name,
        "-nln", f"public.{table_name}",
        "-lco", "GEOMETRY_NAME=geom",
        "-lco", "FID=id",
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


# ── Main ──────────────────────────────────────���───────────────────────────────

def main():
    args = parse_args()

    if not os.path.exists(args.geodatabase):
        sys.exit(f"Error: geodatabase not found: {args.geodatabase}")
    if not os.path.exists(args.solris_tif):
        sys.exit(f"Error: SOLRIS TIF not found: {args.solris_tif}")
    if not os.path.exists(_SOLRIS_LOOKUP_CSV):
        sys.exit(f"Error: SOLRIS lookup CSV not found: {_SOLRIS_LOOKUP_CSV}")
    if not os.path.exists(_WF_LOOKUP_CSV):
        sys.exit(f"Error: water filtration lookup CSV not found: {_WF_LOOKUP_CSV}")

    print("Loading ecosystem service lookup tables...")
    lookup = load_es_lookup(_SOLRIS_LOOKUP_CSV, _WF_LOOKUP_CSV)
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
    aq_landscape_change = 0.0

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

        solris_code = sample_solris(x, y, layer_srs, solris_ds, solris_srs)
        if solris_code is not None:
            feat.SetField("solris_code", solris_code)

        old_vals = es_values(lookup, solris_code) if solris_code else {}

        totals = {}
        aq_weighted_sum = 0.0
        aq_total_area = 0.0

        for col, new_code_val, new_vals in change_configs:
            raw = feat.GetField(col)
            if raw is None:
                continue
            area_ha = float(raw) * _ACRES_TO_HA
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
            aq_landscape_change += aq_weighted_sum  # Σ(aq_change_i × area_ha_i) across all sites

        for field_name, value in totals.items():
            feat.SetField(field_name, round(value, 6))

        layer.SetFeature(feat)
        updated += 1

    layer_name = layer.GetName()
    gdb_ds.FlushCache()
    gdb_ds = None
    solris_ds = None

    print(f"\nDone. Updated {updated} features, skipped {skipped} (no geometry).")

    total_area_ha = sum(context_areas.values())
    aq_before = landscape_aq(lookup, context_areas)
    aq_after = aq_before + aq_landscape_change / total_area_ha
    area_label = args.boundary_geojson or "full SOLRIS extent"
    print(f"\nLandscape aesthetic quality ({area_label}):")
    print(f"  Before: {aq_before:.3f}")
    print(f"  After:  {aq_after:.3f}")
    print(f"  Delta:  {aq_after - aq_before:+.3f}")

    table_name = args.supabase_table or layer_name
    print(f"\nUploading to Supabase table '{table_name}'...")
    upload_to_supabase(args.geodatabase, layer_name, table_name)


if __name__ == "__main__":
    main()
