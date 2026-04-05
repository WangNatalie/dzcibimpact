#!/usr/bin/env python3
"""
Land Cover Change Impact Analysis Pipeline

Intersects a land-cover-change raster with the SOLRIS 3.0 land cover raster,
polygonizes the result, and computes per-feature ecosystem service deltas
between the existing (old) SOLRIS class and a user-specified new SOLRIS class.
Outputs a GeoPackage and optionally uploads it to Supabase.

Steps:
    1.  (Optional) Clip the change raster to a GeoJSON boundary
    2a. Warp SOLRIS to match the change raster CRS / resolution / extent
    2b. Mask SOLRIS to pixels where the change raster is non-zero
    2c. Polygonize the masked raster  →  field "old_solris_code"
    3.  Compute area_ha + ecosystem-service delta fields per feature
    4.  Write to GeoPackage
    5.  Upload to Supabase via ogr2ogr

Usage:
    python potential_calculator.py \
        --change-tif  GIS/forest_restoration/Area_of_opportunity.tif \
        --new-solris-code 90 \
        [--solris-tif   GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif] \
        [--geojson      path/to/boundary.geojson] \
        [--output-gpkg  GIS/land_cover_change_impact.gpkg] \
        [--table        land_cover_change_impact]

Environment variables (.env):
    SUPABASE_URL   PostgreSQL connection string
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

_OUTPUT_LAYER = "land_cover_change_impact"

_SOLRIS_LOOKUP_CSV = "data/solris_lookup.csv"
_WF_LOOKUP_CSV = "data/water_filtration_lookup.csv"

_VSIMEM_MASKED = "/vsimem/masked_solris.tif"
_VSIMEM_POLYGONIZED = "/vsimem/polygonized.gpkg"


# ── CLI ───────────────────────────────────────────────────────────────────────

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
        help="SOLRIS code representing the new (target) land cover class.",
    )
    parser.add_argument(
        "--solris-tif",
        default="GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif",
        help=f"SOLRIS 3.0 raster [default: GIS/SOLRIS_Version_3_0/SOLRIS_Version_3_0_LAMBERT.tif]",
    )
    parser.add_argument(
        "--geojson",
        default=None,
        help="Optional GeoJSON to clip the change raster before processing.",
    )
    parser.add_argument(
        "--output-gpkg",
        default="GIS/land_cover_change_impact.gpkg",
        help=f"Output GeoPackage path [default: GIS/land_cover_change_impact.gpkg]",
    )
    parser.add_argument(
        "--table",
        default="land_cover_change_impact",
        help=f"Supabase table name [default: land_cover_change_impact]",
    )
    return parser.parse_args()


# ── Lookup tables ─────────────────────────────────────────────────────────────

def load_es_lookup(solris_csv: str, wf_csv: str) -> dict:
    """Build a dict keyed by solris_code → full row of per-ha ES values.

    All columns from solris_lookup.csv are included so that any processor's
    compute_change() can access whatever column it needs. Additionally:
      - total_c_per_ha  (agc + bgc + soc + deoc)
      - wf_value_per_ha (joined from water_filtration_lookup, 0 for non-wetlands)
    """
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

    # Water filtration lookup: wetland_type (= solris_class) → wf_value_per_ha
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
    """Return the full ES per-ha value dict for a SOLRIS code; empty dict if not found."""
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


# ── Step 1b: Clip change raster to GeoJSON ────────────────────────────────────

def clip_to_geojson(change_tif: str, geojson: str, output: str) -> None:
    """Clip the change raster to the GeoJSON boundary using gdal.Warp."""
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
        sys.exit("Error: gdal.Warp (clip) returned None — check inputs.")
    ds.FlushCache()
    ds = None


# ── Steps 2a + 2b: Warp SOLRIS → mask to change pixels ───────────────────────

def warp_solris_to_match(solris_tif: str, reference_tif: str, output: str) -> None:
    """Warp SOLRIS to match the CRS, resolution, and extent of the reference raster."""
    ref_ds = gdal.Open(reference_tif, gdal.GA_ReadOnly)
    if ref_ds is None:
        sys.exit(f"Error: cannot open reference raster: {reference_tif}")

    gt = ref_ds.GetGeoTransform()
    x_min = gt[0]
    y_max = gt[3]
    x_max = x_min + gt[1] * ref_ds.RasterXSize
    y_min = y_max + gt[5] * ref_ds.RasterYSize  # gt[5] is negative
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
        sys.exit("Error: gdal.Warp (SOLRIS resample) returned None — check inputs.")
    ds.FlushCache()
    ds = None


def create_masked_solris(solris_path: str, change_path: str) -> None:
    """Write a masked SOLRIS raster to /vsimem/:
    pixels where change != 0 → SOLRIS value; all others → 0 (NoData).
    """
    solris_ds = gdal.Open(solris_path, gdal.GA_ReadOnly)
    change_ds = gdal.Open(change_path, gdal.GA_ReadOnly)

    solris_arr = solris_ds.GetRasterBand(1).ReadAsArray()
    change_arr = change_ds.GetRasterBand(1).ReadAsArray()

    change_nodata = change_ds.GetRasterBand(1).GetNoDataValue()

    # Where change is non-zero (and not NoData), keep the SOLRIS code
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


# ── Step 2c: Polygonize masked SOLRIS ─────────────────────────────────────────

def polygonize_masked(srs_wkt: str) -> ogr.DataSource:
    """Polygonize the masked SOLRIS raster (in /vsimem/) into a GPKG DataSource.

    Returns the in-memory DataSource (caller must keep a reference to keep it alive).
    """
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


# ── Step 3: Compute delta fields and write to GeoPackage ─────────────────────

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
) -> None:
    """Iterate polygonized features, compute ES change fields, write to GeoPackage.

    Change fields are driven entirely by the CHANGE_FIELDS / compute_change()
    protocol on each discovered *Processor class — no hardcoding required.
    """
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
    for name, ftype in [
        ("old_solris_code", ogr.OFTInteger),
        ("new_solris_code", ogr.OFTInteger),
        ("area_ha", ogr.OFTReal),
    ]:
        _add_field(defn, out_layer, name, ftype)

    for cls in processors:
        for field_name in cls.CHANGE_FIELDS:
            _add_field(defn, out_layer, field_name, ogr.OFTReal)

    out_layer.StartTransaction()
    src_layer.ResetReading()
    aq_weighted_change = 0.0

    for feat in src_layer:
        old_code = feat.GetField("old_solris_code")
        if old_code is None or old_code == 0:
            continue

        geom = feat.GetGeometryRef()
        if geom is None:
            continue

        # Promote to MultiPolygon for consistency
        if geom.GetGeometryType() == ogr.wkbPolygon:
            multi = ogr.Geometry(ogr.wkbMultiPolygon)
            multi.AddGeometry(geom)
            geom = multi

        area_m2 = geom.GetArea()
        area_ha = area_m2 / 10000.0

        old_vals = es_values(lookup, old_code)

        out_feat = ogr.Feature(out_layer.GetLayerDefn())
        out_feat.SetGeometry(geom)
        out_feat.SetField("old_solris_code", old_code)
        out_feat.SetField("new_solris_code", new_code)
        out_feat.SetField("area_ha", round(area_ha, 6))

        for cls in processors:
            for field_name, value in cls.compute_change(
                area_ha, old_vals, new_vals,
                context_areas=context_areas,
                old_code=old_code,
                new_code=new_code,
            ).items():
                out_feat.SetField(field_name, round(value, 6))
                if field_name == "change_aesthetic_score":
                    aq_weighted_change += value * area_ha

        out_layer.CreateFeature(out_feat)

    out_layer.CommitTransaction()
    out_ds.FlushCache()
    out_ds = None

    return aq_weighted_change


# ── Step 5: Upload to Supabase ────────────────────────────────────────────────

def upload_to_supabase(gpkg_path: str, table_name: str) -> None:
    """Upload the impact GeoPackage to Supabase via ogr2ogr."""
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
        _OUTPUT_LAYER,
        "-nln", f"public.{table_name}",
        "-nlt", "MULTIPOLYGON",
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


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    if not os.path.exists(args.change_tif):
        sys.exit(f"Error: change TIF not found: {args.change_tif}")
    if not os.path.exists(args.solris_tif):
        sys.exit(f"Error: SOLRIS TIF not found: {args.solris_tif}")
    if args.geojson and not os.path.exists(args.geojson):
        sys.exit(f"Error: GeoJSON not found: {args.geojson}")

    if not os.path.exists(_SOLRIS_LOOKUP_CSV):
        sys.exit(f"Error: SOLRIS lookup CSV not found: {_SOLRIS_LOOKUP_CSV}")
    if not os.path.exists(_WF_LOOKUP_CSV):
        sys.exit(f"Error: water filtration lookup CSV not found: {_WF_LOOKUP_CSV}")

    os.makedirs(os.path.dirname(os.path.abspath(args.output_gpkg)), exist_ok=True)

    print("\nLoading ecosystem service lookup tables...")
    lookup = load_es_lookup(_SOLRIS_LOOKUP_CSV, _WF_LOOKUP_CSV)
    if args.new_solris_code not in lookup:
        sys.exit(
            f"Error: new SOLRIS code {args.new_solris_code} not found in lookup. "
            f"Available codes: {sorted(lookup.keys())}"
        )
    print(f"  Loaded {len(lookup)} SOLRIS classes.")

    if args.geojson:
        print(f"\nComputing SOLRIS composition within boundary: {args.geojson}")
        context_areas = compute_solris_areas(args.solris_tif, args.geojson)
    else:
        print("\nNo boundary provided — using full SOLRIS raster for rarity context...")
        context_areas = compute_solris_areas(args.solris_tif)
    print(f"  Found {len(context_areas)} SOLRIS classes.")

    with tempfile.TemporaryDirectory() as tmpdir:
        change_tif = args.change_tif

        if args.geojson:
            print(f"\nStep 1b: Clipping change raster to GeoJSON: {args.geojson}")
            clipped_change = os.path.join(tmpdir, "change_clipped.tif")
            clip_to_geojson(change_tif, args.geojson, clipped_change)
            change_tif = clipped_change
            print(f"  → {clipped_change}")

        print("\nStep 2a: Warping SOLRIS to match change raster...")
        resampled_solris = os.path.join(tmpdir, "solris_resampled.tif")
        warp_solris_to_match(args.solris_tif, change_tif, resampled_solris)
        print(f"  → {resampled_solris}")

        print("\nStep 2b: Masking SOLRIS to change area pixels...")
        create_masked_solris(resampled_solris, change_tif)
        print(f"  → {_VSIMEM_MASKED}")

        # Read the SRS from the masked raster for later use
        masked_ds = gdal.Open(_VSIMEM_MASKED, gdal.GA_ReadOnly)
        srs_wkt = masked_ds.GetProjection()
        masked_ds = None

        print("\nStep 2c: Polygonizing masked SOLRIS raster...")
        poly_ds = polygonize_masked(srs_wkt)
        src_count = poly_ds.GetLayer(_OUTPUT_LAYER).GetFeatureCount()
        print(f"  → {src_count} polygons (before filtering zero/null codes)")

    print(f"\nStep 3+4: Computing ES delta fields and writing GeoPackage...")
    aq_weighted_change = write_impact_gpkg(poly_ds, args.output_gpkg, args.new_solris_code, lookup, srs_wkt, context_areas=context_areas)
    poly_ds = None

    # Clean up vsimem
    if gdal.VSIStatL(_VSIMEM_MASKED):
        gdal.Unlink(_VSIMEM_MASKED)
    if gdal.VSIStatL(_VSIMEM_POLYGONIZED):
        gdal.Unlink(_VSIMEM_POLYGONIZED)

    print(f"  → {args.output_gpkg}")

    total_area_ha = sum(context_areas.values())
    aq_before = landscape_aq(lookup, context_areas)
    aq_after = aq_before + aq_weighted_change / total_area_ha
    area_label = "boundary" if args.geojson else "full SOLRIS extent"
    print(f"\nLandscape aesthetic quality ({area_label}):")
    print(f"  Before: {aq_before:.3f}")
    print(f"  After:  {aq_after:.3f}")
    print(f"  Delta:  {aq_after - aq_before:+.3f}")

    print("\nStep 5: Uploading to Supabase...")
    upload_to_supabase(args.output_gpkg, args.table)

    print("\nDone.")


if __name__ == "__main__":
    main()
