"""
gis_helpers.py
--------------
Shared GIS / raster utilities used across the processing pipeline.
"""

import os
import sys
import tempfile

import numpy as np
from osgeo import gdal, osr


def clip_raster_to_geojson(tif: str, geojson: str, output: str, nodata: int = 0) -> None:
    """Clip a raster to a GeoJSON boundary using gdal.Warp.

    Args:
        tif:     Input raster path.
        geojson: GeoJSON boundary path.
        output:  Output raster path.
        nodata:  NoData value to assign to clipped-out pixels (default 0).
    """
    opts = gdal.WarpOptions(
        cutlineDSName=geojson,
        cropToCutline=True,
        dstNodata=nodata,
        format="GTiff",
        creationOptions=["COMPRESS=LZW", "TILED=YES"],
        multithread=True,
        warpOptions=["NUM_THREADS=ALL_CPUS"],
    )
    ds = gdal.Warp(output, tif, options=opts)
    if ds is None:
        sys.exit(f"Error: gdal.Warp (clip) returned None — check inputs: {tif}, {geojson}")
    ds.FlushCache()
    ds = None


def compute_solris_areas(solris_tif: str, geojson: str | None = None) -> dict:
    """Return {solris_code: area_ha} for all SOLRIS classes in a raster.

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


def sample_solris(
    x: float,
    y: float,
    src_srs: osr.SpatialReference,
    solris_ds: gdal.Dataset,
    solris_srs: osr.SpatialReference,
) -> int | None:
    """Return the SOLRIS code at (x, y), reprojecting from src_srs if needed.

    Returns None if the point falls outside the raster extent or on a nodata pixel.
    """
    if not src_srs.IsSame(solris_srs):
        transform = osr.CoordinateTransformation(src_srs, solris_srs)
        x, y, _ = transform.TransformPoint(x, y)

    gt = solris_ds.GetGeoTransform()
    px = round((x - gt[0]) / gt[1])
    py = round((y - gt[3]) / gt[5])

    if px < 0 or py < 0 or px >= solris_ds.RasterXSize or py >= solris_ds.RasterYSize:
        return None

    value = int(solris_ds.GetRasterBand(1).ReadAsArray(px, py, 1, 1)[0][0])
    return value if value != 0 else None
