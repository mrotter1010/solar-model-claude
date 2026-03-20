"""Fetch DEM elevation data and compute slope for buildability analysis.

Downloads 10m resolution elevation data from USGS 3DEP via the
National Map REST API, caches GeoTIFFs locally, and computes terrain
slope from the elevation array.
"""

import math
from pathlib import Path

import numpy as np
import rasterio
import requests
from shapely.geometry import Polygon

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# 3DEP REST endpoint
_3DEP_URL = (
    "https://elevation.nationalmap.gov/arcgis/rest/services/"
    "3DEPElevation/ImageServer/exportImage"
)
_TARGET_RESOLUTION_M = 10.0
_REQUEST_TIMEOUT_S = 120
_DEFAULT_CACHE_DIR = Path("data/buildability/dem")

# Meters per degree of latitude (approximate, constant)
_METERS_PER_DEG_LAT = 111_000.0


def fetch_dem(
    polygon: Polygon, cache_dir: Path | None = None
) -> tuple[np.ndarray, dict]:
    """Fetch DEM elevation data for the bounding box of a polygon.

    Attempts py3dep first, then falls back to the USGS 3DEP REST API.
    Returns ~10m resolution elevation data in EPSG:4326.

    Args:
        polygon: Analysis polygon in EPSG:4326 (WGS84).
        cache_dir: Directory for cached GeoTIFFs. Defaults to
            data/buildability/dem/.

    Returns:
        Tuple of (data, metadata) where:
            - data: float32 numpy array of elevation values in meters
            - metadata: dict with 'transform', 'crs', 'width', 'height',
              'dtype', and 'bounds_4326' keys

    Raises:
        RuntimeError: If both py3dep and the REST fallback fail.
    """
    if cache_dir is None:
        cache_dir = _DEFAULT_CACHE_DIR

    bounds_4326 = polygon.bounds  # (west, south, east, north)
    buffered_bbox = _buffer_bbox(bounds_4326)

    # Check cache
    cache_path = _get_cache_path(cache_dir, bounds_4326)
    if cache_path.exists():
        logger.info(f"Loading cached DEM raster: {cache_path}")
        return _read_geotiff(cache_path, buffered_bbox)

    # Try py3dep first, fall back to REST
    tiff_bytes = _fetch_with_fallback(buffered_bbox)

    # Save to cache
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(tiff_bytes)
    logger.info(f"Cached DEM raster ({len(tiff_bytes)} bytes): {cache_path}")

    return _read_geotiff(cache_path, buffered_bbox)


def compute_slope(elevation: np.ndarray, transform: rasterio.transform.Affine) -> np.ndarray:
    """Compute terrain slope in degrees from an elevation array.

    Uses numpy gradient to compute dz/dx and dz/dy, then calculates
    slope as arctan(sqrt(dz_dx^2 + dz_dy^2)). Pixel sizes are
    converted from degrees to meters assuming EPSG:4326 CRS.

    Args:
        elevation: 2D float array of elevation values in meters.
        transform: Rasterio affine transform (provides pixel size).

    Returns:
        2D numpy array of slope values in degrees.
    """
    # Get pixel size from transform
    x_res = abs(transform.a)  # degrees per pixel (longitude)
    y_res = abs(transform.e)  # degrees per pixel (latitude)

    # Convert degrees to meters
    # Use center latitude of the raster for longitude scaling
    n_rows = elevation.shape[0]
    # Top-left lat from transform, center is halfway down
    center_lat = transform.f + transform.e * (n_rows / 2)
    cos_lat = math.cos(math.radians(center_lat))

    x_res_m = x_res * _METERS_PER_DEG_LAT * cos_lat
    y_res_m = y_res * _METERS_PER_DEG_LAT

    logger.debug(
        f"Slope computation: pixel size = {x_res_m:.2f}m x {y_res_m:.2f}m "
        f"(center_lat={center_lat:.4f})"
    )

    # Compute gradient (dz/dy along axis 0, dz/dx along axis 1)
    dz_dy, dz_dx = np.gradient(elevation, y_res_m, x_res_m)

    # Slope in degrees
    slope = np.degrees(np.arctan(np.sqrt(dz_dx**2 + dz_dy**2)))

    return slope.astype(np.float32)


def _buffer_bbox(
    bounds_4326: tuple[float, float, float, float],
) -> tuple[float, float, float, float]:
    """Add a small buffer to a WGS84 bounding box.

    Args:
        bounds_4326: (west, south, east, north) in EPSG:4326.

    Returns:
        Buffered (west, south, east, north).
    """
    west, south, east, north = bounds_4326
    buffer = 0.001
    return (west - buffer, south - buffer, east + buffer, north + buffer)


def _get_cache_path(
    cache_dir: Path, bounds_4326: tuple[float, float, float, float]
) -> Path:
    """Build a cache file path from bounding box coordinates.

    Args:
        cache_dir: Cache directory.
        bounds_4326: (west, south, east, north) in EPSG:4326.

    Returns:
        Path like cache_dir/dem_-105.2800_40.0050_-105.2600_40.0250.tif
    """
    west, south, east, north = bounds_4326
    name = f"dem_{west:.4f}_{south:.4f}_{east:.4f}_{north:.4f}.tif"
    return cache_dir / name


def _fetch_with_fallback(
    bbox: tuple[float, float, float, float],
) -> bytes:
    """Try py3dep first, then fall back to 3DEP REST API.

    Args:
        bbox: (west, south, east, north) in EPSG:4326.

    Returns:
        Raw GeoTIFF bytes.

    Raises:
        RuntimeError: If both methods fail.
    """
    py3dep_error = _try_py3dep(bbox)
    if py3dep_error is None:
        # py3dep doesn't return raw TIFF bytes easily; skip to REST
        pass
    else:
        logger.info(f"py3dep unavailable ({py3dep_error}), using REST fallback")

    return _fetch_3dep_rest(bbox)


def _try_py3dep(
    bbox: tuple[float, float, float, float],
) -> str | None:
    """Check if py3dep can handle CRS operations.

    Args:
        bbox: (west, south, east, north) in EPSG:4326.

    Returns:
        None if py3dep works, or an error description string if it fails.
    """
    try:
        import py3dep  # noqa: F811

        # Quick CRS test — this is where it fails in broken PROJ envs
        py3dep.get_map(
            "DEM", bbox, resolution=30, geo_crs=4326, crs=4326
        )
        return None
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"


def _fetch_3dep_rest(
    bbox: tuple[float, float, float, float],
) -> bytes:
    """Fetch elevation GeoTIFF from the USGS 3DEP REST API.

    Args:
        bbox: (west, south, east, north) in EPSG:4326.

    Returns:
        Raw GeoTIFF bytes.

    Raises:
        RuntimeError: If the request fails or returns invalid data.
    """
    west, south, east, north = bbox
    center_lat = (south + north) / 2
    cos_lat = math.cos(math.radians(center_lat))

    width = max(1, int((east - west) * _METERS_PER_DEG_LAT * cos_lat / _TARGET_RESOLUTION_M))
    height = max(1, int((north - south) * _METERS_PER_DEG_LAT / _TARGET_RESOLUTION_M))

    params = {
        "bbox": f"{west},{south},{east},{north}",
        "bboxSR": "4326",
        "imageSR": "4326",
        "size": f"{width},{height}",
        "format": "tiff",
        "f": "image",
        "interpolation": "RSP_BilinearInterpolation",
    }

    logger.info(
        f"Fetching DEM from 3DEP REST: bbox=({west:.4f}, {south:.4f}, "
        f"{east:.4f}, {north:.4f}), size={width}x{height}"
    )

    try:
        response = requests.get(
            _3DEP_URL, params=params, timeout=_REQUEST_TIMEOUT_S
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"3DEP REST request failed: {exc}") from exc

    content_type = response.headers.get("content-type", "")
    if "image/tiff" not in content_type:
        raise RuntimeError(
            f"3DEP REST returned unexpected content type '{content_type}': "
            f"{response.content[:500].decode('utf-8', errors='replace')}"
        )

    if response.content[:2] not in (b"II", b"MM"):
        raise RuntimeError(
            "3DEP REST response is not a valid TIFF "
            f"(magic bytes: {response.content[:4]!r})"
        )

    logger.debug(f"3DEP REST returned {len(response.content)} bytes")
    return response.content


def _read_geotiff(
    path: Path, bbox_4326: tuple[float, float, float, float]
) -> tuple[np.ndarray, dict]:
    """Read a DEM GeoTIFF and return the data array with metadata.

    Args:
        path: Path to the GeoTIFF file.
        bbox_4326: Request bounding box for metadata.

    Returns:
        Tuple of (data, metadata).
    """
    with rasterio.open(path) as ds:
        data = ds.read(1).astype(np.float32)
        metadata = {
            "transform": ds.transform,
            "crs": "EPSG:4326",
            "width": ds.width,
            "height": ds.height,
            "dtype": "float32",
            "bounds_4326": bbox_4326,
        }

    logger.info(
        f"DEM raster: {data.shape[1]}x{data.shape[0]} pixels, "
        f"elevation {data.min():.1f}–{data.max():.1f} m"
    )
    return data, metadata
