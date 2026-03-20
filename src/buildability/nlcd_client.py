"""Fetch NLCD land cover raster data from the MRLC GeoServer WCS.

Downloads 2021 NLCD Land Cover data for a given analysis polygon,
caches GeoTIFFs locally, and returns the raster array with metadata.
"""

import base64
import re
from pathlib import Path

import numpy as np
import pyproj
import rasterio
import requests
from shapely.geometry import Polygon

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# MRLC WCS endpoint and layer
_WCS_URL = "https://www.mrlc.gov/geoserver/mrlc_display/wcs"
_LAYER = "mrlc_display:NLCD_2021_Land_Cover_L48"
_WCS_VERSION = "1.1.1"
_NATIVE_CRS_URN = "urn:ogc:def:crs:EPSG::3857"
_NATIVE_RESOLUTION_M = 30.0
_REQUEST_TIMEOUT_S = 120
_DEFAULT_CACHE_DIR = Path("data/buildability/nlcd")

# NLCD 2021 land cover class codes
NLCD_CLASSES: dict[int, str] = {
    11: "Open Water",
    12: "Perennial Ice/Snow",
    21: "Developed, Open Space",
    22: "Developed, Low Intensity",
    23: "Developed, Medium Intensity",
    24: "Developed, High Intensity",
    31: "Barren Land",
    41: "Deciduous Forest",
    42: "Evergreen Forest",
    43: "Mixed Forest",
    52: "Shrub/Scrub",
    71: "Grassland/Herbaceous",
    81: "Pasture/Hay",
    82: "Cultivated Crops",
    90: "Woody Wetlands",
    95: "Emergent Herbaceous Wetlands",
}

# CRS objects (PROJ strings for environments without PROJ database)
_WGS84 = pyproj.CRS("+proj=longlat +datum=WGS84 +no_defs")
_WEB_MERCATOR = pyproj.CRS(
    "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 "
    "+x_0=0 +y_0=0 +k=1 +units=m +no_defs"
)
_TO_3857 = pyproj.Transformer.from_crs(_WGS84, _WEB_MERCATOR, always_xy=True)


def fetch_nlcd(
    polygon: Polygon, cache_dir: Path | None = None
) -> tuple[np.ndarray, dict]:
    """Fetch NLCD land cover data for the bounding box of a polygon.

    Downloads a GeoTIFF from the MRLC GeoServer WCS at native 30m
    resolution, caches it locally, and returns the raster array with
    metadata.

    Args:
        polygon: Analysis polygon in EPSG:4326 (WGS84).
        cache_dir: Directory for cached GeoTIFFs. Defaults to
            data/buildability/nlcd/.

    Returns:
        Tuple of (data, metadata) where:
            - data: uint8 numpy array of NLCD land cover class codes
            - metadata: dict with 'transform', 'crs', 'width', 'height',
              'dtype', and 'bounds_epsg3857' keys

    Raises:
        RuntimeError: If the WCS request fails or returns invalid data.
    """
    if cache_dir is None:
        cache_dir = _DEFAULT_CACHE_DIR

    # Compute bbox in EPSG:3857
    bounds_4326 = polygon.bounds  # (minx, miny, maxx, maxy) = (W, S, E, N)
    bbox_3857 = _transform_bbox_to_3857(bounds_4326)

    # Check cache
    cache_path = _get_cache_path(cache_dir, bounds_4326)
    if cache_path.exists():
        logger.info(f"Loading cached NLCD raster: {cache_path}")
        return _read_geotiff(cache_path, bbox_3857)

    # Fetch from WCS
    tiff_bytes = _fetch_wcs_coverage(bbox_3857)

    # Save to cache
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(tiff_bytes)
    logger.info(f"Cached NLCD raster ({len(tiff_bytes)} bytes): {cache_path}")

    return _read_geotiff(cache_path, bbox_3857)


def _transform_bbox_to_3857(
    bounds_4326: tuple[float, float, float, float],
) -> tuple[float, float, float, float]:
    """Transform a WGS84 bounding box to EPSG:3857 with a small buffer.

    Args:
        bounds_4326: (west, south, east, north) in EPSG:4326.

    Returns:
        (x_min, y_min, x_max, y_max) in EPSG:3857.
    """
    west, south, east, north = bounds_4326
    # Add ~0.001 degree buffer for edge coverage
    buffer = 0.001
    west -= buffer
    south -= buffer
    east += buffer
    north += buffer

    x_min, y_min = _TO_3857.transform(west, south)
    x_max, y_max = _TO_3857.transform(east, north)
    return (x_min, y_min, x_max, y_max)


def _get_cache_path(
    cache_dir: Path, bounds_4326: tuple[float, float, float, float]
) -> Path:
    """Build a cache file path from the bounding box coordinates.

    Args:
        cache_dir: Cache directory.
        bounds_4326: (west, south, east, north) in EPSG:4326.

    Returns:
        Path like cache_dir/nlcd_-105.2800_40.0050_-105.2600_40.0250.tif
    """
    west, south, east, north = bounds_4326
    name = (
        f"nlcd_{west:.4f}_{south:.4f}_{east:.4f}_{north:.4f}.tif"
    )
    return cache_dir / name


def _fetch_wcs_coverage(
    bbox_3857: tuple[float, float, float, float],
) -> bytes:
    """Fetch a GeoTIFF from the MRLC WCS endpoint.

    Uses WCS 1.1.1 with EPSG:3857 native CRS and 30m grid resolution.
    The response is multipart MIME with the GeoTIFF base64-encoded in
    the second part.

    Args:
        bbox_3857: (x_min, y_min, x_max, y_max) in EPSG:3857.

    Returns:
        Raw GeoTIFF bytes.

    Raises:
        RuntimeError: If the request fails or the response is not a valid
            multipart MIME response containing a GeoTIFF.
    """
    x_min, y_min, x_max, y_max = bbox_3857

    params = {
        "service": "WCS",
        "version": _WCS_VERSION,
        "request": "GetCoverage",
        "identifier": _LAYER,
        "format": "image/tiff",
        "GridBaseCRS": _NATIVE_CRS_URN,
        "boundingbox": (
            f"{x_min},{y_min},{x_max},{y_max},{_NATIVE_CRS_URN}"
        ),
        "GridType": "urn:ogc:def:method:WCS:1.1:2dSimpleGrid",
        "GridOffsets": f"{_NATIVE_RESOLUTION_M},-{_NATIVE_RESOLUTION_M}",
    }

    logger.info(
        f"Fetching NLCD from WCS: bbox_3857="
        f"({x_min:.0f}, {y_min:.0f}, {x_max:.0f}, {y_max:.0f})"
    )

    try:
        response = requests.get(
            _WCS_URL, params=params, timeout=_REQUEST_TIMEOUT_S
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(
            f"NLCD WCS request failed: {exc}"
        ) from exc

    # Check for XML error response
    content_type = response.headers.get("content-type", "")
    if "xml" in content_type and "multipart" not in content_type:
        raise RuntimeError(
            f"NLCD WCS returned an error: {response.text[:500]}"
        )

    return _extract_tiff_from_multipart(response.content, content_type)


def _extract_tiff_from_multipart(
    content: bytes, content_type: str
) -> bytes:
    """Extract and decode the GeoTIFF from a WCS 1.1.1 multipart response.

    WCS 1.1.1 returns a multipart MIME response where the coverage data
    is base64-encoded in the part with Content-Type: image/tiff.

    Args:
        content: Raw response body.
        content_type: Content-Type header value.

    Returns:
        Decoded GeoTIFF bytes.

    Raises:
        RuntimeError: If the response cannot be parsed or contains no TIFF.
    """
    boundary_match = re.search(r'boundary="?([^";\s]+)"?', content_type)
    if not boundary_match:
        # Maybe the response is a plain TIFF
        if content[:2] in (b"II", b"MM"):
            return content
        raise RuntimeError(
            f"Cannot parse WCS response: no MIME boundary in "
            f"Content-Type '{content_type}'"
        )

    boundary = boundary_match.group(1).encode()
    parts = content.split(b"--" + boundary)

    for part in parts:
        if b"image/tiff" not in part[:500]:
            continue

        # Separate headers from body
        header_end = part.find(b"\r\n\r\n")
        if header_end == -1:
            continue
        headers_raw = part[:header_end].decode("utf-8", errors="replace")
        body = part[header_end + 4 :]

        # Strip trailing boundary markers
        body = body.rstrip(b"\r\n- ")

        # Decode if base64-encoded
        if "base64" in headers_raw.lower():
            try:
                tiff_bytes = base64.b64decode(body)
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to base64-decode TIFF data: {exc}"
                ) from exc
        else:
            tiff_bytes = body

        if tiff_bytes[:2] not in (b"II", b"MM"):
            raise RuntimeError(
                "Decoded data is not a valid TIFF "
                f"(magic bytes: {tiff_bytes[:4]!r})"
            )

        logger.debug(f"Extracted TIFF: {len(tiff_bytes)} bytes")
        return tiff_bytes

    raise RuntimeError(
        "No image/tiff part found in WCS multipart response"
    )


def _read_geotiff(
    path: Path, bbox_3857: tuple[float, float, float, float]
) -> tuple[np.ndarray, dict]:
    """Read a GeoTIFF and return the data array with metadata.

    The CRS is set to EPSG:3857 since the MRLC WCS multipart response
    strips the CRS from the embedded GeoTIFF.

    Args:
        path: Path to the GeoTIFF file.
        bbox_3857: Original request bbox for metadata.

    Returns:
        Tuple of (data, metadata).
    """
    with rasterio.open(path) as ds:
        data = ds.read(1)
        metadata = {
            "transform": ds.transform,
            "crs": "EPSG:3857",
            "width": ds.width,
            "height": ds.height,
            "dtype": str(ds.dtypes[0]),
            "bounds_epsg3857": bbox_3857,
        }

    logger.info(
        f"NLCD raster: {data.shape[1]}x{data.shape[0]} pixels, "
        f"{len(np.unique(data))} unique values"
    )
    return data, metadata
