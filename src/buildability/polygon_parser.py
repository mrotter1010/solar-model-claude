"""Parse KMZ/KML files and create buffer polygons for buildability analysis.

Produces Shapely Polygons in EPSG:4326 (WGS84) from either:
- A KMZ/KML file containing a polygon boundary
- A lat/lon point with a radius buffer
"""

import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import pyproj
from shapely.geometry import Point, Polygon
from shapely.ops import transform

from src.buildability.models import BuildabilityConfig
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# KML namespace
_KML_NS = {"kml": "http://www.opengis.net/kml/2.2"}


def parse_kmz(kmz_path: str | Path) -> Polygon:
    """Parse a KMZ or KML file and extract the first polygon geometry.

    KMZ files are zip archives containing a .kml file. KML files are
    parsed directly as XML. Coordinates are extracted from the first
    <Polygon> element found in the document.

    Args:
        kmz_path: Path to a .kmz or .kml file.

    Returns:
        Shapely Polygon in EPSG:4326 (WGS84).

    Raises:
        ValueError: If the file doesn't exist, has an unsupported extension,
            contains no polygon, or has unparseable coordinates.
    """
    path = Path(kmz_path)

    if not path.exists():
        raise ValueError(f"KMZ/KML file not found: {path}")

    suffix = path.suffix.lower()
    if suffix not in (".kmz", ".kml"):
        raise ValueError(
            f"Unsupported file extension '{suffix}'. Expected .kmz or .kml"
        )

    kml_string = _read_kml_content(path, suffix)
    polygon = _extract_polygon_from_kml(kml_string, path)

    logger.info(
        f"Parsed polygon from {path.name}: "
        f"bounds={tuple(round(b, 4) for b in polygon.bounds)}"
    )
    return polygon


def _read_kml_content(path: Path, suffix: str) -> str:
    """Read KML XML content from a .kmz or .kml file.

    Args:
        path: Path to the file.
        suffix: File extension (.kmz or .kml).

    Returns:
        KML XML content as a string.

    Raises:
        ValueError: If KMZ archive contains no .kml file or is not a valid zip.
    """
    if suffix == ".kmz":
        try:
            with zipfile.ZipFile(path, "r") as zf:
                kml_files = [n for n in zf.namelist() if n.lower().endswith(".kml")]
                if not kml_files:
                    raise ValueError(
                        f"KMZ archive contains no .kml file: {path}"
                    )
                kml_filename = kml_files[0]
                logger.debug(f"Extracting {kml_filename} from {path.name}")
                return zf.read(kml_filename).decode("utf-8")
        except zipfile.BadZipFile:
            raise ValueError(f"File is not a valid KMZ (zip) archive: {path}")
    else:
        return path.read_text(encoding="utf-8")


def _extract_polygon_from_kml(kml_string: str, source_path: Path) -> Polygon:
    """Extract the first Polygon from KML XML content.

    Args:
        kml_string: KML XML content.
        source_path: Original file path (for error messages).

    Returns:
        Shapely Polygon in EPSG:4326.

    Raises:
        ValueError: If no polygon is found or coordinates are unparseable.
    """
    try:
        root = ET.fromstring(kml_string)
    except ET.ParseError as exc:
        raise ValueError(f"Invalid KML XML in {source_path}: {exc}")

    polygon_elem = root.find(".//kml:Polygon", _KML_NS)
    if polygon_elem is None:
        raise ValueError(f"No <Polygon> element found in {source_path}")

    coords_elem = polygon_elem.find(
        "kml:outerBoundaryIs/kml:LinearRing/kml:coordinates", _KML_NS
    )
    if coords_elem is None or not coords_elem.text:
        raise ValueError(
            f"No coordinates found in polygon in {source_path}"
        )

    coords_text = coords_elem.text.strip()
    try:
        points = []
        for point_str in coords_text.split():
            parts = point_str.split(",")
            lon = float(parts[0])
            lat = float(parts[1])
            # Altitude (parts[2]) is ignored
            points.append((lon, lat))
    except (IndexError, ValueError) as exc:
        raise ValueError(
            f"Failed to parse coordinates in {source_path}: {exc}"
        )

    if len(points) < 3:
        raise ValueError(
            f"Polygon requires at least 3 points, got {len(points)} "
            f"in {source_path}"
        )

    return Polygon(points)


def create_buffer(
    latitude: float, longitude: float, radius_km: float
) -> Polygon:
    """Create a circular buffer polygon around a geographic point.

    Projects the point to a local UTM zone to create an accurate circular
    buffer in meters, then reprojects back to EPSG:4326.

    Args:
        latitude: Latitude in decimal degrees.
        longitude: Longitude in decimal degrees.
        radius_km: Buffer radius in kilometers.

    Returns:
        Shapely Polygon (approximate circle) in EPSG:4326 (WGS84).
    """
    # Determine the appropriate UTM zone for this location
    utm_crs = _get_utm_crs(latitude, longitude)
    wgs84 = pyproj.CRS("+proj=longlat +datum=WGS84 +no_defs")

    # Transform point to UTM, buffer in meters, transform back
    to_utm = pyproj.Transformer.from_crs(wgs84, utm_crs, always_xy=True)
    to_wgs84 = pyproj.Transformer.from_crs(utm_crs, wgs84, always_xy=True)

    utm_x, utm_y = to_utm.transform(longitude, latitude)
    circle_utm = Point(utm_x, utm_y).buffer(radius_km * 1000, resolution=64)
    circle_wgs84 = transform(to_wgs84.transform, circle_utm)

    logger.info(
        f"Created {radius_km} km buffer at ({latitude}, {longitude}): "
        f"bounds={tuple(round(b, 4) for b in circle_wgs84.bounds)}"
    )
    return circle_wgs84


def _get_utm_crs(latitude: float, longitude: float) -> pyproj.CRS:
    """Determine the UTM CRS for a given lat/lon.

    Uses a PROJ string instead of EPSG codes for compatibility with
    environments where the PROJ database may not be fully configured.

    Args:
        latitude: Latitude in decimal degrees.
        longitude: Longitude in decimal degrees.

    Returns:
        pyproj CRS for the appropriate UTM zone.
    """
    utm_zone = int((longitude + 180) / 6) + 1
    hemisphere = "+north" if latitude >= 0 else "+south"
    return pyproj.CRS(
        f"+proj=utm +zone={utm_zone} {hemisphere} +datum=WGS84 +units=m +no_defs"
    )


def get_analysis_polygon(config: BuildabilityConfig) -> Polygon:
    """Get the analysis polygon from a BuildabilityConfig.

    Single entry point for the standalone buildability pipeline.
    Routes to parse_kmz or create_buffer based on config fields.

    Args:
        config: BuildabilityConfig with location and boundary source.

    Returns:
        Shapely Polygon in EPSG:4326 (WGS84).
    """
    if config.kmz_file_path is not None:
        return parse_kmz(config.kmz_file_path)

    radius = config.radius_km if config.radius_km is not None else 1.0
    return create_buffer(config.latitude, config.longitude, radius)


def get_analysis_polygon_from_site(site_config: "SiteConfig") -> Polygon | None:
    """Get the analysis polygon from a CSV pipeline SiteConfig.

    Returns None if buildable land assessment is disabled for this site.

    Args:
        site_config: SiteConfig from the CSV pipeline.

    Returns:
        Shapely Polygon in EPSG:4326, or None if assessment is disabled.
    """
    if not site_config.buildable_land_assessment:
        return None

    if site_config.kmz_file_path is not None:
        return parse_kmz(site_config.kmz_file_path)

    radius = (
        site_config.analysis_radius_km
        if site_config.analysis_radius_km is not None
        else 1.0
    )
    return create_buffer(site_config.latitude, site_config.longitude, radius)
