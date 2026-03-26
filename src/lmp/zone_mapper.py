"""Geographic zone auto-detection for ISO/RTO pricing zones."""

import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

from src.config.schema import SiteConfig
from src.lmp.models import PricingZone

logger = logging.getLogger(__name__)

# Module-level cache for PJM geo data (loaded once, reused across calls)
_pjm_gdf: gpd.GeoDataFrame | None = None
_pjm_lookup: pd.DataFrame | None = None

# Paths to PJM geo data files
_GEO_DIR = Path("data/geo")
_EIA_GEOJSON = _GEO_DIR / "eia_service_territories.geojson"
_PJM_LOOKUP_CSV = _GEO_DIR / "utility_to_pjm_zone.csv"


def get_iso_from_latlon(lat: float, lon: float) -> str | None:
    """Determine which ISO territory a lat/lon falls in using bounding boxes.

    These are rough approximations for first-pass filtering. PJM especially
    has irregular boundaries; precise PJM membership uses the EIA shapefile
    lookup in get_pjm_zone().

    Args:
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.

    Returns:
        ISO identifier ("pjm", "ercot", "caiso") or None if no match.
    """
    # CAISO: California — check before PJM since longitude ranges don't overlap
    if 32.0 <= lat <= 42.0 and -124.5 <= lon <= -114.0:
        return "caiso"

    # ERCOT: Texas
    if 25.5 <= lat <= 36.5 and -106.5 <= lon <= -93.5:
        return "ercot"

    # PJM: Mid-Atlantic / Ohio Valley
    if 36.5 <= lat <= 42.5 and -83.5 <= lon <= -74.0:
        return "pjm"

    return None


def get_pjm_zone(lat: float, lon: float) -> str | None:
    """Look up PJM pricing zone via EIA service territory shapefile.

    Uses point-in-polygon lookup against EIA utility service territories,
    then maps the utility to a PJM transmission zone. Results are cached
    at module level to avoid re-reading files on every call.

    Args:
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.

    Returns:
        PJM zone code (e.g., "AEP", "PECO", "DOM") or None if not in PJM
        or if geo data files are not available.
    """
    global _pjm_gdf, _pjm_lookup

    # Load geo data on first call
    if _pjm_gdf is None:
        try:
            _pjm_gdf = gpd.read_file(_EIA_GEOJSON)
            logger.info("Loaded EIA service territories from %s", _EIA_GEOJSON)
        except Exception as exc:
            logger.warning(
                "EIA service territory file not found or unreadable: %s. "
                "PJM zone auto-detection unavailable. (%s)",
                _EIA_GEOJSON,
                exc,
            )
            return None

    if _pjm_lookup is None:
        try:
            _pjm_lookup = pd.read_csv(_PJM_LOOKUP_CSV)
            logger.info("Loaded PJM zone lookup from %s", _PJM_LOOKUP_CSV)
        except Exception as exc:
            logger.warning(
                "PJM zone lookup file not found or unreadable: %s. "
                "PJM zone auto-detection unavailable. (%s)",
                _PJM_LOOKUP_CSV,
                exc,
            )
            return None

    # Point-in-polygon lookup
    point = gpd.points_from_xy([lon], [lat], crs="EPSG:4326")
    point_gdf = gpd.GeoDataFrame(geometry=point, crs="EPSG:4326")

    # Ensure matching CRS
    if _pjm_gdf.crs != point_gdf.crs:
        _pjm_gdf = _pjm_gdf.to_crs("EPSG:4326")

    joined = gpd.sjoin(point_gdf, _pjm_gdf, how="left", predicate="within")

    if joined.empty or joined.iloc[0].get("NAME") is None:
        return None

    utility_name = joined.iloc[0]["NAME"]

    # Look up PJM zone from utility name
    match = _pjm_lookup[_pjm_lookup["utility_name"] == utility_name]
    if match.empty:
        logger.debug(
            "Utility '%s' not found in PJM zone lookup table", utility_name
        )
        return None

    return match.iloc[0]["pjm_zone"]


def get_ercot_zone(lat: float, lon: float) -> str:
    """Map lat/lon to an ERCOT load zone using simple geographic rules.

    These are rough approximations suitable for zone-level pricing.
    Users can override via site_config.lmp_zone.

    Args:
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.

    Returns:
        ERCOT load zone name (e.g., "LZ_HOUSTON", "LZ_NORTH").
    """
    # Houston metro area
    if 29.5 <= lat <= 30.2 and -95.8 <= lon <= -94.9:
        return "LZ_HOUSTON"

    # Dallas/Fort Worth
    if 32.3 <= lat <= 33.3 and -97.8 <= lon <= -96.3:
        return "LZ_NORTH"

    # San Antonio (CPS Energy)
    if 29.0 <= lat <= 29.8 and -98.8 <= lon <= -98.0:
        return "LZ_CPS"

    # Austin / LCRA territory
    if 30.0 <= lat <= 31.0 and -98.5 <= lon <= -97.0:
        return "LZ_LCRA"

    # West Texas
    if lon < -101.0:
        return "LZ_WEST"

    # South Texas
    if lat < 28.5:
        return "LZ_SOUTH"

    # Fallback
    return "LZ_SOUTH"


def get_caiso_zone(lat: float, lon: float) -> str:
    """Map lat/lon to a CAISO trading hub using latitude thresholds.

    Args:
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees (unused, kept for API consistency).

    Returns:
        CAISO trading hub name ("NP15", "ZP26", "SP15").
    """
    if lat >= 37.0:
        return "NP15"
    if lat >= 35.5:
        return "ZP26"
    return "SP15"


def resolve_pricing_zone(site_config: SiteConfig) -> PricingZone:
    """Resolve the pricing zone for a site, auto-detecting if needed.

    Logic:
        1. If site_config.lmp_zone is provided, use it directly (skip auto-detect).
        2. Determine ISO: use site_config.iso if set, else get_iso_from_latlon().
        3. If ISO is None, raise ValueError.
        4. Auto-detect zone using the ISO-specific mapper.
        5. Return PricingZone.

    Args:
        site_config: Site configuration with location and optional LMP fields.

    Returns:
        PricingZone with resolved ISO, zone name, and zone type.

    Raises:
        ValueError: If ISO cannot be determined from config or coordinates.
    """
    # Step 1: Determine ISO
    iso = site_config.iso
    if iso is None:
        iso = get_iso_from_latlon(site_config.latitude, site_config.longitude)

    if iso is None:
        raise ValueError(
            f"Could not determine ISO for site at "
            f"({site_config.latitude}, {site_config.longitude}). "
            f"Set the 'iso' field explicitly in site config."
        )

    # Step 2: If explicit lmp_zone provided, use it directly
    if site_config.lmp_zone is not None:
        return PricingZone(
            iso=iso,
            zone_name=site_config.lmp_zone,
            zone_type="zone",
            description=f"User-specified zone for {iso.upper()}",
        )

    # Step 3: Auto-detect zone based on ISO
    zone_name: str | None = None
    zone_type = "zone"

    if iso == "pjm":
        zone_name = get_pjm_zone(site_config.latitude, site_config.longitude)
        zone_type = "zone"
        if zone_name is None:
            logger.warning(
                "Could not auto-detect PJM zone for (%s, %s). "
                "Set lmp_zone explicitly.",
                site_config.latitude,
                site_config.longitude,
            )
            raise ValueError(
                f"Could not auto-detect PJM zone for site at "
                f"({site_config.latitude}, {site_config.longitude}). "
                f"Set 'lmp_zone' explicitly in site config."
            )

    elif iso == "ercot":
        zone_name = get_ercot_zone(
            site_config.latitude, site_config.longitude
        )
        zone_type = "zone"

    elif iso == "caiso":
        zone_name = get_caiso_zone(
            site_config.latitude, site_config.longitude
        )
        zone_type = "hub"

    else:
        raise ValueError(f"Unsupported ISO: '{iso}'.")

    return PricingZone(
        iso=iso,
        zone_name=zone_name,
        zone_type=zone_type,
        description=f"Auto-detected {zone_type} for {iso.upper()}",
    )
