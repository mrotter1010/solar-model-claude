"""Open-Meteo API client for ERA5 weather data and elevation lookup.

Fetches hourly ERA5 reanalysis data and site elevation via Open-Meteo's
free REST APIs. No API key required.
"""

import calendar
import json
import urllib.error
import urllib.request
from pathlib import Path

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_ELEVATION_URL = "https://api.open-meteo.com/v1/elevation"

# In-memory cache for elevation lookups (elevation doesn't change)
_elevation_cache: dict[tuple[float, float], float] = {}


def get_elevation_m(latitude: float, longitude: float) -> float:
    """Look up site elevation via the Open-Meteo Elevation API.

    Results are cached in memory keyed on (lat, lon). Unlike ERA5 data,
    this function raises on failure rather than returning a default — the
    bias correction model requires an accurate elevation input.

    Args:
        latitude: Site latitude in degrees.
        longitude: Site longitude in degrees.

    Returns:
        Elevation in meters above sea level.

    Raises:
        ClimateDataError: If the API call fails or returns unexpected data.
    """
    key = (latitude, longitude)
    if key in _elevation_cache:
        logger.debug("Elevation cache hit for (%.4f, %.4f)", latitude, longitude)
        return _elevation_cache[key]

    url = (
        f"{OPEN_METEO_ELEVATION_URL}"
        f"?latitude={latitude}&longitude={longitude}"
    )

    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())

        if "elevation" not in data or not data["elevation"]:
            raise ValueError(f"Unexpected response: {data}")

        elevation = float(data["elevation"][0])
        _elevation_cache[key] = elevation
        logger.debug(
            "Elevation lookup: (%.4f, %.4f) = %.0f m",
            latitude, longitude, elevation,
        )
        return elevation

    except Exception as exc:
        from src.utils.exceptions import ClimateDataError

        raise ClimateDataError(
            f"Elevation lookup failed for ({latitude}, {longitude}). "
            f"Please supply elevation manually or check network connectivity.",
            context={
                "location": (latitude, longitude),
                "url": url,
                "error": str(exc),
            },
        ) from exc


def fetch_era5_land_data(
    lat: float,
    lon: float,
    year: int,
    cache_dir: Path = Path("data/climate/era5"),
) -> dict[str, list[float]] | None:
    """Fetch snow depth and precipitation from ERA5 via Open-Meteo API.

    Uses Open-Meteo's Historical Weather API backed by ERA5 reanalysis.
    Results are cached locally as JSON for subsequent runs.
    Best-effort function: any failure returns None and the caller falls back
    to defaults. Never raises exceptions.

    Args:
        lat: Site latitude in degrees.
        lon: Site longitude in degrees.
        year: Data year to retrieve.
        cache_dir: Directory for caching extracted JSON results.

    Returns:
        Dict with keys:
            - "snow_depth_cm": list of 8760 hourly values in centimeters.
            - "monthly_precip_inches": list of 12 monthly totals in inches.
        Returns None if retrieval fails.
    """
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)

        cache_path = cache_dir / f"era5_{lat}_{lon}_{year}.json"
        if cache_path.exists():
            logger.debug(f"ERA5 cache hit: {cache_path.name}")
            return _load_cache(cache_path)

        logger.info(
            f"Fetching ERA5 data from Open-Meteo for ({lat}, {lon}), "
            f"year {year}"
        )

        result = _fetch_from_open_meteo(lat, lon, year)

        _save_cache(cache_path, result)
        logger.info(
            f"Open-Meteo processed: snow_depth {len(result['snow_depth_cm'])} "
            f"hours, precip {len(result['monthly_precip_inches'])} months"
        )

        return result

    except Exception:
        logger.warning(
            f"Unexpected error fetching ERA5 data for ({lat}, {lon})",
            exc_info=True,
        )
        return None


def _fetch_from_open_meteo(
    lat: float, lon: float, year: int
) -> dict[str, list[float]]:
    """Fetch snow depth and precipitation from Open-Meteo Historical API.

    Args:
        lat: Site latitude in degrees.
        lon: Site longitude in degrees.
        year: Data year to retrieve.

    Returns:
        Dict with "snow_depth_cm" (8760/8784 floats) and
        "monthly_precip_inches" (12 floats).

    Raises:
        ValueError: If the API returns an unexpected number of hours.
        urllib.error.URLError: If the HTTP request fails.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "hourly": "snow_depth,precipitation",
        "timezone": "auto",
    }
    url = OPEN_METEO_URL + "?" + "&".join(
        f"{k}={v}" for k, v in params.items()
    )

    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode())

    hourly = data["hourly"]
    timestamps = hourly["time"]
    snow_raw = hourly["snow_depth"]
    precip_raw = hourly["precipitation"]

    # Validate expected hour count
    expected = 8784 if calendar.isleap(year) else 8760
    if len(snow_raw) not in (8760, 8784):
        raise ValueError(
            f"Open-Meteo returned {len(snow_raw)} snow_depth values, "
            f"expected {expected}"
        )

    # Snow depth: meters -> cm, None -> 0.0, clip negatives
    snow_depth_cm = [
        max((v if v is not None else 0.0) * 100.0, 0.0)
        for v in snow_raw
    ]

    # Precipitation: mm per hour -> monthly totals in inches
    monthly_precip_mm = [0.0] * 12
    for ts_str, val in zip(timestamps, precip_raw):
        v = val if val is not None else 0.0
        if v > 0.0:
            month = int(ts_str[5:7])  # "2023-01-15T00:00" -> "01" -> 1
            monthly_precip_mm[month - 1] += v

    monthly_precip_inches = [mm / 25.4 for mm in monthly_precip_mm]

    logger.debug(
        f"Open-Meteo grid point: ({lat}, {lon}), "
        f"snow hours={len(snow_depth_cm)}, "
        f"total precip={sum(monthly_precip_mm):.1f} mm"
    )

    return {
        "snow_depth_cm": snow_depth_cm,
        "monthly_precip_inches": monthly_precip_inches,
    }


def _load_cache(cache_path: Path) -> dict[str, list[float]]:
    """Load cached ERA5 results from a JSON file.

    Args:
        cache_path: Path to the JSON cache file.

    Returns:
        Dict with "snow_depth_cm" and "monthly_precip_inches".
    """
    with cache_path.open("r") as f:
        return json.load(f)


def _save_cache(
    cache_path: Path, data: dict[str, list[float]]
) -> None:
    """Save ERA5 results to a JSON cache file.

    Args:
        cache_path: Path to write the JSON cache file.
        data: Dict with "snow_depth_cm" and "monthly_precip_inches".
    """
    with cache_path.open("w") as f:
        json.dump(data, f)
    logger.debug(f"ERA5 cache saved: {cache_path.name}")
