"""Weather data file cache manager for avoiding redundant NSRDB API calls."""

import math
import re
from datetime import datetime, timezone
from pathlib import Path

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Pattern: nsrdb_{lat}_{lon}_{YYYYMMDD}.csv
CACHE_FILENAME_PATTERN = re.compile(
    r"nsrdb_([-\d.]+)_([-\d.]+)_(\d{8})\.csv"
)


class CacheManager:
    """Manages cached NSRDB weather data files on disk.

    Args:
        cache_dir: Directory for storing cached weather CSV files.
    """

    def __init__(self, cache_dir: Path = Path("data/climate")) -> None:
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_cached_file(
        self, lat: float, lon: float, max_age_days: int = 365
    ) -> Path | None:
        """Find an exact-match cached file for the given coordinates.

        Args:
            lat: Latitude to match.
            lon: Longitude to match.
            max_age_days: Maximum age in days before a cached file is stale.

        Returns:
            Path to cached file if found and fresh, None otherwise.
        """
        for filepath in self.cache_dir.glob("nsrdb_*.csv"):
            match = CACHE_FILENAME_PATTERN.match(filepath.name)
            if not match:
                continue

            file_lat = float(match.group(1))
            file_lon = float(match.group(2))
            file_date_str = match.group(3)

            if file_lat != lat or file_lon != lon:
                continue

            # Check age
            file_date = datetime.strptime(file_date_str, "%Y%m%d").replace(
                tzinfo=timezone.utc
            )
            age_days = (datetime.now(timezone.utc) - file_date).days

            if age_days > max_age_days:
                logger.debug(
                    f"Cache file {filepath.name} is stale ({age_days} days old)"
                )
                return None

            logger.info(f"Cache hit for ({lat}, {lon}): {filepath.name}")
            return filepath

        logger.debug(f"No cache hit for ({lat}, {lon})")
        return None

    def find_nearest_cache(
        self, lat: float, lon: float, max_distance_km: float = 50.0
    ) -> tuple[Path, float] | None:
        """Find the nearest cached file within max_distance_km.

        Args:
            lat: Target latitude.
            lon: Target longitude.
            max_distance_km: Maximum acceptable distance in kilometers.

        Returns:
            Tuple of (path, distance_km) for the nearest cache, or None.
        """
        best: tuple[Path, float] | None = None

        for filepath in self.cache_dir.glob("nsrdb_*.csv"):
            match = CACHE_FILENAME_PATTERN.match(filepath.name)
            if not match:
                continue

            file_lat = float(match.group(1))
            file_lon = float(match.group(2))

            distance = _calculate_distance(lat, lon, file_lat, file_lon)

            if distance > max_distance_km:
                continue

            if best is None or distance < best[1]:
                best = (filepath, distance)

        if best:
            logger.info(
                f"Nearest cache for ({lat}, {lon}): {best[0].name} "
                f"({best[1]:.1f} km away)"
            )
        else:
            logger.debug(
                f"No cache within {max_distance_km} km of ({lat}, {lon})"
            )

        return best

    def save_weather_data(self, lat: float, lon: float, data: str) -> Path:
        """Save weather data CSV to the cache directory.

        Args:
            lat: Latitude of the data location.
            lon: Longitude of the data location.
            data: Raw CSV string from NSRDB.

        Returns:
            Path to the saved cache file.
        """
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        filename = f"nsrdb_{lat}_{lon}_{date_str}.csv"
        filepath = self.cache_dir / filename

        filepath.write_text(data)
        logger.info(f"Saved weather data to cache: {filename}")

        return filepath


def _calculate_distance(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    """Calculate distance between two points using the Haversine formula.

    Args:
        lat1: Latitude of point 1 in degrees.
        lon1: Longitude of point 1 in degrees.
        lat2: Latitude of point 2 in degrees.
        lon2: Longitude of point 2 in degrees.

    Returns:
        Distance in kilometers.
    """
    earth_radius_km = 6371.0

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return earth_radius_km * c
