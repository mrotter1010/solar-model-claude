"""Orchestrator for coordinating climate data retrieval across sites."""

from pathlib import Path

from src.climate.cache_manager import CacheManager
from src.climate.nsrdb_client import NSRDBClient
from src.climate.weather_formatter import WeatherFormatter
from src.config.loader import get_unique_locations
from src.config.schema import SiteConfig
from src.utils.exceptions import ClimateDataError
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

MAX_RETRIES = 3


class ClimateOrchestrator:
    """Coordinates cache lookups, API fetches, and formatting for site locations.

    Args:
        nsrdb_client: Client for NSRDB API calls.
        cache_manager: Manager for weather data file caching.
        formatter: Formatter for converting NSRDB data to PySAM format.
    """

    def __init__(
        self,
        nsrdb_client: NSRDBClient,
        cache_manager: CacheManager,
        formatter: WeatherFormatter,
    ) -> None:
        self.nsrdb_client = nsrdb_client
        self.cache_manager = cache_manager
        self.formatter = formatter

    def fetch_climate_data(
        self,
        sites: list[SiteConfig],
        year: int = 2024,
        max_age_days: int = 365,
        max_cache_distance_km: float = 50.0,
    ) -> dict[tuple[float, float], Path]:
        """Fetch climate data for all unique site locations.

        Deduplicates locations, checks cache before calling API, and returns
        a mapping from (lat, lon) to weather file paths.

        Args:
            sites: List of site configurations to fetch data for.
            year: Weather data year to retrieve.
            max_age_days: Maximum cache file age in days.
            max_cache_distance_km: Maximum distance for nearest-cache fallback.

        Returns:
            Dict mapping (lat, lon) tuples to weather file paths.
        """
        unique_locations = get_unique_locations(sites)
        results: dict[tuple[float, float], Path] = {}
        cache_hits = 0
        api_calls = 0

        for lat, lon in unique_locations:
            # Check cache first
            cached_path = self.cache_manager.get_cached_file(
                lat, lon, max_age_days=max_age_days
            )
            if cached_path is not None:
                results[(lat, lon)] = cached_path
                cache_hits += 1
                continue

            # Cache miss — fetch from API
            try:
                raw_csv = self.nsrdb_client.fetch_weather_data(lat, lon, year)
                api_calls += 1
            except ClimateDataError as e:
                path = self.handle_api_failure(
                    lat, lon, e, max_cache_distance_km=max_cache_distance_km
                )
                results[(lat, lon)] = path
                continue

            # Save raw CSV to cache
            cache_path = self.cache_manager.save_weather_data(lat, lon, raw_csv)

            # Format and save PySAM-compatible file alongside cache
            df = self.formatter.format_for_pysam(raw_csv, lat, lon)
            pysam_path = cache_path.with_suffix(".pysam.csv")
            self.formatter.save_to_csv(df, pysam_path, lat, lon)

            results[(lat, lon)] = cache_path

        logger.info(
            f"Climate data summary: {len(unique_locations)} unique locations, "
            f"{cache_hits} cache hits, {api_calls} API calls"
        )
        return results

    def handle_api_failure(
        self,
        lat: float,
        lon: float,
        error: ClimateDataError,
        max_cache_distance_km: float = 50.0,
    ) -> Path:
        """Handle an API failure with interactive user prompts.

        Offers retry, nearest cache fallback (if available), or abort.

        Args:
            lat: Latitude of the failed location.
            lon: Longitude of the failed location.
            error: The original API error.
            max_cache_distance_km: Maximum distance for nearest-cache fallback.

        Returns:
            Path to recovered weather file.

        Raises:
            ClimateDataError: If user chooses to abort.
        """
        logger.error(f"API failure for ({lat}, {lon}): {error}")

        nearest = self.cache_manager.find_nearest_cache(
            lat, lon, max_distance_km=max_cache_distance_km
        )

        for attempt in range(MAX_RETRIES):
            # Build prompt options
            options = ["1) Retry API call"]
            if nearest is not None:
                nearest_path, nearest_dist = nearest
                options.append(
                    f"2) Use nearest cache: {nearest_path.name} "
                    f"({nearest_dist:.1f} km away)"
                )
            abort_num = 3 if nearest else 2
            options.append(f"{abort_num}) Abort")

            prompt = (
                f"\nAPI call failed for ({lat}, {lon}): {error.message}\n"
                + "\n".join(options)
                + "\nChoice: "
            )
            choice = input(prompt).strip()

            if choice == "1":
                # Retry
                try:
                    raw_csv = self.nsrdb_client.fetch_weather_data(lat, lon)
                    cache_path = self.cache_manager.save_weather_data(
                        lat, lon, raw_csv
                    )
                    df = self.formatter.format_for_pysam(raw_csv, lat, lon)
                    pysam_path = cache_path.with_suffix(".pysam.csv")
                    self.formatter.save_to_csv(df, pysam_path, lat, lon)
                    logger.info(
                        f"Retry successful for ({lat}, {lon}) on attempt {attempt + 1}"
                    )
                    return cache_path
                except ClimateDataError as retry_error:
                    logger.warning(
                        f"Retry {attempt + 1}/{MAX_RETRIES} failed for ({lat}, {lon})"
                    )
                    error = retry_error
                    continue

            if choice == "2" and nearest is not None:
                nearest_path, nearest_dist = nearest
                logger.info(
                    f"Using nearest cache for ({lat}, {lon}): "
                    f"{nearest_path.name} ({nearest_dist:.1f} km)"
                )
                return nearest_path

            # Abort (choice matches abort number, or any unrecognized input)
            logger.error(f"User aborted climate data fetch for ({lat}, {lon})")
            raise ClimateDataError(
                f"Climate data fetch aborted by user for ({lat}, {lon})",
                context={"location": (lat, lon), "original_error": str(error)},
            )

        # Exhausted all retries
        logger.error(
            f"All {MAX_RETRIES} retries exhausted for ({lat}, {lon})"
        )
        raise ClimateDataError(
            f"Failed to fetch climate data after {MAX_RETRIES} retries",
            context={"location": (lat, lon), "last_error": str(error)},
        )
