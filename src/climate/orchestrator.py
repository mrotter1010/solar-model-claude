"""Orchestrator for coordinating climate data retrieval across sites."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd

from src.climate.cache_manager import CacheManager
from src.climate.nsrdb_client import NSRDBClient
from src.climate.soiling_calculator import calculate_monthly_soiling
from src.climate.solcast_parser import parse_solcast_file
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
        era5_client: Optional callable for ERA5-Land data retrieval
            (snow depth + precipitation). Expected signature:
            (lat, lon, year) -> dict | None.
    """

    def __init__(
        self,
        nsrdb_client: NSRDBClient,
        cache_manager: CacheManager,
        formatter: WeatherFormatter,
        era5_client: Callable[..., dict[str, Any] | None] | None = None,
    ) -> None:
        self.nsrdb_client = nsrdb_client
        self.cache_manager = cache_manager
        self.formatter = formatter
        self.era5_client = era5_client

    def fetch_climate_data(
        self,
        sites: list[SiteConfig],
        year: int | str = "tmy",
        max_age_days: int = 365,
        max_cache_distance_km: float = 50.0,
    ) -> dict[tuple[float, float], dict[str, Any]]:
        """Fetch climate data for all unique site locations.

        Deduplicates locations, checks cache before calling API, and returns
        a mapping from (lat, lon) to climate result dicts.

        Args:
            sites: List of site configurations to fetch data for.
            year: Weather data year to retrieve.
            max_age_days: Maximum cache file age in days.
            max_cache_distance_km: Maximum distance for nearest-cache fallback.

        Returns:
            Dict mapping (lat, lon) tuples to dicts with keys:
                - "weather_file": Path to cached weather file.
                - "monthly_soiling": list[float] | None, 12 monthly soiling
                  loss percentages from ERA5 precipitation, or None.
        """
        unique_locations = get_unique_locations(sites)
        results: dict[tuple[float, float], dict[str, Any]] = {}
        cache_hits = 0
        api_calls = 0

        for lat, lon in unique_locations:
            # Step 0: Check for Solcast resource file at this location
            solcast_file = next(
                (
                    site.resource_file_path
                    for site in sites
                    if site.latitude == lat
                    and site.longitude == lon
                    and site.resource_file_path is not None
                ),
                None,
            )

            if solcast_file is not None:
                solcast_result = parse_solcast_file(solcast_file, lat, lon)
                pysam_path = solcast_result["weather_file"]
                logger.info(
                    f"Using Solcast resource file for ({lat}, {lon}): {pysam_path}"
                )

                # ERA5 for precipitation (soiling) — still needed even with Solcast
                monthly_soiling = None
                if self.era5_client is not None:
                    # TODO M-future: average multiple ERA5 years for TMY instead of using single year
                    era5_year = 2024 if year == "tmy" else year
                    era5_data = self.era5_client(lat, lon, era5_year)
                    if era5_data is not None:
                        monthly_precip_inches = era5_data["monthly_precip_inches"]
                        monthly_soiling = calculate_monthly_soiling(
                            monthly_precip_inches
                        )
                    else:
                        logger.warning(
                            f"ERA5-Land data unavailable for ({lat}, {lon}), "
                            f"soiling will not be modeled"
                        )

                # Injecting ERA5 snow data would require rewriting the commercial
                # resource file. We pass Solcast files directly to PySAM unmodified.
                if not solcast_result["has_snow_depth"]:
                    prompt = (
                        f"\nWARNING: Solcast file for ({lat}, {lon}) does not "
                        f"include a Snow Depth column. Snow losses will NOT be "
                        f"modeled for this location.\n"
                        f"Proceed without snow losses? (y/n): "
                    )
                    choice = input(prompt).strip().lower()
                    if choice != "y":
                        raise ClimateDataError(
                            f"User aborted: Solcast file for ({lat}, {lon}) "
                            f"missing Snow Depth column",
                            context={
                                "location": (lat, lon),
                                "file": str(solcast_file),
                            },
                        )

                results[(lat, lon)] = {
                    "weather_file": pysam_path,
                    "monthly_soiling": monthly_soiling,
                    "data_source": "solcast",
                    "solcast_metadata": solcast_result["metadata"],
                }
                continue

            # Step 1: Get NSRDB data (from cache or API)
            cached_path = self.cache_manager.get_cached_file(
                lat, lon, year=year, max_age_days=max_age_days
            )
            if cached_path is not None:
                cache_path = cached_path
                raw_csv = cached_path.read_text()
                cache_hits += 1
            else:
                # Cache miss — fetch from API
                try:
                    raw_csv = self.nsrdb_client.fetch_weather_data(lat, lon, year)
                    api_calls += 1
                except ClimateDataError as e:
                    df, metadata = self.handle_api_failure(
                        lat, lon, e, year=year, max_cache_distance_km=max_cache_distance_km
                    )
                    results[(lat, lon)] = {
                        "weather_df": df,
                        "weather_metadata": metadata,
                        "monthly_soiling": None,
                        "data_source": "nsrdb",
                    }
                    continue

                # Save raw CSV to cache
                cache_path = self.cache_manager.save_weather_data(lat, lon, year, raw_csv)

            # Step 2: ERA5-Land — snow depth + precipitation for soiling
            snow_depth_cm = None
            monthly_soiling = None
            if self.era5_client is not None:
                # TODO M-future: average multiple ERA5 years for TMY instead of using single year
                era5_year = 2024 if year == "tmy" else year
                era5_data = self.era5_client(lat, lon, era5_year)
                if era5_data is not None:
                    snow_depth_cm = era5_data["snow_depth_cm"]
                    # Trim leap day (Feb 29) when ERA5 year is a leap year
                    # but weather data is TMY (always 8760 hours, no Feb 29).
                    # Feb 29 starts at hour index 1416 (Jan=744h + Feb1-28=672h).
                    if year == "tmy" and len(snow_depth_cm) == 8784:
                        snow_depth_cm = (
                            snow_depth_cm[:1416] + snow_depth_cm[1440:]
                        )
                    monthly_precip_inches = era5_data["monthly_precip_inches"]
                    monthly_soiling = calculate_monthly_soiling(
                        monthly_precip_inches
                    )
                else:
                    logger.warning(
                        f"ERA5-Land data unavailable for ({lat}, {lon}), "
                        f"using default snow/soiling"
                    )

            # Step 3: Format PySAM-compatible data (returned in-memory for
            # pipeline to apply bias correction and save to corrected cache)
            df, metadata = self.formatter.format_for_pysam(
                raw_csv, lat, lon,
                snow_depth_cm=snow_depth_cm,
            )

            results[(lat, lon)] = {
                "weather_df": df,
                "weather_metadata": metadata,
                "monthly_soiling": monthly_soiling,
                "data_source": "nsrdb",
            }

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
        year: int | str = "tmy",
        max_cache_distance_km: float = 50.0,
    ) -> tuple[pd.DataFrame, dict]:
        """Handle an API failure with interactive user prompts.

        Offers retry, nearest cache fallback (if available), or abort.

        Args:
            lat: Latitude of the failed location.
            lon: Longitude of the failed location.
            error: The original API error.
            max_cache_distance_km: Maximum distance for nearest-cache fallback.

        Returns:
            Tuple of (formatted DataFrame, metadata dict) for bias correction.

        Raises:
            ClimateDataError: If user chooses to abort.
        """
        logger.error(f"API failure for ({lat}, {lon}): {error}")

        nearest = self.cache_manager.find_nearest_cache(
            lat, lon, year=year, max_distance_km=max_cache_distance_km
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
                    raw_csv = self.nsrdb_client.fetch_weather_data(lat, lon, year)
                    self.cache_manager.save_weather_data(
                        lat, lon, year, raw_csv
                    )
                    df, metadata = self.formatter.format_for_pysam(
                        raw_csv, lat, lon, snow_depth_cm=None
                    )
                    logger.info(
                        f"Retry successful for ({lat}, {lon}) on attempt {attempt + 1}"
                    )
                    return df, metadata
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
                raw_csv = nearest_path.read_text()
                df, metadata = self.formatter.format_for_pysam(
                    raw_csv, lat, lon, snow_depth_cm=None
                )
                return df, metadata

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
