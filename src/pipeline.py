"""Main pipeline entry point for climate data retrieval."""

from pathlib import Path

from src.climate.cache_manager import CacheManager
from src.climate.config import ClimateConfig
from src.climate.nsrdb_client import NSRDBClient
from src.climate.orchestrator import ClimateOrchestrator
from src.climate.precipitation_client import PrecipitationClient
from src.climate.weather_formatter import WeatherFormatter
from src.config.loader import load_config
from src.config.schema import SiteConfig
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def run_climate_data_pipeline(
    config_csv: Path, year: int = 2024
) -> list[SiteConfig]:
    """Load sites from CSV and fetch climate data for all locations.

    Args:
        config_csv: Path to CSV file with site configurations.
        year: Weather data year to retrieve.

    Returns:
        List of SiteConfig objects with weather_file_path assigned.
    """
    # Load and validate site configurations
    sites = load_config(config_csv)
    logger.info(f"Loaded {len(sites)} sites from {config_csv}")

    # Build climate pipeline components from config
    config = ClimateConfig()
    nsrdb_client = NSRDBClient(api_key=config.api_key, email=config.api_email)
    cache_manager = CacheManager(cache_dir=config.cache_dir)
    formatter = WeatherFormatter()

    precipitation_client = None
    if config.precipitation_enabled:
        precipitation_client = PrecipitationClient(api_token=config.ncei_token)

    orchestrator = ClimateOrchestrator(
        nsrdb_client=nsrdb_client,
        cache_manager=cache_manager,
        formatter=formatter,
        precipitation_client=precipitation_client,
    )

    # Fetch climate data for all unique locations
    location_to_file = orchestrator.fetch_climate_data(
        sites,
        year=year,
        max_age_days=config.cache_max_age_days,
        max_cache_distance_km=config.max_cache_distance_km,
    )

    # Assign weather file paths to each site
    for site in sites:
        if site.location in location_to_file:
            site.weather_file_path = location_to_file[site.location]

    print_summary(sites, location_to_file)
    return sites


def print_summary(
    sites: list[SiteConfig],
    location_to_file: dict[tuple[float, float], Path],
) -> None:
    """Log a summary of the climate data pipeline results.

    Args:
        sites: List of site configurations.
        location_to_file: Mapping from (lat, lon) to weather file paths.
    """
    sites_with_data = sum(1 for s in sites if s.weather_file_path is not None)
    logger.info(
        f"Pipeline summary: {len(sites)} total sites, "
        f"{len(location_to_file)} unique locations, "
        f"{sites_with_data} sites with weather data assigned"
    )
