"""Main pipeline: CSV → climate data → PySAM simulation → output files."""

from pathlib import Path

from src.climate.cache_manager import CacheManager
from src.climate.config import ClimateConfig
from src.climate.nsrdb_client import NSRDBClient
from src.climate.orchestrator import ClimateOrchestrator
from src.climate.precipitation_client import PrecipitationClient
from src.climate.weather_formatter import WeatherFormatter
from src.config.loader import load_config
from src.config.schema import SiteConfig
from src.outputs.output_writer import OutputWriter
from src.pysam_integration.cec_database import CECDatabase
from src.pysam_integration.model_configurator import ModelConfigurator
from src.pysam_integration.simulator import BatchSimulator, PySAMSimulator
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


class SolarModelingPipeline:
    """End-to-end pipeline: CSV → climate → PySAM → outputs.

    Args:
        output_dir: Root directory for all output files.
    """

    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir
        self.cec_db = CECDatabase()
        self.configurator = ModelConfigurator(cec_database=self.cec_db)
        self.batch_simulator = BatchSimulator(
            configurator=self.configurator,
            simulator=PySAMSimulator(),
        )
        self.output_writer = OutputWriter(output_dir=output_dir)

    def run(
        self, csv_path: Path, skip_climate: bool = False
    ) -> dict[str, object]:
        """Execute the full modeling pipeline.

        Args:
            csv_path: Path to input CSV with site configurations.
            skip_climate: If True, skip climate data fetch (sites must already
                have weather_file_path assigned).

        Returns:
            Dict with keys: total_sites, successful, failed,
            timeseries_files, summary_files, error_files.
        """
        # Step 1: Load site configs
        logger.info(f"Loading site configurations from {csv_path}")
        site_configs = load_config(csv_path)
        logger.info(f"Loaded {len(site_configs)} sites")

        # Step 2: Climate data retrieval
        if not skip_climate:
            logger.info("Fetching climate data...")
            site_configs = run_climate_data_pipeline(csv_path)
        else:
            logger.info("Skipping climate data fetch (skip_climate=True)")

        # Verify all sites have weather files
        sites_without_weather = [
            s for s in site_configs if s.weather_file_path is None
        ]
        if sites_without_weather:
            names = [s.site_name for s in sites_without_weather]
            logger.warning(
                f"{len(sites_without_weather)} sites missing weather data: {names}"
            )

        # Step 3: Run PySAM simulations
        logger.info("Running PySAM simulations...")
        successful, failed = self.batch_simulator.run_batch(site_configs)

        # Step 4: Write outputs
        logger.info("Writing output files...")
        timeseries_files: list[Path] = []
        summary_files: list[Path] = []
        error_files: list[Path] = []

        # Build a lookup from site_name to SiteConfig for output writing
        site_lookup = {s.site_name: s for s in site_configs}

        for result in successful:
            site = site_lookup[result.site_name]
            ts_path, summary_path = self.output_writer.write_outputs(
                simulation_result=result,
                site_config=site,
                shading_pct=site.shading_percent,
            )
            if ts_path is not None:
                timeseries_files.append(ts_path)
            summary_files.append(summary_path)

        for result in failed:
            site = site_lookup[result.site_name]
            _, error_path = self.output_writer.write_outputs(
                simulation_result=result,
                site_config=site,
                shading_pct=site.shading_percent,
            )
            error_files.append(error_path)

        # Log summary
        logger.info(
            f"Pipeline complete: {len(successful)} succeeded, "
            f"{len(failed)} failed out of {len(site_configs)} total"
        )

        return {
            "total_sites": len(site_configs),
            "successful": len(successful),
            "failed": len(failed),
            "timeseries_files": timeseries_files,
            "summary_files": summary_files,
            "error_files": error_files,
        }
