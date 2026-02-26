"""PySAM simulation execution engine with batch processing."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.pysam_integration.exceptions import (
    PySAMConfigurationError,
    SimulationExecutionError,
)
from src.pysam_integration.model_configurator import ModelConfigurator, PySAMModelConfig
from src.config.schema import SiteConfig
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


@dataclass
class SimulationResult:
    """Result container for a single PySAM simulation run."""

    site_name: str
    run_name: str
    customer: str
    success: bool
    error_message: str | None = None
    hourly_data: pd.DataFrame | None = None
    weather_year: int | None = None
    simulation_timestamp: str = field(default="", init=False)

    def __post_init__(self) -> None:
        """Set simulation timestamp to current UTC time in ISO 8601."""
        self.simulation_timestamp = datetime.now(tz=timezone.utc).isoformat()


class PySAMSimulator:
    """Executes PySAM simulations and extracts hourly timeseries results."""

    def execute_simulation(self, model_config: PySAMModelConfig) -> SimulationResult:
        """Execute a PySAM simulation and return results.

        Args:
            model_config: Fully configured PySAM model from ModelConfigurator.

        Returns:
            SimulationResult with hourly data on success, or error details on failure.
        """
        site = model_config.site_config
        logger.info(f"Executing simulation for site: {site.site_name}")

        try:
            model_config.model.execute()
        except Exception as exc:
            error_msg = f"PySAM execution failed for {site.site_name}: {exc}"
            logger.error(error_msg)
            return SimulationResult(
                site_name=site.site_name,
                run_name=site.run_name,
                customer=site.customer,
                success=False,
                error_message=error_msg,
            )

        # Extract weather year from file
        weather_year = self._extract_weather_year(site.weather_file_path)

        # Extract timeseries data
        hourly_data = self._extract_timeseries(model_config, weather_year)

        logger.info(
            f"Simulation complete for {site.site_name}: "
            f"{len(hourly_data)} hourly records"
        )

        return SimulationResult(
            site_name=site.site_name,
            run_name=site.run_name,
            customer=site.customer,
            success=True,
            hourly_data=hourly_data,
            weather_year=weather_year,
        )

    def _extract_timeseries(
        self, model_config: PySAMModelConfig, weather_year: int | None
    ) -> pd.DataFrame:
        """Extract hourly timeseries from PySAM model outputs.

        Args:
            model_config: Model config after successful execution.
            weather_year: Year for timestamp generation.

        Returns:
            DataFrame with columns: timestamp, ac_gross, ac_net,
            poa_irradiance, cell_temperature, inverter_efficiency.
        """
        outputs = model_config.model.Outputs

        # PySAM standard output arrays
        gen = list(outputs.gen)  # AC output in kW
        poa = list(outputs.subarray1_poa_eff)  # POA irradiance W/m2
        cell_temp = list(outputs.subarray1_celltemp)  # Cell temperature C
        inv_eff = list(outputs.inv_eff)  # Inverter efficiency %

        # Build timestamps
        year = weather_year or 2023
        timestamps = pd.date_range(
            start=f"{year}-01-01", periods=len(gen), freq="h"
        )

        return pd.DataFrame(
            {
                "timestamp": timestamps,
                "ac_gross": gen,  # Shading haircut deferred to Prompt 5
                "ac_net": gen,
                "poa_irradiance": poa,
                "cell_temperature": cell_temp,
                "inverter_efficiency": inv_eff,
            }
        )

    def _extract_weather_year(self, weather_file_path: Path | None) -> int | None:
        """Extract the weather year from a PySAM-format weather file.

        PySAM weather files have 2 header rows before column names.
        Reads the first data row to get the Year value.

        Args:
            weather_file_path: Path to the weather CSV file.

        Returns:
            Year as integer, or None if extraction fails.
        """
        if weather_file_path is None:
            return None

        try:
            df = pd.read_csv(weather_file_path, skiprows=2, nrows=1)
            year = int(df["Year"].iloc[0])
            logger.debug(f"Extracted weather year: {year}")
            return year
        except Exception as exc:
            logger.warning(
                f"Could not extract weather year from {weather_file_path}: {exc}"
            )
            return None


class BatchSimulator:
    """Runs simulations across multiple sites with graceful failure handling."""

    def __init__(
        self,
        configurator: ModelConfigurator,
        simulator: PySAMSimulator | None = None,
    ) -> None:
        self.configurator = configurator
        self.simulator = simulator or PySAMSimulator()

    def run_batch(
        self, site_configs: list[SiteConfig]
    ) -> tuple[list[SimulationResult], list[SimulationResult]]:
        """Run simulations for multiple sites, continuing on failure.

        Args:
            site_configs: List of validated site configurations.

        Returns:
            Tuple of (successful_results, failed_results).
        """
        successful: list[SimulationResult] = []
        failed: list[SimulationResult] = []

        logger.info(f"Starting batch simulation for {len(site_configs)} sites")

        for i, site_config in enumerate(site_configs, 1):
            logger.info(
                f"Processing site {i}/{len(site_configs)}: {site_config.site_name}"
            )

            # Attempt configuration
            try:
                model_config = self.configurator.configure_model(site_config)
            except PySAMConfigurationError as exc:
                error_msg = (
                    f"Configuration failed for {site_config.site_name}: {exc}"
                )
                logger.error(error_msg)
                failed.append(
                    SimulationResult(
                        site_name=site_config.site_name,
                        run_name=site_config.run_name,
                        customer=site_config.customer,
                        success=False,
                        error_message=error_msg,
                    )
                )
                continue

            # Attempt simulation
            result = self.simulator.execute_simulation(model_config)
            if result.success:
                successful.append(result)
            else:
                failed.append(result)

        logger.info(
            f"Batch complete: {len(successful)} succeeded, {len(failed)} failed "
            f"out of {len(site_configs)} total"
        )

        return successful, failed
