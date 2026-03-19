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
    loss_data: dict[str, object] | None = None
    weather_year: str | None = None
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

        model = model_config.model

        try:
            model.execute()
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

        outputs = model.Outputs
        logger.info(
            f"  annual_energy={outputs.annual_energy:.0f} kWh, "
            f"CF={outputs.capacity_factor:.2f}%"
        )

        # Extract weather year from file
        weather_year = self._extract_weather_year(site.weather_file_path)

        # Extract timeseries data
        hourly_data = self._extract_timeseries(model_config, weather_year)

        # Extract loss data, scalars, and monthly arrays for reporting
        loss_data = self._extract_loss_data(outputs)
        loss_data["bifacial"] = site.bifacial

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
            loss_data=loss_data,
            weather_year=weather_year,
        )

    def _extract_timeseries(
        self, model_config: PySAMModelConfig, weather_year: str | None
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
        dc_net = list(outputs.dc_net)  # DC output after losses in kW
        poa_eff = list(outputs.subarray1_poa_eff)  # Effective POA W/m2
        poa_nom = list(outputs.subarray1_poa_nom)  # Nominal POA W/m2
        cell_temp = list(outputs.subarray1_celltemp)  # Cell temperature C
        inv_eff = list(outputs.inv_eff)  # Inverter efficiency %

        # Build timestamps (use 2023 as placeholder for TMY or missing year)
        if weather_year is None or weather_year == "tmy":
            ts_year = 2023
        else:
            ts_year = int(weather_year)
        timestamps = pd.date_range(
            start=f"{ts_year}-01-01", periods=len(gen), freq="h"
        )

        return pd.DataFrame(
            {
                "timestamp": timestamps,
                "ac_gross": gen,
                "ac_net": gen,
                "dc_net": dc_net,
                "poa_irradiance": poa_eff,
                "poa_nominal": poa_nom,
                "cell_temperature": cell_temp,
                "inverter_efficiency": inv_eff,
            }
        )

    @staticmethod
    def _extract_loss_data(outputs: object) -> dict[str, object]:
        """Extract annual energy totals, loss percentages, scalars, and monthly arrays.

        Args:
            outputs: PySAM model Outputs group after successful execution.

        Returns:
            Dict with categorized output values for reporting.
        """

        def _get(name: str, default: float = 0.0) -> float:
            return float(getattr(outputs, name, default))

        def _get_monthly(name: str) -> list[float]:
            raw = getattr(outputs, name, None)
            if raw is not None and hasattr(raw, "__len__") and len(raw) == 12:
                return [float(v) for v in raw]
            return [0.0] * 12

        # GHI timeseries: average daytime GHI and annual total
        avg_daytime_ghi = 0.0
        annual_ghi_kwh_m2 = 0.0
        gh_raw = getattr(outputs, "gh", None)
        if gh_raw is not None and hasattr(gh_raw, "__len__") and len(gh_raw) >= 8760:
            gh_values = [float(v) for v in gh_raw]
            annual_ghi_kwh_m2 = sum(gh_values) / 1000  # Wh/m² → kWh/m²
            daylight = [v for v in gh_values if v > 0]
            if daylight:
                avg_daytime_ghi = sum(daylight) / len(daylight)
        else:
            logger.warning("GHI timeseries (gh) not available from PySAM outputs")

        return {
            # Annual energy totals (kWh)
            "annual_dc_nominal": _get("annual_dc_nominal"),
            "annual_dc_gross": _get("annual_dc_gross"),
            "annual_dc_net": _get("annual_dc_net"),
            "annual_ac_gross": _get("annual_ac_gross"),
            "annual_energy": _get("annual_energy"),
            # Annual loss percentages (%)
            "annual_poa_shading_loss_percent": _get("annual_poa_shading_loss_percent"),
            "annual_poa_soiling_loss_percent": _get("annual_poa_soiling_loss_percent"),
            "annual_poa_cover_loss_percent": _get("annual_poa_cover_loss_percent"),
            "annual_dc_module_loss_percent": _get("annual_dc_module_loss_percent"),
            "annual_dc_mismatch_loss_percent": _get("annual_dc_mismatch_loss_percent"),
            "annual_dc_diodes_loss_percent": _get("annual_dc_diodes_loss_percent"),
            "annual_dc_wiring_loss_percent": _get("annual_dc_wiring_loss_percent"),
            "annual_dc_nameplate_loss_percent": _get("annual_dc_nameplate_loss_percent"),
            "annual_dc_tracking_loss_percent": _get("annual_dc_tracking_loss_percent"),
            "annual_bifacial_electrical_mismatch_percent": _get(
                "annual_bifacial_electrical_mismatch_percent"
            ),
            "annual_ac_inv_clip_loss_percent": _get("annual_ac_inv_clip_loss_percent"),
            "annual_ac_inv_eff_loss_percent": _get("annual_ac_inv_eff_loss_percent"),
            "annual_ac_wiring_loss_percent": _get("annual_ac_wiring_loss_percent"),
            "annual_xfmr_loss_percent": _get("annual_xfmr_loss_percent"),
            "annual_ac_perf_adj_loss_percent": _get("annual_ac_perf_adj_loss_percent"),
            "annual_poa_rear_gain_percent": _get("annual_poa_rear_gain_percent"),
            # Snow loss (from Townsend/Powers model when en_snow_model=1)
            "annual_dc_snow_loss_percent": _get("annual_dc_snow_loss_percent"),
            "annual_snow_loss": _get("annual_snow_loss"),
            "monthly_snow_loss": _get_monthly("monthly_snow_loss"),
            # Key scalars
            "capacity_factor": _get("capacity_factor"),
            "capacity_factor_ac": _get("capacity_factor_ac"),
            "kwh_per_kw": _get("kwh_per_kw"),
            "performance_ratio": _get("performance_ratio"),
            # Monthly arrays (12-element lists)
            "monthly_energy": _get_monthly("monthly_energy"),
            "monthly_poa_eff": _get_monthly("monthly_poa_eff"),
            # GHI metrics
            "avg_daytime_ghi_wm2": avg_daytime_ghi,
            "annual_ghi_kwh_m2": annual_ghi_kwh_m2,
        }

    def _extract_weather_year(self, weather_file_path: Path | None) -> str | None:
        """Extract the weather year from a PySAM-format weather file.

        Returns "tmy" for TMY files (detected by filename containing "_tmy_"),
        or the year as a string for single-year files.

        Args:
            weather_file_path: Path to the weather CSV file.

        Returns:
            "tmy" for TMY files, year string (e.g. "2024") for single-year,
            or None if extraction fails.
        """
        if weather_file_path is None:
            return None

        # TMY files are cached with "_tmy_" in the filename
        if "_tmy_" in weather_file_path.name:
            logger.debug(f"Detected TMY weather file: {weather_file_path.name}")
            return "tmy"

        try:
            df = pd.read_csv(weather_file_path, skiprows=2, nrows=1)
            year = str(int(df["Year"].iloc[0]))
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
        self,
        site_configs: list[SiteConfig],
        soiling_lookup: dict[tuple[float, float], list[float] | None] | None = None,
    ) -> tuple[list[SimulationResult], list[SimulationResult]]:
        """Run simulations for multiple sites, continuing on failure.

        Args:
            site_configs: List of validated site configurations.
            soiling_lookup: Optional mapping from (lat, lon) to 12 monthly
                soiling loss percentages. If None or key missing, the
                configurator falls back to default 5%.

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
            monthly_soiling = None
            if soiling_lookup is not None:
                monthly_soiling = soiling_lookup.get(site_config.location)
            try:
                model_config = self.configurator.configure_model(
                    site_config, monthly_soiling=monthly_soiling
                )
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
