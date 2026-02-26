"""Output generation for simulation results: timeseries CSVs, summary JSONs, error JSONs."""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.config.schema import SiteConfig
from src.pysam_integration.simulator import SimulationResult
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


@dataclass
class SummaryMetrics:
    """Site metadata, system specs, and production metrics for a successful simulation."""

    # Site metadata
    site_name: str
    run_name: str
    customer: str
    weather_year: int | None

    # System specs
    dc_size_mw: float
    ac_installed_mw: float
    ac_poi_mw: float
    panel_model: str
    inverter_model: str
    racking: str
    tilt: float
    azimuth: float

    # Production metrics
    annual_energy_mwh: float
    net_capacity_factor: float
    specific_yield: float
    performance_ratio: float

    # Shading
    shading_pct_applied: float

    # Timestamps
    simulation_timestamp: str

    # Errors/warnings
    errors: list[str] = field(default_factory=list)


@dataclass
class ErrorReport:
    """Details for a failed simulation."""

    site_name: str
    run_name: str
    customer: str
    error_message: str
    simulation_timestamp: str
    report_timestamp: str = field(default="", init=False)

    # Site config subset for debugging
    latitude: float | None = None
    longitude: float | None = None
    dc_size_mw: float | None = None
    panel_model: str | None = None
    inverter_model: str | None = None

    def __post_init__(self) -> None:
        """Set report timestamp to current UTC time."""
        self.report_timestamp = datetime.now(tz=timezone.utc).isoformat()


class OutputWriter:
    """Writes simulation outputs: timeseries CSVs, summary JSONs, and error JSONs."""

    def __init__(self, output_dir: Path) -> None:
        """Initialize output writer and create subdirectories.

        Args:
            output_dir: Root output directory.
        """
        self.output_dir = output_dir
        self.timeseries_dir = output_dir / "timeseries"
        self.results_dir = output_dir / "results"
        self.timeseries_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"OutputWriter initialized: {output_dir}")

    def write_outputs(
        self,
        simulation_result: SimulationResult,
        site_config: SiteConfig,
        shading_pct: float,
    ) -> tuple[Path | None, Path]:
        """Write all outputs for a simulation result.

        Args:
            simulation_result: Result from PySAM simulation.
            site_config: Original site configuration.
            shading_pct: Shading percentage to apply as haircut.

        Returns:
            Tuple of (timeseries_path or None if failed, summary_or_error_path).
        """
        filename_base = (
            f"{site_config.run_name}_{site_config.site_name}".replace(" ", "_")
        )

        if not simulation_result.success:
            logger.info(f"Writing error report for {site_config.site_name}")
            error_path = self._write_error(
                simulation_result, site_config, filename_base
            )
            return None, error_path

        # Apply shading haircut
        hourly_data = self._apply_shading_haircut(
            simulation_result.hourly_data, shading_pct
        )

        # Calculate metrics
        metrics = self._calculate_metrics(
            hourly_data, site_config, simulation_result, shading_pct
        )

        # Write files
        timeseries_path = self._write_timeseries(hourly_data, filename_base)
        summary_path = self._write_summary(metrics, filename_base)

        logger.info(
            f"Outputs written for {site_config.site_name}: "
            f"{timeseries_path.name}, {summary_path.name}"
        )

        return timeseries_path, summary_path

    def _apply_shading_haircut(
        self, hourly_data: pd.DataFrame, shading_pct: float
    ) -> pd.DataFrame:
        """Apply shading loss percentage to AC output.

        Args:
            hourly_data: DataFrame with ac_gross column (kW).
            shading_pct: Shading loss percentage (0-100).

        Returns:
            DataFrame copy with ac_net = ac_gross * (1 - shading_pct/100).
        """
        df = hourly_data.copy()
        df["ac_net"] = df["ac_gross"] * (1 - shading_pct / 100)
        return df

    def _calculate_metrics(
        self,
        hourly_data: pd.DataFrame,
        site_config: SiteConfig,
        simulation_result: SimulationResult,
        shading_pct: float,
    ) -> SummaryMetrics:
        """Calculate production summary metrics from hourly data.

        Args:
            hourly_data: DataFrame with ac_net and poa_irradiance columns.
            site_config: Site configuration for system specs.
            simulation_result: For metadata (timestamps, weather year).
            shading_pct: Shading percentage that was applied.

        Returns:
            SummaryMetrics with all fields populated.
        """
        num_hours = len(hourly_data)

        # ac_net is in kW; sum of hourly kW values = kWh
        annual_energy_kwh = float(hourly_data["ac_net"].sum())
        annual_energy_mwh = annual_energy_kwh / 1000

        # Net capacity factor: actual MWh / (installed AC MW * hours)
        net_capacity_factor = annual_energy_mwh / (
            site_config.ac_installed_mw * num_hours
        )

        # Specific yield: kWh per kWp (DC capacity in kWp = dc_size_mw * 1000)
        dc_capacity_kwp = site_config.dc_size_mw * 1000
        specific_yield = annual_energy_kwh / dc_capacity_kwp

        # Performance ratio: actual / ideal
        # POA irradiance is W/m², hourly sum = Wh/m², /1000 = kWh/m²
        total_poa_kwh_per_m2 = float(hourly_data["poa_irradiance"].sum()) / 1000
        # Ideal energy = (POA kWh/m² / STC 1 kW/m²) * DC capacity kW = kWh
        ideal_energy_kwh = total_poa_kwh_per_m2 * dc_capacity_kwp
        performance_ratio = (
            annual_energy_kwh / ideal_energy_kwh if ideal_energy_kwh > 0 else 0.0
        )

        return SummaryMetrics(
            site_name=site_config.site_name,
            run_name=site_config.run_name,
            customer=site_config.customer,
            weather_year=simulation_result.weather_year,
            dc_size_mw=site_config.dc_size_mw,
            ac_installed_mw=site_config.ac_installed_mw,
            ac_poi_mw=site_config.ac_poi_mw,
            panel_model=site_config.panel_model,
            inverter_model=site_config.inverter_model,
            racking=site_config.racking,
            tilt=site_config.tilt,
            azimuth=site_config.azimuth,
            annual_energy_mwh=round(annual_energy_mwh, 3),
            net_capacity_factor=round(net_capacity_factor, 6),
            specific_yield=round(specific_yield, 2),
            performance_ratio=round(performance_ratio, 4),
            shading_pct_applied=shading_pct,
            simulation_timestamp=simulation_result.simulation_timestamp,
        )

    def _write_timeseries(self, hourly_data: pd.DataFrame, filename_base: str) -> Path:
        """Write hourly timeseries to CSV.

        Args:
            hourly_data: DataFrame with timestamp and power columns.
            filename_base: Base filename (without extension).

        Returns:
            Path to the written CSV file.
        """
        path = self.timeseries_dir / f"{filename_base}_8760.csv"
        hourly_data.to_csv(path, index=False, float_format="%.4f")
        logger.debug(f"Timeseries written: {path}")
        return path

    def _write_summary(self, metrics: SummaryMetrics, filename_base: str) -> Path:
        """Write summary metrics to JSON.

        Args:
            metrics: Calculated summary metrics.
            filename_base: Base filename (without extension).

        Returns:
            Path to the written JSON file.
        """
        path = self.results_dir / f"{filename_base}_summary.json"
        path.write_text(json.dumps(asdict(metrics), indent=2, default=str))
        logger.debug(f"Summary written: {path}")
        return path

    def _write_error(
        self,
        simulation_result: SimulationResult,
        site_config: SiteConfig,
        filename_base: str,
    ) -> Path:
        """Write error report to JSON.

        Args:
            simulation_result: Failed simulation result.
            site_config: Site configuration for debugging context.
            filename_base: Base filename (without extension).

        Returns:
            Path to the written JSON file.
        """
        report = ErrorReport(
            site_name=simulation_result.site_name,
            run_name=simulation_result.run_name,
            customer=simulation_result.customer,
            error_message=simulation_result.error_message or "Unknown error",
            simulation_timestamp=simulation_result.simulation_timestamp,
            latitude=site_config.latitude,
            longitude=site_config.longitude,
            dc_size_mw=site_config.dc_size_mw,
            panel_model=site_config.panel_model,
            inverter_model=site_config.inverter_model,
        )
        path = self.results_dir / f"{filename_base}_error.json"
        path.write_text(json.dumps(asdict(report), indent=2, default=str))
        logger.info(f"Error report written: {path}")
        return path
