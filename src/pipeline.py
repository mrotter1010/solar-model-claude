"""Main pipeline: CSV → climate data → PySAM simulation → output files."""

from pathlib import Path
from typing import Any

import pandas as pd

from src.climate.cache_manager import CacheManager
from src.climate.config import ClimateConfig
from src.climate.era5_client import fetch_era5_land_data
from src.climate.nsrdb_client import NSRDBClient
from src.climate.orchestrator import ClimateOrchestrator
from src.climate.weather_formatter import WeatherFormatter
from src.config.loader import load_config
from src.config.schema import SiteConfig
from src.database.writer import save_run_to_db
from src.climate.open_meteo_client import get_elevation_m
from src.models.nsrdb_bias_correction import (
    apply_bias_correction,
    get_model_version as get_bias_model_version,
)
from src.models.subhourly_correction import (
    compute_weather_features,
    get_model_version,
    predict_correction,
)
from src.models.timeseries_adjustment import apply_correction
from src.outputs.output_writer import OutputWriter
from src.reporting.report_generator import generate_report
from src.pysam_integration.cec_database import CECDatabase
from src.pysam_integration.model_configurator import ModelConfigurator
from src.pysam_integration.simulator import BatchSimulator, PySAMSimulator, SimulationResult
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def run_climate_data_pipeline(
    config_csv: Path, year: int | str = "tmy"
) -> tuple[list[SiteConfig], dict[tuple[float, float], list[float] | None]]:
    """Load sites from CSV and fetch climate data for all locations.

    Args:
        config_csv: Path to CSV file with site configurations.
        year: Weather data year to retrieve.

    Returns:
        Tuple of:
            - List of SiteConfig objects with weather_file_path assigned.
            - Dict mapping (lat, lon) to monthly soiling losses (12 floats)
              or None if ERA5 data was unavailable.
    """
    # Load and validate site configurations
    sites = load_config(config_csv)
    logger.info(f"Loaded {len(sites)} sites from {config_csv}")

    # Build climate pipeline components from config
    config = ClimateConfig()
    nsrdb_client = NSRDBClient(api_key=config.api_key, email=config.api_email)
    cache_manager = CacheManager(cache_dir=config.cache_dir)
    formatter = WeatherFormatter()

    era5_client = fetch_era5_land_data

    orchestrator = ClimateOrchestrator(
        nsrdb_client=nsrdb_client,
        cache_manager=cache_manager,
        formatter=formatter,
        era5_client=era5_client,
    )

    # Fetch climate data for all unique locations
    location_results = orchestrator.fetch_climate_data(
        sites,
        year=year,
        max_age_days=config.cache_max_age_days,
        max_cache_distance_km=config.max_cache_distance_km,
    )

    # Extract weather file paths and soiling data from orchestrator results
    soiling_lookup: dict[tuple[float, float], list[float] | None] = {}
    for site in sites:
        if site.location in location_results:
            result = location_results[site.location]
            site.weather_file_path = result["weather_file"]
            site.data_source = result.get("data_source", "nsrdb")
            site.solcast_metadata = result.get("solcast_metadata")
            soiling_lookup[site.location] = result["monthly_soiling"]

    print_summary(sites, location_results)
    return sites, soiling_lookup


def print_summary(
    sites: list[SiteConfig],
    location_results: dict[tuple[float, float], dict[str, Any]],
) -> None:
    """Log a summary of the climate data pipeline results.

    Args:
        sites: List of site configurations.
        location_results: Mapping from (lat, lon) to climate result dicts.
    """
    sites_with_data = sum(1 for s in sites if s.weather_file_path is not None)
    logger.info(
        f"Pipeline summary: {len(sites)} total sites, "
        f"{len(location_results)} unique locations, "
        f"{sites_with_data} sites with weather data assigned"
    )


def _print_run_summary(rows: list[tuple[str, str, str, str]]) -> None:
    """Print a formatted summary table of all pipeline runs.

    Args:
        rows: List of (run_name, site_name, run_id, status) tuples.
    """
    if not rows:
        return

    headers = ("Run Name", "Site Name", "Run ID", "Status")
    # Calculate column widths from data and headers
    widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(val))

    fmt = f"{{:<{widths[0]}}}  {{:<{widths[1]}}}  {{:<{widths[2]}}}  {{:<{widths[3]}}}"
    sep = "-" * (sum(widths) + 6)  # 6 = 3 gaps × 2 spaces each

    print()
    print(fmt.format(*headers))
    print(sep)
    for row in rows:
        print(fmt.format(*row))
    print()


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

    def _apply_nsrdb_bias_correction(
        self, site: SiteConfig
    ) -> dict | None:
        """Apply NSRDB bias correction to a site's weather file.

        Reads the SAM-format weather CSV, applies monthly GHI/DNI correction
        factors from the trained model, and writes the corrected data back
        to the same file path so PySAM picks it up transparently.

        Skips correction for Solcast-sourced data or sites without weather files.

        Args:
            site: Site configuration with weather file path and location.

        Returns:
            Metadata dict with correction details, or None if skipped.
        """
        # Skip for non-NSRDB data sources
        if site.data_source != "nsrdb":
            logger.debug(
                f"Bias correction skipped for {site.site_name}: "
                f"data_source={site.data_source}"
            )
            return None

        # Skip if Solcast resource file is set
        if site.resource_file_path is not None:
            logger.debug(
                f"Bias correction skipped for {site.site_name}: "
                f"resource_file_path is set (Solcast)"
            )
            return None

        if site.weather_file_path is None or not site.weather_file_path.exists():
            logger.debug(
                f"Bias correction skipped for {site.site_name}: "
                f"no weather file"
            )
            return None

        try:
            # Look up elevation
            elevation_m = get_elevation_m(site.latitude, site.longitude)

            # Read the SAM weather CSV (2-row metadata header + data)
            header_df = pd.read_csv(site.weather_file_path, nrows=1)
            weather_df = pd.read_csv(site.weather_file_path, skiprows=2)

            # Apply bias correction
            corrected_df, correction_metadata = apply_bias_correction(
                weather_df, site.latitude, site.longitude, elevation_m,
            )

            # Extract original metadata for rewriting the header
            tz = 0
            elev = 0
            if "Time Zone" in header_df.columns:
                tz = int(header_df["Time Zone"].iloc[0])
            elif "Local Time Zone" in header_df.columns:
                tz = int(header_df["Local Time Zone"].iloc[0])
            if "Elevation" in header_df.columns:
                elev = int(header_df["Elevation"].iloc[0])

            # Write corrected data back to the same file path
            with site.weather_file_path.open("w") as f:
                f.write("Latitude,Longitude,Time Zone,Elevation\n")
                f.write(
                    f"{site.latitude},{site.longitude},{tz},{elev}\n"
                )
                corrected_df.to_csv(f, index=False)

            logger.info(
                f"Applied NSRDB bias correction for {site.site_name}: "
                f"GHI factor={correction_metadata['mean_ghi_correction_factor']:.4f}, "
                f"DNI factor={correction_metadata['mean_dni_correction_factor']:.4f}"
            )
            return correction_metadata

        except Exception as exc:
            logger.warning(
                f"Bias correction failed for {site.site_name}: {exc}"
            )
            return None

    def _apply_subhourly_correction(
        self, result: SimulationResult, site: SiteConfig
    ) -> dict[str, object]:
        """Apply subhourly resolution correction to a simulation result.

        Modifies result.hourly_data["ac_gross"] in place when the predicted
        correction is non-zero. Returns a metadata dict with correction details
        and raw (pre-correction) annual energy for the summary.

        Failures are logged as warnings and do not halt the pipeline.

        Args:
            result: Successful simulation result with hourly_data.
            site: Site configuration with weather file and system parameters.

        Returns:
            Dict with correction metadata, or empty dict if correction was
            skipped due to missing data or errors.
        """
        if result.hourly_data is None or site.weather_file_path is None:
            logger.debug(
                f"Subhourly correction skipped for {site.site_name}: "
                "missing hourly data or weather file"
            )
            return {}

        if "dc_net" not in result.hourly_data.columns:
            logger.debug(
                f"Subhourly correction skipped for {site.site_name}: "
                "dc_net column not in hourly data"
            )
            return {}

        try:
            # Capture raw (pre-correction) annual energy
            raw_ac_gross_kwh = float(result.hourly_data["ac_gross"].sum())

            # Compute weather features from the site's weather file
            weather_features = compute_weather_features(site.weather_file_path)

            # Get system parameters
            dcac_ratio = site.dc_size_mw / site.ac_installed_mw
            cf_60min = float(result.loss_data["capacity_factor_ac"])

            # Predict correction (clamped to >= 0 by the model)
            correction_pct = predict_correction(
                dcac_ratio=dcac_ratio,
                gcr=site.gcr,
                racking=site.racking,
                latitude=site.latitude,
                longitude=site.longitude,
                cf_60min=cf_60min,
                weather_features=weather_features,
            )

            # Apply correction to ac_gross if non-zero
            if correction_pct > 0:
                ac_capacity_kw = site.ac_installed_mw * 1000
                adjusted = apply_correction(
                    hourly_gen_kwh=result.hourly_data["ac_gross"].tolist(),
                    correction_pct=correction_pct,
                    dc_hourly_kwh=result.hourly_data["dc_net"].tolist(),
                    ac_capacity_kw=ac_capacity_kw,
                )
                result.hourly_data["ac_gross"] = adjusted

            model_version = get_model_version()

            # Compute raw annual energy post-shading for comparability
            shading_factor = 1 - site.shading_percent / 100
            raw_annual_energy_mwh = round(
                raw_ac_gross_kwh * shading_factor / 1000, 3
            )

            logger.info(
                f"Subhourly correction for {site.site_name}: "
                f"{correction_pct:.3f}% (model {model_version})"
            )

            return {
                "subhourly_correction_pct": round(correction_pct, 4),
                "subhourly_model_version": model_version,
                "raw_annual_energy_mwh": raw_annual_energy_mwh,
            }

        except FileNotFoundError as exc:
            logger.warning(
                f"Subhourly correction skipped for {site.site_name}: {exc}"
            )
            return {}
        except Exception as exc:
            logger.warning(
                f"Subhourly correction failed for {site.site_name}: {exc}"
            )
            return {}

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
            timeseries_files, summaries, error_files, report_files.
        """
        # Step 1: Load site configs
        logger.info(f"Loading site configurations from {csv_path}")
        site_configs = load_config(csv_path)
        logger.info(f"Loaded {len(site_configs)} sites")

        # Step 2: Climate data retrieval
        soiling_lookup: dict[tuple[float, float], list[float] | None] = {}
        if not skip_climate:
            logger.info("Fetching climate data...")
            site_configs, soiling_lookup = run_climate_data_pipeline(csv_path)
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

        # Step 2.5: Apply NSRDB bias correction to weather files
        bias_correction_lookup: dict[str, dict] = {}
        for site in site_configs:
            bias_meta = self._apply_nsrdb_bias_correction(site)
            if bias_meta is not None:
                bias_correction_lookup[site.site_name] = bias_meta

        # Step 3: Run PySAM simulations
        logger.info("Running PySAM simulations...")
        successful, failed = self.batch_simulator.run_batch(
            site_configs, soiling_lookup=soiling_lookup
        )

        # Step 4: Write outputs
        logger.info("Writing output files...")
        timeseries_files: list[Path] = []
        summaries: list[dict] = []
        error_files: list[Path] = []

        # Build a lookup from site_name to SiteConfig for output writing
        site_lookup = {s.site_name: s for s in site_configs}

        report_files: list[Path] = []

        # Collect (run_name, site_name, run_id, status) for end-of-pipeline summary
        run_summary_rows: list[tuple[str, str, str, str]] = []

        for result in successful:
            site = site_lookup[result.site_name]

            # Apply subhourly resolution correction (modifies ac_gross in place)
            correction_metadata = self._apply_subhourly_correction(result, site)

            ts_path, summary = self.output_writer.write_outputs(
                simulation_result=result,
                site_config=site,
                shading_pct=site.shading_percent,
            )
            if ts_path is not None:
                timeseries_files.append(ts_path)
            # Merge bias correction metadata into summary and loss_data
            if result.site_name in bias_correction_lookup:
                bias_meta = bias_correction_lookup[result.site_name]
                summary.update(bias_meta)
                # Inject into loss_data so the PDF report narrative
                # can include bias correction methodology text
                if result.loss_data is not None:
                    for key in (
                        "bias_correction_applied",
                        "bias_correction_model_version",
                        "mean_ghi_correction_factor",
                        "mean_dni_correction_factor",
                    ):
                        result.loss_data[key] = bias_meta[key]

            if correction_metadata:
                summary.update(correction_metadata)
                # Inject into loss_data so waterfall chart and narrative
                # include the subhourly correction step
                if result.loss_data is not None:
                    result.loss_data["subhourly_correction_pct"] = (
                        correction_metadata["subhourly_correction_pct"]
                    )
                    result.loss_data["subhourly_model_version"] = (
                        correction_metadata["subhourly_model_version"]
                    )
            summaries.append(summary)

            # Generate PDF report if requested and loss_data is available
            report_path = None
            if site.report and result.loss_data:
                reports_dir = self.output_dir / "reports"
                try:
                    report_path = generate_report(
                        site_config=site.model_dump(),
                        loss_data=result.loss_data,
                        output_dir=reports_dir,
                    )
                    if report_path is not None:
                        report_files.append(report_path)
                        logger.info(f"Report generated: {report_path}")
                    else:
                        logger.warning(
                            f"Report generation returned None for {site.site_name}"
                        )
                except Exception as exc:
                    logger.warning(
                        f"Report generation failed for {site.site_name}: {exc}"
                    )

            # Save to database (non-fatal if DB is unavailable)
            try:
                run = save_run_to_db(
                    site_config=site,
                    summary=summary,
                    timeseries_path=ts_path,
                    report_path=report_path,
                    climate_path=site.weather_file_path,
                )
                run_summary_rows.append(
                    (site.run_name, site.site_name, str(run.id), "success")
                )
            except Exception as exc:
                logger.warning(
                    f"Database save failed for {site.site_name}: {exc}"
                )
                run_summary_rows.append(
                    (site.run_name, site.site_name, "N/A", "success (db_error)")
                )

        for result in failed:
            site = site_lookup[result.site_name]
            _, error_path = self.output_writer.write_outputs(
                simulation_result=result,
                site_config=site,
                shading_pct=site.shading_percent,
            )
            error_files.append(error_path)
            run_summary_rows.append(
                (site.run_name, site.site_name, "N/A", "failed")
            )

        # Print run summary table
        _print_run_summary(run_summary_rows)

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
            "summaries": summaries,
            "error_files": error_files,
            "report_files": report_files,
        }
