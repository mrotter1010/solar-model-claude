"""Main pipeline: CSV → climate data → PySAM simulation → output files."""

from datetime import datetime, timezone
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
from src.bess.dispatch_runner import run_bess_dispatch, run_ftm_dispatch
from src.bess.sizing_optimizer import (
    compute_ftm_bess_npv,
    compute_ftm_project_economics,
    compute_ftm_solar_revenue,
    run_ftm_sizing_optimization,
    run_sizing_optimization,
)
from src.lmp.client import fetch_lmp
from src.lmp.zone_mapper import resolve_pricing_zone
from src.rates.tou_mapper import get_month_ranges
from src.rates.bill_runner import run_bill_calculation
from src.reporting.report_generator import generate_report
from src.pysam_integration.cec_database import CECDatabase
from src.pysam_integration.model_configurator import ModelConfigurator
from src.pysam_integration.simulator import BatchSimulator, PySAMSimulator, SimulationResult
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def run_climate_data_pipeline(
    config_csv: Path, year: int | str = "tmy"
) -> tuple[
    list[SiteConfig],
    dict[tuple[float, float], list[float] | None],
    dict[str, dict],
]:
    """Load sites from CSV, fetch climate data, and apply bias correction.

    Bias correction is applied once per unique location and saved to
    data/climate/cache/corrected/. All sites sharing the same lat/lon
    reuse the same corrected file, preventing triple-correction.

    Args:
        config_csv: Path to CSV file with site configurations.
        year: Weather data year to retrieve.

    Returns:
        Tuple of:
            - List of SiteConfig objects with weather_file_path assigned.
            - Dict mapping (lat, lon) to monthly soiling losses (12 floats)
              or None if ERA5 data was unavailable.
            - Dict mapping site_name to bias correction metadata.
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

    # Apply bias correction per unique location and assign weather files
    soiling_lookup: dict[tuple[float, float], list[float] | None] = {}
    bias_correction_lookup: dict[str, dict] = {}
    corrected_cache_dir = Path("data/climate/cache/corrected")
    corrected_cache_dir.mkdir(parents=True, exist_ok=True)

    for (lat, lon), result in location_results.items():
        soiling_lookup[(lat, lon)] = result["monthly_soiling"]
        data_source = result.get("data_source", "nsrdb")

        if data_source == "solcast":
            # Solcast: assign file directly, no bias correction
            for site in sites:
                if site.location == (lat, lon):
                    site.weather_file_path = result["weather_file"]
                    site.data_source = "solcast"
                    site.solcast_metadata = result.get("solcast_metadata")
            continue

        # NSRDB: check corrected cache, apply bias correction if needed
        weather_df = result["weather_df"]
        weather_metadata = result["weather_metadata"]
        corrected_path = _find_corrected_cache(corrected_cache_dir, lat, lon, year)
        bias_meta: dict | None = None

        if corrected_path is None:
            try:
                elevation_m = get_elevation_m(lat, lon)
                corrected_df, bias_meta = apply_bias_correction(
                    weather_df, lat, lon, elevation_m,
                )
                date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
                corrected_path = (
                    corrected_cache_dir
                    / f"corrected_{lat}_{lon}_{year}_{date_str}.csv"
                )
                formatter.save_to_csv(
                    corrected_df, corrected_path, lat, lon,
                    metadata=weather_metadata,
                )
                logger.info(
                    f"Saved corrected weather for ({lat}, {lon}): "
                    f"GHI CF={bias_meta['mean_ghi_correction_factor']:.4f}, "
                    f"DNI CF={bias_meta['mean_dni_correction_factor']:.4f}"
                )
            except Exception as exc:
                logger.warning(
                    f"Bias correction failed for ({lat}, {lon}): {exc}. "
                    f"Using uncorrected data."
                )
                date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
                corrected_path = (
                    corrected_cache_dir
                    / f"corrected_{lat}_{lon}_{year}_{date_str}.csv"
                )
                formatter.save_to_csv(
                    weather_df, corrected_path, lat, lon,
                    metadata=weather_metadata,
                )
        else:
            logger.info(
                f"Corrected cache hit for ({lat}, {lon}): "
                f"{corrected_path.name}"
            )

        # Assign corrected path to ALL sites at this location
        for site in sites:
            if site.location == (lat, lon):
                site.weather_file_path = corrected_path
                site.data_source = "nsrdb"
                if bias_meta is not None:
                    bias_correction_lookup[site.site_name] = bias_meta

    print_summary(sites, location_results)
    return sites, soiling_lookup, bias_correction_lookup


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


def _find_corrected_cache(
    cache_dir: Path, lat: float, lon: float, year: int | str
) -> Path | None:
    """Find an existing corrected cache file for the given coordinates.

    Args:
        cache_dir: Corrected cache directory to search.
        lat: Latitude to match.
        lon: Longitude to match.
        year: Data year or "tmy" to match.

    Returns:
        Path to the most recent corrected cache file, or None.
    """
    pattern = f"corrected_{lat}_{lon}_{year}_*.csv"
    matches = sorted(cache_dir.glob(pattern))
    return matches[-1] if matches else None


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

        # Step 2: Climate data retrieval + bias correction (per unique location)
        soiling_lookup: dict[tuple[float, float], list[float] | None] = {}
        bias_correction_lookup: dict[str, dict] = {}
        if not skip_climate:
            logger.info("Fetching climate data...")
            site_configs, soiling_lookup, bias_correction_lookup = (
                run_climate_data_pipeline(csv_path)
            )
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
        successful, failed = self.batch_simulator.run_batch(
            site_configs, soiling_lookup=soiling_lookup
        )

        # Step 4: Write outputs
        logger.info("Writing output files...")
        timeseries_files: list[Path] = []
        summaries: list[dict] = []
        error_files: list[Path] = []

        # Build a lookup from run_name to SiteConfig for output writing
        # (run_name is unique per row; site_name can repeat across runs)
        site_lookup = {s.run_name: s for s in site_configs}

        report_files: list[Path] = []

        # Collect (run_name, site_name, run_id, status) for end-of-pipeline summary
        run_summary_rows: list[tuple[str, str, str, str]] = []

        for result in successful:
            site = site_lookup[result.run_name]

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

            # Bill calculation (non-fatal: production results are still valid)
            bill_calc_result = None
            if site.bill_calculation and result.hourly_data is not None:
                shading_factor = 1 - site.shading_percent / 100
                hourly_production_kwh = (
                    result.hourly_data["ac_gross"] * shading_factor
                ).tolist()
                bill_calc_result = run_bill_calculation(site, hourly_production_kwh)
                bill_savings = (
                    bill_calc_result.bill_savings if bill_calc_result else None
                )
                if bill_savings is not None:
                    summary["bill_savings"] = {
                        "annual_bill_without_solar": (
                            bill_savings.bill_without_solar.annual_total
                        ),
                        "annual_bill_with_solar": (
                            bill_savings.bill_with_solar.annual_total
                        ),
                        "annual_savings": bill_savings.annual_savings,
                        "savings_percent": bill_savings.savings_percent,
                        "avoided_cost_per_kwh": bill_savings.avoided_cost_per_kwh,
                        "annual_demand_savings": bill_savings.annual_demand_savings,
                        "annual_export_kwh": bill_savings.bill_with_solar.annual_export_kwh,
                        "annual_export_credits": bill_savings.bill_with_solar.annual_export_credits,
                        "nem_true_up_credit": bill_savings.bill_with_solar.nem_true_up_credit,
                        "monthly_detail": [
                            {
                                "month": wi.month,
                                "bill_without": wo.total,
                                "bill_with": wi.total,
                                "savings": s,
                            }
                            for wo, wi, s in zip(
                                bill_savings.bill_without_solar.monthly,
                                bill_savings.bill_with_solar.monthly,
                                bill_savings.monthly_savings,
                            )
                        ],
                    }

            # ============================================================
            # FTM (front-of-meter) wholesale dispatch path
            # ============================================================
            if (
                site.dispatch_mode == "ftm"
                and site.bess_dispatch_required
                and result.hourly_data is not None
            ):
                try:
                    # Compute production (independent of bill_calculation)
                    shading_factor_ftm = 1 - site.shading_percent / 100
                    ftm_production_kwh = (
                        result.hourly_data["ac_gross"] * shading_factor_ftm
                    ).tolist()

                    # 1. Resolve pricing zone
                    pricing_zone = resolve_pricing_zone(site)

                    # 2. Fetch LMP data
                    lmp_data = fetch_lmp(
                        iso=pricing_zone.iso,
                        zone=pricing_zone.zone_name,
                        market=site.lmp_market,
                        year=site.lmp_year,
                    )

                    # 3. Get load (parasitic, optional)
                    ftm_load_kwh = None
                    if bill_calc_result is not None:
                        ftm_load_kwh = bill_calc_result.load_profile.hourly_kwh

                    # 4. FTM sizing optimization or single dispatch
                    if site.bess_optimization_required:
                        sizing_result = run_ftm_sizing_optimization(
                            site_config=site,
                            production_kwh=ftm_production_kwh,
                            lmp_data=lmp_data,
                            load_kwh=ftm_load_kwh,
                        )
                        ftm_dispatch_result = sizing_result.dispatch_result
                        ftm_project_economics = sizing_result.economics

                        # Extract economics from sizing winner
                        annual_solar_revenue = ftm_project_economics.solar_revenue or 0.0
                        bess_arbitrage_revenue = ftm_project_economics.bess_arbitrage_revenue or 0.0
                        ancillary_revenue = ftm_project_economics.ancillary_revenue or 0.0
                        gross_revenue = ftm_project_economics.gross_revenue or 0.0
                        ftm_bess_npv = ftm_project_economics.bess_npv

                        # Build bess_sizing summary (same structure as BTM)
                        summary["bess_sizing"] = {
                            "optimal_power_mw": ftm_project_economics.optimal_power_mw,
                            "optimal_duration_hr": ftm_project_economics.optimal_duration_hr,
                            "optimal_capacity_kwh": ftm_project_economics.optimal_capacity_kwh,
                            "bess_npv": ftm_project_economics.bess_npv,
                            "total_project_npv": ftm_project_economics.total_project_npv,
                            "system_lcoe_per_kwh": ftm_project_economics.system_lcoe_per_kwh,
                            "total_installed_cost": ftm_project_economics.total_installed_cost,
                            "solar_cost": ftm_project_economics.solar_cost,
                            "bess_cost": ftm_project_economics.bess_cost,
                            "total_annual_savings": ftm_project_economics.total_annual_savings,
                            "bess_incremental_savings": ftm_project_economics.bess_incremental_savings,
                            "annual_production_mwh": ftm_project_economics.annual_production_mwh,
                            "lifetime_generation_mwh": ftm_project_economics.lifetime_generation_mwh,
                            "combos_evaluated": ftm_project_economics.combos_evaluated,
                            "project_lifetime_years": site.project_lifetime_years,
                            "discount_rate_pct": site.discount_rate_pct,
                            "rate_escalation_pct": site.rate_escalation_pct,
                            "sweep_results": [
                                {
                                    "power_mw": c.power_mw,
                                    "duration_hr": c.duration_hr,
                                    "capacity_kwh": c.capacity_kwh,
                                    "bess_npv": c.bess_npv,
                                    "installed_cost": c.installed_cost,
                                    "bess_incremental_savings": c.bess_incremental_savings,
                                }
                                for c in sizing_result.all_combos
                            ],
                        }

                        logger.info(
                            "FTM sizing complete for %s: winner=%.1f MW/%.1f hr, "
                            "NPV=$%.0f (%d combos)",
                            site.run_name,
                            sizing_result.winner.power_mw,
                            sizing_result.winner.duration_hr,
                            ftm_bess_npv,
                            len(sizing_result.all_combos),
                        )
                    else:
                        # 5. Single FTM dispatch (no sizing sweep)
                        ftm_dispatch_result = run_ftm_dispatch(
                            site_config=site,
                            production_kwh=ftm_production_kwh,
                            lmp_data=lmp_data,
                            load_kwh=ftm_load_kwh,
                        )

                        # 6. Compute FTM economics
                        annual_solar_revenue = compute_ftm_solar_revenue(
                            ftm_dispatch_result, lmp_data.prices
                        )
                        bess_arbitrage_revenue = (
                            (ftm_dispatch_result.ftm_revenue or 0.0)
                            - annual_solar_revenue
                        )

                        bess_power_kw = site.bess_power_mw * 1000
                        bess_capacity_kwh = bess_power_kw * site.bess_duration_hr
                        ftm_installed_cost = (
                            site.bess_installed_cost_per_kwh * bess_capacity_kwh
                        )

                        ftm_bess_npv = compute_ftm_bess_npv(
                            bess_arbitrage_revenue=bess_arbitrage_revenue,
                            bess_installed_cost=ftm_installed_cost,
                            bess_annual_degradation_pct=(
                                ftm_dispatch_result.metrics.estimated_annual_degradation_pct
                            ),
                            revenue_escalation_pct=site.rate_escalation_pct,
                            bess_opex_per_kw_year=site.bess_opex_per_kw_year,
                            bess_power_kw=bess_power_kw,
                            ancillary_revenue_per_kw_year=site.ancillary_revenue_per_kw_year,
                            discount_rate_pct=site.discount_rate_pct,
                            project_lifetime_years=site.project_lifetime_years,
                        )

                        ftm_project_economics = compute_ftm_project_economics(
                            bess_npv=ftm_bess_npv,
                            bess_installed_cost=ftm_installed_cost,
                            bess_arbitrage_revenue=bess_arbitrage_revenue,
                            bess_power_mw=site.bess_power_mw,
                            bess_duration_hr=site.bess_duration_hr,
                            bess_opex_per_kw_year=site.bess_opex_per_kw_year,
                            ancillary_revenue_per_kw_year=site.ancillary_revenue_per_kw_year,
                            site_config=site,
                            annual_solar_revenue=annual_solar_revenue,
                            annual_production_kwh=sum(ftm_production_kwh),
                        )

                        ancillary_revenue = (
                            site.ancillary_revenue_per_kw_year * site.bess_power_mw * 1000
                        )
                        gross_revenue = (
                            annual_solar_revenue
                            + bess_arbitrage_revenue
                            + ancillary_revenue
                        )

                        logger.info(
                            "FTM dispatch complete for %s: revenue=$%.0f, "
                            "solar=$%.0f, arbitrage=$%.0f, NPV=$%.0f",
                            site.run_name,
                            ftm_dispatch_result.ftm_revenue or 0.0,
                            annual_solar_revenue,
                            bess_arbitrage_revenue,
                            ftm_bess_npv,
                        )

                    # 7. Build summary dicts (shared by sizing and single dispatch)
                    summary["lmp"] = {
                        "iso": pricing_zone.iso,
                        "zone": pricing_zone.zone_name,
                        "market": site.lmp_market,
                        "year": lmp_data.year,
                        "mean_lmp": lmp_data.mean_price,
                        "min_lmp": lmp_data.min_price,
                        "max_lmp": lmp_data.max_price,
                    }

                    summary["ftm_economics"] = {
                        "annual_solar_revenue": annual_solar_revenue,
                        "annual_bess_arbitrage_revenue": bess_arbitrage_revenue,
                        "annual_ancillary_revenue": ancillary_revenue,
                        "annual_gross_revenue": gross_revenue,
                        "total_project_npv": ftm_project_economics.total_project_npv,
                        "bess_npv": ftm_bess_npv,
                        "solar_npv": ftm_project_economics.solar_npv,
                        "system_lcoe_per_kwh": ftm_project_economics.system_lcoe_per_kwh,
                        "total_installed_cost": ftm_project_economics.total_installed_cost,
                    }

                    summary["bess_dispatch"] = {
                        "dispatch_mode": "ftm",
                        "bess_power_mw": ftm_project_economics.optimal_power_mw,
                        "bess_duration_hr": ftm_project_economics.optimal_duration_hr,
                        "bess_capacity_kwh": ftm_dispatch_result.config.capacity_kwh,
                        "bess_strategy": site.bess_strategy,
                        "ftm_revenue": ftm_dispatch_result.ftm_revenue,
                        "annual_cycles": ftm_dispatch_result.metrics.annual_cycles,
                        "annual_throughput_kwh": ftm_dispatch_result.metrics.annual_throughput_kwh,
                        "average_daily_cycles": ftm_dispatch_result.metrics.average_daily_cycles,
                        "capacity_utilization_pct": ftm_dispatch_result.metrics.capacity_utilization_pct,
                        "total_curtailed_kwh": ftm_dispatch_result.metrics.total_curtailed_kwh,
                        "estimated_annual_degradation_pct": ftm_dispatch_result.metrics.estimated_annual_degradation_pct,
                        "charging_source": ftm_dispatch_result.metrics.charging_source,
                        "monthly_solver_status": ftm_dispatch_result.monthly_solve_status,
                        "heatmap_data": ftm_dispatch_result.heatmap_data,
                    }

                    # Dispatch profile for the most active month
                    _month_names = [
                        "January", "February", "March", "April",
                        "May", "June", "July", "August",
                        "September", "October", "November", "December",
                    ]
                    hm = ftm_dispatch_result.heatmap_data
                    month_activity = [
                        sum(abs(v) for v in row) for row in hm
                    ]
                    peak_month = month_activity.index(max(month_activity))
                    ftm_month_ranges = get_month_ranges()
                    m_start, m_end = ftm_month_ranges[peak_month]
                    n_days = (m_end - m_start) // 24

                    avg_load = [0.0] * 24
                    avg_solar = [0.0] * 24
                    avg_battery = [0.0] * 24
                    avg_export = [0.0] * 24
                    for day in range(n_days):
                        for h in range(24):
                            idx = m_start + day * 24 + h
                            avg_solar[h] += ftm_production_kwh[idx]
                            hd = ftm_dispatch_result.hourly_dispatch[idx]
                            avg_battery[h] += hd.discharge_kw - hd.charge_kw
                            avg_export[h] += hd.solar_export_kw
                    avg_solar = [v / n_days for v in avg_solar]
                    avg_battery = [v / n_days for v in avg_battery]
                    avg_export = [v / n_days for v in avg_export]

                    summary["bess_dispatch"]["dispatch_profile_month"] = (
                        _month_names[peak_month]
                    )
                    summary["bess_dispatch"]["dispatch_profile_load"] = avg_load
                    summary["bess_dispatch"]["dispatch_profile_solar"] = avg_solar
                    summary["bess_dispatch"]["dispatch_profile_battery"] = avg_battery
                    summary["bess_dispatch"]["dispatch_profile_export"] = avg_export

                except Exception as e:
                    logger.warning(
                        f"FTM dispatch failed for {site.run_name}: {e}"
                    )

            # ============================================================
            # BTM (behind-the-meter) BESS paths — UNCHANGED
            # ============================================================

            # BESS sizing optimization (M14c)
            elif (
                site.bess_optimization_required
                and site.bess_dispatch_required
                and bill_calc_result is not None
            ):
                try:
                    sizing_result = run_sizing_optimization(
                        site_config=site,
                        production_kwh=hourly_production_kwh,
                        rate_schedule=bill_calc_result.rate_schedule,
                        load_profile=bill_calc_result.load_profile,
                        solar_only_bill=bill_calc_result.bill_savings.bill_with_solar,
                        bill_without_solar=bill_calc_result.bill_savings.bill_without_solar,
                    )
                    # Use winner's dispatch result and bill comparison
                    # for downstream reporting
                    dispatch_result = sizing_result.dispatch_result
                    bill_comparison = sizing_result.bill_comparison
                    # Add sizing results to summary dict
                    summary["bess_sizing"] = {
                        "optimal_power_mw": sizing_result.economics.optimal_power_mw,
                        "optimal_duration_hr": sizing_result.economics.optimal_duration_hr,
                        "optimal_capacity_kwh": sizing_result.economics.optimal_capacity_kwh,
                        "bess_npv": sizing_result.economics.bess_npv,
                        "total_project_npv": sizing_result.economics.total_project_npv,
                        "system_lcoe_per_kwh": sizing_result.economics.system_lcoe_per_kwh,
                        "total_installed_cost": sizing_result.economics.total_installed_cost,
                        "solar_cost": sizing_result.economics.solar_cost,
                        "bess_cost": sizing_result.economics.bess_cost,
                        "total_annual_savings": sizing_result.economics.total_annual_savings,
                        "bess_incremental_savings": sizing_result.economics.bess_incremental_savings,
                        "annual_production_mwh": sizing_result.economics.annual_production_mwh,
                        "lifetime_generation_mwh": sizing_result.economics.lifetime_generation_mwh,
                        "combos_evaluated": sizing_result.economics.combos_evaluated,
                        "project_lifetime_years": site.project_lifetime_years,
                        "discount_rate_pct": site.discount_rate_pct,
                        "rate_escalation_pct": site.rate_escalation_pct,
                        "sweep_results": [
                            {
                                "power_mw": c.power_mw,
                                "duration_hr": c.duration_hr,
                                "capacity_kwh": c.capacity_kwh,
                                "bess_npv": c.bess_npv,
                                "installed_cost": c.installed_cost,
                                "bess_incremental_savings": c.bess_incremental_savings,
                            }
                            for c in sizing_result.all_combos
                        ],
                    }
                    # Also populate bess_dispatch from winner so PDF/DB
                    # reporting works unchanged
                    summary["bess_dispatch"] = {
                        "bess_power_mw": sizing_result.economics.optimal_power_mw,
                        "bess_duration_hr": sizing_result.economics.optimal_duration_hr,
                        "bess_capacity_kwh": sizing_result.economics.optimal_capacity_kwh,
                        "bess_strategy": site.bess_strategy,
                        "solar_only_annual_bill": bill_comparison.solar_only_annual_bill,
                        "solar_plus_bess_annual_bill": bill_comparison.solar_plus_bess_annual_bill,
                        "bess_incremental_savings": bill_comparison.bess_incremental_savings,
                        "bess_demand_savings": bill_comparison.bess_demand_savings,
                        "bess_energy_savings": bill_comparison.bess_energy_savings,
                        "annual_cycles": dispatch_result.metrics.annual_cycles,
                        "annual_throughput_kwh": dispatch_result.metrics.annual_throughput_kwh,
                        "average_daily_cycles": dispatch_result.metrics.average_daily_cycles,
                        "capacity_utilization_pct": dispatch_result.metrics.capacity_utilization_pct,
                        "total_curtailed_kwh": dispatch_result.metrics.total_curtailed_kwh,
                        "estimated_annual_degradation_pct": dispatch_result.metrics.estimated_annual_degradation_pct,
                        "total_export_kwh": dispatch_result.metrics.total_export_kwh,
                        "total_export_hours": dispatch_result.metrics.total_export_hours,
                        "charging_source": dispatch_result.metrics.charging_source,
                        "solar_only_export_kwh": bill_comparison.solar_only_export_kwh,
                        "solar_only_export_credits": bill_comparison.solar_only_export_credits,
                        "solar_plus_bess_export_kwh": bill_comparison.solar_plus_bess_export_kwh,
                        "solar_plus_bess_export_credits": bill_comparison.solar_plus_bess_export_credits,
                        "monthly_solver_status": dispatch_result.monthly_solve_status,
                        "heatmap_data": dispatch_result.heatmap_data,
                    }

                    # Dispatch profile for the most active month
                    _month_names = [
                        "January", "February", "March", "April",
                        "May", "June", "July", "August",
                        "September", "October", "November", "December",
                    ]
                    hm = dispatch_result.heatmap_data
                    month_activity = [
                        sum(abs(v) for v in row) for row in hm
                    ]
                    peak_month = month_activity.index(max(month_activity))
                    month_ranges = get_month_ranges()
                    m_start, m_end = month_ranges[peak_month]
                    n_days = (m_end - m_start) // 24
                    load_kwh = bill_calc_result.load_profile.hourly_kwh

                    avg_load = [0.0] * 24
                    avg_solar = [0.0] * 24
                    avg_battery = [0.0] * 24
                    avg_export = [0.0] * 24
                    for day in range(n_days):
                        for h in range(24):
                            idx = m_start + day * 24 + h
                            avg_load[h] += load_kwh[idx]
                            avg_solar[h] += hourly_production_kwh[idx]
                            hd = dispatch_result.hourly_dispatch[idx]
                            avg_battery[h] += hd.discharge_kw - hd.charge_kw
                            avg_export[h] += hd.export_kw
                    avg_load = [v / n_days for v in avg_load]
                    avg_solar = [v / n_days for v in avg_solar]
                    avg_battery = [v / n_days for v in avg_battery]
                    avg_export = [v / n_days for v in avg_export]

                    summary["bess_dispatch"]["dispatch_profile_month"] = (
                        _month_names[peak_month]
                    )
                    summary["bess_dispatch"]["dispatch_profile_load"] = avg_load
                    summary["bess_dispatch"]["dispatch_profile_solar"] = avg_solar
                    summary["bess_dispatch"]["dispatch_profile_battery"] = avg_battery
                    summary["bess_dispatch"]["dispatch_profile_export"] = avg_export

                except Exception as e:
                    logger.warning(
                        f"BESS sizing optimization failed for {site.run_name}: {e}"
                    )
            # BESS single-dispatch optimization
            elif site.bess_dispatch_required and bill_calc_result is not None:
                try:
                    dispatch_result, bess_comparison = run_bess_dispatch(
                        site_config=site,
                        production_kwh=hourly_production_kwh,
                        rate_schedule=bill_calc_result.rate_schedule,
                        load_profile=bill_calc_result.load_profile,
                        solar_only_bill=bill_calc_result.bill_savings.bill_with_solar,
                    )
                    summary["bess_dispatch"] = {
                        "bess_power_mw": site.bess_power_mw,
                        "bess_duration_hr": site.bess_duration_hr,
                        "bess_capacity_kwh": dispatch_result.config.capacity_kwh,
                        "bess_strategy": site.bess_strategy,
                        "solar_only_annual_bill": bess_comparison.solar_only_annual_bill,
                        "solar_plus_bess_annual_bill": bess_comparison.solar_plus_bess_annual_bill,
                        "bess_incremental_savings": bess_comparison.bess_incremental_savings,
                        "bess_demand_savings": bess_comparison.bess_demand_savings,
                        "bess_energy_savings": bess_comparison.bess_energy_savings,
                        "annual_cycles": dispatch_result.metrics.annual_cycles,
                        "annual_throughput_kwh": dispatch_result.metrics.annual_throughput_kwh,
                        "average_daily_cycles": dispatch_result.metrics.average_daily_cycles,
                        "capacity_utilization_pct": dispatch_result.metrics.capacity_utilization_pct,
                        "total_curtailed_kwh": dispatch_result.metrics.total_curtailed_kwh,
                        "estimated_annual_degradation_pct": dispatch_result.metrics.estimated_annual_degradation_pct,
                        "total_export_kwh": dispatch_result.metrics.total_export_kwh,
                        "total_export_hours": dispatch_result.metrics.total_export_hours,
                        "charging_source": dispatch_result.metrics.charging_source,
                        "solar_only_export_kwh": bess_comparison.solar_only_export_kwh,
                        "solar_only_export_credits": bess_comparison.solar_only_export_credits,
                        "solar_plus_bess_export_kwh": bess_comparison.solar_plus_bess_export_kwh,
                        "solar_plus_bess_export_credits": bess_comparison.solar_plus_bess_export_credits,
                        "monthly_solver_status": dispatch_result.monthly_solve_status,
                        "heatmap_data": dispatch_result.heatmap_data,
                    }

                    # Compute dispatch profile for the most active month
                    _month_names = [
                        "January", "February", "March", "April",
                        "May", "June", "July", "August",
                        "September", "October", "November", "December",
                    ]
                    hm = dispatch_result.heatmap_data
                    month_activity = [
                        sum(abs(v) for v in row) for row in hm
                    ]
                    peak_month = month_activity.index(max(month_activity))
                    month_ranges = get_month_ranges()
                    m_start, m_end = month_ranges[peak_month]
                    n_days = (m_end - m_start) // 24
                    load_kwh = bill_calc_result.load_profile.hourly_kwh

                    avg_load = [0.0] * 24
                    avg_solar = [0.0] * 24
                    avg_battery = [0.0] * 24
                    avg_export = [0.0] * 24
                    for day in range(n_days):
                        for h in range(24):
                            idx = m_start + day * 24 + h
                            avg_load[h] += load_kwh[idx]
                            avg_solar[h] += hourly_production_kwh[idx]
                            hd = dispatch_result.hourly_dispatch[idx]
                            avg_battery[h] += hd.discharge_kw - hd.charge_kw
                            avg_export[h] += hd.export_kw
                    avg_load = [v / n_days for v in avg_load]
                    avg_solar = [v / n_days for v in avg_solar]
                    avg_battery = [v / n_days for v in avg_battery]
                    avg_export = [v / n_days for v in avg_export]

                    summary["bess_dispatch"]["dispatch_profile_month"] = (
                        _month_names[peak_month]
                    )
                    summary["bess_dispatch"]["dispatch_profile_load"] = avg_load
                    summary["bess_dispatch"]["dispatch_profile_solar"] = avg_solar
                    summary["bess_dispatch"]["dispatch_profile_battery"] = avg_battery
                    summary["bess_dispatch"]["dispatch_profile_export"] = avg_export

                except Exception as e:
                    logger.warning(f"BESS dispatch failed for {site.run_name}: {e}")
            elif site.bess_dispatch_required and bill_calc_result is None:
                logger.warning(
                    f"BESS dispatch requires bill calculation — skipping for {site.run_name}"
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
                        summary=summary,
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
            site = site_lookup[result.run_name]
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
