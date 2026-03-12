"""Database write service for persisting pipeline run results."""

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from src.config.schema import SiteConfig
from src.database.connection import get_session
from src.database.models import Customer, Run, RunInput, RunResult, Site
from src.utils.exceptions import SolarModelError
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def _get_git_hash() -> str | None:
    """Get the short git hash of the current HEAD.

    Returns None if git is not available or not in a repo.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def build_data_sources(site_config: SiteConfig) -> str:
    """Build the data_sources string for a pipeline run.

    Args:
        site_config: Site configuration with data_source and optional
            resource_file_path / solcast_metadata fields.

    Returns:
        For NSRDB runs: the legacy plain-text string.
        For Solcast runs: a JSON string with structured provider info.
    """
    if site_config.data_source == "solcast":
        filename = (
            site_config.resource_file_path.name
            if site_config.resource_file_path is not None
            else "unknown"
        )
        return json.dumps({
            "solar_resource": {
                "provider": "Solcast",
                "file": filename,
                "format": "SAM CSV",
            },
            "supplementary": {
                "provider": "Open-Meteo ERA5",
                "data": ["precipitation"],
            },
        })
    return "NSRDB v4.0.0, Open-Meteo ERA5"


def _upsert_customer(session: Session, customer_name: str) -> Customer:
    """Find existing customer by name, or create a new one."""
    customer = session.query(Customer).filter_by(name=customer_name).first()
    if customer is None:
        customer = Customer(name=customer_name)
        session.add(customer)
        session.flush()
        logger.info(f"Created customer: {customer_name}")
    return customer


def _upsert_site(
    session: Session,
    customer: Customer,
    site_name: str,
    latitude: float,
    longitude: float,
) -> Site:
    """Find existing site by name, or create a new one.

    If the site exists, verifies lat/lon matches. Raises on mismatch.
    """
    site = session.query(Site).filter_by(name=site_name).first()
    if site is None:
        site = Site(
            customer_id=customer.id,
            name=site_name,
            latitude=latitude,
            longitude=longitude,
        )
        session.add(site)
        session.flush()
        logger.info(f"Created site: {site_name}")
        return site

    # Safety check: coordinates must match
    if (site.latitude, site.longitude) != (latitude, longitude):
        raise SolarModelError(
            f"Site '{site_name}' exists with coordinates "
            f"({site.latitude}, {site.longitude}) but run has "
            f"({latitude}, {longitude})",
            context={
                "site_name": site_name,
                "existing_coords": (site.latitude, site.longitude),
                "new_coords": (latitude, longitude),
            },
        )
    return site


def save_run_to_db(
    site_config: SiteConfig,
    summary: dict,
    timeseries_path: Path | None = None,
    report_path: Path | None = None,
    climate_path: Path | None = None,
    database_url: str | None = None,
) -> Run:
    """Save a completed pipeline run to the database.

    Wraps the entire operation in a single transaction. Creates customer
    and site records if they don't exist, then writes the run, inputs,
    and results.

    Args:
        site_config: Validated SiteConfig from the CSV input.
        summary: Summary results dict (same structure as the summary JSON).
        timeseries_path: Path to the 8760 timeseries CSV, if generated.
        report_path: Path to the PDF report, if generated.
        climate_path: Path to the climate/weather data file.
        database_url: Optional database URL override.

    Returns:
        The created Run ORM object.

    Raises:
        SolarModelError: If site coordinate mismatch is detected.
    """
    with get_session(url=database_url) as session:
        # 1. Upsert customer and site
        customer = _upsert_customer(session, site_config.customer)
        site = _upsert_site(
            session,
            customer,
            site_config.site_name,
            site_config.latitude,
            site_config.longitude,
        )

        # 2. Create run record
        loss_data = summary.get("loss_data") or {}

        run = Run(
            site_id=site.id,
            run_name=site_config.run_name,
            timestamp=datetime.now(timezone.utc),
            git_hash=_get_git_hash(),
            data_sources=build_data_sources(site_config),
            weather_year=summary.get("weather_year", 0),
            status="success",
        )
        session.add(run)
        session.flush()

        # 3. Create run_inputs from SiteConfig fields
        run_input = RunInput(
            run_id=run.id,
            latitude=site_config.latitude,
            longitude=site_config.longitude,
            bess_dispatch_required=site_config.bess_dispatch_required,
            bess_optimization_required=site_config.bess_optimization_required,
            dc_size_mw=site_config.dc_size_mw,
            ac_installed_mw=site_config.ac_installed_mw,
            ac_poi_mw=site_config.ac_poi_mw,
            racking=site_config.racking,
            tilt=site_config.tilt,
            azimuth=site_config.azimuth,
            module_orientation=site_config.module_orientation,
            number_of_modules=site_config.number_of_modules,
            ground_clearance_height_m=site_config.ground_clearance_height_m,
            panel_model=site_config.panel_model,
            bifacial=site_config.bifacial,
            inverter_model=site_config.inverter_model,
            gcr=site_config.gcr,
            shading_pct=site_config.shading_percent,
            dc_wiring_loss_pct=site_config.dc_wiring_loss_percent,
            ac_wiring_loss_pct=site_config.ac_wiring_loss_percent,
            transformer_losses_pct=site_config.transformer_losses_percent,
            degradation_pct=site_config.degradation_percent,
            availability_pct=site_config.availability_percent,
            module_mismatch_pct=site_config.module_mismatch_percent,
            lid_pct=site_config.lid_percent,
        )
        session.add(run_input)

        # 4. Create run_results from summary metrics and loss_data
        run_result = RunResult(
            run_id=run.id,
            annual_energy_mwh=summary.get("annual_energy_mwh"),
            net_capacity_factor=summary.get("net_capacity_factor"),
            capacity_factor_dc=loss_data.get("capacity_factor"),
            capacity_factor_ac=loss_data.get("capacity_factor_ac"),
            specific_yield=summary.get("specific_yield"),
            performance_ratio=summary.get("performance_ratio"),
            annual_dc_nominal=loss_data.get("annual_dc_nominal"),
            annual_snow_loss_pct=loss_data.get("annual_dc_snow_loss_percent"),
            bias_correction_applied=summary.get("bias_correction_applied"),
            bias_correction_model_version=summary.get("bias_correction_model_version"),
            mean_ghi_correction_factor=summary.get("mean_ghi_correction_factor"),
            mean_dni_correction_factor=summary.get("mean_dni_correction_factor"),
            subhourly_correction_pct=summary.get("subhourly_correction_pct"),
            subhourly_model_version=summary.get("subhourly_model_version"),
            raw_annual_energy_mwh=summary.get("raw_annual_energy_mwh"),
            monthly_energy=loss_data.get("monthly_energy"),
            monthly_snow_loss=loss_data.get("monthly_snow_loss"),
            loss_data=loss_data or None,
            timeseries_file_path=str(timeseries_path) if timeseries_path else None,
            report_file_path=str(report_path) if report_path else None,
            climate_file_path=str(climate_path) if climate_path else None,
        )
        session.add(run_result)

        # Flush to assign IDs, then expunge so the object is usable
        # after the session closes.
        session.flush()
        session.expunge(run)

        logger.info(
            f"Saved run {run.id} for site '{site_config.site_name}' "
            f"(customer='{site_config.customer}')"
        )
        return run
