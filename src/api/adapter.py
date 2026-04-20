"""Adapter layer: maps API request schemas to pipeline SiteConfig objects."""

import re
from datetime import datetime
from pathlib import Path

from src.api.schemas.common import Losses
from src.api.schemas.requests import (
    BESSRequest,
    BillSavingsRequest,
    BuildabilityRequest,
    ProductionRequest,
)
from src.config.schema import SiteConfig


def _sanitize_run_name(run_name: str) -> str:
    """Replace URL-unfriendly characters with underscores.

    Ensures run_names are safe for use in URL path segments
    (e.g., GET /analyses/{run_id}/report) without percent-encoding.

    Args:
        run_name: Raw run name (auto-generated or user-provided).

    Returns:
        Sanitized run name with spaces, slashes, and quotes replaced.
    """
    return re.sub(r"[\s/\"']+", "_", run_name)


def _build_common_config_kwargs(
    site: object,
    system: object,
    losses: Losses | None,
    resource: object | None,
) -> dict:
    """Build the SiteConfig kwargs common to all request types.

    Args:
        site: Location model with name, latitude, longitude, etc.
        system: SystemDesign model with capacity, module, inverter, etc.
        losses: Optional Losses model (defaults applied if None).
        resource: Optional ResourceOverrides model.

    Returns:
        Dict of keyword arguments for SiteConfig construction.
    """
    losses = losses or Losses()

    # Generate run_name if not provided, then sanitize for URL safety
    run_name = site.run_name or (
        f"{site.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    run_name = _sanitize_run_name(run_name)

    # Infer tilt from racking if not specified
    if system.tilt is not None:
        tilt = system.tilt
    else:
        tilt = 25.0 if system.racking == "fixed" else 60.0

    # Default ac_poi_mw to ac_capacity_mw
    ac_poi_mw = system.ac_poi_mw or system.ac_capacity_mw

    # Resource file overrides
    resource_file_path = None
    ground_truth_data_file = None
    if resource is not None:
        resource_file_path = resource.resource_file_path
        ground_truth_data_file = resource.ground_truth_file

    return {
        # Project info
        "run_name": run_name,
        "site_name": site.name,
        "customer": site.customer,
        # Location
        "latitude": site.latitude,
        "longitude": site.longitude,
        # System capacity
        "dc_size_mw": system.dc_capacity_mw,
        "ac_installed_mw": system.ac_capacity_mw,
        "ac_poi_mw": ac_poi_mw,
        # System design
        "racking": system.racking,
        "tilt": tilt,
        "azimuth": system.azimuth,
        "module_orientation": system.module_orientation,
        "number_of_modules": system.num_modules,
        "ground_clearance_height_m": system.ground_clearance_m,
        # Equipment
        "panel_model": system.module,
        "bifacial": system.bifacial,
        "inverter_model": system.inverter,
        # Layout
        "gcr": system.gcr,
        # Losses
        "shading_percent": losses.shading_pct,
        "dc_wiring_loss_percent": losses.dc_wiring_pct,
        "ac_wiring_loss_percent": losses.ac_wiring_pct,
        "transformer_losses_percent": losses.transformer_pct,
        "degradation_percent": losses.degradation_pct,
        "availability_percent": losses.availability_pct,
        "module_mismatch_percent": losses.mismatch_pct,
        "lid_percent": losses.lid_pct,
        # Reporting — API users always get a PDF
        "report": True,
        # Resource overrides
        "resource_file_path": resource_file_path,
        "ground_truth_data_file": ground_truth_data_file,
    }


def production_request_to_site_config(request: ProductionRequest) -> SiteConfig:
    """Map a ProductionRequest to a flat SiteConfig for the pipeline.

    Args:
        request: Validated API request with site, system, losses, and
            resource sections.

    Returns:
        A fully populated SiteConfig ready for pipeline execution.
    """
    kwargs = _build_common_config_kwargs(
        site=request.site,
        system=request.system,
        losses=request.losses,
        resource=request.resource,
    )
    return SiteConfig(
        **kwargs,
        # Disable bill/BESS/buildability for production-only runs
        bill_calculation=False,
        bess_dispatch_required=False,
        bess_optimization_required=False,
        buildable_land_assessment=False,
    )


def bill_savings_request_to_site_config(
    request: BillSavingsRequest,
) -> SiteConfig:
    """Map a BillSavingsRequest to a flat SiteConfig for the pipeline.

    Args:
        request: Validated API request with site, system, bill, losses,
            and resource sections.

    Returns:
        A fully populated SiteConfig with bill_calculation enabled.
    """
    kwargs = _build_common_config_kwargs(
        site=request.site,
        system=request.system,
        losses=request.losses,
        resource=request.resource,
    )
    bill = request.bill

    # Rate source: inline RateSchedule object or file/URDB lookup
    rate_file_path = bill.rate_file_path
    utility_name = bill.utility_name
    tariff_name = bill.tariff_name
    rate_schedule = None
    if bill.rate is not None:
        rate_schedule = bill.rate
        rate_file_path = None
        utility_name = None
        tariff_name = None

    return SiteConfig(
        **kwargs,
        # Enable bill calculation
        bill_calculation=True,
        # Rate source
        rate_file_path=rate_file_path,
        utility_name=utility_name,
        tariff_name=tariff_name,
        rate_schedule=rate_schedule,
        # Load source
        load_profile_path=bill.load_profile_path,
        load_type=bill.load_type,
        annual_consumption_kwh=bill.annual_consumption_kwh,
        peak_demand_kw=bill.peak_demand_kw,
        # Disable BESS/buildability
        bess_dispatch_required=False,
        bess_optimization_required=False,
        buildable_land_assessment=False,
    )


def bess_request_to_site_config(request: BESSRequest) -> SiteConfig:
    """Map a BESSRequest to a flat SiteConfig for the pipeline.

    Args:
        request: Validated API request with site, system, bess, and
            optional bill, ftm, bess_economics, losses, resource sections.

    Returns:
        A fully populated SiteConfig with BESS dispatch enabled.
    """
    kwargs = _build_common_config_kwargs(
        site=request.site,
        system=request.system,
        losses=request.losses,
        resource=request.resource,
    )

    bess = request.bess

    # BESS configuration
    kwargs["bess_dispatch_required"] = True
    kwargs["bess_power_mw"] = bess.power_mw
    kwargs["bess_duration_hr"] = bess.duration_hr
    kwargs["bess_rte_percent"] = bess.rte_pct
    kwargs["bess_min_soc_percent"] = bess.min_soc_pct
    kwargs["bess_max_soc_percent"] = bess.max_soc_pct
    kwargs["bess_strategy"] = bess.strategy
    kwargs["bess_installed_cost_per_kwh"] = bess.installed_cost_per_kwh
    kwargs["bess_cycles_warranty"] = bess.cycles_warranty
    kwargs["bess_solar_only_charging"] = bess.solar_only_charging
    kwargs["bess_grid_only_charging"] = bess.grid_only_charging

    # Bill configuration (required for BTM, optional for FTM)
    if request.bill is not None:
        bill = request.bill
        kwargs["bill_calculation"] = True

        # Rate source: inline RateSchedule object or file/URDB lookup
        if bill.rate is not None:
            kwargs["rate_schedule"] = bill.rate
            kwargs["rate_file_path"] = None
            kwargs["utility_name"] = None
            kwargs["tariff_name"] = None
        else:
            kwargs["rate_file_path"] = bill.rate_file_path
            kwargs["utility_name"] = bill.utility_name
            kwargs["tariff_name"] = bill.tariff_name

        kwargs["load_profile_path"] = bill.load_profile_path
        kwargs["load_type"] = bill.load_type
        kwargs["annual_consumption_kwh"] = bill.annual_consumption_kwh
        kwargs["peak_demand_kw"] = bill.peak_demand_kw
    else:
        kwargs["bill_calculation"] = False

    # FTM configuration
    if request.ftm is not None:
        ftm = request.ftm
        kwargs["dispatch_mode"] = ftm.dispatch_mode
        kwargs["iso"] = ftm.iso
        kwargs["lmp_zone"] = ftm.lmp_zone
        kwargs["lmp_node_ids"] = ftm.lmp_node_ids
        kwargs["lmp_market"] = ftm.lmp_market
        kwargs["lmp_year"] = ftm.lmp_year
        kwargs["ancillary_revenue_per_kw_year"] = ftm.ancillary_revenue_per_kw_year

    # BESS economics / sizing optimization
    if request.bess_economics is not None:
        econ = request.bess_economics
        kwargs["bess_optimization_required"] = econ.optimize
        kwargs["discount_rate_pct"] = econ.discount_rate_pct
        kwargs["project_lifetime_years"] = econ.project_lifetime_years
        kwargs["rate_escalation_pct"] = econ.rate_escalation_pct
        kwargs["solar_cost_per_kw_dc"] = econ.solar_cost_per_kw_dc
        kwargs["solar_cost_per_kw_ac"] = econ.solar_cost_per_kw_ac
        kwargs["solar_opex_per_kw_dc_year"] = econ.solar_opex_per_kw_dc_year
        kwargs["bess_opex_per_kw_year"] = econ.bess_opex_per_kw_year
        kwargs["bess_power_min_mw"] = econ.power_min_mw
        kwargs["bess_power_max_mw"] = econ.power_max_mw
        kwargs["bess_duration_min_hr"] = econ.duration_min_hr
        kwargs["bess_duration_max_hr"] = econ.duration_max_hr
    else:
        kwargs["bess_optimization_required"] = False

    kwargs["buildable_land_assessment"] = False

    return SiteConfig(**kwargs)


# Dummy system design values for buildability-only requests.
# SiteConfig requires these fields for Pydantic validation even though
# buildability analysis only uses lat/lon and buildability fields.
# These are never sent to PySAM — they exist solely to satisfy the
# schema. A future run_from_configs() refactor would eliminate this.
_BUILDABILITY_DUMMY_SYSTEM = {
    "dc_size_mw": 1.0,
    "ac_installed_mw": 1.0,
    "ac_poi_mw": 1.0,
    "racking": "tracker",
    "tilt": 60.0,
    "azimuth": 180.0,
    "module_orientation": "portrait",
    "number_of_modules": 1,
    "ground_clearance_height_m": 1.5,
    "panel_model": "Placeholder",
    "bifacial": False,
    "inverter_model": "Placeholder",
    "gcr": 0.35,
    "shading_percent": 0.0,
    "dc_wiring_loss_percent": 0.0,
    "ac_wiring_loss_percent": 0.0,
    "transformer_losses_percent": 0.0,
    "degradation_percent": 0.0,
    "availability_percent": 0.0,
    "module_mismatch_percent": 0.0,
    "lid_percent": 0.0,
}


def buildability_request_to_site_config(
    request: BuildabilityRequest,
) -> SiteConfig:
    """Map a BuildabilityRequest to a SiteConfig for buildability analysis.

    Buildability only needs lat/lon and buildability flags from SiteConfig.
    All system design fields are filled with dummy placeholder values to
    satisfy SiteConfig validation (see _BUILDABILITY_DUMMY_SYSTEM).

    When a KMZ/KML file is provided without lat/lon, placeholder
    coordinates (0.0, 0.0) are used — the buildability analyzer will
    override them with the polygon centroid.

    Args:
        request: Validated BuildabilityRequest with site location and
            optional buildability configuration.

    Returns:
        A SiteConfig with buildable_land_assessment=True.
    """
    site = request.site
    run_name = site.run_name or (
        f"{site.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    run_name = _sanitize_run_name(run_name)

    # Map optional buildability config fields
    kmz_file_path = None
    analysis_radius_km = None
    if request.buildability is not None:
        kmz_file_path = request.buildability.kmz_file_path
        analysis_radius_km = request.buildability.analysis_radius_km

    # Use 0.0 placeholders when lat/lon not provided (KMZ centroid
    # will override in the analyzer)
    latitude = site.latitude if site.latitude is not None else 0.0
    longitude = site.longitude if site.longitude is not None else 0.0

    return SiteConfig(
        # Project info
        run_name=run_name,
        site_name=site.name,
        customer=site.customer,
        # Location
        latitude=latitude,
        longitude=longitude,
        # Dummy system design (not used by buildability)
        **_BUILDABILITY_DUMMY_SYSTEM,
        # Buildability enabled
        buildable_land_assessment=True,
        kmz_file_path=kmz_file_path,
        analysis_radius_km=analysis_radius_km,
        # Disable everything else
        report=False,
        bill_calculation=False,
        bess_dispatch_required=False,
        bess_optimization_required=False,
    )
