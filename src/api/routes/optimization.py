"""Optimization route for the Solar Model API."""

import json
import re
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException

from src.api.schemas.optimization import OptimizationRequest, OptimizationResponse
from src.config.schema import SiteConfig
from src.optimization.bess_optimizer import optimize_solar_bess
from src.optimization.defaults import (
    DEFAULT_INVERTER_NAME,
    DEFAULT_MODULE_AREA_M2,
    DEFAULT_MODULE_NAME,
    DEFAULT_MODULE_POWER_W,
)
from src.optimization.solar_optimizer import optimize_solar
from src.pipeline import SolarModelingPipeline, run_climate_data_pipeline
from src.pysam_integration.cec_database import CECDatabase
from src.reporting.report_generator import generate_report
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

router = APIRouter(prefix="/analyses", tags=["analyses"])


def _sanitize_run_name(run_name: str) -> str:
    """Replace URL-unfriendly characters with underscores."""
    return re.sub(r"[\s/\"']+", "_", run_name)


def _resolve_buildability_inputs(
    request: OptimizationRequest,
) -> dict[str, float]:
    """Resolve buildable_acres and optionally lat/lon from buildability run.

    When buildability_run_id is provided, reads the buildability results
    JSON. If the request is missing latitude/longitude, also extracts
    those from the buildability result (the centroid computed during
    the buildability analysis).

    Args:
        request: Validated optimization request.

    Returns:
        Dict with 'buildable_acres' and optionally 'latitude', 'longitude'.

    Raises:
        HTTPException: If buildability_run_id results file not found,
            unreadable, or missing required fields.
    """
    if request.buildable_acres is not None:
        return {"buildable_acres": request.buildable_acres}

    run_id = request.buildability_run_id
    results_path = Path("outputs/api") / run_id / "results.json"

    if not results_path.exists():
        raise HTTPException(
            status_code=404,
            detail=(
                f"Buildability run '{run_id}' not found. "
                f"No results file at {results_path}. "
                "Run a buildability analysis first via POST /analyses/buildability."
            ),
        )

    try:
        data = json.loads(results_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read buildability results for '{run_id}': {exc}",
        ) from exc

    acres = data.get("buildable_acres")
    if acres is None:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Buildability results for '{run_id}' do not contain "
                "'buildable_acres'. Was this a buildability analysis?"
            ),
        )

    result: dict[str, float] = {"buildable_acres": float(acres)}

    # Extract lat/lon from buildability result when not provided in request
    if request.latitude is None and "latitude" in data:
        result["latitude"] = float(data["latitude"])
    if request.longitude is None and "longitude" in data:
        result["longitude"] = float(data["longitude"])

    return result


def _resolve_module_specs(
    module_name: str | None,
) -> tuple[str, float, float]:
    """Resolve module name, power (W), and area (m2) from CEC or defaults.

    Args:
        module_name: CEC module name, or None for defaults.

    Returns:
        Tuple of (resolved_module_name, module_power_w, module_area_m2).

    Raises:
        HTTPException: If module_name not found in CEC database.
    """
    if module_name is None:
        return DEFAULT_MODULE_NAME, DEFAULT_MODULE_POWER_W, DEFAULT_MODULE_AREA_M2

    try:
        cec = CECDatabase()
        params = cec.get_module_params(module_name)
        return module_name, params.pmax, params.area
    except Exception as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Module '{module_name}' not found in CEC database: {exc}",
        ) from exc


def _resolve_inverter_name(inverter_name: str | None) -> str:
    """Resolve inverter name from request or defaults.

    Args:
        inverter_name: CEC inverter name, or None for default.

    Returns:
        Resolved inverter name.

    Raises:
        HTTPException: If inverter_name not found in CEC database.
    """
    if inverter_name is None:
        return DEFAULT_INVERTER_NAME

    try:
        cec = CECDatabase()
        cec.get_inverter_params(inverter_name)
        return inverter_name
    except Exception as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Inverter '{inverter_name}' not found in CEC database: {exc}",
        ) from exc


def _build_template_site_config(
    request: OptimizationRequest,
    module_name: str,
    inverter_name: str,
) -> SiteConfig:
    """Build a minimal template SiteConfig for the optimizer.

    The optimizer overrides gcr, dc_size_mw, ac_installed_mw, and racking
    per sweep point via model_copy(update=...). This template only needs
    to be valid enough for PySAM to accept.

    Args:
        request: Validated optimization request.
        module_name: Resolved CEC module name.
        inverter_name: Resolved CEC inverter name.

    Returns:
        Template SiteConfig.
    """
    run_name = _sanitize_run_name(
        f"optimize_{request.latitude}_{request.longitude}_"
        f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )

    tilt = 25.0 if request.racking == "fixed" else 60.0

    return SiteConfig(
        run_name=run_name,
        site_name=f"Optimization {request.latitude:.4f}, {request.longitude:.4f}",
        customer="API",
        latitude=request.latitude,
        longitude=request.longitude,
        # System capacity — placeholder values, overridden per sweep point
        dc_size_mw=1.0,
        ac_installed_mw=1.0,
        ac_poi_mw=1.0,
        # System design
        racking=request.racking,
        tilt=tilt,
        azimuth=180.0,
        module_orientation="portrait",
        number_of_modules=1,
        ground_clearance_height_m=1.5,
        # Equipment
        panel_model=module_name,
        bifacial=False,
        inverter_model=inverter_name,
        # Layout — placeholder, overridden per sweep point
        gcr=0.35,
        # Standard loss defaults
        shading_percent=0.0,
        dc_wiring_loss_percent=2.0,
        ac_wiring_loss_percent=0.5,
        transformer_losses_percent=1.0,
        degradation_percent=0.5,
        availability_percent=2.5,
        module_mismatch_percent=2.0,
        lid_percent=1.5,
        # Disable pipeline features
        report=False,
        bill_calculation=False,
        bess_dispatch_required=False,
        bess_optimization_required=False,
        buildable_land_assessment=False,
    )


def _strip_sweep_results(result: dict) -> dict:
    """Strip bulky sweep_results/hourly data from optimizer output for persistence.

    Keeps winners and metadata. Drops per-point sweep data and 8760 arrays
    to keep the results JSON manageable.

    Args:
        result: Raw dict from optimize_solar or optimize_solar_bess.

    Returns:
        Slimmed dict suitable for JSON persistence.
    """
    slim = {
        "mode": result["mode"],
        "winners": result["winners"],
        "sweep_metadata": result["sweep_metadata"],
    }

    # Strip ac_hourly_kw from winner dicts to avoid 8760-element arrays
    for winner_dict in slim["winners"].values():
        if isinstance(winner_dict, dict):
            winner_dict.pop("ac_hourly_kw", None)

    if "bess_adds_value" in result:
        slim["bess_adds_value"] = result["bess_adds_value"]
    if "bess_winner_detail" in result:
        detail = result["bess_winner_detail"]
        if detail is not None and "solar" in detail:
            detail["solar"].pop("ac_hourly_kw", None)
        slim["bess_winner_detail"] = detail

    return slim


def _pick_report_winner(result: dict, report_winner: str) -> dict:
    """Select the winner configuration to use for the PDF report.

    Args:
        result: Raw dict from optimize_solar or optimize_solar_bess.
        report_winner: User preference — "auto", "max_production",
            "max_yield", "lcoe", or "npv".

    Returns:
        Winner sweep-point dict with gcr, mw_dc, mw_ac, etc.
    """
    if report_winner == "auto":
        mode = result["mode"]
        if mode == "solar_bess":
            return result["bess_winner_detail"]["solar"]
        elif mode == "npv":
            return result["winners"]["npv"]
        elif mode == "lcoe":
            return result["winners"]["lcoe"]
        else:
            return result["winners"]["max_production"]

    winner = result["winners"].get(report_winner)
    if winner is None:
        logger.warning(
            f"Requested report_winner '{report_winner}' not available "
            f"in mode '{result['mode']}' — falling back to max_production"
        )
        winner = result["winners"]["max_production"]
    return winner


@router.post("/optimize", response_model=OptimizationResponse)
def run_optimization(request: OptimizationRequest) -> OptimizationResponse:
    """Run a solar layout optimization sweep.

    Supports four modes based on request content:
    - production: no economic inputs
    - lcoe: economic inputs without revenue
    - npv: economic + revenue inputs
    - solar_bess: bess_enabled=True with dispatch configuration
    """
    try:
        # --- Resolve inputs ---
        buildability_inputs = _resolve_buildability_inputs(request)
        buildable_acres = buildability_inputs["buildable_acres"]

        # Backfill lat/lon from buildability result when not in request
        if request.latitude is None:
            if "latitude" not in buildability_inputs:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "latitude is required and was not found in the "
                        "buildability result either."
                    ),
                )
            request.latitude = buildability_inputs["latitude"]
        if request.longitude is None:
            if "longitude" not in buildability_inputs:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "longitude is required and was not found in the "
                        "buildability result either."
                    ),
                )
            request.longitude = buildability_inputs["longitude"]

        module_name, module_power_w, module_area_m2 = _resolve_module_specs(
            request.module_name
        )
        inverter_name = _resolve_inverter_name(request.inverter_name)

        base_config = _build_template_site_config(
            request, module_name, inverter_name
        )

        # --- Fetch weather data (once, reused by all sweep points) ---
        logger.info(
            f"Fetching weather data for optimization at "
            f"({request.latitude}, {request.longitude})"
        )
        run_climate_data_pipeline([base_config])
        if base_config.weather_file_path is None:
            raise HTTPException(
                status_code=502,
                detail=(
                    "Failed to fetch weather data for "
                    f"({request.latitude}, {request.longitude}). "
                    "NSRDB may be temporarily unavailable."
                ),
            )

        output_dir = Path("outputs/api") / base_config.run_name
        output_dir.mkdir(parents=True, exist_ok=True)

        # --- Run optimizer ---
        if request.bess_enabled:
            result = _run_bess_optimization(
                request, base_config, buildable_acres,
                module_power_w, module_area_m2,
            )
        else:
            result = _run_solar_optimization(
                request, base_config, buildable_acres,
                module_power_w, module_area_m2,
            )

        # --- Build response ---
        slim = _strip_sweep_results(result)
        response = OptimizationResponse(
            run_id=base_config.run_name,
            mode=result["mode"],
            winners=slim["winners"],
            bess_adds_value=slim.get("bess_adds_value"),
            bess_winner_detail=slim.get("bess_winner_detail"),
            sweep_metadata=slim["sweep_metadata"],
        )

        # --- Persist ---
        results_path = output_dir / "results.json"
        results_path.write_text(
            response.model_dump_json(indent=2), encoding="utf-8"
        )
        logger.info(f"Optimization results saved to {results_path}")

        # --- Generate PDF report for the selected winner ---
        try:
            winner = _pick_report_winner(result, request.report_winner)
            winner_config = base_config.model_copy(update={
                "gcr": winner["gcr"],
                "dc_size_mw": winner["mw_dc"],
                "ac_installed_mw": winner["mw_ac"],
                "ac_poi_mw": winner["mw_ac"],
            })

            pipeline = SolarModelingPipeline(output_dir=output_dir)
            pysam_results = pipeline.run_from_configs(
                [winner_config], skip_climate=True,
            )

            if pysam_results["successful"] > 0:
                loss_data = pysam_results["summaries"][0].get("loss_data")
                if loss_data:
                    reports_dir = output_dir / "reports"
                    reports_dir.mkdir(parents=True, exist_ok=True)
                    generate_report(
                        site_config=winner_config.model_dump(),
                        loss_data=loss_data,
                        output_dir=reports_dir,
                        summary={"optimization": result},
                    )
                    logger.info("Optimization PDF report generated")
                else:
                    logger.warning(
                        "Winner PySAM run succeeded but loss_data is empty "
                        "— skipping report"
                    )
            else:
                logger.warning(
                    "Winner PySAM run failed — skipping report generation"
                )
        except Exception as report_exc:
            logger.warning(
                f"Optimization report generation failed: {report_exc}"
            )

        return response

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Optimization error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _run_solar_optimization(
    request: OptimizationRequest,
    base_config: SiteConfig,
    buildable_acres: float,
    module_power_w: float,
    module_area_m2: float,
) -> dict:
    """Run solar-only optimization (production/lcoe/npv modes).

    Args:
        request: Validated optimization request.
        base_config: Template SiteConfig.
        buildable_acres: Resolved buildable acreage.
        module_power_w: Module STC power (W).
        module_area_m2: Module area (m2).

    Returns:
        Raw dict from optimize_solar().
    """
    return optimize_solar(
        base_site_config=base_config,
        buildable_acres=buildable_acres,
        module_power_w=module_power_w,
        module_area_m2=module_area_m2,
        utilization_factor=request.utilization_factor,
        racking=request.racking,
        gcr_range=request.gcr_range,
        gcr_step=request.gcr_step,
        dcac_range=request.dcac_range,
        dcac_step=request.dcac_step,
        solar_cost_per_kw_dc=request.solar_cost_per_kw_dc,
        solar_opex_per_kw_dc_year=request.solar_opex_per_kw_dc_year,
        discount_rate_pct=request.discount_rate_pct,
        project_lifetime_years=request.project_lifetime_years,
        degradation_pct=request.degradation_pct,
        itc_pct=request.itc_pct,
        energy_price_per_kwh=request.energy_price_per_kwh,
        energy_cost_escalator_pct=request.energy_cost_escalator_pct,
    )


def _run_bess_optimization(
    request: OptimizationRequest,
    base_config: SiteConfig,
    buildable_acres: float,
    module_power_w: float,
    module_area_m2: float,
) -> dict:
    """Run solar+BESS joint optimization.

    Args:
        request: Validated optimization request.
        base_config: Template SiteConfig.
        buildable_acres: Resolved buildable acreage.
        module_power_w: Module STC power (W).
        module_area_m2: Module area (m2).

    Returns:
        Raw dict from optimize_solar_bess().
    """
    # Build dispatch-mode-specific inputs
    rate_schedule = None
    load_profile = None
    lmp_data = None

    if request.dispatch_mode == "btm":
        if request.rate_schedule is not None:
            from src.rates.models import RateSchedule
            rate_schedule = RateSchedule(**request.rate_schedule)
        if request.load_profile_kwh is not None:
            from src.rates.models import LoadProfile
            load_profile = LoadProfile(
                hourly_kwh=request.load_profile_kwh,
                source="api",
            )

    elif request.dispatch_mode == "ftm":
        if request.lmp_prices is not None:
            from src.lmp.models import LMPData
            lmp_data = LMPData(
                prices=request.lmp_prices,
                iso=request.lmp_iso or "unknown",
                zone=request.lmp_zone or "unknown",
                market="DAY_AHEAD_HOURLY",
                year=2024,
            )

    return optimize_solar_bess(
        base_site_config=base_config,
        buildable_acres=buildable_acres,
        dispatch_mode=request.dispatch_mode,
        module_power_w=module_power_w,
        module_area_m2=module_area_m2,
        utilization_factor=request.utilization_factor,
        racking=request.racking,
        gcr_range=request.gcr_range,
        gcr_step=request.gcr_step,
        dcac_range=request.dcac_range,
        dcac_step=request.dcac_step,
        bess_power_fractions=request.bess_power_fractions,
        bess_durations_hr=request.bess_durations_hr,
        charging_mode=request.charging_mode,
        solar_cost_per_kw_dc=request.solar_cost_per_kw_dc or 1200.0,
        solar_opex_per_kw_dc_year=request.solar_opex_per_kw_dc_year or 20.0,
        discount_rate_pct=request.discount_rate_pct or 7.0,
        project_lifetime_years=request.project_lifetime_years or 25,
        degradation_pct=request.degradation_pct or 0.5,
        itc_pct=request.itc_pct or 30.0,
        energy_price_per_kwh=request.energy_price_per_kwh,
        energy_cost_escalator_pct=request.energy_cost_escalator_pct,
        bess_cost_per_kwh=request.bess_cost_per_kwh or 340.0,
        bess_opex_per_kw_year=request.bess_opex_per_kw_year or 25.0,
        rate_schedule=rate_schedule,
        load_profile=load_profile,
        lmp_data=lmp_data,
    )
