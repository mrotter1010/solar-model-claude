"""Batch execution engine.

Takes validated rows from the batch validator and dispatches each to the
appropriate analysis function (buildability, production, optimization,
optimization_bess). Results are collected into a BatchResult for downstream
Excel report generation.
"""

from __future__ import annotations

import logging
import math
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from src.buildability.analyzer import run_buildability_analysis
from src.buildability.models import BuildabilityConfig, BuildabilityResult
from src.config.schema import SiteConfig
from src.optimization.bess_optimizer import optimize_solar_bess
from src.optimization.solar_optimizer import optimize_solar
from src.pipeline import SolarModelingPipeline, run_climate_data_pipeline

logger = logging.getLogger(__name__)

# Default equipment names (matching template instructions)
DEFAULT_MODULE = "LONGi Green Energy Technology Co. Ltd. LR5-72HBD-550M"
DEFAULT_INVERTER = "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]"

# Default loss percentages (matching template documentation)
DEFAULT_LOSSES: dict[str, float] = {
    "shading_percent": 0.0,
    "dc_wiring_loss_percent": 2.0,
    "ac_wiring_loss_percent": 0.5,
    "transformer_losses_percent": 1.0,
    "degradation_percent": 0.5,
    "availability_percent": 2.5,
    "module_mismatch_percent": 2.0,
    "lid_percent": 1.5,
}

# Template column → SiteConfig loss field mapping
_LOSS_FIELD_MAP: dict[str, str] = {
    "shading_pct": "shading_percent",
    "dc_wiring_pct": "dc_wiring_loss_percent",
    "ac_wiring_pct": "ac_wiring_loss_percent",
    "transformer_pct": "transformer_losses_percent",
    "degradation_pct": "degradation_percent",
    "availability_pct": "availability_percent",
    "mismatch_pct": "module_mismatch_percent",
    "lid_pct": "lid_percent",
}



@dataclass
class RowResult:
    """Result from processing a single batch row."""

    row_index: int
    name: str
    analysis_type: str
    status: str
    error: str | None
    runtime_seconds: float
    results: dict | None


@dataclass
class BatchResult:
    """Aggregated result from processing all batch rows."""

    run_id: str
    total_rows: int
    succeeded: int
    failed: int
    total_runtime_seconds: float
    row_results: list[RowResult]
    output_dir: Path


# ── Type coercion helpers ──────────────────────────────────────────────


def _is_missing(value: object) -> bool:
    """Check if a value is None, NaN, or empty/whitespace string."""
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _coerce_bool(value: object) -> bool:
    """Convert a value to bool, handling string representations."""
    if _is_missing(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in ("true", "yes", "1")
    return False


def _coerce_float(value: object, default: float | None = None) -> float | None:
    """Convert a value to float, returning default if None/empty/NaN."""
    if _is_missing(value):
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _coerce_int(value: object, default: int | None = None) -> int | None:
    """Convert a value to int, returning default if None/empty/NaN."""
    if _is_missing(value):
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


# ── Field mapping functions ────────────────────────────────────────────


def map_row_to_site_config(row: dict) -> SiteConfig:
    """Map a validated batch row to a SiteConfig for PySAM simulation.

    Handles field name translation (template columns to SiteConfig fields),
    type coercion, and default value application for optional fields.

    Args:
        row: A validated batch row dict with template column names.

    Returns:
        A fully populated SiteConfig ready for simulation.
    """
    name = str(row.get("name", "Unknown"))
    racking_raw = row.get("racking")
    racking = "tracker" if _is_missing(racking_raw) else str(racking_raw).strip().lower()

    module_raw = row.get("module")
    panel_model = DEFAULT_MODULE if _is_missing(module_raw) else str(module_raw).strip()
    inverter_raw = row.get("inverter")
    inverter_model = DEFAULT_INVERTER if _is_missing(inverter_raw) else str(inverter_raw).strip()

    config_data: dict = {
        # Project info
        "run_name": name,
        "site_name": name,
        "customer": "Batch",
        # Location
        "latitude": float(row["latitude"]),
        "longitude": float(row["longitude"]),
        # System capacity
        "dc_size_mw": float(row["dc_capacity_mw"]),
        "ac_installed_mw": float(row["ac_capacity_mw"]),
        "ac_poi_mw": float(row["ac_capacity_mw"]),
        # System design
        "racking": racking,
        "tilt": _coerce_float(
            row.get("tilt"), 60.0 if racking == "tracker" else 25.0
        ),
        "azimuth": _coerce_float(row.get("azimuth"), 180.0),
        "module_orientation": "portrait",
        "number_of_modules": 1,
        "ground_clearance_height_m": 1.5,
        # Equipment
        "panel_model": panel_model,
        "bifacial": _coerce_bool(row.get("bifacial")),
        "inverter_model": inverter_model,
        # Layout
        "gcr": float(row["gcr"]),
    }

    # Loss fields with defaults
    for template_col, config_field in _LOSS_FIELD_MAP.items():
        default = DEFAULT_LOSSES[config_field]
        config_data[config_field] = _coerce_float(row.get(template_col), default)

    return SiteConfig(**config_data)


def _build_optimization_site_config(row: dict) -> SiteConfig:
    """Build a base SiteConfig for optimization runs.

    The optimizer overrides gcr, dc_size_mw, and ac_installed_mw per sweep
    point, so this provides valid placeholder values for those fields.

    Args:
        row: A validated batch row dict with template column names.

    Returns:
        A SiteConfig suitable as a base for optimize_solar().
    """
    name = str(row.get("name", "Unknown"))
    racking_raw = row.get("racking")
    racking = "tracker" if _is_missing(racking_raw) else str(racking_raw).strip().lower()

    module_raw = row.get("module")
    panel_model = DEFAULT_MODULE if _is_missing(module_raw) else str(module_raw).strip()
    inverter_raw = row.get("inverter")
    inverter_model = DEFAULT_INVERTER if _is_missing(inverter_raw) else str(inverter_raw).strip()

    config_data: dict = {
        "run_name": name,
        "site_name": name,
        "customer": "Batch",
        "latitude": float(row["latitude"]),
        "longitude": float(row["longitude"]),
        # Placeholders — optimizer overrides these per sweep point
        "dc_size_mw": 1.0,
        "ac_installed_mw": 1.0,
        "ac_poi_mw": 1.0,
        "gcr": 0.40,
        # System design
        "racking": racking,
        "tilt": _coerce_float(
            row.get("tilt"), 60.0 if racking == "tracker" else 25.0
        ),
        "azimuth": _coerce_float(row.get("azimuth"), 180.0),
        "module_orientation": "portrait",
        "number_of_modules": 1,
        "ground_clearance_height_m": 1.5,
        # Equipment
        "panel_model": panel_model,
        "bifacial": _coerce_bool(row.get("bifacial")),
        "inverter_model": inverter_model,
    }

    # Loss fields with defaults
    for template_col, config_field in _LOSS_FIELD_MAP.items():
        default = DEFAULT_LOSSES[config_field]
        config_data[config_field] = _coerce_float(row.get(template_col), default)

    return SiteConfig(**config_data)


def map_row_to_buildability_config(row: dict) -> BuildabilityConfig:
    """Map a validated batch row to a BuildabilityConfig.

    Args:
        row: A validated batch row dict with template column names.

    Returns:
        BuildabilityConfig for buildability analysis.
    """
    return BuildabilityConfig(
        latitude=float(row["latitude"]),
        longitude=float(row["longitude"]),
    )


# ── Result extraction helpers ──────────────────────────────────────────


def _extract_buildability_results(result: BuildabilityResult) -> dict:
    """Extract key metrics from a BuildabilityResult into a flat dict."""
    dominant = None
    if result.class_breakdown:
        dominant = result.class_breakdown[0].get("name")

    return {
        "buildable_acres": result.buildable_acres,
        "buildable_pct": result.buildable_pct,
        "total_acres": result.total_area_acres,
        "dominant_land_cover": dominant,
        "mean_slope": result.slope_stats.get("mean_degrees"),
        "tracker_suitable_pct": result.slope_stats.get("pct_below_tracker_limit"),
    }


def _extract_production_results(
    pipeline_result: dict, site_config: SiteConfig
) -> dict:
    """Extract key metrics from a production pipeline result."""
    summaries = pipeline_result.get("summaries", [])
    if not summaries:
        raise ValueError("Production pipeline returned no summaries")

    summary = summaries[0]
    dc_mw = summary.get("dc_size_mw")
    ac_mw = summary.get("ac_installed_mw")
    dcac = round(dc_mw / ac_mw, 3) if dc_mw and ac_mw and ac_mw > 0 else None

    # net_capacity_factor and performance_ratio are ratios (0-1) from
    # the pipeline; convert to percentages (0-100) to match the _pct
    # suffix and the workbook's '0.0"%"' format.
    cf_ratio = summary.get("net_capacity_factor")
    pr_ratio = summary.get("performance_ratio")

    return {
        "annual_energy_mwh": summary.get("annual_energy_mwh"),
        "capacity_factor_ac_pct": round(cf_ratio * 100, 2) if cf_ratio is not None else None,
        "specific_yield_kwh_per_kw": summary.get("specific_yield"),
        "performance_ratio_pct": round(pr_ratio * 100, 2) if pr_ratio is not None else None,
        "dc_capacity_mw": dc_mw,
        "ac_capacity_mw": ac_mw,
        "gcr": site_config.gcr,
        "dcac_ratio": dcac,
    }


def _extract_optimization_results(opt_result: dict) -> dict:
    """Extract key metrics from a solar optimization result."""
    winners = opt_result.get("winners", {})
    mode = opt_result.get("mode", "production")
    metadata = opt_result.get("sweep_metadata", {})

    if mode == "npv" and "npv" in winners:
        winner = winners["npv"]
        winner_type = "npv"
    elif mode == "lcoe" and "lcoe" in winners:
        winner = winners["lcoe"]
        winner_type = "lcoe"
    else:
        winner = winners.get("max_production", {})
        winner_type = "production"

    # capacity_factor_ac from PySAM is already a percentage (e.g. 21.5
    # means 21.5%); no conversion needed.
    cf_pct = winner.get("capacity_factor_ac")

    return {
        "winner_type": winner_type,
        "winner_gcr": winner.get("gcr"),
        "winner_dcac": winner.get("dcac_ratio"),
        "winner_mw_dc": winner.get("mw_dc"),
        "winner_mw_ac": winner.get("mw_ac"),
        "winner_annual_mwh": winner.get("annual_energy_mwh"),
        "winner_cf_pct": round(cf_pct, 2) if cf_pct is not None else None,
        "winner_lcoe_per_mwh": winner.get("lcoe_per_mwh"),
        "winner_npv": winner.get("npv"),
        "buildable_acres": metadata.get("buildable_acres"),
    }


def _extract_optimization_bess_results(bess_result: dict) -> dict:
    """Extract key metrics from a solar+BESS optimization result."""
    winners = bess_result.get("winners", {})
    bess_adds_value = bess_result.get("bess_adds_value", False)
    bess_detail = bess_result.get("bess_winner_detail")
    metadata = bess_result.get("sweep_metadata", {})

    solar_npv_winner = winners.get("solar_only_npv", {})

    # capacity_factor_ac from PySAM is already a percentage (e.g. 21.5
    # means 21.5%); no conversion needed.
    cf_pct = solar_npv_winner.get("capacity_factor_ac")

    result = {
        "winner_type": "npv",
        "winner_gcr": solar_npv_winner.get("gcr"),
        "winner_dcac": solar_npv_winner.get("dcac_ratio"),
        "winner_mw_dc": solar_npv_winner.get("mw_dc"),
        "winner_mw_ac": solar_npv_winner.get("mw_ac"),
        "winner_annual_mwh": solar_npv_winner.get("annual_energy_mwh"),
        "winner_cf_pct": round(cf_pct, 2) if cf_pct is not None else None,
        "winner_lcoe_per_mwh": solar_npv_winner.get("lcoe_per_mwh"),
        "winner_npv": solar_npv_winner.get("npv"),
        "buildable_acres": metadata.get("buildable_acres"),
        "bess_adds_value": bess_adds_value,
        "bess_power_mw": None,
        "bess_duration_hr": None,
        "bess_npv": None,
        "combined_npv": None,
    }

    if bess_detail is not None:
        bess_info = bess_detail.get("bess", {})
        combined_info = bess_detail.get("combined", {})
        result["bess_power_mw"] = bess_info.get("power_mw")
        result["bess_duration_hr"] = bess_info.get("duration_hr")
        result["bess_npv"] = bess_info.get("npv")
        result["combined_npv"] = combined_info.get("npv")

    return result


# ── Dispatch functions (one per analysis type) ─────────────────────────


def _run_buildability(row: dict, output_dir: Path) -> dict:
    """Run buildability analysis for a single batch row.

    Args:
        row: Validated batch row dict.
        output_dir: Directory for output files.

    Returns:
        Dict of key buildability metrics.
    """
    config = map_row_to_buildability_config(row)
    result = run_buildability_analysis(config, output_dir=output_dir)
    return _extract_buildability_results(result)


def _run_production(row: dict, output_dir: Path) -> dict:
    """Run production (PySAM) analysis for a single batch row.

    Args:
        row: Validated batch row dict.
        output_dir: Directory for output files.

    Returns:
        Dict of key production metrics.
    """
    site_config = map_row_to_site_config(row)
    pipeline = SolarModelingPipeline(output_dir=output_dir)
    result = pipeline.run_from_configs([site_config])

    if result["failed"] > 0:
        raise RuntimeError(
            f"Production simulation failed for {site_config.site_name}"
        )

    return _extract_production_results(result, site_config)


def _build_sweep_kwargs(row: dict) -> dict:
    """Build common sweep and economic keyword args from a batch row.

    Extracts GCR/DCAC sweep ranges, utilization factor, and economic
    parameters (all-or-nothing for lcoe/npv mode) from the row dict.

    Args:
        row: Validated batch row dict.

    Returns:
        Dict of keyword arguments for optimize_solar() or optimize_solar_bess().
    """
    kwargs: dict = {}

    # Sweep range overrides
    gcr_min = _coerce_float(row.get("gcr_min"))
    gcr_max = _coerce_float(row.get("gcr_max"))
    if gcr_min is not None and gcr_max is not None:
        kwargs["gcr_range"] = (gcr_min, gcr_max)

    dcac_min = _coerce_float(row.get("dcac_min"))
    dcac_max = _coerce_float(row.get("dcac_max"))
    if dcac_min is not None and dcac_max is not None:
        kwargs["dcac_range"] = (dcac_min, dcac_max)

    utilization = _coerce_float(row.get("utilization_factor"))
    if utilization is not None:
        kwargs["utilization_factor"] = utilization

    return kwargs


def _build_economic_kwargs(row: dict, require_defaults: bool = False) -> dict:
    """Build economic keyword args from a batch row.

    For optional economics (optimization), all six must be present or none.
    For required economics (optimization_bess), defaults are applied.

    Args:
        row: Validated batch row dict.
        require_defaults: If True, apply defaults for missing economic params.

    Returns:
        Dict of economic keyword arguments.
    """
    kwargs: dict = {}

    solar_cost = _coerce_float(row.get("solar_cost_per_kw_dc"))
    solar_opex = _coerce_float(row.get("solar_opex_per_kw_dc_year"))
    discount = _coerce_float(row.get("discount_rate_pct"))
    lifetime = _coerce_int(row.get("project_lifetime_years"))
    degradation = _coerce_float(row.get("degradation_pct"))
    itc = _coerce_float(row.get("itc_pct"))

    all_present = all(
        v is not None
        for v in [solar_cost, solar_opex, discount, lifetime, degradation, itc]
    )

    if all_present or require_defaults:
        kwargs["solar_cost_per_kw_dc"] = solar_cost if solar_cost is not None else 1000.0
        kwargs["solar_opex_per_kw_dc_year"] = solar_opex if solar_opex is not None else 10.0
        kwargs["discount_rate_pct"] = discount if discount is not None else 7.0
        kwargs["project_lifetime_years"] = lifetime if lifetime is not None else 25
        kwargs["degradation_pct"] = degradation if degradation is not None else 0.5
        kwargs["itc_pct"] = itc if itc is not None else 30.0

        # Revenue params (all-or-nothing for npv mode)
        energy_price = _coerce_float(row.get("energy_price_per_kwh"))
        escalator = _coerce_float(row.get("energy_cost_escalator_pct"))

        if energy_price is not None and escalator is not None:
            kwargs["energy_price_per_kwh"] = energy_price
            kwargs["energy_cost_escalator_pct"] = escalator
        elif require_defaults:
            kwargs["energy_price_per_kwh"] = energy_price if energy_price is not None else 0.04
            kwargs["energy_cost_escalator_pct"] = escalator if escalator is not None else 2.0

    return kwargs


def _run_optimization(row: dict, output_dir: Path) -> dict:
    """Run solar layout optimization for a single batch row.

    Args:
        row: Validated batch row dict.
        output_dir: Directory for output files.

    Returns:
        Dict of key optimization metrics.
    """
    site_config = _build_optimization_site_config(row)

    # Fetch weather data — the optimizer calls PySAM directly and expects
    # site_config.weather_file_path to already be set with climate data
    # (including albedo). The production path gets this for free via
    # SolarModelingPipeline, but optimization bypasses the pipeline.
    updated_configs, _, _ = run_climate_data_pipeline([site_config])
    site_config = updated_configs[0]

    buildable_acres = float(row["buildable_acres"])
    racking_raw = row.get("racking")
    racking = "tracker" if _is_missing(racking_raw) else str(racking_raw).strip().lower()

    kwargs = _build_sweep_kwargs(row)
    kwargs.update(_build_economic_kwargs(row, require_defaults=False))

    result = optimize_solar(
        base_site_config=site_config,
        buildable_acres=buildable_acres,
        racking=racking,
        **kwargs,
    )

    return _extract_optimization_results(result)


def _run_optimization_bess(row: dict, output_dir: Path) -> dict:
    """Run solar+BESS optimization for a single batch row.

    Fetches LMP data for the site location and runs the joint solar+BESS
    GCR x DC/AC x BESS power/duration sweep.

    Args:
        row: Validated batch row dict.
        output_dir: Directory for output files.

    Returns:
        Dict of key optimization+BESS metrics.
    """
    from src.lmp.client import fetch_lmp
    from src.lmp.zone_mapper import resolve_pricing_zone

    site_config = _build_optimization_site_config(row)

    # Fetch weather data — same reason as _run_optimization(): the optimizer
    # calls PySAM directly and needs weather_file_path (incl. albedo) set.
    updated_configs, _, _ = run_climate_data_pipeline([site_config])
    site_config = updated_configs[0]

    buildable_acres = float(row["buildable_acres"])
    racking_raw = row.get("racking")
    racking = "tracker" if _is_missing(racking_raw) else str(racking_raw).strip().lower()
    dispatch_raw = row.get("dispatch_mode")
    dispatch_mode = "ftm" if _is_missing(dispatch_raw) else str(dispatch_raw).strip().lower()
    charging_raw = row.get("charging_mode")
    charging_mode = "solar_and_grid" if _is_missing(charging_raw) else str(charging_raw).strip().lower()

    kwargs = _build_sweep_kwargs(row)
    kwargs.update(_build_economic_kwargs(row, require_defaults=True))

    # BESS cost params
    bess_cost = _coerce_float(row.get("bess_cost_per_kwh"), 275.0)
    bess_opex = _coerce_float(row.get("bess_opex_per_kw_year"), 0.0)

    # FTM requires LMP data — resolve pricing zone from coordinates
    lmp_data = None
    if dispatch_mode == "ftm":
        pricing_zone = resolve_pricing_zone(site_config)
        lmp_data = fetch_lmp(
            iso=pricing_zone.iso,
            zone=pricing_zone.zone_name,
            market="DAY_AHEAD_HOURLY",
        )

    result = optimize_solar_bess(
        base_site_config=site_config,
        buildable_acres=buildable_acres,
        dispatch_mode=dispatch_mode,
        racking=racking,
        charging_mode=charging_mode,
        bess_cost_per_kwh=bess_cost,
        bess_opex_per_kw_year=bess_opex,
        lmp_data=lmp_data,
        **kwargs,
    )

    return _extract_optimization_bess_results(result)


# ── Main execution function ────────────────────────────────────────────


def execute_batch(
    valid_rows: list[dict],
    output_base_dir: Path | None = None,
    progress_callback: Callable[[int, int, str, str], None] | None = None,
) -> BatchResult:
    """Execute a batch of validated analysis rows.

    Loops through rows sequentially, dispatches each to the appropriate
    analysis function, and collects results. Each row is wrapped in
    try/except so one failure does not block subsequent rows.

    Args:
        valid_rows: List of validated row dicts from the batch validator.
        output_base_dir: Root directory for output files. If None,
            defaults to ``outputs/api/``.
        progress_callback: Called after each row completes as
            ``callback(current_row, total_rows, site_name, status)``.

    Returns:
        BatchResult with per-row results and aggregate statistics.
    """
    run_id = uuid.uuid4().hex[:12]

    if output_base_dir is None:
        output_base_dir = Path("outputs/api")

    output_dir = output_base_dir / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Starting batch execution: run_id=%s, rows=%d, output=%s",
        run_id,
        len(valid_rows),
        output_dir,
    )

    # Build dispatch table inside the function so that test mocks
    # (which replace module-level names) are picked up at call time.
    dispatch: dict[str, Callable[[dict, Path], dict]] = {
        "buildability": _run_buildability,
        "production": _run_production,
        "optimization": _run_optimization,
        "optimization_bess": _run_optimization_bess,
    }

    row_results: list[RowResult] = []
    succeeded = 0
    failed = 0
    batch_start = time.monotonic()

    for i, row in enumerate(valid_rows, start=1):
        name = str(row.get("name", f"Row {i}"))
        analysis_type = str(row.get("analysis_type", "unknown")).strip().lower()

        row_output_dir = output_dir / f"row_{i}_{name}"
        row_output_dir.mkdir(parents=True, exist_ok=True)

        row_start = time.monotonic()

        handler = dispatch.get(analysis_type)
        if handler is None:
            row_result = RowResult(
                row_index=i,
                name=name,
                analysis_type=analysis_type,
                status="failed",
                error=f"Unknown analysis_type: {analysis_type!r}",
                runtime_seconds=0.0,
                results=None,
            )
            failed += 1
        else:
            try:
                results = handler(row, row_output_dir)
                elapsed = time.monotonic() - row_start
                row_result = RowResult(
                    row_index=i,
                    name=name,
                    analysis_type=analysis_type,
                    status="success",
                    error=None,
                    runtime_seconds=round(elapsed, 2),
                    results=results,
                )
                succeeded += 1
                logger.info(
                    "Row %d/%d (%s) completed: %s in %.1fs",
                    i,
                    len(valid_rows),
                    name,
                    analysis_type,
                    elapsed,
                )
            except Exception as exc:
                elapsed = time.monotonic() - row_start
                error_msg = str(exc) if str(exc) else repr(exc)
                row_result = RowResult(
                    row_index=i,
                    name=name,
                    analysis_type=analysis_type,
                    status="failed",
                    error=error_msg,
                    runtime_seconds=round(elapsed, 2),
                    results=None,
                )
                failed += 1
                logger.warning(
                    "Row %d/%d (%s) failed: %s — %s",
                    i,
                    len(valid_rows),
                    name,
                    analysis_type,
                    error_msg,
                )

        row_results.append(row_result)

        if progress_callback is not None:
            try:
                progress_callback(i, len(valid_rows), name, row_result.status)
            except Exception as cb_exc:
                logger.warning("progress_callback raised: %s", cb_exc)

    total_runtime = round(time.monotonic() - batch_start, 2)

    logger.info(
        "Batch complete: run_id=%s, %d/%d succeeded in %.1fs",
        run_id,
        succeeded,
        len(valid_rows),
        total_runtime,
    )

    return BatchResult(
        run_id=run_id,
        total_rows=len(valid_rows),
        succeeded=succeeded,
        failed=failed,
        total_runtime_seconds=total_runtime,
        row_results=row_results,
        output_dir=output_dir,
    )
