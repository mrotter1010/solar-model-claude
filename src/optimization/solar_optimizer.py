"""Solar layout optimization.

GCR × DC/AC ratio sweep that selects the optimal system design for a given
site and buildable land area. Supports three modes based on which inputs
are provided:

- production mode: no economic inputs. Returns max_production (highest
  annual MWh) and max_yield (highest capacity_factor_ac) winners.
- lcoe mode: full economic inputs provided. Adds the lcoe (minimum
  $/MWh) winner.
- npv mode: full economic + revenue inputs. Adds the npv (maximum NPV)
  winner.

Public entry point: optimize_solar().

Internal building blocks:
- _run_single_pysam(): run a single PySAM simulation with optional
  subhourly clipping correction applied.
- build_sweep_grid(): generate the (gcr, dcac_ratio) grid to sweep.
"""

from collections.abc import Callable

from src.config.schema import SiteConfig
from src.optimization.capacity import capacity_from_acreage
from src.optimization.clipping_correction import (
    apply_subhourly_clipping_correction,
)
from src.optimization.defaults import (
    DEFAULT_DCAC_RANGE,
    DEFAULT_DCAC_STEP,
    DEFAULT_GCR_RANGE_FIXED,
    DEFAULT_GCR_RANGE_TRACKER,
    DEFAULT_GCR_STEP,
    DEFAULT_MODULE_AREA_M2,
    DEFAULT_MODULE_POWER_W,
    DEFAULT_UTILIZATION_FACTOR,
)
from src.optimization.economics import compute_lcoe, compute_solar_npv
from src.pysam_integration.cec_database import CECDatabase
from src.pysam_integration.model_configurator import ModelConfigurator
from src.pysam_integration.simulator import PySAMSimulator
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def _run_single_pysam(
    site_config: SiteConfig,
    configurator: ModelConfigurator,
    simulator: PySAMSimulator,
    apply_clipping_correction: bool = True,
) -> dict:
    """Run a single PySAM simulation and return key outputs.

    Optionally applies the subhourly clipping correction (mutates the result
    in place) using the shared helper from clipping_correction.py.

    Args:
        site_config: Site configuration (already has gcr, dc_size_mw,
            ac_installed_mw, racking set to the candidate values).
        configurator: ModelConfigurator instance (pre-built with CEC database).
        simulator: PySAMSimulator instance.
        apply_clipping_correction: If True, apply the subhourly clipping
            correction to the result before extracting outputs.

    Returns:
        Dict with keys:
            - annual_energy_kwh: Annual AC energy (post-correction if applied).
            - capacity_factor_ac: AC capacity factor as a decimal.
            - clipping_correction_pct: The correction percentage applied.
              0.0 if not applied or if correction helper returned empty metadata.
            - ac_hourly_kw: 8760-length list of hourly AC generation (kW).
    """
    model_config = configurator.configure_model(site_config)
    result = simulator.execute_simulation(model_config)

    clipping_correction_pct = 0.0
    if apply_clipping_correction:
        metadata = apply_subhourly_clipping_correction(result, site_config)
        clipping_correction_pct = float(
            metadata.get("subhourly_correction_pct", 0.0)
        )

    ac_hourly_kw = result.hourly_data["ac_gross"].tolist()
    annual_energy_kwh = float(result.loss_data["annual_energy"])
    capacity_factor_ac = float(result.loss_data["capacity_factor_ac"])

    return {
        "annual_energy_kwh": annual_energy_kwh,
        "capacity_factor_ac": capacity_factor_ac,
        "clipping_correction_pct": clipping_correction_pct,
        "ac_hourly_kw": ac_hourly_kw,
    }


def build_sweep_grid(
    racking: str,
    gcr_range: tuple[float, float] | None = None,
    gcr_step: float = 0.05,
    dcac_range: tuple[float, float] = (1.15, 1.60),
    dcac_step: float = 0.05,
) -> list[tuple[float, float]]:
    """Build the (gcr, dcac_ratio) sweep grid for layout optimization.

    Both endpoints are inclusive. Each value is rounded to 2 decimals to
    avoid floating-point drift. No duplicates are produced.

    Args:
        racking: "tracker" or "fixed". Determines default gcr_range when
            gcr_range is None.
        gcr_range: (min, max) GCR range. If None, uses racking-specific
            defaults (tracker: 0.30-0.60; fixed: 0.40-0.70).
        gcr_step: Step size between GCR values. Must be > 0.
        dcac_range: (min, max) DC/AC ratio range.
        dcac_step: Step size between DC/AC values. Must be > 0.

    Returns:
        List of (gcr, dcac_ratio) tuples, both rounded to 2 decimals.

    Raises:
        ValueError: If racking is not "tracker" or "fixed", if any step
            is <= 0, or if any range has min > max.
    """
    if racking not in ("tracker", "fixed"):
        raise ValueError(
            f"Invalid racking: {racking!r}. Must be 'tracker' or 'fixed'."
        )
    if gcr_step <= 0:
        raise ValueError(f"gcr_step must be > 0, got {gcr_step}")
    if dcac_step <= 0:
        raise ValueError(f"dcac_step must be > 0, got {dcac_step}")

    if gcr_range is None:
        gcr_range = (
            DEFAULT_GCR_RANGE_TRACKER
            if racking == "tracker"
            else DEFAULT_GCR_RANGE_FIXED
        )

    gcr_min, gcr_max = gcr_range
    dcac_min, dcac_max = dcac_range

    if gcr_min > gcr_max:
        raise ValueError(
            f"gcr_range inverted: min ({gcr_min}) > max ({gcr_max})"
        )
    if dcac_min > dcac_max:
        raise ValueError(
            f"dcac_range inverted: min ({dcac_min}) > max ({dcac_max})"
        )

    gcr_values = _inclusive_range(gcr_min, gcr_max, gcr_step)
    dcac_values = _inclusive_range(dcac_min, dcac_max, dcac_step)

    grid: list[tuple[float, float]] = []
    seen: set[tuple[float, float]] = set()
    for g in gcr_values:
        for d in dcac_values:
            pair = (g, d)
            if pair not in seen:
                seen.add(pair)
                grid.append(pair)

    return grid


def _inclusive_range(start: float, stop: float, step: float) -> list[float]:
    """Generate values from start to stop (inclusive) at step intervals.

    Each value is rounded to 2 decimals to avoid float accumulation drift.
    If step overshoots stop by less than half a step, stop is not included.
    """
    values: list[float] = []
    current = start
    # Use a small epsilon so that values like 0.60 aren't excluded due to
    # tiny accumulated FP error.
    epsilon = step / 1000
    while current <= stop + epsilon:
        values.append(round(current, 2))
        current += step

    # Deduplicate while preserving order (in case of float drift or explicit
    # single-value ranges).
    seen: set[float] = set()
    unique: list[float] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            unique.append(v)
    return unique


def _empty_point(gcr: float, dcac: float, mw_dc: float, mw_ac: float,
                 num_modules: int) -> dict:
    """Build a per-sweep-point dict with all metric fields set to None.

    The sweep will overwrite the fields it computes; fields not computed
    in the current mode (e.g. lcoe_per_mwh in production mode) remain None.
    """
    return {
        "gcr": gcr,
        "dcac_ratio": dcac,
        "mw_dc": mw_dc,
        "mw_ac": mw_ac,
        "num_modules": num_modules,
        # Production metrics (always populated on success)
        "annual_energy_kwh": None,
        "annual_energy_mwh": None,
        "capacity_factor_ac": None,
        "clipping_correction_pct": None,
        # LCOE metrics (populated only in lcoe/npv modes)
        "capex_total": None,
        "opex_per_year": None,
        "net_capex": None,
        "lcoe_per_mwh": None,
        "lcoe_per_kwh": None,
        # NPV metrics (populated only in npv mode)
        "annual_revenue_year1": None,
        "npv": None,
        "simple_payback_years": None,
        "irr": None,
        "failed": False,
        "error": None,
    }


def optimize_solar(
    base_site_config: SiteConfig,
    buildable_acres: float,
    module_power_w: float = DEFAULT_MODULE_POWER_W,
    module_area_m2: float = DEFAULT_MODULE_AREA_M2,
    utilization_factor: float = DEFAULT_UTILIZATION_FACTOR,
    racking: str = "tracker",
    gcr_range: tuple[float, float] | None = None,
    gcr_step: float = DEFAULT_GCR_STEP,
    dcac_range: tuple[float, float] = DEFAULT_DCAC_RANGE,
    dcac_step: float = DEFAULT_DCAC_STEP,
    # Economic inputs — ALL OPTIONAL (all-or-nothing)
    solar_cost_per_kw_dc: float | None = None,
    solar_opex_per_kw_dc_year: float | None = None,
    discount_rate_pct: float | None = None,
    project_lifetime_years: int | None = None,
    degradation_pct: float | None = None,
    itc_pct: float | None = None,
    # Revenue inputs — ALL OPTIONAL (all-or-nothing, requires economics)
    energy_price_per_kwh: float | None = None,
    energy_cost_escalator_pct: float | None = None,
    progress_callback: Callable[[int, int, dict], None] | None = None,
    retain_hourly: bool = False,
) -> dict:
    """Run the GCR × DC/AC ratio sweep and select winning design(s).

    Supports three modes, inferred from which inputs are provided:

    - **production mode** (no economic inputs): returns max_production
      (highest annual MWh) and max_yield (highest capacity_factor_ac).
    - **lcoe mode** (all economic inputs): adds lcoe winner
      (minimum $/MWh).
    - **npv mode** (economic + revenue inputs): adds npv winner
      (maximum NPV).

    Economic inputs are all-or-nothing: pass all six or none. Revenue
    inputs are all-or-nothing and require economics.

    Args:
        base_site_config: Site configuration to use as a template. gcr,
            dc_size_mw, ac_installed_mw, and racking are overridden per
            sweep point.
        buildable_acres: Buildable land area (acres).
        module_power_w: Module STC power (W).
        module_area_m2: Module area (m²).
        utilization_factor: Fraction of buildable area used for modules.
        racking: "tracker" or "fixed".
        gcr_range: Optional (min, max) GCR range. None uses racking defaults.
        gcr_step: Step between GCR values.
        dcac_range: (min, max) DC/AC ratio range.
        dcac_step: Step between DC/AC values.
        solar_cost_per_kw_dc: Capex rate ($/kW-DC). Required for lcoe/npv.
        solar_opex_per_kw_dc_year: O&M rate ($/kW-DC/yr). Required for lcoe/npv.
        discount_rate_pct: Discount rate (percent, e.g. 7.0). Required for lcoe/npv.
        project_lifetime_years: Analysis period (years). Required for lcoe/npv.
        degradation_pct: Annual degradation (percent, e.g. 0.5). Required for lcoe/npv.
        itc_pct: Investment tax credit (percent, e.g. 30.0). Required for lcoe/npv.
        energy_price_per_kwh: Flat energy price ($/kWh). Required for npv.
        energy_cost_escalator_pct: Annual price escalation (percent, e.g. 2.0).
            Required for npv.
        progress_callback: Optional callable invoked after each sweep point
            as callback(index_1based, total, point_dict). Exceptions from
            the callback are logged but do not abort the sweep.
        retain_hourly: If True, include the full 8760-element "ac_hourly_kw"
            list in each sweep_results entry (None for failed points).
            Defaults to False to keep memory footprint small for callers
            that only need summary metrics.

    Returns:
        Dict with keys:
            - mode: "production" | "lcoe" | "npv".
            - winners: Dict keyed on selection metric. Always includes
              max_production and max_yield; also includes lcoe (lcoe/npv
              modes) and npv (npv mode).
            - sweep_results: List of per-sweep-point dicts in grid order.
              When retain_hourly=True, each entry includes an
              "ac_hourly_kw" key (8760-length list, or None on failure).
            - sweep_metadata: Dict of sweep configuration parameters,
              including a "retain_hourly" flag.

    Raises:
        ValueError: If economic or revenue inputs are partially provided,
            or if revenue inputs are given without economics.
        RuntimeError: If every sweep point fails.
    """
    # --- Mode detection ---
    economic_inputs = [
        solar_cost_per_kw_dc,
        solar_opex_per_kw_dc_year,
        discount_rate_pct,
        project_lifetime_years,
        degradation_pct,
        itc_pct,
    ]
    has_economics = all(x is not None for x in economic_inputs)
    partial_economics = (
        any(x is not None for x in economic_inputs) and not has_economics
    )

    if partial_economics:
        raise ValueError(
            "Partial economic inputs provided. Either supply ALL of "
            "{solar_cost_per_kw_dc, solar_opex_per_kw_dc_year, "
            "discount_rate_pct, project_lifetime_years, degradation_pct, "
            "itc_pct} or NONE of them."
        )

    has_revenue = (
        energy_price_per_kwh is not None
        and energy_cost_escalator_pct is not None
    )
    partial_revenue = (energy_price_per_kwh is not None) != (
        energy_cost_escalator_pct is not None
    )

    if partial_revenue:
        raise ValueError(
            "Partial revenue inputs provided. Either supply both "
            "energy_price_per_kwh and energy_cost_escalator_pct or neither."
        )

    if has_revenue and not has_economics:
        raise ValueError(
            "Revenue inputs require full economic inputs. NPV mode "
            "requires both cost and revenue."
        )

    if has_economics and has_revenue:
        mode = "npv"
    elif has_economics:
        mode = "lcoe"
    else:
        mode = "production"

    # --- Build sweep grid ---
    # Resolve the GCR range that was actually used (for reporting)
    resolved_gcr_range = gcr_range
    if resolved_gcr_range is None:
        resolved_gcr_range = (
            DEFAULT_GCR_RANGE_TRACKER
            if racking == "tracker"
            else DEFAULT_GCR_RANGE_FIXED
        )

    grid = build_sweep_grid(
        racking=racking,
        gcr_range=gcr_range,
        gcr_step=gcr_step,
        dcac_range=dcac_range,
        dcac_step=dcac_step,
    )
    total = len(grid)

    configurator = ModelConfigurator(cec_database=CECDatabase())
    simulator = PySAMSimulator()

    # Convert percent inputs to decimal rates where applicable
    discount_rate = discount_rate_pct / 100.0 if has_economics else None
    degradation_rate = degradation_pct / 100.0 if has_economics else None
    itc_rate = itc_pct / 100.0 if has_economics else None
    energy_cost_escalator = (
        energy_cost_escalator_pct / 100.0 if has_revenue else None
    )

    sweep_results: list[dict] = []
    failed_count = 0

    for i, (gcr, dcac) in enumerate(grid, start=1):
        cap = capacity_from_acreage(
            buildable_acres=buildable_acres,
            gcr=gcr,
            module_area_m2=module_area_m2,
            module_power_w=module_power_w,
            utilization_factor=utilization_factor,
        )
        mw_dc = cap["mw_dc"]
        mw_ac = round(mw_dc / dcac, 3)
        num_modules = cap["num_modules"]

        point = _empty_point(gcr, dcac, mw_dc, mw_ac, num_modules)
        if retain_hourly:
            # Pre-seed so the key is always present for this point (None on
            # failure). Populated from pysam_out on success.
            point["ac_hourly_kw"] = None

        try:
            site_config = base_site_config.model_copy(
                update={
                    "gcr": gcr,
                    "dc_size_mw": mw_dc,
                    "ac_installed_mw": mw_ac,
                    "racking": racking,
                }
            )

            pysam_out = _run_single_pysam(
                site_config=site_config,
                configurator=configurator,
                simulator=simulator,
                apply_clipping_correction=True,
            )

            annual_energy_kwh = pysam_out["annual_energy_kwh"]
            point["annual_energy_kwh"] = annual_energy_kwh
            point["annual_energy_mwh"] = round(annual_energy_kwh / 1000, 3)
            point["capacity_factor_ac"] = pysam_out["capacity_factor_ac"]
            point["clipping_correction_pct"] = pysam_out[
                "clipping_correction_pct"
            ]
            if retain_hourly:
                point["ac_hourly_kw"] = pysam_out["ac_hourly_kw"]

            if has_economics:
                capex_total = mw_dc * 1000 * solar_cost_per_kw_dc
                opex_per_year = mw_dc * 1000 * solar_opex_per_kw_dc_year

                lcoe = compute_lcoe(
                    capex_total=capex_total,
                    annual_energy_kwh=annual_energy_kwh,
                    opex_per_year=opex_per_year,
                    discount_rate=discount_rate,
                    project_lifetime_years=project_lifetime_years,
                    degradation_rate=degradation_rate,
                    itc_rate=itc_rate,
                )

                point["capex_total"] = round(capex_total, 2)
                point["opex_per_year"] = round(opex_per_year, 2)
                point["net_capex"] = lcoe["net_capex"]
                point["lcoe_per_mwh"] = lcoe["lcoe_per_mwh"]
                point["lcoe_per_kwh"] = lcoe["lcoe_per_kwh"]

                if has_revenue:
                    annual_revenue_year1 = (
                        annual_energy_kwh * energy_price_per_kwh
                    )
                    npv_result = compute_solar_npv(
                        capex_total=capex_total,
                        annual_revenue=annual_revenue_year1,
                        opex_per_year=opex_per_year,
                        discount_rate=discount_rate,
                        project_lifetime_years=project_lifetime_years,
                        degradation_rate=degradation_rate,
                        energy_cost_escalator=energy_cost_escalator,
                        itc_rate=itc_rate,
                    )
                    point["annual_revenue_year1"] = round(
                        annual_revenue_year1, 2
                    )
                    point["npv"] = npv_result["npv"]
                    point["simple_payback_years"] = npv_result[
                        "simple_payback_years"
                    ]
                    point["irr"] = npv_result["irr"]

        except Exception as exc:
            point["failed"] = True
            point["error"] = str(exc) if str(exc) else repr(exc)
            failed_count += 1
            logger.warning(
                f"Sweep point failed at gcr={gcr}, dcac={dcac}: {exc}"
            )

        sweep_results.append(point)

        if progress_callback is not None:
            try:
                progress_callback(i, total, point)
            except Exception as cb_exc:
                logger.warning(
                    f"progress_callback raised at index {i}: {cb_exc}"
                )

    valid = [p for p in sweep_results if not p["failed"]]
    if not valid:
        raise RuntimeError("All sweep points failed")

    # --- Winner selection ---
    # max(..., key=...) breaks ties by first-encountered (stable). min()
    # does the same. Verified deterministic.
    winners: dict = {
        "max_production": max(valid, key=lambda p: p["annual_energy_kwh"]),
        "max_yield": max(valid, key=lambda p: p["capacity_factor_ac"]),
    }
    if has_economics:
        winners["lcoe"] = min(valid, key=lambda p: p["lcoe_per_mwh"])
    if has_revenue:
        winners["npv"] = max(valid, key=lambda p: p["npv"])

    # --- Metadata ---
    sweep_metadata: dict = {
        "mode": mode,
        "racking": racking,
        "buildable_acres": buildable_acres,
        "utilization_factor": utilization_factor,
        "module_power_w": module_power_w,
        "module_area_m2": module_area_m2,
        "gcr_range": resolved_gcr_range,
        "dcac_range": dcac_range,
        "total_combinations": total,
        "failed_combinations": failed_count,
        # Economic metadata — populated only in lcoe/npv modes
        "itc_pct": itc_pct if has_economics else None,
        "discount_rate_pct": discount_rate_pct if has_economics else None,
        "project_lifetime_years": (
            project_lifetime_years if has_economics else None
        ),
        "degradation_pct": degradation_pct if has_economics else None,
        "solar_cost_per_kw_dc": (
            solar_cost_per_kw_dc if has_economics else None
        ),
        "solar_opex_per_kw_dc_year": (
            solar_opex_per_kw_dc_year if has_economics else None
        ),
        # Revenue metadata — populated only in npv mode
        "energy_price_per_kwh": energy_price_per_kwh if has_revenue else None,
        "energy_cost_escalator_pct": (
            energy_cost_escalator_pct if has_revenue else None
        ),
        # Whether each sweep_results entry carries the 8760 hourly AC array
        "retain_hourly": retain_hourly,
    }

    return {
        "mode": mode,
        "winners": winners,
        "sweep_results": sweep_results,
        "sweep_metadata": sweep_metadata,
    }
