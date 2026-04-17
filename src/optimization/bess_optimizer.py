"""Joint solar + BESS layout optimization.

Brute-force sweeps every (gcr, dcac) solar configuration (via optimize_solar
in NPV mode) crossed with every (bess_power, bess_duration) BESS configuration.
Winner is the combination that maximizes solar_npv + bess_npv.

Public entry point: optimize_solar_bess().

Dispatch modes:
- "btm" (behind-the-meter): requires rate_schedule + load_profile.
- "ftm" (front-of-the-meter): requires lmp_data.

Charging modes:
- "solar_and_grid" (default): BESS can charge from solar or grid.
- "solar_only": BESS only charges from excess solar (preserves ITC eligibility).

_evaluate_bess_config() runs the full-year BESS dispatch LP (BTM via
run_bess_dispatch or FTM via run_ftm_dispatch) and computes the BESS NPV
via compute_bess_npv / compute_ftm_bess_npv. For BTM, the solar-only bill
is precomputed once per solar config in optimize_solar_bess and forwarded
into every BESS combo for that solar design.
"""

from collections.abc import Callable

from src.bess.dispatch_runner import run_bess_dispatch, run_ftm_dispatch
from src.bess.sizing_optimizer import (
    compute_bess_npv,
    compute_ftm_bess_npv,
    compute_ftm_solar_revenue,
)
from src.config.schema import SiteConfig
from src.optimization.defaults import (
    DEFAULT_BESS_COST_PER_KWH,
    DEFAULT_BESS_OPEX_PER_KW_YEAR,
    DEFAULT_DCAC_RANGE,
    DEFAULT_DCAC_STEP,
    DEFAULT_DEGRADATION_PCT,
    DEFAULT_DISCOUNT_RATE_PCT,
    DEFAULT_ENERGY_COST_ESCALATOR_PCT,
    DEFAULT_GCR_STEP,
    DEFAULT_ITC_PCT,
    DEFAULT_MODULE_AREA_M2,
    DEFAULT_MODULE_POWER_W,
    DEFAULT_PROJECT_LIFETIME_YEARS,
    DEFAULT_SOLAR_COST_PER_KW_DC,
    DEFAULT_SOLAR_OPEX_PER_KW_DC_YEAR,
    DEFAULT_UTILIZATION_FACTOR,
)
from src.optimization.solar_optimizer import optimize_solar
from src.rates.bill_calculator import calculate_bill
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


# Default BESS sweep dimensions
DEFAULT_BESS_POWER_FRACTIONS = [0.15, 0.25, 0.40, 0.55, 0.70]
DEFAULT_BESS_DURATIONS_HR = [2.0, 4.0]


def build_bess_sweep_grid(
    solar_mw_ac: float,
    power_fractions: list[float] | None = None,
    durations_hr: list[float] | None = None,
) -> list[dict]:
    """Build a list of BESS (power_mw, duration_hr) combos sized to solar_mw_ac.

    BESS power is computed as a fraction of the solar AC size and paired with
    each duration. Power is rounded to 3 decimals; capacity = power × duration.

    Args:
        solar_mw_ac: Solar AC capacity in MW used to scale BESS power.
        power_fractions: Fractions of solar_mw_ac to use as BESS power.
            Defaults to [0.15, 0.25, 0.40, 0.55, 0.70].
        durations_hr: BESS storage durations in hours.
            Defaults to [2.0, 4.0].

    Returns:
        List of dicts, one per combo, each with keys:
            - power_mw: BESS rated power (MW).
            - duration_hr: Storage duration (hours).
            - capacity_mwh: Energy capacity = power_mw × duration_hr (MWh).

    Raises:
        ValueError: If solar_mw_ac is not positive or either list is empty.
    """
    if solar_mw_ac <= 0:
        raise ValueError(f"solar_mw_ac must be > 0, got {solar_mw_ac}")

    if power_fractions is None:
        power_fractions = list(DEFAULT_BESS_POWER_FRACTIONS)
    if durations_hr is None:
        durations_hr = list(DEFAULT_BESS_DURATIONS_HR)

    if not power_fractions:
        raise ValueError("power_fractions must be a non-empty list")
    if not durations_hr:
        raise ValueError("durations_hr must be a non-empty list")

    grid: list[dict] = []
    for frac in power_fractions:
        power_mw = round(frac * solar_mw_ac, 3)
        for dur in durations_hr:
            grid.append(
                {
                    "power_mw": power_mw,
                    "duration_hr": dur,
                    "capacity_mwh": round(power_mw * dur, 3),
                }
            )
    return grid


def _evaluate_bess_config(
    site_config: SiteConfig,
    solar_production_kwh: list[float],
    bess_power_mw: float,
    bess_duration_hr: float,
    dispatch_mode: str,
    charging_mode: str,
    rate_schedule: object | None = None,
    load_profile: object | None = None,
    solar_only_bill: object | None = None,
    lmp_data: object | None = None,
    bess_cost_per_kwh: float = DEFAULT_BESS_COST_PER_KWH,
    bess_opex_per_kw_year: float = DEFAULT_BESS_OPEX_PER_KW_YEAR,
    itc_pct: float = DEFAULT_ITC_PCT,
    discount_rate_pct: float = DEFAULT_DISCOUNT_RATE_PCT,
    project_lifetime_years: int = DEFAULT_PROJECT_LIFETIME_YEARS,
    rate_escalation_pct: float = DEFAULT_ENERGY_COST_ESCALATOR_PCT,
) -> dict:
    """Evaluate a single BESS configuration for a given solar design.

    Runs a full-year BESS dispatch LP — run_bess_dispatch for BTM or
    run_ftm_dispatch for FTM — and computes the BESS NPV via
    compute_bess_npv or compute_ftm_bess_npv. The gross BESS capex
    (capacity × $/kWh) is reported in the returned dict; the NPV is
    computed against the ITC-adjusted installed cost (gross × (1-ITC)).
    BTM revenue is bess_incremental_savings from the BESSBillComparison;
    FTM revenue is the battery arbitrage slice of ftm_revenue (total FTM
    revenue minus the solar-only export revenue at LMP prices).

    Args:
        site_config: Base site configuration. A temp copy is made with
            BESS fields applied (model_copy does NOT re-run validators,
            so bess_dispatch_required is not required on the base).
        solar_production_kwh: 8760 hourly solar production in kWh.
        bess_power_mw: BESS rated power in MW.
        bess_duration_hr: BESS storage duration in hours.
        dispatch_mode: "btm" or "ftm".
        charging_mode: "solar_and_grid" or "solar_only".
        rate_schedule: BTM rate schedule (required for BTM).
        load_profile: BTM load profile (required for BTM).
        solar_only_bill: Precomputed solar-only bill (required for BTM).
        lmp_data: FTM LMP price data (required for FTM).
        bess_cost_per_kwh: Installed BESS cost ($/kWh).
        bess_opex_per_kw_year: Annual BESS O&M ($/kW/yr).
        itc_pct: Investment tax credit (%) applied to BESS capex for NPV.
        discount_rate_pct: Discount rate (%).
        project_lifetime_years: NPV analysis horizon (years).
        rate_escalation_pct: Annual rate / revenue escalation (%) used in
            NPV. For BTM this is the energy cost escalator; for FTM it is
            the wholesale revenue escalator.

    Returns:
        Dict with BESS evaluation results:
            - bess_power_mw, bess_duration_hr, bess_capacity_mwh
            - bess_capex: gross capex = capacity_mwh × 1000 × cost_per_kwh.
            - bess_opex_per_year: power_mw × 1000 × opex_per_kw_year.
            - bess_annual_revenue: Year-1 incremental savings (BTM) or
              arbitrage revenue (FTM).
            - bess_npv: NPV computed with ITC-adjusted installed cost.
            - dispatch_mode, charging_mode.
            - failed: bool.
            - error: str or None.
    """
    bess_capacity_mwh = round(bess_power_mw * bess_duration_hr, 3)
    bess_capex_gross = bess_capacity_mwh * 1000 * bess_cost_per_kwh
    bess_opex_per_year = bess_power_mw * 1000 * bess_opex_per_kw_year

    result: dict = {
        "bess_power_mw": bess_power_mw,
        "bess_duration_hr": bess_duration_hr,
        "bess_capacity_mwh": bess_capacity_mwh,
        "bess_capex": round(bess_capex_gross, 2),
        "bess_opex_per_year": round(bess_opex_per_year, 2),
        "bess_annual_revenue": 0.0,
        "bess_npv": 0.0,
        "dispatch_mode": dispatch_mode,
        "charging_mode": charging_mode,
        "failed": False,
        "error": None,
    }

    # ITC-adjusted installed cost used for NPV. Gross capex is still
    # reported above so the UI can display the pre-ITC number.
    itc_rate = itc_pct / 100.0
    bess_installed_cost_net = bess_capex_gross * (1 - itc_rate)
    bess_power_kw = bess_power_mw * 1000.0

    # Build the temp SiteConfig the dispatch runners expect. model_copy
    # skips validators, so we can safely set BESS fields on a base site
    # where bess_dispatch_required=False.
    temp_site_config = site_config.model_copy(
        update={
            "bess_power_mw": bess_power_mw,
            "bess_duration_hr": bess_duration_hr,
            "bess_installed_cost_per_kwh": bess_cost_per_kwh,
            "bess_solar_only_charging": charging_mode == "solar_only",
            "bess_grid_only_charging": False,
        }
    )

    try:
        if dispatch_mode == "btm":
            if solar_only_bill is None:
                raise ValueError(
                    "BTM dispatch requires a precomputed solar_only_bill."
                )

            dispatch_out = run_bess_dispatch(
                temp_site_config,
                solar_production_kwh,
                rate_schedule,
                load_profile,
                solar_only_bill,
            )
            # With solar_only_bill provided, run_bess_dispatch returns a
            # (DispatchResult, BESSBillComparison) tuple.
            dispatch_result, bill_comparison = dispatch_out

            bess_annual_revenue = bill_comparison.bess_incremental_savings
            annual_degradation_pct = (
                dispatch_result.metrics.estimated_annual_degradation_pct
            )

            bess_npv = compute_bess_npv(
                bess_incremental_savings=bess_annual_revenue,
                bess_installed_cost=bess_installed_cost_net,
                bess_annual_degradation_pct=annual_degradation_pct,
                rate_escalation_pct=rate_escalation_pct,
                bess_opex_per_kw_year=bess_opex_per_kw_year,
                bess_power_kw=bess_power_kw,
                discount_rate_pct=discount_rate_pct,
                project_lifetime_years=project_lifetime_years,
            )
        else:  # ftm
            dispatch_result = run_ftm_dispatch(
                temp_site_config,
                solar_production_kwh,
                lmp_data,
            )

            # Split total FTM revenue into solar-export vs battery-arbitrage
            # so the BESS NPV reflects only the battery's value-add. The
            # solar export revenue is already folded into solar_npv upstream.
            annual_solar_revenue = compute_ftm_solar_revenue(
                dispatch_result, lmp_data.prices
            )
            bess_arbitrage_revenue = (
                (dispatch_result.ftm_revenue or 0.0) - annual_solar_revenue
            )
            bess_annual_revenue = bess_arbitrage_revenue
            annual_degradation_pct = (
                dispatch_result.metrics.estimated_annual_degradation_pct
            )

            bess_npv = compute_ftm_bess_npv(
                bess_arbitrage_revenue=bess_arbitrage_revenue,
                bess_installed_cost=bess_installed_cost_net,
                bess_annual_degradation_pct=annual_degradation_pct,
                revenue_escalation_pct=rate_escalation_pct,
                bess_opex_per_kw_year=bess_opex_per_kw_year,
                bess_power_kw=bess_power_kw,
                ancillary_revenue_per_kw_year=(
                    site_config.ancillary_revenue_per_kw_year
                ),
                discount_rate_pct=discount_rate_pct,
                project_lifetime_years=project_lifetime_years,
            )
    except Exception as exc:
        result["failed"] = True
        result["error"] = str(exc) if str(exc) else repr(exc)
        return result

    result["bess_annual_revenue"] = round(bess_annual_revenue, 2)
    result["bess_npv"] = round(bess_npv, 2)
    return result


def _validate_dispatch_inputs(
    dispatch_mode: str,
    charging_mode: str,
    rate_schedule: object | None,
    load_profile: object | None,
    lmp_data: object | None,
) -> None:
    """Validate dispatch_mode / charging_mode and required input bundles.

    Raises:
        ValueError: If dispatch_mode or charging_mode is invalid, if required
            inputs are missing for the selected mode, or if BTM and FTM input
            bundles are both provided.
    """
    if dispatch_mode not in ("btm", "ftm"):
        raise ValueError(
            f"dispatch_mode must be 'btm' or 'ftm', got {dispatch_mode!r}"
        )
    if charging_mode not in ("solar_and_grid", "solar_only"):
        raise ValueError(
            "charging_mode must be 'solar_and_grid' or 'solar_only', "
            f"got {charging_mode!r}"
        )

    btm_inputs_provided = (
        rate_schedule is not None or load_profile is not None
    )
    ftm_inputs_provided = lmp_data is not None

    if btm_inputs_provided and ftm_inputs_provided:
        raise ValueError(
            "Cannot provide both BTM (rate_schedule/load_profile) and FTM "
            "(lmp_data) inputs. Choose one dispatch mode."
        )

    if dispatch_mode == "btm":
        if rate_schedule is None or load_profile is None:
            raise ValueError(
                "BTM dispatch requires both rate_schedule and load_profile."
            )
    else:  # ftm
        if lmp_data is None:
            raise ValueError("FTM dispatch requires lmp_data.")


def optimize_solar_bess(
    base_site_config: SiteConfig,
    buildable_acres: float,
    dispatch_mode: str,
    # Equipment
    module_power_w: float = DEFAULT_MODULE_POWER_W,
    module_area_m2: float = DEFAULT_MODULE_AREA_M2,
    utilization_factor: float = DEFAULT_UTILIZATION_FACTOR,
    racking: str = "tracker",
    # Solar sweep ranges
    gcr_range: tuple[float, float] | None = None,
    gcr_step: float = DEFAULT_GCR_STEP,
    dcac_range: tuple[float, float] = DEFAULT_DCAC_RANGE,
    dcac_step: float = DEFAULT_DCAC_STEP,
    # BESS configuration
    bess_power_fractions: list[float] | None = None,
    bess_durations_hr: list[float] | None = None,
    charging_mode: str = "solar_and_grid",
    # Economic inputs — all required for NPV mode
    solar_cost_per_kw_dc: float = DEFAULT_SOLAR_COST_PER_KW_DC,
    solar_opex_per_kw_dc_year: float = DEFAULT_SOLAR_OPEX_PER_KW_DC_YEAR,
    discount_rate_pct: float = DEFAULT_DISCOUNT_RATE_PCT,
    project_lifetime_years: int = DEFAULT_PROJECT_LIFETIME_YEARS,
    degradation_pct: float = DEFAULT_DEGRADATION_PCT,
    itc_pct: float = DEFAULT_ITC_PCT,
    energy_price_per_kwh: float | None = None,
    energy_cost_escalator_pct: float | None = None,
    bess_cost_per_kwh: float = DEFAULT_BESS_COST_PER_KWH,
    bess_opex_per_kw_year: float = DEFAULT_BESS_OPEX_PER_KW_YEAR,
    # BTM-specific inputs
    rate_schedule: object | None = None,
    load_profile: object | None = None,
    # FTM-specific inputs
    lmp_data: object | None = None,
    # Callbacks
    progress_callback: Callable[[int, int, str, dict], None] | None = None,
) -> dict:
    """Run a joint solar + BESS layout optimization sweep.

    Invokes optimize_solar() in NPV mode to build the solar sweep, then for
    each valid solar point sweeps every BESS power/duration combination and
    sums solar_npv + bess_npv to pick the combined-NPV winner.

    Args:
        base_site_config: Site configuration template.
        buildable_acres: Buildable land (acres).
        dispatch_mode: "btm" or "ftm" (required).
        module_power_w: Module STC power (W).
        module_area_m2: Module area (m²).
        utilization_factor: Fraction of acreage used for modules.
        racking: "tracker" or "fixed".
        gcr_range: Optional (min, max) GCR range.
        gcr_step: GCR step size.
        dcac_range: (min, max) DC/AC range.
        dcac_step: DC/AC step size.
        bess_power_fractions: BESS power as fractions of solar_mw_ac.
        bess_durations_hr: BESS durations in hours.
        charging_mode: "solar_and_grid" or "solar_only".
        solar_cost_per_kw_dc: Solar capex ($/kW-DC).
        solar_opex_per_kw_dc_year: Solar O&M ($/kW-DC/yr).
        discount_rate_pct: Discount rate (%).
        project_lifetime_years: Analysis period (years).
        degradation_pct: Annual degradation (%).
        itc_pct: Investment tax credit (%).
        energy_price_per_kwh: Flat energy price ($/kWh) for solar NPV.
        energy_cost_escalator_pct: Annual price escalation (%).
        bess_cost_per_kwh: BESS capex ($/kWh).
        bess_opex_per_kw_year: BESS O&M ($/kW/yr).
        rate_schedule: BTM rate schedule (required when dispatch_mode="btm").
        load_profile: BTM load profile (required when dispatch_mode="btm").
        lmp_data: FTM LMP price data (required when dispatch_mode="ftm").
        progress_callback: Called as (index, total, stage, result_dict).
            stage is "solar_sweep" (forwarded via a local wrapper) or
            "bess_sweep".

    Returns:
        Dict with keys:
            - mode: "solar_bess".
            - winners: dict with max_production, max_yield, solar_only_npv,
              and solar_bess_npv (best combined config).
            - bess_adds_value: True iff best combined NPV beats solar-only NPV.
            - bess_winner_detail: {solar, bess, combined} with full fields.
            - solar_sweep_results: sweep_results from optimize_solar().
            - bess_sweep_results: Flat list of per-combo result dicts.
            - sweep_metadata: Combined solar + BESS metadata.

    Raises:
        ValueError: On input validation failure or from optimize_solar().
        RuntimeError: If the solar sweep yields no valid points.
    """
    # --- Input validation ---
    _validate_dispatch_inputs(
        dispatch_mode=dispatch_mode,
        charging_mode=charging_mode,
        rate_schedule=rate_schedule,
        load_profile=load_profile,
        lmp_data=lmp_data,
    )

    # --- Solar sweep (NPV mode) ---
    # optimize_solar enforces that all economic + revenue inputs are present
    # for NPV mode. We forward a solar_sweep-stage wrapper so the caller sees
    # unified progress events.
    def _solar_progress(i: int, total: int, point: dict) -> None:
        if progress_callback is not None:
            try:
                progress_callback(i, total, "solar_sweep", point)
            except Exception as cb_exc:
                logger.warning(
                    "progress_callback raised during solar_sweep at "
                    "index %d: %s",
                    i,
                    cb_exc,
                )

    solar_result = optimize_solar(
        base_site_config=base_site_config,
        buildable_acres=buildable_acres,
        module_power_w=module_power_w,
        module_area_m2=module_area_m2,
        utilization_factor=utilization_factor,
        racking=racking,
        gcr_range=gcr_range,
        gcr_step=gcr_step,
        dcac_range=dcac_range,
        dcac_step=dcac_step,
        solar_cost_per_kw_dc=solar_cost_per_kw_dc,
        solar_opex_per_kw_dc_year=solar_opex_per_kw_dc_year,
        discount_rate_pct=discount_rate_pct,
        project_lifetime_years=project_lifetime_years,
        degradation_pct=degradation_pct,
        itc_pct=itc_pct,
        energy_price_per_kwh=energy_price_per_kwh,
        energy_cost_escalator_pct=energy_cost_escalator_pct,
        progress_callback=_solar_progress,
        # Retain the 8760 hourly AC array for every sweep point so that
        # _evaluate_bess_config can feed it into the BESS dispatch LP.
        retain_hourly=True,
    )

    if solar_result["mode"] != "npv":
        # optimize_solar would have raised before getting here if NPV inputs
        # were missing, but be explicit: solar+BESS requires NPV mode.
        raise ValueError(
            "optimize_solar_bess requires NPV mode in the solar sweep "
            "(energy_price_per_kwh + energy_cost_escalator_pct). Got mode="
            f"{solar_result['mode']!r}."
        )

    solar_sweep_results: list[dict] = solar_result["sweep_results"]
    valid_solar = [p for p in solar_sweep_results if not p["failed"]]

    # --- Compute total BESS combo count for progress reporting ---
    # Resolve effective BESS dimensions so total_bess_combinations is stable
    # (build_bess_sweep_grid will apply these same defaults per solar config).
    effective_power_fractions = (
        list(bess_power_fractions)
        if bess_power_fractions is not None
        else list(DEFAULT_BESS_POWER_FRACTIONS)
    )
    effective_durations_hr = (
        list(bess_durations_hr)
        if bess_durations_hr is not None
        else list(DEFAULT_BESS_DURATIONS_HR)
    )
    n_bess_per_solar = (
        len(effective_power_fractions) * len(effective_durations_hr)
    )
    total_bess_combinations = len(valid_solar) * n_bess_per_solar

    # --- Precompute solar-only bills (BTM only, once per solar config) ---
    # For BTM, each solar config has a unique 8760 production series and so
    # a unique "solar but no BESS" bill. Computing this once per config and
    # reusing across all BESS combos saves N_bess bill calculations per solar
    # design. FTM mode has no bill; no precomputation needed.
    solar_only_bills: dict[tuple[float, float], object | None] = {}
    if dispatch_mode == "btm":
        for solar_point in valid_solar:
            key = (solar_point["gcr"], solar_point["dcac_ratio"])
            solar_production_kwh = solar_point.get("ac_hourly_kw") or []
            try:
                bill = calculate_bill(
                    load_kwh=load_profile.hourly_kwh,
                    production_kwh=solar_production_kwh,
                    rate=rate_schedule,
                )
            except Exception as bill_exc:
                logger.warning(
                    "solar_only_bill precomputation failed at "
                    "gcr=%.2f dcac=%.2f: %s",
                    solar_point["gcr"], solar_point["dcac_ratio"], bill_exc,
                )
                bill = None
            solar_only_bills[key] = bill

    # --- BESS sweep: every valid solar × every BESS combo ---
    bess_sweep_results: list[dict] = []
    failed_bess_count = 0
    bess_index = 0

    for solar_point in valid_solar:
        point_mw_ac = solar_point["mw_ac"]
        point_gcr = solar_point["gcr"]
        point_dcac = solar_point["dcac_ratio"]

        try:
            bess_grid = build_bess_sweep_grid(
                solar_mw_ac=point_mw_ac,
                power_fractions=effective_power_fractions,
                durations_hr=effective_durations_hr,
            )
        except Exception as grid_exc:
            # Should not happen given validated inputs, but fail gracefully.
            logger.warning(
                "build_bess_sweep_grid failed at gcr=%.2f dcac=%.2f: %s",
                point_gcr, point_dcac, grid_exc,
            )
            continue

        # Pull the precomputed BTM solar-only bill for this design (None for FTM).
        solar_only_bill_for_point = (
            solar_only_bills.get((point_gcr, point_dcac))
            if dispatch_mode == "btm"
            else None
        )

        for bess_combo in bess_grid:
            bess_index += 1
            result: dict = {
                "solar_gcr": point_gcr,
                "solar_dcac": point_dcac,
                "solar_mw_ac": point_mw_ac,
                "bess_power_mw": bess_combo["power_mw"],
                "bess_duration_hr": bess_combo["duration_hr"],
                "bess_capacity_mwh": bess_combo["capacity_mwh"],
                "solar_npv": None,
                "bess_npv": None,
                "combined_npv": None,
                "bess_capex": None,
                "bess_opex_per_year": None,
                "bess_annual_revenue": None,
                "dispatch_mode": dispatch_mode,
                "charging_mode": charging_mode,
                "failed": False,
                "error": None,
            }

            # Hourly AC series from the solar sweep for this design. With
            # retain_hourly=True (set by optimize_solar_bess), the 8760 list
            # is present and is forwarded into run_bess_dispatch /
            # run_ftm_dispatch.
            solar_production_kwh = solar_point.get("ac_hourly_kw") or []

            try:
                eval_out = _evaluate_bess_config(
                    site_config=base_site_config,
                    solar_production_kwh=solar_production_kwh,
                    bess_power_mw=bess_combo["power_mw"],
                    bess_duration_hr=bess_combo["duration_hr"],
                    dispatch_mode=dispatch_mode,
                    charging_mode=charging_mode,
                    rate_schedule=rate_schedule,
                    load_profile=load_profile,
                    solar_only_bill=solar_only_bill_for_point,
                    lmp_data=lmp_data,
                    bess_cost_per_kwh=bess_cost_per_kwh,
                    bess_opex_per_kw_year=bess_opex_per_kw_year,
                    itc_pct=itc_pct,
                    discount_rate_pct=discount_rate_pct,
                    project_lifetime_years=project_lifetime_years,
                    rate_escalation_pct=(
                        energy_cost_escalator_pct
                        if energy_cost_escalator_pct is not None
                        else DEFAULT_ENERGY_COST_ESCALATOR_PCT
                    ),
                )
            except Exception as exc:
                failed_bess_count += 1
                result["failed"] = True
                result["error"] = str(exc) if str(exc) else repr(exc)
                logger.warning(
                    "BESS eval failed at gcr=%.2f dcac=%.2f "
                    "power=%.3fMW dur=%.1fhr: %s",
                    point_gcr,
                    point_dcac,
                    bess_combo["power_mw"],
                    bess_combo["duration_hr"],
                    exc,
                )
            else:
                if eval_out.get("failed"):
                    failed_bess_count += 1
                    result["failed"] = True
                    result["error"] = eval_out.get("error")
                else:
                    solar_npv = solar_point["npv"]
                    bess_npv = eval_out["bess_npv"]
                    result["solar_npv"] = solar_npv
                    result["bess_npv"] = bess_npv
                    result["combined_npv"] = solar_npv + bess_npv
                    result["bess_capex"] = eval_out["bess_capex"]
                    result["bess_opex_per_year"] = eval_out[
                        "bess_opex_per_year"
                    ]
                    result["bess_annual_revenue"] = eval_out[
                        "bess_annual_revenue"
                    ]

            bess_sweep_results.append(result)

            if progress_callback is not None:
                try:
                    progress_callback(
                        bess_index,
                        total_bess_combinations,
                        "bess_sweep",
                        result,
                    )
                except Exception as cb_exc:
                    logger.warning(
                        "progress_callback raised during bess_sweep at "
                        "index %d: %s",
                        bess_index,
                        cb_exc,
                    )

    # --- Winner selection ---
    # Always carry forward the solar-only winners.
    solar_winners = solar_result["winners"]
    winners: dict = {
        "max_production": solar_winners["max_production"],
        "max_yield": solar_winners["max_yield"],
        "solar_only_npv": solar_winners["npv"],
    }

    valid_bess = [r for r in bess_sweep_results if not r["failed"]]

    if not valid_bess:
        # No BESS combos succeeded — fall back to solar-only winner.
        logger.warning(
            "All %d BESS evaluations failed or skipped — "
            "returning solar-only result.",
            len(bess_sweep_results),
        )
        winners["solar_bess_npv"] = None
        bess_winner_detail: dict | None = None
        bess_adds_value = False
    else:
        best = max(valid_bess, key=lambda r: r["combined_npv"])
        winners["solar_bess_npv"] = best

        # Locate the matching solar_point for the winner (by gcr + dcac).
        matching_solar = next(
            p
            for p in valid_solar
            if p["gcr"] == best["solar_gcr"]
            and p["dcac_ratio"] == best["solar_dcac"]
        )

        total_capex = (
            (matching_solar.get("capex_total") or 0.0) + best["bess_capex"]
        )
        total_opex = (
            (matching_solar.get("opex_per_year") or 0.0)
            + best["bess_opex_per_year"]
        )

        bess_winner_detail = {
            "solar": matching_solar,
            "bess": {
                "power_mw": best["bess_power_mw"],
                "duration_hr": best["bess_duration_hr"],
                "capacity_mwh": best["bess_capacity_mwh"],
                "capex": best["bess_capex"],
                "opex_per_year": best["bess_opex_per_year"],
                "annual_revenue": best["bess_annual_revenue"],
                "npv": best["bess_npv"],
                "dispatch_mode": best["dispatch_mode"],
                "charging_mode": best["charging_mode"],
            },
            "combined": {
                "npv": best["combined_npv"],
                "solar_npv": best["solar_npv"],
                "bess_npv": best["bess_npv"],
                "total_capex": round(total_capex, 2),
                "total_opex": round(total_opex, 2),
            },
        }

        solar_only_best_npv = solar_winners["npv"]["npv"]
        bess_adds_value = best["combined_npv"] > solar_only_best_npv

    # --- Metadata ---
    sweep_metadata: dict = {
        # Start from solar metadata and layer on BESS fields.
        **solar_result["sweep_metadata"],
        "mode": "solar_bess",
        "dispatch_mode": dispatch_mode,
        "charging_mode": charging_mode,
        "total_solar_combinations": (
            solar_result["sweep_metadata"]["total_combinations"]
        ),
        "total_bess_combinations": total_bess_combinations,
        "failed_bess_combinations": failed_bess_count,
        "bess_power_fractions": effective_power_fractions,
        "bess_durations_hr": effective_durations_hr,
        "bess_cost_per_kwh": bess_cost_per_kwh,
        "bess_opex_per_kw_year": bess_opex_per_kw_year,
    }

    return {
        "mode": "solar_bess",
        "winners": winners,
        "bess_adds_value": bess_adds_value,
        "bess_winner_detail": bess_winner_detail,
        "solar_sweep_results": solar_sweep_results,
        "bess_sweep_results": bess_sweep_results,
        "sweep_metadata": sweep_metadata,
    }
