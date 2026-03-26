"""BESS sizing optimization: NPV calculation and project economics."""

from src.bess.dispatch_runner import run_bess_dispatch, run_ftm_dispatch
from src.bess.models import DispatchResult, ProjectEconomics, SizingResult, SweepComboResult
from src.config.schema import SiteConfig
from src.lmp.models import LMPData
from src.rates.models import BillResult, LoadProfile, RateSchedule
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Sweep grid step sizes
POWER_STEP_MW = 0.1  # default for ranges <= 5 MW
DURATION_STEP_HR = 0.5


def _adaptive_power_step(power_range_mw: float) -> float:
    """Choose power step size based on the span of the power range.

    Args:
        power_range_mw: Difference between power max and power min (MW).

    Returns:
        Step size in MW: 0.1 for <= 5 MW, 0.5 for <= 20 MW, 1.0 otherwise.
    """
    if power_range_mw <= 5.0:
        return 0.1
    if power_range_mw <= 20.0:
        return 0.5
    return 1.0


def _float_range_inclusive(start: float, stop: float, step: float) -> list[float]:
    """Generate a list of floats from start to stop (inclusive), with rounding.

    Args:
        start: First value in the range.
        stop: Last value in the range (inclusive).
        step: Step size between values.

    Returns:
        List of rounded float values from start to stop.
    """
    if start == stop:
        return [round(start, 2)]
    n_steps = round((stop - start) / step)
    return [round(start + i * step, 2) for i in range(n_steps + 1)]


def build_sweep_grid(site_config: SiteConfig) -> list[tuple[float, float]]:
    """Generate list of (power_mw, duration_hr) combos from config.

    Power range uses explicit bess_power_min/max_mw if provided, otherwise
    defaults to 10%-30% of ac_installed_mw (solar+BESS case). Duration range
    always comes from bess_duration_min/max_hr.

    Args:
        site_config: Site configuration with BESS sizing fields.

    Returns:
        List of (power_mw, duration_hr) tuples for the sizing sweep.
    """
    # Power range
    if (
        site_config.bess_power_min_mw is not None
        and site_config.bess_power_max_mw is not None
    ):
        power_min = site_config.bess_power_min_mw
        power_max = site_config.bess_power_max_mw
    else:
        power_min = round(site_config.ac_installed_mw * 0.10, 1)
        power_max = round(site_config.ac_installed_mw * 0.30, 1)

    # Duration range
    duration_min = site_config.bess_duration_min_hr
    duration_max = site_config.bess_duration_max_hr

    power_step = _adaptive_power_step(power_max - power_min)
    power_values = _float_range_inclusive(power_min, power_max, power_step)
    duration_values = _float_range_inclusive(duration_min, duration_max, DURATION_STEP_HR)

    grid = [
        (p, d) for p in power_values for d in duration_values
    ]

    logger.info(
        "Sweep grid: %d power × %d duration = %d combos (%.1f–%.1f MW, %.1f–%.1f hr)",
        len(power_values),
        len(duration_values),
        len(grid),
        power_min,
        power_max,
        duration_min,
        duration_max,
    )

    return grid


def compute_bess_npv(
    bess_incremental_savings: float,
    bess_installed_cost: float,
    bess_annual_degradation_pct: float,
    rate_escalation_pct: float,
    bess_opex_per_kw_year: float,
    bess_power_kw: float,
    discount_rate_pct: float,
    project_lifetime_years: int,
) -> float:
    """Compute net present value of a BESS investment.

    Discounts annual net cash flows (savings minus O&M) over the project
    lifetime, accounting for rate escalation and battery degradation.

    Args:
        bess_incremental_savings: Year 1 incremental savings from BESS ($/yr).
        bess_installed_cost: Total upfront BESS cost ($).
        bess_annual_degradation_pct: Annual capacity degradation (%).
        rate_escalation_pct: Annual electricity rate escalation (%).
        bess_opex_per_kw_year: BESS O&M cost per kW per year ($/kW/yr).
        bess_power_kw: BESS rated power in kW.
        discount_rate_pct: Discount rate for NPV calculation (%).
        project_lifetime_years: Analysis period in years.

    Returns:
        Net present value of the BESS investment ($).
    """
    bess_opex = bess_opex_per_kw_year * bess_power_kw
    discount_rate = discount_rate_pct / 100
    escalation_rate = rate_escalation_pct / 100
    degradation_rate = bess_annual_degradation_pct / 100

    npv = -bess_installed_cost
    for n in range(1, project_lifetime_years + 1):
        savings_n = (
            bess_incremental_savings
            * (1 + escalation_rate) ** (n - 1)
            * (1 - degradation_rate) ** (n - 1)
        )
        net_cash_flow_n = savings_n - bess_opex
        npv += net_cash_flow_n / (1 + discount_rate) ** n

    return npv


def compute_project_economics(
    bess_npv: float,
    bess_installed_cost: float,
    bess_incremental_savings: float,
    bess_power_mw: float,
    bess_duration_hr: float,
    bess_opex_per_kw_year: float,
    site_config: SiteConfig,
    solar_only_bill: BillResult,
    bill_without_solar: BillResult,
    annual_production_kwh: float,
) -> ProjectEconomics:
    """Compute full project economics for a solar+BESS configuration.

    Calculates solar NPV, combines with BESS NPV, and computes LCOE
    and other financial metrics. For grid-only BESS, solar components
    are zeroed out.

    Args:
        bess_npv: Pre-computed BESS NPV ($).
        bess_installed_cost: Total BESS installed cost ($).
        bess_incremental_savings: Year 1 BESS incremental savings ($/yr).
        bess_power_mw: BESS rated power in MW.
        bess_duration_hr: BESS storage duration in hours.
        bess_opex_per_kw_year: BESS O&M cost per kW per year ($/kW/yr).
        site_config: Site configuration with solar cost and financial fields.
        solar_only_bill: Annual bill with solar but no BESS.
        bill_without_solar: Annual bill with no solar and no BESS.
        annual_production_kwh: Year 1 annual solar production in kWh.

    Returns:
        ProjectEconomics with all fields populated.
    """
    discount_rate = site_config.discount_rate_pct / 100
    escalation_rate = site_config.rate_escalation_pct / 100
    degradation_rate = site_config.degradation_percent / 100
    lifetime = site_config.project_lifetime_years

    bess_capacity_kwh = bess_power_mw * 1000 * bess_duration_hr
    bess_opex = bess_opex_per_kw_year * bess_power_mw * 1000

    if site_config.bess_grid_only_charging:
        # Grid-only: no solar component
        solar_cost = 0.0
        solar_opex = 0.0
        solar_savings = 0.0
        solar_npv = 0.0
        total_project_npv = bess_npv
        system_lcoe_per_kwh = None
        annual_production_mwh = annual_production_kwh / 1000
        lifetime_generation_mwh = 0.0
    else:
        solar_cost = (
            site_config.dc_size_mw * 1000 * (site_config.solar_cost_per_kw_dc or 0.0)
            + site_config.ac_installed_mw * 1000 * (site_config.solar_cost_per_kw_ac or 0.0)
        )
        solar_opex = site_config.solar_opex_per_kw_dc_year * site_config.dc_size_mw * 1000
        solar_savings = bill_without_solar.annual_total - solar_only_bill.annual_total

        # Solar NPV: same year-by-year pattern as BESS NPV
        solar_npv = -solar_cost
        for n in range(1, lifetime + 1):
            savings_n = (
                solar_savings
                * (1 + escalation_rate) ** (n - 1)
                * (1 - degradation_rate) ** (n - 1)
            )
            net_cash_flow_n = savings_n - solar_opex
            solar_npv += net_cash_flow_n / (1 + discount_rate) ** n

        total_project_npv = solar_npv + bess_npv
        annual_production_mwh = annual_production_kwh / 1000

        # Lifetime generation with degradation
        lifetime_generation_mwh = (
            annual_production_kwh
            * sum((1 - degradation_rate) ** (n - 1) for n in range(1, lifetime + 1))
            / 1000
        )

        # LCOE
        lifetime_opex = (solar_opex + bess_opex) * lifetime
        total_cost_for_lcoe = solar_cost + bess_installed_cost + lifetime_opex
        if lifetime_generation_mwh > 0:
            system_lcoe_per_kwh = total_cost_for_lcoe / (lifetime_generation_mwh * 1000)
        else:
            system_lcoe_per_kwh = None

    total_installed_cost = solar_cost + bess_installed_cost
    total_annual_savings = solar_savings + bess_incremental_savings

    return ProjectEconomics(
        total_project_npv=total_project_npv,
        solar_npv=solar_npv,
        bess_npv=bess_npv,
        system_lcoe_per_kwh=system_lcoe_per_kwh,
        total_installed_cost=total_installed_cost,
        solar_cost=solar_cost,
        bess_cost=bess_installed_cost,
        total_annual_savings=total_annual_savings,
        bess_incremental_savings=bess_incremental_savings,
        annual_production_mwh=annual_production_mwh,
        lifetime_generation_mwh=lifetime_generation_mwh,
        optimal_power_mw=bess_power_mw,
        optimal_duration_hr=bess_duration_hr,
        optimal_capacity_kwh=bess_capacity_kwh,
        combos_evaluated=0,
    )


def compute_ftm_bess_npv(
    bess_arbitrage_revenue: float,
    bess_installed_cost: float,
    bess_annual_degradation_pct: float,
    revenue_escalation_pct: float,
    bess_opex_per_kw_year: float,
    bess_power_kw: float,
    ancillary_revenue_per_kw_year: float,
    discount_rate_pct: float,
    project_lifetime_years: int,
) -> float:
    """Compute net present value of an FTM BESS investment.

    Discounts annual net cash flows (arbitrage revenue minus O&M plus
    ancillary revenue) over the project lifetime, accounting for revenue
    escalation and battery degradation.

    Args:
        bess_arbitrage_revenue: Year 1 BESS arbitrage revenue ($).
        bess_installed_cost: Total upfront BESS cost ($).
        bess_annual_degradation_pct: Annual capacity degradation (%).
        revenue_escalation_pct: Annual revenue escalation (%).
        bess_opex_per_kw_year: BESS O&M cost per kW per year ($/kW/yr).
        bess_power_kw: BESS rated power in kW.
        ancillary_revenue_per_kw_year: Ancillary services revenue ($/kW/yr).
            Flat annual amount, no escalation (fixed contract assumption).
        discount_rate_pct: Discount rate for NPV calculation (%).
        project_lifetime_years: Analysis period in years.

    Returns:
        Net present value of the FTM BESS investment ($).
    """
    bess_opex = bess_opex_per_kw_year * bess_power_kw
    ancillary = ancillary_revenue_per_kw_year * bess_power_kw
    discount_rate = discount_rate_pct / 100
    escalation_rate = revenue_escalation_pct / 100
    degradation_rate = bess_annual_degradation_pct / 100

    npv = -bess_installed_cost
    for n in range(1, project_lifetime_years + 1):
        revenue_n = (
            bess_arbitrage_revenue
            * (1 + escalation_rate) ** (n - 1)
            * (1 - degradation_rate) ** (n - 1)
        )
        net_opex = bess_opex - ancillary
        cash_flow_n = revenue_n - net_opex
        npv += cash_flow_n / (1 + discount_rate) ** n

    return npv


def compute_ftm_project_economics(
    bess_npv: float,
    bess_installed_cost: float,
    bess_arbitrage_revenue: float,
    bess_power_mw: float,
    bess_duration_hr: float,
    bess_opex_per_kw_year: float,
    ancillary_revenue_per_kw_year: float,
    site_config: SiteConfig,
    annual_solar_revenue: float,
    annual_production_kwh: float,
) -> ProjectEconomics:
    """Compute full project economics for an FTM solar+BESS configuration.

    Calculates solar NPV from LMP revenue, combines with BESS NPV, and
    computes LCOE and other financial metrics for wholesale dispatch.

    Args:
        bess_npv: Pre-computed BESS NPV ($).
        bess_installed_cost: Total BESS installed cost ($).
        bess_arbitrage_revenue: Year 1 BESS arbitrage revenue ($).
        bess_power_mw: BESS rated power in MW.
        bess_duration_hr: BESS storage duration in hours.
        bess_opex_per_kw_year: BESS O&M cost per kW per year ($/kW/yr).
        ancillary_revenue_per_kw_year: Ancillary services revenue ($/kW/yr).
        site_config: Site configuration with solar cost and financial fields.
        annual_solar_revenue: Year 1 solar revenue at LMP prices ($).
        annual_production_kwh: Year 1 annual solar production in kWh.

    Returns:
        ProjectEconomics with all fields populated including FTM-specific fields.
    """
    discount_rate = site_config.discount_rate_pct / 100
    escalation_rate = site_config.rate_escalation_pct / 100
    degradation_rate = site_config.degradation_percent / 100
    lifetime = site_config.project_lifetime_years

    bess_capacity_kwh = bess_power_mw * 1000 * bess_duration_hr
    bess_opex = bess_opex_per_kw_year * bess_power_mw * 1000

    # Detect whether solar cost data was provided
    solar_cost_provided = (
        (site_config.solar_cost_per_kw_dc is not None and site_config.solar_cost_per_kw_dc > 0)
        or (site_config.solar_cost_per_kw_ac is not None and site_config.solar_cost_per_kw_ac > 0)
    )

    # Solar cost and opex
    solar_cost = (
        site_config.dc_size_mw * 1000 * (site_config.solar_cost_per_kw_dc or 0.0)
        + site_config.ac_installed_mw * 1000 * (site_config.solar_cost_per_kw_ac or 0.0)
    )
    solar_opex = site_config.solar_opex_per_kw_dc_year * site_config.dc_size_mw * 1000

    annual_production_mwh = annual_production_kwh / 1000

    # Lifetime generation with degradation
    lifetime_generation_mwh = (
        annual_production_kwh
        * sum((1 - degradation_rate) ** (n - 1) for n in range(1, lifetime + 1))
        / 1000
    )

    if solar_cost_provided:
        # Solar NPV: revenue stream from LMP sales
        solar_npv: float | None = -solar_cost
        for n in range(1, lifetime + 1):
            revenue_n = (
                annual_solar_revenue
                * (1 + escalation_rate) ** (n - 1)
                * (1 - degradation_rate) ** (n - 1)
            )
            net_cash_flow_n = revenue_n - solar_opex
            solar_npv += net_cash_flow_n / (1 + discount_rate) ** n

        total_project_npv = solar_npv + bess_npv

        # LCOE
        lifetime_opex = (solar_opex + bess_opex) * lifetime
        total_cost_for_lcoe = solar_cost + bess_installed_cost + lifetime_opex
        if lifetime_generation_mwh > 0:
            system_lcoe_per_kwh = total_cost_for_lcoe / (lifetime_generation_mwh * 1000)
        else:
            system_lcoe_per_kwh = None
    else:
        # No solar cost data — cannot compute meaningful LCOE or solar NPV
        solar_npv = None
        total_project_npv = bess_npv
        system_lcoe_per_kwh = None

    total_installed_cost = solar_cost + bess_installed_cost
    ancillary = ancillary_revenue_per_kw_year * bess_power_mw * 1000
    annual_gross_revenue = annual_solar_revenue + bess_arbitrage_revenue + ancillary

    return ProjectEconomics(
        total_project_npv=total_project_npv,
        solar_npv=solar_npv,
        bess_npv=bess_npv,
        system_lcoe_per_kwh=system_lcoe_per_kwh,
        total_installed_cost=total_installed_cost,
        solar_cost=solar_cost,
        bess_cost=bess_installed_cost,
        total_annual_savings=annual_gross_revenue,
        bess_incremental_savings=bess_arbitrage_revenue,
        annual_production_mwh=annual_production_mwh,
        lifetime_generation_mwh=lifetime_generation_mwh,
        optimal_power_mw=bess_power_mw,
        optimal_duration_hr=bess_duration_hr,
        optimal_capacity_kwh=bess_capacity_kwh,
        combos_evaluated=0,
        solar_revenue=annual_solar_revenue,
        bess_arbitrage_revenue=bess_arbitrage_revenue,
        ancillary_revenue=ancillary,
        gross_revenue=annual_gross_revenue,
    )


def compute_ftm_solar_revenue(
    dispatch_result: DispatchResult,
    lmp_prices: list[float],
) -> float:
    """Compute annual solar-only revenue from FTM dispatch at LMP prices.

    Sums hourly solar export times LMP price, converting from kWh to MWh.
    This isolates solar revenue from battery arbitrage revenue.

    Args:
        dispatch_result: FTM dispatch result with solar_export_kw populated.
        lmp_prices: 8760 hourly LMP prices in $/MWh.

    Returns:
        Annual solar revenue in dollars.
    """
    return sum(
        h.solar_export_kw * lmp_prices[h.hour] / 1000
        for h in dispatch_result.hourly_dispatch
    )


def run_sizing_optimization(
    site_config: SiteConfig,
    production_kwh: list[float],
    rate_schedule: RateSchedule,
    load_profile: LoadProfile,
    solar_only_bill: BillResult,
    bill_without_solar: BillResult,
) -> SizingResult:
    """Run BESS sizing optimization over a power/duration sweep grid.

    Evaluates every (power_mw, duration_hr) combo from the sweep grid,
    running full dispatch + bill calculation for each. Selects the combo
    with the highest BESS NPV, then computes full project economics.

    Args:
        site_config: Site configuration with BESS optimization fields.
        production_kwh: 8760 hourly solar production in kWh.
        rate_schedule: Validated rate schedule with energy/demand structures.
        load_profile: Hourly load profile (8760 values).
        solar_only_bill: Pre-computed bill with solar but no BESS.
        bill_without_solar: Pre-computed bill with no solar and no BESS.

    Returns:
        SizingResult with winner, economics, all combos, and dispatch result.

    Raises:
        ValueError: If all sizing combos fail dispatch.
    """
    grid = build_sweep_grid(site_config)
    total = len(grid)
    all_combos: list[SweepComboResult] = []

    logger.info("Starting BESS sizing sweep: %d combos", total)

    for i, (power_mw, duration_hr) in enumerate(grid):
        try:
            temp_config = site_config.model_copy(
                update={"bess_power_mw": power_mw, "bess_duration_hr": duration_hr}
            )

            dispatch_result, bill_comparison = run_bess_dispatch(
                temp_config, production_kwh, rate_schedule, load_profile, solar_only_bill
            )

            bess_incremental_savings = bill_comparison.bess_incremental_savings
            metrics = dispatch_result.metrics
            installed_cost = (
                site_config.bess_installed_cost_per_kwh * power_mw * 1000 * duration_hr
            )

            bess_npv = compute_bess_npv(
                bess_incremental_savings=bess_incremental_savings,
                bess_installed_cost=installed_cost,
                bess_annual_degradation_pct=metrics.estimated_annual_degradation_pct,
                rate_escalation_pct=site_config.rate_escalation_pct,
                bess_opex_per_kw_year=site_config.bess_opex_per_kw_year,
                bess_power_kw=power_mw * 1000,
                discount_rate_pct=site_config.discount_rate_pct,
                project_lifetime_years=site_config.project_lifetime_years,
            )

            combo = SweepComboResult(
                power_mw=power_mw,
                duration_hr=duration_hr,
                capacity_kwh=power_mw * 1000 * duration_hr,
                installed_cost=installed_cost,
                bess_incremental_savings=bess_incremental_savings,
                bess_npv=bess_npv,
                annual_cycles=metrics.annual_cycles,
                annual_degradation_pct=metrics.estimated_annual_degradation_pct,
                solver_status=dispatch_result.monthly_solve_status,
            )
            all_combos.append(combo)

            logger.info(
                "  Combo %d/%d: %.1f MW / %.1f hr → NPV $%s",
                i + 1, total, power_mw, duration_hr, f"{bess_npv:,.0f}",
            )

        except Exception:
            logger.warning(
                "  Combo %d/%d: %.1f MW / %.1f hr → FAILED, skipping",
                i + 1, total, power_mw, duration_hr,
                exc_info=True,
            )

    if not all_combos:
        raise ValueError("All sizing combos failed dispatch")

    # Select winner: highest BESS NPV
    winner = max(all_combos, key=lambda c: c.bess_npv)

    logger.info(
        "Winner: %.1f MW / %.1f hr → NPV $%s (%d combos evaluated)",
        winner.power_mw, winner.duration_hr, f"{winner.bess_npv:,.0f}", len(all_combos),
    )

    # Re-run dispatch for winner to get full results
    winner_config = site_config.model_copy(
        update={"bess_power_mw": winner.power_mw, "bess_duration_hr": winner.duration_hr}
    )
    winner_dispatch, winner_bill_comparison = run_bess_dispatch(
        winner_config, production_kwh, rate_schedule, load_profile, solar_only_bill
    )

    # Compute project economics
    economics = compute_project_economics(
        bess_npv=winner.bess_npv,
        bess_installed_cost=winner.installed_cost,
        bess_incremental_savings=winner.bess_incremental_savings,
        bess_power_mw=winner.power_mw,
        bess_duration_hr=winner.duration_hr,
        bess_opex_per_kw_year=site_config.bess_opex_per_kw_year,
        site_config=site_config,
        solar_only_bill=solar_only_bill,
        bill_without_solar=bill_without_solar,
        annual_production_kwh=sum(production_kwh),
    )
    economics.combos_evaluated = len(all_combos)

    return SizingResult(
        winner=winner,
        economics=economics,
        all_combos=all_combos,
        dispatch_result=winner_dispatch,
        bill_comparison=winner_bill_comparison,
    )


def run_ftm_sizing_optimization(
    site_config: SiteConfig,
    production_kwh: list[float],
    lmp_data: LMPData,
    load_kwh: list[float] | None = None,
) -> SizingResult:
    """Run FTM BESS sizing optimization over a power/duration sweep grid.

    Evaluates every (power_mw, duration_hr) combo from the sweep grid,
    running full FTM dispatch + revenue calculation for each. Selects the
    combo with the highest BESS NPV, then computes full project economics.

    Args:
        site_config: Site configuration with BESS optimization fields.
        production_kwh: 8760 hourly solar production in kWh.
        lmp_data: Hourly LMP price data (8760 values in $/MWh).
        load_kwh: 8760 hourly parasitic/auxiliary load in kWh.
            If None, zeros are used (pure solar+storage).

    Returns:
        SizingResult with winner, economics, all combos, and dispatch result.

    Raises:
        ValueError: If all sizing combos fail dispatch.
    """
    grid = build_sweep_grid(site_config)
    total = len(grid)
    all_combos: list[SweepComboResult] = []

    logger.info("Starting FTM BESS sizing sweep: %d combos", total)

    for i, (power_mw, duration_hr) in enumerate(grid):
        try:
            temp_config = site_config.model_copy(
                update={"bess_power_mw": power_mw, "bess_duration_hr": duration_hr}
            )

            dispatch_result = run_ftm_dispatch(
                temp_config, production_kwh, lmp_data, load_kwh
            )

            power_kw = power_mw * 1000
            installed_cost = (
                site_config.bess_installed_cost_per_kwh * power_kw * duration_hr
            )

            annual_solar_revenue = compute_ftm_solar_revenue(
                dispatch_result, lmp_data.prices
            )
            bess_arbitrage_revenue = (
                (dispatch_result.ftm_revenue or 0.0) - annual_solar_revenue
            )

            metrics = dispatch_result.metrics
            bess_npv = compute_ftm_bess_npv(
                bess_arbitrage_revenue=bess_arbitrage_revenue,
                bess_installed_cost=installed_cost,
                bess_annual_degradation_pct=metrics.estimated_annual_degradation_pct,
                revenue_escalation_pct=site_config.rate_escalation_pct,
                bess_opex_per_kw_year=site_config.bess_opex_per_kw_year,
                bess_power_kw=power_kw,
                ancillary_revenue_per_kw_year=site_config.ancillary_revenue_per_kw_year,
                discount_rate_pct=site_config.discount_rate_pct,
                project_lifetime_years=site_config.project_lifetime_years,
            )

            combo = SweepComboResult(
                power_mw=power_mw,
                duration_hr=duration_hr,
                capacity_kwh=power_kw * duration_hr,
                installed_cost=installed_cost,
                bess_incremental_savings=bess_arbitrage_revenue,
                bess_npv=bess_npv,
                annual_cycles=metrics.annual_cycles,
                annual_degradation_pct=metrics.estimated_annual_degradation_pct,
                solver_status=dispatch_result.monthly_solve_status,
            )
            all_combos.append(combo)

            logger.info(
                "  Combo %d/%d: %.1f MW / %.1f hr → NPV $%s",
                i + 1, total, power_mw, duration_hr, f"{bess_npv:,.0f}",
            )

        except Exception:
            logger.warning(
                "  Combo %d/%d: %.1f MW / %.1f hr → FAILED, skipping",
                i + 1, total, power_mw, duration_hr,
                exc_info=True,
            )

    if not all_combos:
        raise ValueError("All FTM sizing combos failed dispatch")

    # Select winner: highest BESS NPV
    winner = max(all_combos, key=lambda c: c.bess_npv)

    logger.info(
        "FTM Winner: %.1f MW / %.1f hr → NPV $%s (%d combos evaluated)",
        winner.power_mw, winner.duration_hr, f"{winner.bess_npv:,.0f}", len(all_combos),
    )

    # Re-run dispatch for winner to get full results
    winner_config = site_config.model_copy(
        update={"bess_power_mw": winner.power_mw, "bess_duration_hr": winner.duration_hr}
    )
    winner_dispatch = run_ftm_dispatch(
        winner_config, production_kwh, lmp_data, load_kwh
    )

    # Compute FTM project economics
    winner_solar_revenue = compute_ftm_solar_revenue(
        winner_dispatch, lmp_data.prices
    )
    economics = compute_ftm_project_economics(
        bess_npv=winner.bess_npv,
        bess_installed_cost=winner.installed_cost,
        bess_arbitrage_revenue=winner.bess_incremental_savings,
        bess_power_mw=winner.power_mw,
        bess_duration_hr=winner.duration_hr,
        bess_opex_per_kw_year=site_config.bess_opex_per_kw_year,
        ancillary_revenue_per_kw_year=site_config.ancillary_revenue_per_kw_year,
        site_config=site_config,
        annual_solar_revenue=winner_solar_revenue,
        annual_production_kwh=sum(production_kwh),
    )
    economics.combos_evaluated = len(all_combos)

    return SizingResult(
        winner=winner,
        economics=economics,
        all_combos=all_combos,
        dispatch_result=winner_dispatch,
    )
