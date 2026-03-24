"""Full-year BESS dispatch runner orchestrating monthly LP solves."""

from collections import defaultdict

from src.bess.metrics import compute_battery_metrics, compute_heatmap_data
from src.bess.models import (
    BESSBillComparison,
    BESSConfig,
    DispatchResult,
    HourlyDispatch,
)
from src.bess.optimizer import solve_month
from src.config.schema import SiteConfig
from src.rates.bill_calculator import calculate_bill, get_export_rate_for_hour
from src.rates.models import BillResult, LoadProfile, RateSchedule
from src.rates.tou_mapper import get_month_ranges, map_hours_to_periods
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def run_bess_dispatch(
    site_config: SiteConfig,
    production_kwh: list[float],
    rate_schedule: RateSchedule,
    load_profile: LoadProfile,
    solar_only_bill: BillResult | None = None,
) -> DispatchResult | tuple[DispatchResult, BESSBillComparison]:
    """Run full-year BESS dispatch optimization across 12 months.

    Builds a BESSConfig from the site configuration, then solves each month
    sequentially using the LP optimizer, carrying SOC forward between months.

    When solar_only_bill is provided, also computes the solar+BESS bill and
    returns a bill comparison alongside the dispatch result.

    Args:
        site_config: Site configuration with BESS fields populated.
        production_kwh: 8760 hourly solar production in kWh
            (equals kW for 1-hr intervals).
        rate_schedule: Validated rate schedule with energy/demand structures.
        load_profile: Hourly load profile (8760 values).
        solar_only_bill: Pre-computed bill with solar but no BESS. When
            provided, bill comparison is computed and returned.

    Returns:
        DispatchResult when solar_only_bill is None (backward compatible).
        Tuple of (DispatchResult, BESSBillComparison) when solar_only_bill
        is provided.
    """
    config = BESSConfig.from_site_config(site_config)
    hour_map = map_hours_to_periods(rate_schedule)
    month_ranges = get_month_ranges()

    has_demand = rate_schedule.demandratestructure is not None
    has_flat_demand = (
        rate_schedule.flatdemandstructure is not None
        and rate_schedule.flatdemandmonths is not None
    )
    nem_active = rate_schedule.net_metering.mode != "none"

    all_hourly: list[HourlyDispatch] = []
    monthly_statuses: list[str] = []
    current_soc = config.min_soc_kwh

    for month_idx in range(12):
        start, end = month_ranges[month_idx]
        month_load = load_profile.hourly_kwh[start:end]
        month_prod = production_kwh[start:end]
        month_hours = hour_map[start:end]
        n_hours = end - start

        # --- Energy rate per hour ---
        # Replicates bill_calculator.py lookup: energyratestructure[period][0].rate
        energy_rate_per_hour = [
            rate_schedule.energyratestructure[hmap["energy_period"]][0].rate
            for hmap in month_hours
        ]

        # --- TOU demand periods and rates ---
        # Replicates bill_calculator.py pattern: group hours by demand_period,
        # look up demandratestructure[period][0].rate per period.
        # Keys are stringified period indices ("period_0", "period_1", etc.)
        # to match solve_month's dict[str, list[int]] interface.
        demand_periods: dict[str, list[int]] = defaultdict(list)
        demand_rates: dict[str, float] = {}

        if has_demand:
            for h_idx, hmap in enumerate(month_hours):
                dp = hmap["demand_period"]
                if dp is not None:
                    period_name = f"period_{dp}"
                    demand_periods[period_name].append(h_idx)
                    if period_name not in demand_rates:
                        demand_rates[period_name] = (
                            rate_schedule.demandratestructure[dp][0].rate
                        )

        # --- Flat (non-coincident) demand rate ---
        # Replicates bill_calculator.py: flatdemandstructure[flatdemandmonths[month]][0].rate
        flat_demand_rate = 0.0
        if has_flat_demand:
            flat_period = rate_schedule.flatdemandmonths[month_idx]
            flat_demand_rate = (
                rate_schedule.flatdemandstructure[flat_period][0].rate
            )

        # --- NEM export rates per hour ---
        # Build per-hour export rates using hour_of_year indices (not month-local).
        # When NEM is inactive, pass None so the optimizer skips export logic.
        month_export_rates: list[float] | None = None
        if nem_active:
            month_export_rates = [
                get_export_rate_for_hour(start + h_idx, rate_schedule)
                for h_idx in range(n_hours)
            ]

        # --- Solve this month ---
        result = solve_month(
            load_kw=month_load,
            production_kw=month_prod,
            config=config,
            energy_rate_per_hour=energy_rate_per_hour,
            demand_periods=dict(demand_periods),
            demand_rates=demand_rates,
            flat_demand_rate=flat_demand_rate,
            initial_soc_kwh=current_soc,
            nem_export_rates=month_export_rates,
            solar_only_charging=site_config.bess_solar_only_charging,
            grid_only_charging=site_config.bess_grid_only_charging,
        )

        logger.info(
            f"Month {month_idx + 1}: status={result.solver_status}, "
            f"final_soc={result.final_soc_kwh:.1f} kWh"
        )

        monthly_statuses.append(result.solver_status)
        current_soc = result.final_soc_kwh

        # --- Build hourly dispatch records ---
        # Clamp non-negative fields to 0 to handle LP solver floating-point
        # noise (e.g., -3e-13 for variables bounded at 0).
        has_export = nem_active and len(result.export_kw) == n_hours
        for h_idx in range(n_hours):
            all_hourly.append(
                HourlyDispatch(
                    hour=start + h_idx,
                    charge_kw=max(0.0, result.charge_kw[h_idx]),
                    discharge_kw=max(0.0, result.discharge_kw[h_idx]),
                    soc_kwh=max(0.0, result.soc_kwh[h_idx]),
                    net_load_kw=result.net_load_kw[h_idx],
                    curtailed_kw=max(0.0, result.curtailed_kw[h_idx]),
                    export_kw=(
                        max(0.0, result.export_kw[h_idx])
                        if has_export else 0.0
                    ),
                )
            )

    dispatch_result = DispatchResult(
        config=config,
        hourly_dispatch=all_hourly,
        monthly_solve_status=monthly_statuses,
    )

    # --- Compute metrics and heatmap ---
    if site_config.bess_solar_only_charging:
        charging_source = "solar_only"
    elif site_config.bess_grid_only_charging:
        charging_source = "grid_only"
    else:
        charging_source = "any"
    dispatch_result.metrics = compute_battery_metrics(
        dispatch_result, charging_source=charging_source
    )
    dispatch_result.heatmap_data = compute_heatmap_data(dispatch_result)

    if solar_only_bill is None:
        return dispatch_result

    # --- Build adjusted production and compute solar+BESS bill ---
    # Grid-only mode: bill comparison is load-only vs load+BESS (no solar).
    # Standard mode: adjusted_production includes solar + battery net.
    if site_config.bess_grid_only_charging:
        adjusted_production = [
            all_hourly[t].discharge_kw - all_hourly[t].charge_kw
            for t in range(8760)
        ]
    else:
        adjusted_production = [
            production_kwh[t] + all_hourly[t].discharge_kw - all_hourly[t].charge_kw
            for t in range(8760)
        ]
    solar_plus_bess_bill = calculate_bill(
        load_kwh=load_profile.hourly_kwh,
        production_kwh=adjusted_production,
        rate=rate_schedule,
    )

    bill_comparison = BESSBillComparison(
        solar_only_annual_bill=solar_only_bill.annual_total,
        solar_plus_bess_annual_bill=solar_plus_bess_bill.annual_total,
        bess_incremental_savings=(
            solar_only_bill.annual_total - solar_plus_bess_bill.annual_total
        ),
        bess_demand_savings=(
            solar_only_bill.annual_demand_charges
            + solar_only_bill.annual_flat_demand_charges
            - solar_plus_bess_bill.annual_demand_charges
            - solar_plus_bess_bill.annual_flat_demand_charges
        ),
        bess_energy_savings=(
            solar_only_bill.annual_energy_charges
            - solar_plus_bess_bill.annual_energy_charges
        ),
        solar_only_export_kwh=solar_only_bill.annual_export_kwh,
        solar_only_export_credits=solar_only_bill.annual_export_credits,
        solar_plus_bess_export_kwh=solar_plus_bess_bill.annual_export_kwh,
        solar_plus_bess_export_credits=solar_plus_bess_bill.annual_export_credits,
    )

    logger.info(
        "BESS bill comparison: incremental_savings=$%.0f, "
        "demand_savings=$%.0f, energy_savings=$%.0f",
        bill_comparison.bess_incremental_savings,
        bill_comparison.bess_demand_savings,
        bill_comparison.bess_energy_savings,
    )

    return dispatch_result, bill_comparison
