"""Core bill calculation engine for TOU electricity rates."""

from collections import defaultdict

from src.rates.models import BillResult, MonthlyBillDetail, RateSchedule
from src.rates.tou_mapper import _DAYS_PER_MONTH, get_month_ranges, map_hours_to_periods
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def get_export_rate_for_hour(hour_of_year: int, rate: RateSchedule) -> float:
    """Return the $/kWh export credit rate for a given hour based on NEM mode.

    Determines the export rate by NEM mode:
    - "none": 0.0 (no export credits).
    - "flat_rate": Fixed export_rate from NetMeteringConfig.
    - "match_import": Uses the energy import rate for that hour (same
      period lookup as import charges via energyweekday/weekendschedule).
    - "detailed": Uses export_schedule/export_weekend_schedule and
      export_rate_structure for a separate TOU export rate.

    Weekday vs weekend is determined using the TMY convention: non-leap
    year starting Monday Jan 1 (day_of_week 0=Monday, >=5 is weekend).

    Args:
        hour_of_year: Hour index 0-8759.
        rate: Validated RateSchedule with net_metering config.

    Returns:
        Export credit rate in $/kWh for the given hour.
    """
    nem = rate.net_metering

    if nem.mode == "none":
        return 0.0

    if nem.mode == "flat_rate":
        return nem.export_rate

    # Derive calendar info from hour_of_year (TMY: non-leap, starts Monday)
    day_of_year = hour_of_year // 24
    hour_of_day = hour_of_year % 24
    day_of_week = day_of_year % 7  # 0=Monday
    is_weekend = day_of_week >= 5

    # Find month index from day_of_year
    month_idx = 0
    cumulative_days = 0
    for m, days in enumerate(_DAYS_PER_MONTH):
        if day_of_year < cumulative_days + days:
            month_idx = m
            break
        cumulative_days += days

    if nem.mode == "match_import":
        if is_weekend:
            period = rate.energyweekendschedule[month_idx][hour_of_day]
        else:
            period = rate.energyweekdayschedule[month_idx][hour_of_day]
        return rate.energyratestructure[period][0].rate

    if nem.mode == "detailed":
        if is_weekend:
            period = nem.export_weekend_schedule[month_idx][hour_of_day]
        else:
            period = nem.export_schedule[month_idx][hour_of_day]
        return nem.export_rate_structure[period][0].rate

    return 0.0


def apply_nem_banking(
    monthly_details: list[MonthlyBillDetail],
    rate: RateSchedule,
) -> tuple[list[MonthlyBillDetail], float]:
    """Apply NEM credit banking across 12 months with year-end true-up.

    Each month, new export kWh are added to a running bank. Available
    bank kWh are applied against energy charges (down to $0 floor) using
    the month's average export rate. Unused kWh carry forward. After
    month 12, remaining bank kWh are cashed out at the true-up rate.

    The bank tracks kWh, not dollars, because the true-up rate is
    typically lower than the generation-time export rate.

    Args:
        monthly_details: 12 MonthlyBillDetail records with export data.
        rate: RateSchedule containing net_metering config.

    Returns:
        Tuple of (updated monthly details, true-up credit amount).
    """
    bank_kwh = 0.0
    updated_details: list[MonthlyBillDetail] = []

    for month_idx in range(12):
        detail = monthly_details[month_idx]

        # Add this month's exports to the bank
        bank_kwh += detail.export_kwh

        # Determine average $/kWh rate for this month's exports
        if detail.export_kwh > 0:
            avg_rate = detail.export_credits / detail.export_kwh
        else:
            avg_rate = 0.0

        # Apply banked credits against energy charges (floor $0)
        credits_applied = 0.0
        if avg_rate > 0 and bank_kwh > 0 and detail.energy_charges > 0:
            kwh_to_apply = min(bank_kwh, detail.energy_charges / avg_rate)
            credits_applied = kwh_to_apply * avg_rate
            bank_kwh -= kwh_to_apply

        credits_applied_rounded = round(credits_applied, 2)
        new_total = round(
            detail.energy_charges
            + detail.demand_charges
            + detail.flat_demand_charges
            + detail.fixed_charges
            - credits_applied_rounded,
            2,
        )
        updated = detail.model_copy(update={
            "nem_credits_applied": credits_applied_rounded,
            "nem_credits_banked_kwh": round(bank_kwh, 2),
            "total": new_total,
        })
        updated_details.append(updated)

    # Year-end true-up: cash out remaining bank at true_up_rate
    true_up_credit = round(bank_kwh * rate.net_metering.true_up_rate, 2)

    return updated_details, true_up_credit


def calculate_bill(
    load_kwh: list[float],
    production_kwh: list[float],
    rate: RateSchedule,
    net_load_floor: float = 0.0,
) -> BillResult:
    """Calculate a monthly electricity bill under a TOU rate schedule.

    For each hour, net load = max(load - production, net_load_floor).
    In v1, net_load_floor=0 means no net energy metering credit; excess
    production is curtailed (net load cannot go negative).

    Args:
        load_kwh: 8760 hourly consumption values in kWh.
        production_kwh: 8760 hourly solar production values in kWh.
        rate: Validated RateSchedule with all rate structures and schedules.
        net_load_floor: Minimum net load per hour. Default 0.0 (no NEM).

    Returns:
        BillResult with 12 monthly bill details and computed annual totals.
    """
    # When NEM is active, allow negative net load for export tracking
    nem_active = rate.net_metering.mode != "none"
    if nem_active:
        net_load_floor = -1e9

    # Compute net load for all 8760 hours
    net_load = [
        max(load_kwh[h] - production_kwh[h], net_load_floor)
        for h in range(8760)
    ]

    # Pre-compute TOU mapping and month ranges
    hour_map = map_hours_to_periods(rate)
    month_ranges = get_month_ranges()

    has_demand = rate.demandratestructure is not None
    has_flat_demand = (
        rate.flatdemandstructure is not None and rate.flatdemandmonths is not None
    )

    monthly_details: list[MonthlyBillDetail] = []

    for month_idx in range(12):
        start, end = month_ranges[month_idx]
        month_net = net_load[start:end]
        month_prod = production_kwh[start:end]
        month_load = load_kwh[start:end]
        month_hours = hour_map[start:end]

        # --- Energy charges (import hours only) ---
        energy_charges = 0.0
        for h_idx, hmap in enumerate(month_hours):
            period = hmap["energy_period"]
            tier = rate.energyratestructure[period][0]
            energy_charges += max(month_net[h_idx], 0.0) * tier.rate

        # --- Export tracking (NEM) ---
        month_export_kwh = 0.0
        month_export_credits = 0.0
        if nem_active:
            for h_idx in range(len(month_net)):
                if month_net[h_idx] < 0:
                    hour_export = abs(month_net[h_idx])
                    month_export_kwh += hour_export
                    month_export_credits += hour_export * get_export_rate_for_hour(
                        start + h_idx, rate
                    )

        # --- TOU demand charges (export hours cannot reduce peaks) ---
        demand_charges = 0.0
        if has_demand:
            # Group hours by demand period, find peak kW in each
            period_peaks: dict[int, float] = defaultdict(float)
            for h_idx, hmap in enumerate(month_hours):
                dp = hmap["demand_period"]
                if dp is not None:
                    peak_val = max(month_net[h_idx], 0.0)
                    if peak_val > period_peaks[dp]:
                        period_peaks[dp] = peak_val

            for period, peak_kw in period_peaks.items():
                tier = rate.demandratestructure[period][0]
                demand_charges += peak_kw * tier.rate

        # --- Peak demand (clamped: export hours don't reduce peaks) ---
        month_peak_kw = max(
            (max(v, 0.0) for v in month_net), default=0.0
        )

        # --- Flat (non-coincident) demand charges ---
        flat_demand_charges = 0.0
        if has_flat_demand:
            flat_period = rate.flatdemandmonths[month_idx]
            flat_tier = rate.flatdemandstructure[flat_period][0]
            flat_demand_charges = month_peak_kw * flat_tier.rate

        # --- Fixed charges ---
        fixed_charges = rate.fixed_charges.fixed_charge_first_meter

        monthly_details.append(
            MonthlyBillDetail(
                month=month_idx + 1,
                energy_charges=round(energy_charges, 2),
                demand_charges=round(demand_charges, 2),
                flat_demand_charges=round(flat_demand_charges, 2),
                fixed_charges=round(fixed_charges, 2),
                production_kwh=round(sum(month_prod), 2),
                consumption_kwh=round(sum(month_load), 2),
                peak_demand_kw=round(month_peak_kw, 2),
                export_kwh=round(month_export_kwh, 2),
                export_credits=round(month_export_credits, 2),
            )
        )

    # Apply NEM banking if active
    nem_true_up_credit = 0.0
    if nem_active:
        monthly_details, nem_true_up_credit = apply_nem_banking(
            monthly_details, rate
        )

    bill = BillResult(monthly=monthly_details, nem_true_up_credit=nem_true_up_credit)
    logger.info(
        f"Bill calculated: annual_total=${bill.annual_total:,.2f}, "
        f"consumption={bill.annual_consumption_kwh:,.0f} kWh"
    )
    return bill


def calculate_bill_without_solar(
    load_kwh: list[float],
    rate: RateSchedule,
) -> BillResult:
    """Calculate a bill assuming zero solar production.

    Convenience wrapper around calculate_bill with a zero production array.

    Args:
        load_kwh: 8760 hourly consumption values in kWh.
        rate: Validated RateSchedule.

    Returns:
        BillResult for the load-only scenario.
    """
    return calculate_bill(
        load_kwh=load_kwh,
        production_kwh=[0.0] * 8760,
        rate=rate,
    )
