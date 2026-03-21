"""Core bill calculation engine for TOU electricity rates."""

from collections import defaultdict

from src.rates.models import BillResult, MonthlyBillDetail, RateSchedule
from src.rates.tou_mapper import get_month_ranges, map_hours_to_periods
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


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

        # --- Energy charges ---
        energy_charges = 0.0
        for h_idx, hmap in enumerate(month_hours):
            period = hmap["energy_period"]
            tier = rate.energyratestructure[period][0]
            energy_charges += month_net[h_idx] * tier.rate

        # --- TOU demand charges ---
        demand_charges = 0.0
        if has_demand:
            # Group hours by demand period, find peak kW in each
            period_peaks: dict[int, float] = defaultdict(float)
            for h_idx, hmap in enumerate(month_hours):
                dp = hmap["demand_period"]
                if dp is not None:
                    if month_net[h_idx] > period_peaks[dp]:
                        period_peaks[dp] = month_net[h_idx]

            for period, peak_kw in period_peaks.items():
                tier = rate.demandratestructure[period][0]
                demand_charges += peak_kw * tier.rate

        # --- Flat (non-coincident) demand charges ---
        flat_demand_charges = 0.0
        if has_flat_demand:
            flat_period = rate.flatdemandmonths[month_idx]
            flat_tier = rate.flatdemandstructure[flat_period][0]
            month_peak_kw = max(month_net) if month_net else 0.0
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
                peak_demand_kw=round(max(month_net) if month_net else 0.0, 2),
            )
        )

    bill = BillResult(monthly=monthly_details)
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
