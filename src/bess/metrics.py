"""BESS dispatch performance metrics and heatmap computation."""

from src.bess.models import BatteryMetrics, DispatchResult
from src.rates.tou_mapper import get_month_ranges

# Threshold to distinguish real activity from LP solver noise (kW)
_ACTIVITY_THRESHOLD = 0.1


def compute_battery_metrics(
    result: DispatchResult,
    charging_source: str = "any",
) -> BatteryMetrics:
    """Compute annual battery performance metrics from dispatch results.

    Args:
        result: Full-year dispatch result with 8760 hourly entries.
        charging_source: Battery charging source label
            ("any", "solar_only", "grid_only").

    Returns:
        BatteryMetrics with all computed fields populated.
    """
    config = result.config
    hourly = result.hourly_dispatch

    # --- Throughput and cycling ---
    annual_throughput_kwh = sum(h.discharge_kw for h in hourly)
    annual_cycles = annual_throughput_kwh / config.usable_capacity_kwh
    average_daily_cycles = annual_cycles / 365
    capacity_utilization_pct = (
        annual_throughput_kwh / (config.usable_capacity_kwh * 365) * 100
    )

    # --- Discharge event detection ---
    # A discharge event is a contiguous sequence of hours where discharge_kw > 0.
    event_depths: list[float] = []
    current_event_kwh = 0.0
    in_event = False

    for h in hourly:
        if h.discharge_kw > _ACTIVITY_THRESHOLD:
            current_event_kwh += h.discharge_kw
            in_event = True
        else:
            if in_event:
                event_depths.append(
                    current_event_kwh / config.usable_capacity_kwh * 100
                )
                current_event_kwh = 0.0
                in_event = False

    # Close final event if the year ends mid-discharge
    if in_event:
        event_depths.append(
            current_event_kwh / config.usable_capacity_kwh * 100
        )

    average_discharge_depth_pct = (
        sum(event_depths) / len(event_depths) if event_depths else 0.0
    )
    max_discharge_depth_pct = max(event_depths) if event_depths else 0.0

    # --- Curtailment ---
    total_curtailed_kwh = sum(h.curtailed_kw for h in hourly)

    # --- Activity hours ---
    hours_charging = sum(1 for h in hourly if h.charge_kw > _ACTIVITY_THRESHOLD)
    hours_discharging = sum(
        1 for h in hourly if h.discharge_kw > _ACTIVITY_THRESHOLD
    )
    hours_idle = 8760 - hours_charging - hours_discharging

    # --- Degradation ---
    estimated_annual_degradation_pct = (
        annual_cycles / config.bess_cycles_warranty * 100
    )

    # --- Export ---
    total_export_kwh = sum(h.export_kw for h in hourly)
    total_export_hours = sum(1 for h in hourly if h.export_kw > _ACTIVITY_THRESHOLD)

    return BatteryMetrics(
        annual_throughput_kwh=annual_throughput_kwh,
        annual_cycles=annual_cycles,
        average_daily_cycles=average_daily_cycles,
        capacity_utilization_pct=capacity_utilization_pct,
        average_discharge_depth_pct=average_discharge_depth_pct,
        max_discharge_depth_pct=max_discharge_depth_pct,
        total_curtailed_kwh=total_curtailed_kwh,
        hours_charging=hours_charging,
        hours_discharging=hours_discharging,
        hours_idle=hours_idle,
        estimated_annual_degradation_pct=estimated_annual_degradation_pct,
        total_export_kwh=total_export_kwh,
        total_export_hours=total_export_hours,
        charging_source=charging_source,
    )


def compute_heatmap_data(result: DispatchResult) -> list[list[float]]:
    """Compute a 12x24 average battery power heatmap.

    Each cell is the average battery power (kW) for that month x hour-of-day
    combination. Positive = discharge, negative = charge.

    Args:
        result: Full-year dispatch result with 8760 hourly entries.

    Returns:
        List of 12 lists, each with 24 floats (avg kW per hour-of-day slot).
    """
    month_ranges = get_month_ranges()
    hourly = result.hourly_dispatch
    heatmap: list[list[float]] = []

    for month_idx in range(12):
        start, end = month_ranges[month_idx]
        n_hours = end - start
        n_days = n_hours // 24

        row: list[float] = []
        for hour_of_day in range(24):
            total = 0.0
            for day in range(n_days):
                h = hourly[start + day * 24 + hour_of_day]
                total += h.discharge_kw - h.charge_kw
            row.append(total / n_days)
        heatmap.append(row)

    return heatmap
