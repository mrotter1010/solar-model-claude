"""Map each hour of a TMY year to TOU rate periods.

Uses a non-leap 365-day year starting on Monday January 1,
matching the standard TMY convention used by PySAM and NSRDB.
"""

from src.rates.models import RateSchedule

# Days per month in a non-leap year
_DAYS_PER_MONTH = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

# Hours per month (derived)
_HOURS_PER_MONTH = [d * 24 for d in _DAYS_PER_MONTH]


def get_month_ranges() -> list[tuple[int, int]]:
    """Return the start and end hour indices for each month.

    Returns:
        List of 12 (start_hour, end_hour) tuples. Each range is
        inclusive of start_hour and exclusive of end_hour, suitable
        for slicing: hours[start:end].
    """
    ranges: list[tuple[int, int]] = []
    start = 0
    for hours in _HOURS_PER_MONTH:
        ranges.append((start, start + hours))
        start += hours
    return ranges


def map_hours_to_periods(rate: RateSchedule) -> list[dict]:
    """Map each hour of a TMY year to its TOU rate periods.

    Walks through 8760 hours (365 days, non-leap, starting Monday Jan 1)
    and for each hour determines the calendar month, hour-of-day,
    day-of-week, weekend status, and the energy/demand period indices
    from the rate schedule matrices.

    Args:
        rate: Validated RateSchedule with schedule matrices.

    Returns:
        List of 8760 dicts, each with keys: month, hour, day_of_week,
        is_weekend, energy_period, demand_period.
    """
    has_demand = rate.demandratestructure is not None

    result: list[dict] = []
    hour_of_year = 0
    day_of_week = 0  # 0=Monday for Jan 1

    for month_idx, days_in_month in enumerate(_DAYS_PER_MONTH):
        for _day in range(days_in_month):
            is_weekend = day_of_week >= 5  # Saturday=5, Sunday=6

            for hour in range(24):
                # Energy period
                if is_weekend:
                    energy_period = rate.energyweekendschedule[month_idx][hour]
                else:
                    energy_period = rate.energyweekdayschedule[month_idx][hour]

                # Demand period
                demand_period: int | None = None
                if has_demand:
                    if is_weekend:
                        demand_period = rate.demandweekendschedule[month_idx][hour]
                    else:
                        demand_period = rate.demandweekdayschedule[month_idx][hour]

                result.append({
                    "month": month_idx + 1,  # 1-indexed calendar month
                    "hour": hour,
                    "day_of_week": day_of_week,
                    "is_weekend": is_weekend,
                    "energy_period": energy_period,
                    "demand_period": demand_period,
                })

                hour_of_year += 1

            day_of_week = (day_of_week + 1) % 7

    return result
