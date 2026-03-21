"""Savings analysis: compare electricity bills with and without solar."""

from src.rates.bill_calculator import calculate_bill, calculate_bill_without_solar
from src.rates.models import BillSavings, RateSchedule
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def analyze_savings(
    load_kwh: list[float],
    production_kwh: list[float],
    rate: RateSchedule,
) -> BillSavings:
    """Compare electricity bills with and without solar production.

    Calculates two full bills — one assuming zero solar production and
    one with the provided hourly production array — then computes
    savings metrics including annual dollar savings, savings percentage,
    avoided cost per kWh, and demand charge savings.

    Args:
        load_kwh: 8760 hourly consumption values in kWh.
        production_kwh: 8760 hourly solar production values in kWh.
        rate: Validated RateSchedule with all rate structures and schedules.

    Returns:
        BillSavings with both bill results and computed savings metrics.
    """
    bill_without = calculate_bill_without_solar(load_kwh, rate)
    bill_with = calculate_bill(load_kwh, production_kwh, rate)

    savings = BillSavings(
        bill_without_solar=bill_without,
        bill_with_solar=bill_with,
    )

    logger.info(
        f"Savings analysis: annual_savings=${savings.annual_savings:,.2f} "
        f"({savings.savings_percent:.1f}%), "
        f"avoided_cost=${savings.avoided_cost_per_kwh:.4f}/kWh, "
        f"demand_savings=${savings.annual_demand_savings:,.2f}"
    )

    return savings
