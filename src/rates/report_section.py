"""Bill savings report section: data extraction and chart generation."""

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from src.rates.models import BillSavings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def extract_bill_summary(bill_savings: BillSavings) -> dict:
    """Extract a flat summary dict for the PDF bill analysis section.

    Args:
        bill_savings: Computed bill savings with monthly detail.

    Returns:
        Dict with all fields needed for the PDF bill section.
    """
    return {
        "annual_bill_without_solar": bill_savings.bill_without_solar.annual_total,
        "annual_bill_with_solar": bill_savings.bill_with_solar.annual_total,
        "annual_savings": bill_savings.annual_savings,
        "savings_percent": bill_savings.savings_percent,
        "avoided_cost_per_kwh": bill_savings.avoided_cost_per_kwh,
        "annual_demand_savings": bill_savings.annual_demand_savings,
        "annual_consumption_kwh": (
            bill_savings.bill_without_solar.annual_consumption_kwh
        ),
        "annual_production_kwh": (
            bill_savings.bill_with_solar.annual_production_kwh
        ),
    }


def generate_bill_chart(
    bill_savings: BillSavings, output_path: Path
) -> Path:
    """Generate a grouped bar chart comparing monthly bills with/without solar.

    Args:
        bill_savings: Computed bill savings with monthly detail.
        output_path: Path to save the PNG chart.

    Returns:
        Path to the saved chart PNG.
    """
    months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    without = [m.total for m in bill_savings.bill_without_solar.monthly]
    with_solar = [m.total for m in bill_savings.bill_with_solar.monthly]

    x = range(12)
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar([i - width / 2 for i in x], without, width, label="Without Solar", color="#d35400")
    ax.bar([i + width / 2 for i in x], with_solar, width, label="With Solar", color="#27ae60")

    ax.set_xlabel("Month")
    ax.set_ylabel("Monthly Bill ($)")
    ax.set_title("Monthly Bill Comparison")
    ax.set_xticks(list(x))
    ax.set_xticklabels(months)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path), dpi=150)
    plt.close(fig)

    logger.info("Bill comparison chart saved: %s", output_path)
    return output_path


def generate_load_solar_chart(
    load_kwh: list[float],
    production_kwh: list[float],
    output_path: Path,
) -> Path:
    """Generate average daily load vs solar production line chart.

    Averages each hour across all 365 days to produce a 24-hour profile.

    Args:
        load_kwh: 8760 hourly consumption values in kWh.
        production_kwh: 8760 hourly solar production values in kWh.
        output_path: Path to save the PNG chart.

    Returns:
        Path to the saved chart PNG.
    """
    # Average each hour across 365 days
    avg_load = [0.0] * 24
    avg_prod = [0.0] * 24
    for h in range(8760):
        hour = h % 24
        avg_load[hour] += load_kwh[h]
        avg_prod[hour] += production_kwh[h]
    avg_load = [v / 365 for v in avg_load]
    avg_prod = [v / 365 for v in avg_prod]

    hours = list(range(24))

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(hours, avg_load, label="Load", color="#2c3e50", linewidth=2)
    ax.plot(hours, avg_prod, label="Solar Production", color="#f39c12", linewidth=2)
    ax.fill_between(hours, avg_prod, alpha=0.2, color="#f39c12")

    ax.set_xlabel("Hour")
    ax.set_ylabel("kW")
    ax.set_title("Average Daily Load vs Solar Production")
    ax.set_xticks(hours)
    ax.legend()
    ax.grid(alpha=0.3)

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path), dpi=150)
    plt.close(fig)

    logger.info("Load vs solar chart saved: %s", output_path)
    return output_path
