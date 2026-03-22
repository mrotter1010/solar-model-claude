"""BESS report section: chart generation for battery dispatch analysis."""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

MONTH_LABELS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


def generate_heatmap_chart(
    heatmap_data: list[list[float]], output_path: Path
) -> Path:
    """Generate a 12x24 dispatch heatmap chart.

    Positive values (warm colors) = discharge, negative (cool colors) = charge.
    Colormap is RdBu_r, centered at zero.

    Args:
        heatmap_data: 12x24 matrix of average battery power (kW).
        output_path: Path to save the PNG chart.

    Returns:
        Path to the saved chart PNG.
    """
    data = np.array(heatmap_data)
    max_abs = max(abs(float(data.min())), abs(float(data.max())))
    if max_abs == 0:
        max_abs = 1.0

    fig, ax = plt.subplots(figsize=(10, 5))
    im = ax.imshow(
        data,
        aspect="auto",
        cmap="RdBu_r",
        vmin=-max_abs,
        vmax=max_abs,
        interpolation="nearest",
    )

    ax.set_yticks(range(12))
    ax.set_yticklabels(MONTH_LABELS)
    ax.set_xticks(range(24))
    ax.set_xticklabels([str(h) for h in range(24)], fontsize=8)
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Month")
    ax.set_title("Monthly \u00d7 Hourly Average Dispatch Pattern")

    cbar = fig.colorbar(im, ax=ax, pad=0.02)
    cbar.set_label("Average Battery Power (kW)")

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path), dpi=150)
    plt.close(fig)

    logger.info("BESS heatmap chart saved: %s", output_path)
    return output_path


def generate_dispatch_profile_chart(
    load_kwh: list[float],
    solar_kwh: list[float],
    battery_kw: list[float],
    month_name: str,
    output_path: Path,
) -> Path:
    """Generate a 24-hour average day dispatch profile chart.

    Shows load, solar production, net load, and battery charge/discharge
    for a representative average day in the selected month.

    Args:
        load_kwh: 24-hour average load profile (kW).
        solar_kwh: 24-hour average solar production (kW).
        battery_kw: 24-hour average battery power (kW).
            Positive = discharge, negative = charge.
        month_name: Name of the month for the chart title.
        output_path: Path to save the PNG chart.

    Returns:
        Path to the saved chart PNG.
    """
    hours = list(range(24))

    # Net load = load - solar - battery_dispatch
    # (discharge positive reduces net load, charge negative increases it)
    net_load = [
        load_kwh[h] - solar_kwh[h] - battery_kw[h]
        for h in range(24)
    ]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(hours, load_kwh, label="Load", color="#2c3e50", linewidth=2)
    ax.plot(
        hours, solar_kwh, label="Solar", color="#f39c12", linewidth=2,
    )
    ax.plot(
        hours, net_load, label="Net Load",
        color="#e74c3c", linewidth=2, linestyle="--",
    )

    # Battery fill: green for discharge, blue for charge
    pos_battery = [max(0, b) for b in battery_kw]
    neg_battery = [min(0, b) for b in battery_kw]

    ax.fill_between(
        hours, 0, pos_battery,
        alpha=0.3, color="#27ae60", label="Discharge",
    )
    ax.fill_between(
        hours, 0, neg_battery,
        alpha=0.3, color="#3498db", label="Charge",
    )

    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("kW")
    ax.set_title(f"Average Daily Dispatch Profile \u2014 {month_name}")
    ax.set_xticks(hours)
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(alpha=0.3)
    ax.axhline(y=0, color="black", linewidth=0.5, alpha=0.3)

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path), dpi=150)
    plt.close(fig)

    logger.info("BESS dispatch profile chart saved: %s", output_path)
    return output_path
