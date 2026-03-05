"""matplotlib chart generation for solar production reports."""

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Brand colors
BLUE = "#2563EB"
RED = "#DC2626"
GREEN = "#059669"


def _apply_chart_style(ax: plt.Axes) -> None:
    """Apply common chart styling: gridlines, spines, background.

    Args:
        ax: Matplotlib axes to style.
    """
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_facecolor("white")
    ax.figure.set_facecolor("white")


def generate_monthly_production_chart(
    monthly_data: dict, output_path: Path
) -> Path | None:
    """Generate a vertical bar chart of monthly AC energy production.

    Args:
        monthly_data: Output of extract_monthly_production() with keys
            "months" (12 strings) and "energy_mwh" (12 floats).
        output_path: File path for the saved PNG.

    Returns:
        The output_path on success, or None on failure.
    """
    try:
        if not monthly_data or "months" not in monthly_data or "energy_mwh" not in monthly_data:
            logger.error("Invalid monthly_data: missing 'months' or 'energy_mwh' keys")
            return None

        months = monthly_data["months"]
        energy = monthly_data["energy_mwh"]

        if len(months) != 12 or len(energy) != 12:
            logger.error(f"Invalid monthly_data: expected 12 elements, got {len(months)} months, {len(energy)} values")
            return None

        fig, ax = plt.subplots(figsize=(10, 5))
        _apply_chart_style(ax)

        bars = ax.bar(months, energy, color=BLUE, width=0.7)

        # Value labels on top of each bar
        for bar, val in zip(bars, energy):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                f"{val:,.0f}",
                ha="center",
                va="bottom",
                fontsize=8,
            )

        ax.set_ylabel("Energy (MWh)")
        ax.set_title("Monthly AC Energy Production")
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
        ax.yaxis.grid(True, alpha=0.3)
        ax.set_axisbelow(True)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.tight_layout()
        fig.savefig(output_path, dpi=150)
        plt.close(fig)

        logger.info(f"Monthly production chart saved: {output_path}")
        return output_path

    except Exception as exc:
        logger.error(f"Failed to generate monthly production chart: {exc}")
        plt.close("all")
        return None


def generate_loss_waterfall_chart(
    waterfall_data: list[dict], output_path: Path
) -> Path | None:
    """Generate a horizontal waterfall bar chart of energy losses.

    Args:
        waterfall_data: Output of extract_loss_waterfall() — list of step
            dicts with keys: label, energy_kwh, type, loss_percent, sub_losses.
        output_path: File path for the saved PNG.

    Returns:
        The output_path on success, or None on failure.
    """
    try:
        if not waterfall_data:
            logger.error("Invalid waterfall_data: empty or None")
            return None

        # Build bar positions and values (top-to-bottom → reversed index)
        labels = [step["label"] for step in waterfall_data]
        n = len(labels)

        # Y positions: top-to-bottom means first step gets highest y
        y_positions = list(range(n - 1, -1, -1))

        bar_lefts: list[float] = []
        bar_widths: list[float] = []
        bar_colors: list[str] = []
        bar_labels: list[str] = []

        running_total = 0.0

        for step in waterfall_data:
            step_type = step["type"]
            energy_mwh = step["energy_kwh"] / 1000

            if step_type == "start":
                bar_lefts.append(0.0)
                bar_widths.append(energy_mwh)
                bar_colors.append(BLUE)
                bar_labels.append(f"{energy_mwh:,.0f} MWh")
                running_total = energy_mwh

            elif step_type == "end":
                bar_lefts.append(0.0)
                bar_widths.append(energy_mwh)
                bar_colors.append(BLUE)
                bar_labels.append(f"{energy_mwh:,.0f} MWh")

            elif step_type == "loss":
                # Bar extends LEFT from running total
                bar_lefts.append(running_total - energy_mwh)
                bar_widths.append(energy_mwh)
                bar_colors.append(RED)
                pct = step.get("loss_percent", 0.0)
                bar_labels.append(f"-{energy_mwh:,.0f} MWh ({pct:.1f}%)")
                running_total -= energy_mwh

            elif step_type == "gain":
                bar_lefts.append(running_total)
                bar_widths.append(energy_mwh)
                bar_colors.append(GREEN)
                pct = step.get("loss_percent", 0.0)
                bar_labels.append(f"+{energy_mwh:,.0f} MWh ({pct:.1f}%)")
                running_total += energy_mwh

        fig, ax = plt.subplots(figsize=(10, 5))
        _apply_chart_style(ax)

        bars = ax.barh(
            y_positions, bar_widths, left=bar_lefts,
            color=bar_colors, height=0.6, edgecolor="white", linewidth=0.5,
        )

        # Add labels to the right of each bar
        for bar, label_text in zip(bars, bar_labels):
            x_pos = bar.get_x() + bar.get_width() + 100 / 1000  # small offset in MWh
            ax.text(
                x_pos, bar.get_y() + bar.get_height() / 2,
                label_text,
                ha="left", va="center", fontsize=8,
            )

        ax.set_yticks(y_positions)
        ax.set_yticklabels(labels)
        ax.set_xlabel("Energy (MWh)")
        ax.set_title("Energy Loss Waterfall")
        ax.set_xlim(left=0)
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
        ax.xaxis.grid(True, alpha=0.3)
        ax.set_axisbelow(True)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.tight_layout()
        fig.savefig(output_path, dpi=150)
        plt.close(fig)

        logger.info(f"Loss waterfall chart saved: {output_path}")
        return output_path

    except Exception as exc:
        logger.error(f"Failed to generate loss waterfall chart: {exc}")
        plt.close("all")
        return None
