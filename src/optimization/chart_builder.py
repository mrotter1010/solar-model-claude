"""matplotlib chart generation for layout optimization reports."""

from __future__ import annotations

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Brand colors (matching src/reporting/chart_builder.py)
BLUE = "#2563EB"
RED = "#DC2626"
GREEN = "#059669"
AMBER = "#D97706"


def _apply_chart_style(ax: plt.Axes) -> None:
    """Apply common chart styling: spines, background.

    Args:
        ax: Matplotlib axes to style.
    """
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_facecolor("white")
    ax.figure.set_facecolor("white")


def build_optimization_heatmap(
    sweep_results: list[dict],
    winners: dict,
    mode: str,
    output_path: Path,
) -> Path | None:
    """Build a GCR x DC/AC heatmap of the optimization surface.

    Generates a 2D heatmap with GCR on the y-axis and DC/AC ratio on the
    x-axis.  Color represents either LCOE ($/MWh) or combined NPV ($)
    depending on mode.  The winner is marked with a star.

    Args:
        sweep_results: List of sweep point dicts from optimize_solar or
            optimize_solar_bess.  Each dict must have gcr, dcac_ratio,
            failed, and the metric key for the given mode.
        winners: Winners dict from the optimization result.
        mode: One of "production", "lcoe", "npv", "solar_bess".
        output_path: File path for the saved PNG.

    Returns:
        The output_path on success, None if mode is "production" or if
        no valid data points exist.
    """
    if mode == "production":
        return None

    # Determine metric key and labels
    if mode == "solar_bess":
        metric_key = "combined_npv"
        colorbar_label = "Combined NPV ($)"
        title = "NPV Optimization Surface"
    elif mode == "npv":
        metric_key = "npv"
        colorbar_label = "NPV ($)"
        title = "NPV Optimization Surface"
    else:
        # lcoe
        metric_key = "lcoe_per_mwh"
        colorbar_label = "LCOE ($/MWh)"
        title = "LCOE Optimization Surface"

    # For solar_bess mode, GCR key is solar_gcr
    gcr_key = "solar_gcr" if mode == "solar_bess" else "gcr"
    dcac_key = "solar_dcac" if mode == "solar_bess" else "dcac_ratio"

    # Filter to valid points with the metric available
    valid = [
        p for p in sweep_results
        if not p.get("failed", False)
        and p.get(metric_key) is not None
    ]

    if not valid:
        logger.warning("No valid sweep points for heatmap — skipping")
        return None

    # Extract unique sorted GCR and DC/AC values
    gcr_vals = sorted({round(p[gcr_key], 2) for p in valid})
    dcac_vals = sorted({round(p[dcac_key], 2) for p in valid})

    if len(gcr_vals) < 2 or len(dcac_vals) < 2:
        logger.warning(
            "Need at least 2 GCR and 2 DC/AC values for heatmap — skipping"
        )
        return None

    # Build 2D grid (GCR rows, DC/AC cols)
    gcr_idx = {v: i for i, v in enumerate(gcr_vals)}
    dcac_idx = {v: i for i, v in enumerate(dcac_vals)}

    grid = np.full((len(gcr_vals), len(dcac_vals)), np.nan)
    for p in valid:
        gi = gcr_idx.get(round(p[gcr_key], 2))
        di = dcac_idx.get(round(p[dcac_key], 2))
        if gi is not None and di is not None:
            grid[gi, di] = p[metric_key]

    try:
        fig, ax = plt.subplots(figsize=(10, 5))

        # Choose colormap
        is_npv = mode in ("npv", "solar_bess")
        if is_npv:
            cmap = "RdYlGn"
        else:
            # LCOE: lower is better → reversed sequential blue
            cmap = "YlGnBu_r"

        im = ax.imshow(
            grid,
            aspect="auto",
            cmap=cmap,
            origin="lower",
            interpolation="nearest",
        )

        # Axis ticks
        ax.set_xticks(range(len(dcac_vals)))
        ax.set_xticklabels([f"{v:.2f}" for v in dcac_vals], fontsize=8)
        ax.set_yticks(range(len(gcr_vals)))
        ax.set_yticklabels([f"{v:.2f}" for v in gcr_vals], fontsize=8)

        ax.set_xlabel("DC/AC Ratio")
        ax.set_ylabel("GCR")
        ax.set_title(title)

        # Colorbar
        cbar = fig.colorbar(im, ax=ax, pad=0.02)
        cbar.set_label(colorbar_label)
        if is_npv:
            cbar.ax.yaxis.set_major_formatter(
                ticker.FuncFormatter(lambda x, _: f"${x:,.0f}")
            )
        else:
            cbar.ax.yaxis.set_major_formatter(
                ticker.FuncFormatter(lambda x, _: f"${x:.1f}")
            )

        # Mark the winner
        winner = _find_heatmap_winner(winners, mode)
        if winner is not None:
            w_gcr = round(winner[gcr_key], 2)
            w_dcac = round(winner[dcac_key], 2)
            if w_gcr in gcr_idx and w_dcac in dcac_idx:
                ax.plot(
                    dcac_idx[w_dcac], gcr_idx[w_gcr],
                    marker="*", markersize=18, color="white",
                    markeredgecolor="black", markeredgewidth=1.0,
                )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.tight_layout()
        fig.savefig(output_path, dpi=150)
        plt.close(fig)

        logger.info(f"Optimization heatmap saved: {output_path}")
        return output_path

    except Exception as exc:
        logger.error(f"Failed to generate optimization heatmap: {exc}")
        plt.close("all")
        return None


def _find_heatmap_winner(winners: dict, mode: str) -> dict | None:
    """Locate the relevant winner point for the heatmap star marker.

    Args:
        winners: Winners dict from optimization result.
        mode: Optimization mode string.

    Returns:
        The winner point dict, or None if not found.
    """
    if mode == "solar_bess":
        return winners.get("solar_bess_npv")
    if mode == "npv":
        return winners.get("npv")
    # lcoe
    return winners.get("lcoe")


def build_winner_comparison_chart(
    winners: dict,
    mode: str,
    output_path: Path,
) -> Path | None:
    """Build a grouped bar chart comparing optimization winners.

    Shows Annual Energy (MWh) as bars for each winner type, with
    Capacity Factor (%) annotated on each bar.  If LCOE is available,
    it is shown as a text annotation below each bar.

    Args:
        winners: Winners dict from optimization result.  Keys are
            winner type names (max_production, max_yield, lcoe, npv).
        mode: Optimization mode string (for label selection).
        output_path: File path for the saved PNG.

    Returns:
        The output_path on success, or None on failure.
    """
    # Determine which winners to show and in what order
    winner_order = [
        ("max_production", "Max Production"),
        ("max_yield", "Max Yield"),
        ("lcoe", "LCOE Winner"),
        ("npv", "NPV Winner"),
        ("solar_only_npv", "Solar-Only NPV"),
    ]

    entries: list[tuple[str, str, dict]] = []
    for key, label in winner_order:
        w = winners.get(key)
        if w is not None and isinstance(w, dict) and w.get("annual_energy_mwh") is not None:
            entries.append((key, label, w))

    if not entries:
        logger.warning("No valid winners for comparison chart — skipping")
        return None

    colors_map = {
        "max_production": BLUE,
        "max_yield": GREEN,
        "lcoe": AMBER,
        "npv": RED,
        "solar_only_npv": RED,
    }

    labels = [e[1] for e in entries]
    energies = [e[2]["annual_energy_mwh"] for e in entries]
    cap_factors = [e[2]["capacity_factor_ac"] for e in entries]
    bar_colors = [colors_map.get(e[0], BLUE) for e in entries]

    try:
        fig, ax = plt.subplots(figsize=(10, 5))
        _apply_chart_style(ax)

        x = np.arange(len(labels))
        bar_width = 0.6

        bars = ax.bar(x, energies, bar_width, color=bar_colors, edgecolor="white")

        # Annotate each bar with capacity factor and optionally LCOE
        for i, (bar, cf, entry) in enumerate(zip(bars, cap_factors, entries)):
            # Capacity factor on top of bar
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                f"CF: {cf:.1f}%",
                ha="center", va="bottom", fontsize=8, fontweight="bold",
            )

            # LCOE annotation below bar label (if available)
            lcoe_val = entry[2].get("lcoe_per_mwh")
            if lcoe_val is not None:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    -max(energies) * 0.04,
                    f"${lcoe_val:.2f}/MWh",
                    ha="center", va="top", fontsize=7, color="#666666",
                )

        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=9)
        ax.set_ylabel("Annual Energy (MWh)")
        ax.set_title("Configuration Comparison")
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_formatter(
            ticker.FuncFormatter(lambda v, _: f"{v:,.0f}")
        )
        ax.yaxis.grid(True, alpha=0.3)
        ax.set_axisbelow(True)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.tight_layout()
        fig.savefig(output_path, dpi=150)
        plt.close(fig)

        logger.info(f"Winner comparison chart saved: {output_path}")
        return output_path

    except Exception as exc:
        logger.error(f"Failed to generate winner comparison chart: {exc}")
        plt.close("all")
        return None
