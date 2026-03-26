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


def build_bess_summary_rows(bess_dispatch: dict) -> list[list[str]]:
    """Build summary table rows for the BESS report section.

    Returns a list of [label, value] pairs for the PDF summary table.
    Includes charging source, export rows (when export > 0), and all
    standard BESS metrics.

    Args:
        bess_dispatch: Dict from summary["bess_dispatch"] containing BESS
            metrics and bill comparison data.

    Returns:
        List of [label, value] string pairs for table rows.
    """
    bd = bess_dispatch
    rows = [
        ["Battery Storage Summary", ""],
        [
            "Battery Size",
            f"{bd['bess_power_mw']} MW / {bd['bess_duration_hr']} hr "
            f"({bd['bess_capacity_kwh']:,.0f} kWh)",
        ],
        ["Strategy", str(bd["bess_strategy"])],
    ]

    # Charging source (only show when not default "any")
    charging_source = bd.get("charging_source", "any")
    if charging_source != "any":
        label_map = {
            "solar_only": "Solar Only",
            "grid_only": "Grid Only",
        }
        rows.append([
            "Charging Source",
            label_map.get(charging_source, charging_source),
        ])

    rows.extend([
        [
            "Solar-Only Annual Bill",
            f"${bd['solar_only_annual_bill']:,.0f}",
        ],
        [
            "Solar+BESS Annual Bill",
            f"${bd['solar_plus_bess_annual_bill']:,.0f}",
        ],
        [
            "BESS Incremental Savings",
            f"${bd['bess_incremental_savings']:,.0f}",
        ],
        ["Demand Savings", f"${bd['bess_demand_savings']:,.0f}"],
        ["Energy Savings", f"${bd['bess_energy_savings']:,.0f}"],
        ["Annual Cycles", f"{bd['annual_cycles']:.1f}"],
        [
            "Capacity Utilization",
            f"{bd['capacity_utilization_pct']:.1f}%",
        ],
        [
            "Est. Annual Degradation",
            f"{bd['estimated_annual_degradation_pct']:.2f}%",
        ],
        [
            "Curtailed Energy",
            f"{bd['total_curtailed_kwh']:,.0f} kWh",
        ],
    ])

    # Export rows (only shown when export is present)
    total_export = bd.get("total_export_kwh", 0.0)
    if total_export > 0:
        rows.append(["Total Export", f"{total_export:,.0f} kWh"])
        rows.append(["Export Hours", f"{bd.get('total_export_hours', 0):,}"])

    return rows


def _format_currency(val: float) -> str:
    """Format a dollar value with proper negative sign placement.

    Args:
        val: Dollar amount (positive or negative).

    Returns:
        Formatted string, e.g. "$1,234" or "-$1,234".
    """
    if val < 0:
        return f"(${abs(val):,.0f})"
    return f"${val:,.0f}"


def build_bess_optimization_summary_rows(summary: dict) -> list[list[str]]:
    """Build summary table rows for BESS sizing optimization results.

    Used when the pipeline ran M14c sizing optimization. Reads from both
    summary["bess_sizing"] (economics) and summary["bess_dispatch"]
    (strategy, charging source).

    Args:
        summary: Full pipeline summary dict containing "bess_sizing" and
            "bess_dispatch" keys.

    Returns:
        List of [label, value] string pairs for the PDF summary table.
    """
    bs = summary["bess_sizing"]
    bd = summary.get("bess_dispatch", {})

    rows = [
        ["Battery Storage — Sizing Optimization", ""],
        [
            "Optimal Battery Size",
            f"{bs['optimal_power_mw']:.1f} MW / {bs['optimal_duration_hr']:.1f} hr "
            f"({bs['optimal_capacity_kwh']:,.0f} kWh)",
        ],
        ["Strategy", str(bd.get("bess_strategy", "N/A"))],
    ]

    # Charging source — always displayed for optimization results
    charging_source = bd.get("charging_source", "any")
    source_map = {
        "solar_only": "Solar Only",
        "grid_only": "Grid Only",
        "any": "Grid + Solar",
    }
    rows.append([
        "Charging Source",
        source_map.get(charging_source, charging_source),
    ])

    # NPV and economics
    total_project_npv = bs["total_project_npv"]
    bess_npv = bs["bess_npv"]
    rows.extend([
        ["Total Project NPV", _format_currency(total_project_npv)],
        ["BESS NPV", _format_currency(bess_npv)],
    ])

    lcoe = bs.get("system_lcoe_per_kwh")
    if lcoe is not None:
        rows.append(["System LCOE", f"${lcoe:.4f}/kWh"])
    else:
        rows.append(["System LCOE", "N/A"])

    rows.extend([
        ["Total Installed Cost", f"${bs['total_installed_cost']:,.0f}"],
        ["Year 1 Total Savings", _format_currency(bs["total_annual_savings"])],
        [
            "Year 1 BESS Incremental Savings",
            _format_currency(bs["bess_incremental_savings"]),
        ],
        ["Combos Evaluated", str(bs["combos_evaluated"])],
        [
            "Project Lifetime",
            f"{bs['project_lifetime_years']} years",
        ],
        ["Discount Rate", f"{bs['discount_rate_pct']}%"],
        ["Rate Escalation", f"{bs['rate_escalation_pct']}%"],
    ])

    return rows


def build_ftm_summary_rows(summary: dict) -> list[list[str]]:
    """Build summary table rows for FTM (front-of-meter) dispatch results.

    Reads from summary["bess_dispatch"], summary["lmp"], and
    summary["ftm_economics"] to build revenue-oriented rows appropriate
    for wholesale/merchant plant analysis.

    Args:
        summary: Full pipeline summary dict containing "bess_dispatch",
            "lmp", and "ftm_economics" keys.

    Returns:
        List of [label, value] string pairs for the PDF summary table.
    """
    bd = summary["bess_dispatch"]
    lmp = summary["lmp"]
    econ = summary["ftm_economics"]

    rows = [
        ["Front-of-Meter Battery Storage Summary", ""],
        [
            "Battery Size",
            f"{bd['bess_power_mw']} MW / {bd['bess_duration_hr']} hr "
            f"({bd['bess_capacity_kwh']:,.0f} kWh)",
        ],
        ["Strategy", "Energy Arbitrage"],
        ["Dispatch Mode", "Front-of-Meter (FTM)"],
        ["ISO / Pricing Zone", f"{lmp['iso'].upper()} \u2014 {lmp['zone']}"],
        ["Market", str(lmp["market"])],
        ["LMP Year", str(lmp["year"])],
        ["Mean LMP", f"${lmp['mean_lmp']:.2f}/MWh"],
    ]

    rows.extend([
        ["Annual Solar Revenue", _format_currency(econ["annual_solar_revenue"])],
        [
            "Annual BESS Arbitrage Revenue",
            _format_currency(econ["annual_bess_arbitrage_revenue"]),
        ],
    ])

    ancillary = econ.get("annual_ancillary_revenue", 0.0)
    if ancillary > 0:
        rows.append(["Annual Ancillary Revenue", _format_currency(ancillary)])

    rows.extend([
        ["Annual Gross Revenue", _format_currency(econ["annual_gross_revenue"])],
    ])

    solar_npv = econ.get("solar_npv")
    npv_label = "Total Project NPV"
    if solar_npv is None:
        npv_label = "Total Project NPV (BESS only)"
    rows.extend([
        [npv_label, _format_currency(econ["total_project_npv"])],
        ["BESS NPV", _format_currency(econ["bess_npv"])],
    ])

    lcoe = econ.get("system_lcoe_per_kwh")
    if lcoe is not None:
        rows.append(["System LCOE", f"${lcoe:.4f}/kWh"])
    else:
        rows.append([
            "System LCOE",
            "N/A \u2014 solar cost inputs not provided",
        ])

    # Dispatch metrics
    rows.extend([
        ["Annual Cycles", f"{bd['annual_cycles']:.1f}"],
        [
            "Capacity Utilization",
            f"{bd['capacity_utilization_pct']:.1f}%",
        ],
        [
            "Est. Annual Degradation",
            f"{bd['estimated_annual_degradation_pct']:.2f}%",
        ],
        [
            "Curtailed Energy",
            f"{bd['total_curtailed_kwh']:,.0f} kWh",
        ],
    ])

    return rows


def build_ftm_optimization_summary_rows(summary: dict) -> list[list[str]]:
    """Build summary table rows for FTM sizing optimization results.

    Used when the pipeline ran FTM sizing optimization. Reads from
    summary["bess_sizing"], summary["bess_dispatch"], summary["lmp"],
    and summary["ftm_economics"].

    Args:
        summary: Full pipeline summary dict containing "bess_sizing",
            "bess_dispatch", "lmp", and "ftm_economics" keys.

    Returns:
        List of [label, value] string pairs for the PDF summary table.
    """
    bs = summary["bess_sizing"]
    bd = summary.get("bess_dispatch", {})
    lmp = summary["lmp"]
    econ = summary["ftm_economics"]

    rows = [
        ["FTM Battery Storage \u2014 Sizing Optimization", ""],
        [
            "Optimal Battery Size",
            f"{bs['optimal_power_mw']:.1f} MW / {bs['optimal_duration_hr']:.1f} hr "
            f"({bs['optimal_capacity_kwh']:,.0f} kWh)",
        ],
        ["Strategy", "Energy Arbitrage"],
        ["Dispatch Mode", "Front-of-Meter (FTM)"],
        ["ISO / Pricing Zone", f"{lmp['iso'].upper()} \u2014 {lmp['zone']}"],
        ["Market", str(lmp["market"])],
        ["LMP Year", str(lmp["year"])],
        ["Mean LMP", f"${lmp['mean_lmp']:.2f}/MWh"],
    ]

    # Revenue rows
    rows.extend([
        ["Annual Solar Revenue", _format_currency(econ["annual_solar_revenue"])],
        [
            "Annual BESS Arbitrage Revenue",
            _format_currency(econ["annual_bess_arbitrage_revenue"]),
        ],
    ])

    ancillary = econ.get("annual_ancillary_revenue", 0.0)
    if ancillary > 0:
        rows.append(["Annual Ancillary Revenue", _format_currency(ancillary)])

    rows.extend([
        ["Annual Gross Revenue", _format_currency(econ["annual_gross_revenue"])],
    ])

    solar_npv = econ.get("solar_npv")
    npv_label = "Total Project NPV"
    if solar_npv is None:
        npv_label = "Total Project NPV (BESS only)"
    rows.extend([
        [npv_label, _format_currency(econ["total_project_npv"])],
        ["BESS NPV", _format_currency(econ["bess_npv"])],
    ])

    lcoe = econ.get("system_lcoe_per_kwh")
    if lcoe is not None:
        rows.append(["System LCOE", f"${lcoe:.4f}/kWh"])
    else:
        rows.append([
            "System LCOE",
            "N/A \u2014 solar cost inputs not provided",
        ])

    rows.extend([
        ["Total Installed Cost", f"${econ['total_installed_cost']:,.0f}"],
        ["Combos Evaluated", str(bs["combos_evaluated"])],
        [
            "Project Lifetime",
            f"{bs['project_lifetime_years']} years",
        ],
        ["Discount Rate", f"{bs['discount_rate_pct']}%"],
        ["Rate Escalation", f"{bs['rate_escalation_pct']}%"],
    ])

    # Dispatch metrics
    rows.extend([
        ["Annual Cycles", f"{bd.get('annual_cycles', 0):.1f}"],
        [
            "Capacity Utilization",
            f"{bd.get('capacity_utilization_pct', 0):.1f}%",
        ],
        [
            "Est. Annual Degradation",
            f"{bd.get('estimated_annual_degradation_pct', 0):.2f}%",
        ],
    ])

    return rows


def generate_dispatch_profile_chart(
    load_kwh: list[float],
    solar_kwh: list[float],
    battery_kw: list[float],
    month_name: str,
    output_path: Path,
    export_kw: list[float] | None = None,
) -> Path:
    """Generate a 24-hour average day dispatch profile chart.

    Shows load, solar production, net load, and battery charge/discharge
    for a representative average day in the selected month. When export
    data is provided, export is shown as a filled region below zero.

    Args:
        load_kwh: 24-hour average load profile (kW).
        solar_kwh: 24-hour average solar production (kW).
        battery_kw: 24-hour average battery power (kW).
            Positive = discharge, negative = charge.
        month_name: Name of the month for the chart title.
        output_path: Path to save the PNG chart.
        export_kw: Optional 24-hour average export power (kW).
            Plotted as negative (below x-axis) when provided.

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

    # Export fill: orange below zero (only when export data is present)
    if export_kw is not None and any(v > 0.1 for v in export_kw):
        neg_export = [-v for v in export_kw]
        ax.fill_between(
            hours, 0, neg_export,
            alpha=0.3, color="#e67e22", label="Export",
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
