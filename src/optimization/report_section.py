"""Optimization report section: table row builders for PDF report."""

from __future__ import annotations


def _fmt_currency(val: float) -> str:
    """Format a dollar value with proper negative sign placement.

    Args:
        val: Dollar amount (positive or negative).

    Returns:
        Formatted string, e.g. "$1,234.56" or "($1,234.56)".
    """
    if val < 0:
        return f"(${abs(val):,.2f})"
    return f"${val:,.2f}"


def _fmt_currency_int(val: float) -> str:
    """Format a dollar value as integer with proper negative sign placement.

    Args:
        val: Dollar amount (positive or negative).

    Returns:
        Formatted string, e.g. "$1,234" or "($1,234)".
    """
    if val < 0:
        return f"(${abs(val):,.0f})"
    return f"${val:,.0f}"


def _fmt_pct(val: float) -> str:
    """Format a percentage value with 2 decimal places.

    Args:
        val: Percentage value (e.g. 26.75).

    Returns:
        Formatted string, e.g. "26.75%".
    """
    return f"{val:.2f}%"


def _fmt_int(val: float) -> str:
    """Format a number as integer with comma separators.

    Args:
        val: Numeric value.

    Returns:
        Formatted string, e.g. "70,439".
    """
    return f"{int(round(val)):,}"


def _fmt_gcr_dcac(point: dict) -> str:
    """Format GCR / DC:AC ratio from a sweep point.

    Args:
        point: Sweep point dict with 'gcr' and 'dcac_ratio' keys.

    Returns:
        Formatted string, e.g. "0.50 / 1.20".
    """
    return f"{point['gcr']:.2f} / {point['dcac_ratio']:.2f}"


def _fmt_capacity(point: dict) -> str:
    """Format DC/AC capacity from a sweep point.

    Args:
        point: Sweep point dict with 'mw_dc' and 'mw_ac' keys.

    Returns:
        Formatted string, e.g. "33.3 MW DC / 27.8 MW AC".
    """
    return f"{point['mw_dc']:.1f} MW DC / {point['mw_ac']:.1f} MW AC"


_MODE_LABELS: dict[str, str] = {
    "production": "Production",
    "lcoe": "LCOE",
    "npv": "NPV",
    "solar_bess": "Solar + BESS",
}


def build_optimization_summary_rows(data: dict) -> list[list[str]]:
    """Build summary table rows for the optimization report section.

    Returns a list of [label, value] pairs for the PDF summary table.
    Content varies by optimization mode (production, lcoe, npv, solar_bess).

    Args:
        data: Optimization result dict from optimize_solar or
            optimize_solar_bess. Expected keys: mode, winners,
            sweep_metadata, and optionally bess_adds_value and
            bess_winner_detail.

    Returns:
        List of [label, value] string pairs for table rows.
    """
    mode = data["mode"]
    winners = data["winners"]
    meta = data["sweep_metadata"]

    mode_label = _MODE_LABELS.get(mode, mode)

    rows: list[list[str]] = [
        ["Layout Optimization Summary", ""],
        ["Optimization Mode", mode_label],
        ["Racking", meta["racking"]],
        ["Buildable Acres", _fmt_int(meta["buildable_acres"])],
        ["Configurations Evaluated", _fmt_int(meta["total_combinations"])],
    ]

    # --- Max Production winner (always present) ---
    mp = winners["max_production"]
    rows.extend([
        ["Max Production \u2014 GCR / DC:AC", _fmt_gcr_dcac(mp)],
        ["Max Production \u2014 Capacity", _fmt_capacity(mp)],
        ["Max Production \u2014 Annual Energy", f"{_fmt_int(mp['annual_energy_mwh'])} MWh"],
        ["Max Production \u2014 Capacity Factor", _fmt_pct(mp["capacity_factor_ac"])],
    ])

    # --- Max Yield winner (always present) ---
    my = winners["max_yield"]
    rows.extend([
        ["Max Yield \u2014 GCR / DC:AC", _fmt_gcr_dcac(my)],
        ["Max Yield \u2014 Capacity", _fmt_capacity(my)],
        ["Max Yield \u2014 Annual Energy", f"{_fmt_int(my['annual_energy_mwh'])} MWh"],
        ["Max Yield \u2014 Capacity Factor", _fmt_pct(my["capacity_factor_ac"])],
    ])

    # --- LCOE winner (lcoe, npv, solar_bess modes) ---
    lcoe_winner = winners.get("lcoe")
    if lcoe_winner is not None:
        rows.extend([
            ["LCOE Winner \u2014 GCR / DC:AC", _fmt_gcr_dcac(lcoe_winner)],
            ["LCOE Winner \u2014 Capacity", _fmt_capacity(lcoe_winner)],
            ["LCOE Winner \u2014 LCOE", f"${lcoe_winner['lcoe_per_mwh']:.2f}/MWh"],
            ["LCOE Winner \u2014 Net CapEx", _fmt_currency_int(lcoe_winner["net_capex"])],
        ])

    # --- NPV winner (npv, solar_bess modes) ---
    npv_winner = winners.get("npv") or winners.get("solar_only_npv")
    if npv_winner is not None and npv_winner.get("npv") is not None:
        prefix = "NPV Winner" if mode != "solar_bess" else "Solar-Only NPV Winner"
        rows.extend([
            [f"{prefix} \u2014 GCR / DC:AC", _fmt_gcr_dcac(npv_winner)],
            [f"{prefix} \u2014 NPV", _fmt_currency_int(npv_winner["npv"])],
            [f"{prefix} \u2014 IRR", _fmt_pct(npv_winner["irr"])],
            [
                f"{prefix} \u2014 Payback",
                f"{npv_winner['simple_payback_years']:.1f} years",
            ],
        ])

    # --- Solar+BESS specific rows ---
    if mode == "solar_bess":
        bess_adds_value = data.get("bess_adds_value", False)
        rows.append(["BESS Adds Value", "Yes" if bess_adds_value else "No"])

        detail = data.get("bess_winner_detail")
        if detail is not None:
            solar_pt = detail["solar"]
            bess_info = detail["bess"]
            combined = detail["combined"]

            rows.extend([
                [
                    "BESS Winner \u2014 Solar Config",
                    f"GCR {solar_pt['gcr']:.2f} / DC:AC {solar_pt['dcac_ratio']:.2f}",
                ],
                [
                    "BESS Winner \u2014 BESS Size",
                    f"{bess_info['power_mw']:.1f} MW / {bess_info['duration_hr']:.0f}hr "
                    f"({bess_info['capacity_mwh']:.1f} MWh)",
                ],
                ["BESS Winner \u2014 Combined NPV", _fmt_currency_int(combined["npv"])],
                ["BESS Winner \u2014 Solar NPV", _fmt_currency_int(combined["solar_npv"])],
                ["BESS Winner \u2014 BESS NPV", _fmt_currency_int(combined["bess_npv"])],
            ])

    return rows
