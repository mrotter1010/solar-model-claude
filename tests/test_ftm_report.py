"""Tests for FTM report section summary row builders.

Verifies that build_ftm_summary_rows and build_ftm_optimization_summary_rows
produce correct table rows from FTM pipeline summary dicts, and that the
existing BTM builders remain unchanged.
"""

import pytest

from src.bess.report_section import (
    build_bess_summary_rows,
    build_ftm_optimization_summary_rows,
    build_ftm_summary_rows,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ftm_summary(
    *,
    ancillary_revenue: float = 40000.0,
    include_sizing: bool = False,
) -> dict:
    """Build a synthetic FTM pipeline summary dict.

    Args:
        ancillary_revenue: Annual ancillary revenue (set to 0 to test exclusion).
        include_sizing: If True, include bess_sizing key for optimization.
    """
    gross = 80000.0 + 25000.0 + ancillary_revenue
    summary = {
        "bess_dispatch": {
            "dispatch_mode": "ftm",
            "bess_power_mw": 2.0,
            "bess_duration_hr": 4.0,
            "bess_capacity_kwh": 8000.0,
            "bess_strategy": "global",
            "ftm_revenue": 105000.0,
            "annual_cycles": 300.0,
            "annual_throughput_kwh": 2400000.0,
            "average_daily_cycles": 0.82,
            "capacity_utilization_pct": 85.0,
            "total_curtailed_kwh": 1500.0,
            "estimated_annual_degradation_pct": 1.5,
            "charging_source": "any",
            "monthly_solver_status": ["Optimal"] * 12,
            "heatmap_data": [[10.0] * 24 for _ in range(12)],
        },
        "lmp": {
            "iso": "caiso",
            "zone": "SP15",
            "market": "DAY_AHEAD_HOURLY",
            "year": 2024,
            "mean_lmp": 52.30,
            "min_lmp": -10.0,
            "max_lmp": 250.0,
        },
        "ftm_economics": {
            "annual_solar_revenue": 80000.0,
            "annual_bess_arbitrage_revenue": 25000.0,
            "annual_ancillary_revenue": ancillary_revenue,
            "annual_gross_revenue": gross,
            "total_project_npv": 250000.0,
            "bess_npv": 100000.0,
            "solar_npv": 150000.0,
            "system_lcoe_per_kwh": 0.0450,
            "total_installed_cost": 3000000.0,
        },
    }

    if include_sizing:
        summary["bess_sizing"] = {
            "optimal_power_mw": 2.0,
            "optimal_duration_hr": 4.0,
            "optimal_capacity_kwh": 8000.0,
            "bess_npv": 100000.0,
            "total_project_npv": 250000.0,
            "system_lcoe_per_kwh": 0.0450,
            "total_installed_cost": 3000000.0,
            "solar_cost": 800000.0,
            "bess_cost": 2200000.0,
            "total_annual_savings": 145000.0,
            "bess_incremental_savings": 25000.0,
            "annual_production_mwh": 15000.0,
            "lifetime_generation_mwh": 350000.0,
            "combos_evaluated": 12,
            "project_lifetime_years": 25,
            "discount_rate_pct": 8.0,
            "rate_escalation_pct": 2.0,
        }

    return summary


def _make_btm_bess_dispatch() -> dict:
    """Build a synthetic BTM bess_dispatch dict."""
    return {
        "bess_power_mw": 2.0,
        "bess_duration_hr": 4.0,
        "bess_capacity_kwh": 8000.0,
        "bess_strategy": "peak_shaving",
        "solar_only_annual_bill": 120000.0,
        "solar_plus_bess_annual_bill": 100000.0,
        "bess_incremental_savings": 20000.0,
        "bess_demand_savings": 15000.0,
        "bess_energy_savings": 5000.0,
        "annual_cycles": 250.0,
        "capacity_utilization_pct": 75.0,
        "estimated_annual_degradation_pct": 2.0,
        "total_curtailed_kwh": 500.0,
        "total_export_kwh": 0.0,
        "total_export_hours": 0,
        "charging_source": "any",
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBuildFTMSummaryRows:
    """Verify build_ftm_summary_rows produces correct rows."""

    def test_build_ftm_summary_rows(self) -> None:
        """Synthetic FTM summary: verify key rows present and formatted."""
        summary = _make_ftm_summary()
        rows = build_ftm_summary_rows(summary)

        # Convert to dict for easy lookup
        row_dict = {r[0]: r[1] for r in rows}

        # Header
        assert rows[0][0] == "Front-of-Meter Battery Storage Summary"

        # Battery size
        assert "2.0 MW / 4.0 hr" in row_dict["Battery Size"]
        assert "8,000 kWh" in row_dict["Battery Size"]

        # Strategy and mode
        assert row_dict["Strategy"] == "Energy Arbitrage"
        assert row_dict["Dispatch Mode"] == "Front-of-Meter (FTM)"

        # LMP info
        assert "CAISO" in row_dict["ISO / Pricing Zone"]
        assert "SP15" in row_dict["ISO / Pricing Zone"]
        assert row_dict["Market"] == "DAY_AHEAD_HOURLY"
        assert row_dict["LMP Year"] == "2024"
        assert "$52.30/MWh" in row_dict["Mean LMP"]

        # Revenue rows
        assert "$80,000" in row_dict["Annual Solar Revenue"]
        assert "$25,000" in row_dict["Annual BESS Arbitrage Revenue"]
        assert "Annual Ancillary Revenue" in row_dict
        assert "$40,000" in row_dict["Annual Ancillary Revenue"]
        assert "Annual Gross Revenue" in row_dict

        # NPV and LCOE
        assert "$250,000" in row_dict["Total Project NPV"]
        assert "$100,000" in row_dict["BESS NPV"]
        assert "$0.0450/kWh" in row_dict["System LCOE"]

        # Dispatch metrics
        assert row_dict["Annual Cycles"] == "300.0"
        assert "85.0%" in row_dict["Capacity Utilization"]
        assert "1.50%" in row_dict["Est. Annual Degradation"]
        assert "1,500 kWh" in row_dict["Curtailed Energy"]

    def test_build_ftm_summary_rows_no_ancillary(self) -> None:
        """Ancillary row excluded when ancillary_revenue = 0."""
        summary = _make_ftm_summary(ancillary_revenue=0.0)
        rows = build_ftm_summary_rows(summary)

        labels = [r[0] for r in rows]
        assert "Annual Ancillary Revenue" not in labels

        # Other revenue rows still present
        assert "Annual Solar Revenue" in labels
        assert "Annual BESS Arbitrage Revenue" in labels
        assert "Annual Gross Revenue" in labels

    def test_ftm_report_no_solar_cost(self) -> None:
        """When solar cost is missing, LCOE shows N/A and NPV is BESS only."""
        summary = _make_ftm_summary()
        # Simulate missing solar cost: LCOE = None, solar_npv = None
        summary["ftm_economics"]["system_lcoe_per_kwh"] = None
        summary["ftm_economics"]["solar_npv"] = None

        rows = build_ftm_summary_rows(summary)
        row_dict = {r[0]: r[1] for r in rows}
        labels = [r[0] for r in rows]

        # LCOE should show N/A with explanation
        assert "System LCOE" in labels
        assert "N/A" in row_dict["System LCOE"]
        assert "solar cost" in row_dict["System LCOE"].lower()

        # NPV label should indicate BESS only
        assert "Total Project NPV (BESS only)" in labels
        assert "Total Project NPV" not in labels  # Exact label not present


class TestBuildFTMOptimizationSummaryRows:
    """Verify build_ftm_optimization_summary_rows with sizing data."""

    def test_build_ftm_optimization_summary_rows(self) -> None:
        """Synthetic FTM optimization summary: verify optimization-specific rows."""
        summary = _make_ftm_summary(include_sizing=True)
        rows = build_ftm_optimization_summary_rows(summary)

        row_dict = {r[0]: r[1] for r in rows}
        labels = [r[0] for r in rows]

        # Header indicates optimization
        assert "Sizing Optimization" in rows[0][0]

        # "Optimal Battery Size" (not just "Battery Size")
        assert "Optimal Battery Size" in labels
        assert "Battery Size" not in labels

        # LMP info present
        assert "CAISO" in row_dict["ISO / Pricing Zone"]
        assert row_dict["LMP Year"] == "2024"

        # Revenue rows
        assert "Annual Solar Revenue" in labels
        assert "Annual BESS Arbitrage Revenue" in labels
        assert "Annual Gross Revenue" in labels

        # NPV and economics
        assert "$250,000" in row_dict["Total Project NPV"]
        assert "$100,000" in row_dict["BESS NPV"]
        assert "$0.0450/kWh" in row_dict["System LCOE"]
        assert "$3,000,000" in row_dict["Total Installed Cost"]

        # Optimization-specific fields
        assert row_dict["Combos Evaluated"] == "12"
        assert row_dict["Project Lifetime"] == "25 years"
        assert row_dict["Discount Rate"] == "8.0%"
        assert row_dict["Rate Escalation"] == "2.0%"

        # Dispatch metrics still present
        assert "Annual Cycles" in labels
        assert "Capacity Utilization" in labels

    def test_ftm_optimization_report_no_solar_cost(self) -> None:
        """Optimization summary: LCOE N/A and NPV BESS-only when no solar cost."""
        summary = _make_ftm_summary(include_sizing=True)
        summary["ftm_economics"]["system_lcoe_per_kwh"] = None
        summary["ftm_economics"]["solar_npv"] = None

        rows = build_ftm_optimization_summary_rows(summary)
        row_dict = {r[0]: r[1] for r in rows}
        labels = [r[0] for r in rows]

        # LCOE shows N/A with explanation
        assert "System LCOE" in labels
        assert "N/A" in row_dict["System LCOE"]
        assert "solar cost" in row_dict["System LCOE"].lower()

        # NPV label indicates BESS only
        assert "Total Project NPV (BESS only)" in labels


class TestBTMReportUnchanged:
    """Verify BTM report functions still work correctly."""

    def test_btm_report_unchanged(self) -> None:
        """build_bess_summary_rows produces correct BTM rows."""
        bd = _make_btm_bess_dispatch()
        rows = build_bess_summary_rows(bd)

        row_dict = {r[0]: r[1] for r in rows}
        labels = [r[0] for r in rows]

        # Header is BTM-style
        assert rows[0][0] == "Battery Storage Summary"

        # BTM-specific rows present
        assert "Solar-Only Annual Bill" in labels
        assert "Solar+BESS Annual Bill" in labels
        assert "BESS Incremental Savings" in labels
        assert "Demand Savings" in labels
        assert "Energy Savings" in labels

        # FTM-specific rows NOT present
        assert "Dispatch Mode" not in labels
        assert "ISO / Pricing Zone" not in labels
        assert "Annual Solar Revenue" not in labels
        assert "Annual BESS Arbitrage Revenue" not in labels

        # Values correct
        assert "$120,000" in row_dict["Solar-Only Annual Bill"]
        assert "$100,000" in row_dict["Solar+BESS Annual Bill"]
        assert "$20,000" in row_dict["BESS Incremental Savings"]
        assert row_dict["Strategy"] == "peak_shaving"
