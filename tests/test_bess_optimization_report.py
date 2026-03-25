"""Tests for BESS optimization report section row builders.

Tests build_bess_optimization_summary_rows() formatting and conditional
logic, without rendering actual PDFs.
"""

import pytest

from src.bess.report_section import (
    build_bess_optimization_summary_rows,
    build_bess_summary_rows,
)


# ---------------------------------------------------------------------------
# Test data factories
# ---------------------------------------------------------------------------


def _make_optimization_summary() -> dict:
    """Build a minimal summary dict with both bess_sizing and bess_dispatch."""
    return {
        "bess_sizing": {
            "optimal_power_mw": 2.5,
            "optimal_duration_hr": 4.0,
            "optimal_capacity_kwh": 10000.0,
            "bess_npv": 75000.0,
            "total_project_npv": 200000.0,
            "system_lcoe_per_kwh": 0.0523,
            "total_installed_cost": 600000.0,
            "solar_cost": 350000.0,
            "bess_cost": 250000.0,
            "total_annual_savings": 55000.0,
            "bess_incremental_savings": 12000.0,
            "annual_production_mwh": 25000.0,
            "lifetime_generation_mwh": 500000.0,
            "combos_evaluated": 24,
            "project_lifetime_years": 25,
            "discount_rate_pct": 7.0,
            "rate_escalation_pct": 2.5,
            "sweep_results": [],
        },
        "bess_dispatch": {
            "bess_strategy": "tou_arbitrage",
            "charging_source": "any",
            "bess_power_mw": 2.5,
            "bess_duration_hr": 4.0,
            "bess_capacity_kwh": 10000.0,
            "solar_only_annual_bill": 80000.0,
            "solar_plus_bess_annual_bill": 68000.0,
            "bess_incremental_savings": 12000.0,
            "bess_demand_savings": 8000.0,
            "bess_energy_savings": 4000.0,
            "annual_cycles": 300.0,
            "capacity_utilization_pct": 85.0,
            "estimated_annual_degradation_pct": 1.5,
            "total_curtailed_kwh": 100.0,
            "total_export_kwh": 0.0,
            "total_export_hours": 0,
            "annual_throughput_kwh": 2400000.0,
            "average_daily_cycles": 0.82,
        },
    }


def _make_single_dispatch_summary() -> dict:
    """Build a dispatch-only summary dict (no bess_sizing)."""
    return {
        "bess_dispatch": {
            "bess_power_mw": 1.0,
            "bess_duration_hr": 2.0,
            "bess_capacity_kwh": 2000.0,
            "bess_strategy": "peak_shaving",
            "charging_source": "any",
            "solar_only_annual_bill": 90000.0,
            "solar_plus_bess_annual_bill": 82000.0,
            "bess_incremental_savings": 8000.0,
            "bess_demand_savings": 6000.0,
            "bess_energy_savings": 2000.0,
            "annual_cycles": 250.0,
            "capacity_utilization_pct": 70.0,
            "estimated_annual_degradation_pct": 1.2,
            "total_curtailed_kwh": 50.0,
            "total_export_kwh": 0.0,
            "total_export_hours": 0,
            "annual_throughput_kwh": 500000.0,
            "average_daily_cycles": 0.68,
        },
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBESSOptimizationSummaryRows:
    """Tests for build_bess_optimization_summary_rows()."""

    def test_returns_correct_row_count(self) -> None:
        """Optimization summary should have 14 rows (header + 13 data rows)."""
        summary = _make_optimization_summary()
        rows = build_bess_optimization_summary_rows(summary)
        # Header + Optimal Battery Size + Strategy + Charging Source
        # + Total Project NPV + BESS NPV + System LCOE
        # + Total Installed Cost + Year 1 Total Savings
        # + Year 1 BESS Incremental Savings + Combos Evaluated
        # + Project Lifetime + Discount Rate + Rate Escalation
        assert len(rows) == 14

    def test_all_expected_labels_present(self) -> None:
        """All required labels should appear in the optimization rows."""
        summary = _make_optimization_summary()
        rows = build_bess_optimization_summary_rows(summary)
        labels = [row[0] for row in rows]

        expected_labels = [
            "Battery Storage — Sizing Optimization",
            "Optimal Battery Size",
            "Strategy",
            "Charging Source",
            "Total Project NPV",
            "BESS NPV",
            "System LCOE",
            "Total Installed Cost",
            "Year 1 Total Savings",
            "Year 1 BESS Incremental Savings",
            "Combos Evaluated",
            "Project Lifetime",
            "Discount Rate",
            "Rate Escalation",
        ]
        assert labels == expected_labels

    def test_battery_size_formatting(self) -> None:
        """Optimal Battery Size value should include MW, hr, and kWh."""
        summary = _make_optimization_summary()
        rows = build_bess_optimization_summary_rows(summary)
        size_row = rows[1]
        assert size_row[0] == "Optimal Battery Size"
        assert "2.5 MW" in size_row[1]
        assert "4.0 hr" in size_row[1]
        assert "10,000 kWh" in size_row[1]

    def test_npv_formatting_positive(self) -> None:
        """Positive NPV values should be formatted with $ and commas."""
        summary = _make_optimization_summary()
        rows = build_bess_optimization_summary_rows(summary)
        row_dict = {r[0]: r[1] for r in rows}
        assert row_dict["Total Project NPV"] == "$200,000"
        assert row_dict["BESS NPV"] == "$75,000"

    def test_npv_formatting_negative(self) -> None:
        """Negative NPV values should show negative sign correctly."""
        summary = _make_optimization_summary()
        summary["bess_sizing"]["bess_npv"] = -25000.0
        summary["bess_sizing"]["total_project_npv"] = -10000.0
        rows = build_bess_optimization_summary_rows(summary)
        row_dict = {r[0]: r[1] for r in rows}
        assert row_dict["Total Project NPV"] == "($10,000)"
        assert row_dict["BESS NPV"] == "($25,000)"

    def test_lcoe_formatting(self) -> None:
        """LCOE should show 4 decimal places in $/kWh."""
        summary = _make_optimization_summary()
        rows = build_bess_optimization_summary_rows(summary)
        row_dict = {r[0]: r[1] for r in rows}
        assert row_dict["System LCOE"] == "$0.0523/kWh"

    def test_lcoe_none_shows_na(self) -> None:
        """System LCOE should display 'N/A' when value is None."""
        summary = _make_optimization_summary()
        summary["bess_sizing"]["system_lcoe_per_kwh"] = None
        rows = build_bess_optimization_summary_rows(summary)
        row_dict = {r[0]: r[1] for r in rows}
        assert row_dict["System LCOE"] == "N/A"

    def test_charging_source_solar_only(self) -> None:
        """Solar-only charging should display 'Solar Only'."""
        summary = _make_optimization_summary()
        summary["bess_dispatch"]["charging_source"] = "solar_only"
        rows = build_bess_optimization_summary_rows(summary)
        row_dict = {r[0]: r[1] for r in rows}
        assert row_dict["Charging Source"] == "Solar Only"

    def test_charging_source_grid_only(self) -> None:
        """Grid-only charging should display 'Grid Only'."""
        summary = _make_optimization_summary()
        summary["bess_dispatch"]["charging_source"] = "grid_only"
        rows = build_bess_optimization_summary_rows(summary)
        row_dict = {r[0]: r[1] for r in rows}
        assert row_dict["Charging Source"] == "Grid Only"

    def test_charging_source_any_shows_grid_plus_solar(self) -> None:
        """Default 'any' charging should display 'Grid + Solar'."""
        summary = _make_optimization_summary()
        rows = build_bess_optimization_summary_rows(summary)
        row_dict = {r[0]: r[1] for r in rows}
        assert row_dict["Charging Source"] == "Grid + Solar"

    def test_financial_parameters_displayed(self) -> None:
        """Project lifetime, discount rate, and rate escalation should appear."""
        summary = _make_optimization_summary()
        rows = build_bess_optimization_summary_rows(summary)
        row_dict = {r[0]: r[1] for r in rows}
        assert row_dict["Project Lifetime"] == "25 years"
        assert row_dict["Discount Rate"] == "7.0%"
        assert row_dict["Rate Escalation"] == "2.5%"

    def test_combos_evaluated_as_string(self) -> None:
        """Combos evaluated should be formatted as a plain integer string."""
        summary = _make_optimization_summary()
        rows = build_bess_optimization_summary_rows(summary)
        row_dict = {r[0]: r[1] for r in rows}
        assert row_dict["Combos Evaluated"] == "24"


class TestFallbackToSingleDispatch:
    """Test that single-dispatch rows are used when bess_sizing is absent."""

    def test_single_dispatch_uses_existing_builder(self) -> None:
        """build_bess_summary_rows() is used for dispatch-only summary
        and has different header and row structure."""
        summary = _make_single_dispatch_summary()
        # Single dispatch uses build_bess_summary_rows directly
        rows = build_bess_summary_rows(summary["bess_dispatch"])
        labels = [r[0] for r in rows]
        # Existing builder has "Battery Storage Summary" header
        assert "Battery Storage Summary" in labels
        # And does NOT have optimization-specific labels
        assert "Optimal Battery Size" not in labels
        assert "Total Project NPV" not in labels
        assert "Combos Evaluated" not in labels

    def test_optimization_builder_not_used_without_bess_sizing(self) -> None:
        """Calling optimization builder requires 'bess_sizing' key in summary."""
        summary = _make_single_dispatch_summary()
        assert "bess_sizing" not in summary
        with pytest.raises(KeyError):
            build_bess_optimization_summary_rows(summary)
