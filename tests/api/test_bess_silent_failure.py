"""Tests for BESS silent failure chain fixes (M21 Prompt 7).

Validates:
1. run_bess tool definition requires ["site", "system", "bess", "bill"].
2. bess object requires ["power_mw", "duration_hr"].
3. extract_bess_response returns bess_status="skipped" when bess_dispatch missing.
4. charging_source reflects actual dispatch config when dispatch runs.
"""

from pathlib import Path

import pytest

from src.api.runner import extract_bess_response


# ---------------------------------------------------------------------------
# Helper: minimal pipeline results dict
# ---------------------------------------------------------------------------


def _base_summary(*, include_bess_dispatch: bool = False) -> dict:
    """Build a minimal pipeline summary dict.

    Args:
        include_bess_dispatch: If True, include a realistic bess_dispatch key.
    """
    summary: dict = {
        "site_name": "TestSite",
        "run_name": "test_bess_run",
        "customer": "API",
        "weather_year": "2023",
        "dc_size_mw": 5.0,
        "ac_installed_mw": 4.0,
        "ac_poi_mw": 4.0,
        "panel_model": "TestModule",
        "inverter_model": "TestInverter",
        "racking": "tracker",
        "tilt": 25.0,
        "azimuth": 180.0,
        "annual_energy_mwh": 8000.0,
        "net_capacity_factor": 0.25,
        "specific_yield": 1600.0,
        "performance_ratio": 0.82,
        "shading_pct_applied": 0.0,
        "simulation_timestamp": "2024-01-01T00:00:00+00:00",
        "loss_data": {},
        "errors": [],
    }
    if include_bess_dispatch:
        summary["bess_dispatch"] = {
            "bess_power_mw": 2.5,
            "bess_duration_hr": 4.0,
            "bess_capacity_kwh": 10000.0,
            "bess_strategy": "peak_shaving",
            "annual_cycles": 280.0,
            "annual_throughput_kwh": 2_800_000.0,
            "capacity_utilization_pct": 72.5,
            "total_curtailed_kwh": 150.0,
            "estimated_annual_degradation_pct": 1.2,
            "charging_source": "solar_only",
            "solar_only_annual_bill": 50000.0,
            "solar_plus_bess_annual_bill": 38000.0,
            "bess_incremental_savings": 12000.0,
            "bess_demand_savings": 8000.0,
            "bess_energy_savings": 4000.0,
        }
    return summary


def _wrap_results(summary: dict) -> dict:
    """Wrap a summary dict into the pipeline results structure."""
    return {
        "total_sites": 1,
        "successful": 1,
        "failed": 0,
        "timeseries_files": [],
        "summaries": [summary],
        "error_files": [],
        "report_files": [],
    }


# ---------------------------------------------------------------------------
# 1. Tool definition required arrays
# ---------------------------------------------------------------------------


class TestToolDefinitionRequired:
    """run_bess tool definition has correct required arrays."""

    def test_run_bess_required_excludes_bill(self) -> None:
        """run_bess top-level required has site/system/bess but NOT bill.

        Bill is conditionally required for BTM only (enforced via
        description, not the JSON Schema required array) so that FTM
        dispatch can omit it without a 422.
        """
        from orchestrator.tools.definitions import TOOL_DEFINITIONS

        run_bess = next(
            t for t in TOOL_DEFINITIONS
            if t["function"]["name"] == "run_bess"
        )
        required = run_bess["function"]["parameters"]["required"]
        assert "bill" not in required
        assert set(required) == {"site", "system", "bess"}

    def test_bess_object_requires_power_and_duration(self) -> None:
        """bess object within run_bess requires power_mw and duration_hr."""
        from orchestrator.tools.definitions import TOOL_DEFINITIONS

        run_bess = next(
            t for t in TOOL_DEFINITIONS
            if t["function"]["name"] == "run_bess"
        )
        bess_prop = run_bess["function"]["parameters"]["properties"]["bess"]
        required = bess_prop["required"]
        assert set(required) == {"power_mw", "duration_hr"}


# ---------------------------------------------------------------------------
# 3. extract_bess_response: skipped dispatch
# ---------------------------------------------------------------------------


class TestBESSSkippedDispatch:
    """extract_bess_response surfaces skipped dispatch clearly."""

    def test_skipped_status_when_bess_dispatch_missing(self) -> None:
        """Missing bess_dispatch key → bess_status='skipped'."""
        summary = _base_summary(include_bess_dispatch=False)
        results = _wrap_results(summary)

        response = extract_bess_response(
            results, "test_bess_run", Path("/tmp"), is_ftm=False,
            is_optimization=False,
        )

        assert response.dispatch.bess_status == "skipped"

    def test_skipped_error_message(self) -> None:
        """Skipped dispatch includes descriptive error message."""
        summary = _base_summary(include_bess_dispatch=False)
        results = _wrap_results(summary)

        response = extract_bess_response(
            results, "test_bess_run", Path("/tmp"), is_ftm=False,
            is_optimization=False,
        )

        assert response.dispatch.bess_error is not None
        assert "bill calculation" in response.dispatch.bess_error.lower()

    def test_skipped_numerics_are_zero(self) -> None:
        """Skipped dispatch returns 0.0 for all numeric fields."""
        summary = _base_summary(include_bess_dispatch=False)
        results = _wrap_results(summary)

        response = extract_bess_response(
            results, "test_bess_run", Path("/tmp"), is_ftm=False,
            is_optimization=False,
        )

        d = response.dispatch
        assert d.bess_power_mw == 0.0
        assert d.bess_duration_hr == 0.0
        assert d.bess_capacity_kwh == 0.0
        assert d.annual_cycles == 0.0
        assert d.annual_throughput_kwh == 0.0
        assert d.capacity_utilization_pct == 0.0
        assert d.total_curtailed_kwh == 0.0
        assert d.estimated_annual_degradation_pct == 0.0

    def test_skipped_charging_source_is_unknown(self) -> None:
        """Skipped dispatch sets charging_source='unknown'."""
        summary = _base_summary(include_bess_dispatch=False)
        results = _wrap_results(summary)

        response = extract_bess_response(
            results, "test_bess_run", Path("/tmp"), is_ftm=False,
            is_optimization=False,
        )

        assert response.dispatch.charging_source == "unknown"

    def test_skipped_production_still_returned(self) -> None:
        """Solar production metrics are still returned when dispatch skipped."""
        summary = _base_summary(include_bess_dispatch=False)
        results = _wrap_results(summary)

        response = extract_bess_response(
            results, "test_bess_run", Path("/tmp"), is_ftm=False,
            is_optimization=False,
        )

        assert response.status == "success"
        assert response.production.annual_energy_mwh == 8000.0
        assert response.site_name == "TestSite"


# ---------------------------------------------------------------------------
# 4. charging_source reflects actual config when dispatch runs
# ---------------------------------------------------------------------------


class TestChargingSourceFromDispatch:
    """charging_source reflects actual dispatch result when dispatch runs."""

    def test_solar_only_charging_source(self) -> None:
        """Dispatch with solar_only → charging_source='solar_only'."""
        summary = _base_summary(include_bess_dispatch=True)
        # Already has charging_source="solar_only"
        results = _wrap_results(summary)

        response = extract_bess_response(
            results, "test_bess_run", Path("/tmp"), is_ftm=False,
            is_optimization=False,
        )

        assert response.dispatch.charging_source == "solar_only"

    def test_any_charging_source(self) -> None:
        """Dispatch with any → charging_source='any'."""
        summary = _base_summary(include_bess_dispatch=True)
        summary["bess_dispatch"]["charging_source"] = "any"
        results = _wrap_results(summary)

        response = extract_bess_response(
            results, "test_bess_run", Path("/tmp"), is_ftm=False,
            is_optimization=False,
        )

        assert response.dispatch.charging_source == "any"

    def test_successful_dispatch_status(self) -> None:
        """Successful dispatch → bess_status='success', no error."""
        summary = _base_summary(include_bess_dispatch=True)
        results = _wrap_results(summary)

        response = extract_bess_response(
            results, "test_bess_run", Path("/tmp"), is_ftm=False,
            is_optimization=False,
        )

        assert response.dispatch.bess_status == "success"
        assert response.dispatch.bess_error is None

    def test_successful_dispatch_metrics(self) -> None:
        """Successful dispatch → metrics populated from summary."""
        summary = _base_summary(include_bess_dispatch=True)
        results = _wrap_results(summary)

        response = extract_bess_response(
            results, "test_bess_run", Path("/tmp"), is_ftm=False,
            is_optimization=False,
        )

        d = response.dispatch
        assert d.bess_power_mw == 2.5
        assert d.bess_duration_hr == 4.0
        assert d.bess_capacity_kwh == 10000.0
        assert d.bess_strategy == "peak_shaving"
        assert d.annual_cycles == 280.0


# ---------------------------------------------------------------------------
# 5. Dispatch exception → bess_status="error" with actual message
# ---------------------------------------------------------------------------


class TestBESSDispatchError:
    """extract_bess_response differentiates dispatch exception from skipped."""

    def test_error_status_when_dispatch_exception(self) -> None:
        """bess_dispatch_error in summary → bess_status='error'."""
        summary = _base_summary(include_bess_dispatch=False)
        summary["bess_dispatch_error"] = (
            "unsupported operand type(s) for /: 'LpVariable' and 'float'"
        )
        results = _wrap_results(summary)

        response = extract_bess_response(
            results, "test_bess_run", Path("/tmp"), is_ftm=False,
            is_optimization=False,
        )

        assert response.dispatch.bess_status == "error"

    def test_error_message_contains_exception(self) -> None:
        """Error message includes the actual exception text."""
        summary = _base_summary(include_bess_dispatch=False)
        summary["bess_dispatch_error"] = "ZeroDivisionError: division by zero"
        results = _wrap_results(summary)

        response = extract_bess_response(
            results, "test_bess_run", Path("/tmp"), is_ftm=False,
            is_optimization=False,
        )

        assert response.dispatch.bess_error is not None
        assert "ZeroDivisionError" in response.dispatch.bess_error
        assert "BESS dispatch failed" in response.dispatch.bess_error

    def test_error_numerics_are_zero(self) -> None:
        """Dispatch error returns 0.0 for all numeric fields."""
        summary = _base_summary(include_bess_dispatch=False)
        summary["bess_dispatch_error"] = "some error"
        results = _wrap_results(summary)

        response = extract_bess_response(
            results, "test_bess_run", Path("/tmp"), is_ftm=False,
            is_optimization=False,
        )

        d = response.dispatch
        assert d.bess_power_mw == 0.0
        assert d.bess_capacity_kwh == 0.0
        assert d.annual_cycles == 0.0

    def test_error_vs_skipped_are_distinct(self) -> None:
        """Error (dispatch threw) and skipped (dispatch never ran) are different statuses."""
        # Error case: dispatch_error present
        error_summary = _base_summary(include_bess_dispatch=False)
        error_summary["bess_dispatch_error"] = "LP solver failed"
        error_results = _wrap_results(error_summary)

        error_response = extract_bess_response(
            error_results, "test_bess_run", Path("/tmp"), is_ftm=False,
            is_optimization=False,
        )

        # Skipped case: no dispatch_error, no bess_dispatch
        skipped_summary = _base_summary(include_bess_dispatch=False)
        skipped_results = _wrap_results(skipped_summary)

        skipped_response = extract_bess_response(
            skipped_results, "test_bess_run", Path("/tmp"), is_ftm=False,
            is_optimization=False,
        )

        assert error_response.dispatch.bess_status == "error"
        assert skipped_response.dispatch.bess_status == "skipped"
        assert error_response.dispatch.bess_status != skipped_response.dispatch.bess_status

    def test_error_production_still_returned(self) -> None:
        """Solar production is still returned when dispatch errors."""
        summary = _base_summary(include_bess_dispatch=False)
        summary["bess_dispatch_error"] = "some LP error"
        results = _wrap_results(summary)

        response = extract_bess_response(
            results, "test_bess_run", Path("/tmp"), is_ftm=False,
            is_optimization=False,
        )

        assert response.status == "success"
        assert response.production.annual_energy_mwh == 8000.0
