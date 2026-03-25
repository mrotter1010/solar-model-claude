"""Tests for BESS sizing optimization (run_sizing_optimization)."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.bess.models import (
    BatteryMetrics,
    BESSBillComparison,
    BESSConfig,
    DispatchResult,
    HourlyDispatch,
    SweepComboResult,
)
from src.bess.sizing_optimizer import run_sizing_optimization
from src.config.schema import SiteConfig
from src.rates.models import BillResult, MonthlyBillDetail

# Output directory for manual inspection
_OUTPUT_DIR = Path("outputs/test_results/m14c_sizing")
_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_bill_result(annual_total: float) -> BillResult:
    """Build a minimal BillResult with the given annual total."""
    monthly_charge = annual_total / 12
    return BillResult(
        monthly=[
            MonthlyBillDetail(
                month=m,
                energy_charges=monthly_charge,
                demand_charges=0.0,
                flat_demand_charges=0.0,
                fixed_charges=0.0,
                production_kwh=0.0,
                consumption_kwh=0.0,
                peak_demand_kw=0.0,
            )
            for m in range(1, 13)
        ]
    )


def _base_site_kwargs() -> dict:
    """Return minimal valid SiteConfig kwargs."""
    return {
        "run_name": "SizingTest",
        "site_name": "SizingTestSite",
        "customer": "TestCustomer",
        "latitude": 33.45,
        "longitude": -112.07,
        "dc_size_mw": 6.25,
        "ac_installed_mw": 5.0,
        "ac_poi_mw": 5.0,
        "racking": "tracker",
        "tilt": 55.0,
        "azimuth": 180.0,
        "module_orientation": "portrait",
        "number_of_modules": 1,
        "ground_clearance_height_m": 1.5,
        "panel_model": "CSI Solar Co. Ltd. CS3U-355P",
        "bifacial": False,
        "inverter_model": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
        "gcr": 0.35,
        "shading_percent": 0.0,
        "dc_wiring_loss_percent": 2.0,
        "ac_wiring_loss_percent": 1.0,
        "transformer_losses_percent": 1.0,
        "degradation_percent": 0.5,
        "availability_percent": 2.5,
        "module_mismatch_percent": 1.0,
        "lid_percent": 1.5,
    }


def _optimization_site(**overrides: object) -> SiteConfig:
    """Build a SiteConfig with solar+BESS optimization fields."""
    kw = _base_site_kwargs()
    kw.update({
        "bess_dispatch_required": True,
        "bess_optimization_required": True,
        "bill_calculation": True,
        "utility_name": "Test Utility",
        "tariff_name": "Test Rate",
        "load_type": "MediumOffice",
        "annual_consumption_kwh": 500000.0,
        "solar_cost_per_kw_dc": 1200.0,
        "solar_cost_per_kw_ac": 200.0,
        "solar_opex_per_kw_dc_year": 15.0,
        "bess_opex_per_kw_year": 10.0,
        "discount_rate_pct": 8.0,
        "project_lifetime_years": 25,
        "rate_escalation_pct": 2.0,
        "degradation_percent": 0.5,
        "bess_installed_cost_per_kwh": 275.0,
    })
    kw.update(overrides)
    return SiteConfig(**kw)


def _make_dispatch_result(
    power_kw: float, duration_hr: float
) -> DispatchResult:
    """Build a minimal DispatchResult with metrics populated."""
    capacity_kwh = power_kw * duration_hr
    config = BESSConfig(
        bess_power_kw=power_kw,
        bess_duration_hr=duration_hr,
        capacity_kwh=capacity_kwh,
        usable_capacity_kwh=capacity_kwh * 0.8,
        min_soc_kwh=capacity_kwh * 0.1,
        max_soc_kwh=capacity_kwh * 0.9,
        charge_efficiency=0.938,
        discharge_efficiency=0.938,
        degradation_cost_per_kwh=0.0275,
        bess_cycles_warranty=5000,
        strategy="global",
    )
    metrics = BatteryMetrics(
        annual_throughput_kwh=capacity_kwh * 300,
        annual_cycles=300.0,
        average_daily_cycles=0.82,
        capacity_utilization_pct=82.0,
        average_discharge_depth_pct=70.0,
        max_discharge_depth_pct=95.0,
        total_curtailed_kwh=0.0,
        hours_charging=2000,
        hours_discharging=2000,
        hours_idle=4760,
        estimated_annual_degradation_pct=3.0,
    )
    return DispatchResult(
        config=config,
        hourly_dispatch=[
            HourlyDispatch(
                hour=h,
                charge_kw=0.0,
                discharge_kw=0.0,
                soc_kwh=capacity_kwh * 0.1,
                net_load_kw=100.0,
                curtailed_kw=0.0,
            )
            for h in range(8760)
        ],
        monthly_solve_status=["Optimal"] * 12,
        metrics=metrics,
    )


def _make_bill_comparison(savings: float) -> BESSBillComparison:
    """Build a BESSBillComparison with the given incremental savings."""
    return BESSBillComparison(
        solar_only_annual_bill=300000.0,
        solar_plus_bess_annual_bill=300000.0 - savings,
        bess_incremental_savings=savings,
        bess_demand_savings=savings * 0.6,
        bess_energy_savings=savings * 0.4,
    )


# Sentinel objects for mock args (not used by the function under test,
# but required in the signature)
_DUMMY_PRODUCTION = [100.0] * 8760
_DUMMY_RATE_SCHEDULE = "RATE_SCHEDULE_SENTINEL"
_DUMMY_LOAD_PROFILE = "LOAD_PROFILE_SENTINEL"


# ===========================================================================
# Tests
# ===========================================================================


class TestRunSizingOptimization:
    """Tests for run_sizing_optimization() with mocked dispatch."""

    def test_basic_sweep_with_mocked_dispatch(self) -> None:
        """(a) Mock dispatch returning fixed savings. Verify combo count,
        winner selection, and all SizingResult fields populated.
        """
        # Use a small grid: power 0.5-0.7 MW (3 pts), duration 2.0-2.5 hr (2 pts) = 6 combos
        site = _optimization_site(
            bess_power_min_mw=0.5,
            bess_power_max_mw=0.7,
            bess_duration_min_hr=2.0,
            bess_duration_max_hr=2.5,
        )
        fixed_savings = 20000.0

        def mock_dispatch(site_cfg, prod, rate, load, solar_bill):
            power_kw = site_cfg.bess_power_mw * 1000
            duration_hr = site_cfg.bess_duration_hr
            dispatch = _make_dispatch_result(power_kw, duration_hr)
            comparison = _make_bill_comparison(fixed_savings)
            return dispatch, comparison

        with patch(
            "src.bess.sizing_optimizer.run_bess_dispatch",
            side_effect=mock_dispatch,
        ) as mock_fn:
            result = run_sizing_optimization(
                site, _DUMMY_PRODUCTION, _DUMMY_RATE_SCHEDULE,
                _DUMMY_LOAD_PROFILE,
                _make_bill_result(300000.0),
                _make_bill_result(600000.0),
            )

        # 3 power × 2 duration = 6 combos + 1 re-run for winner = 7 calls
        assert len(result.all_combos) == 6
        assert result.economics.combos_evaluated == 6

        # Winner exists and has all fields
        assert result.winner is not None
        assert result.winner.power_mw > 0
        assert result.winner.duration_hr > 0
        assert result.winner.bess_npv is not None

        # Economics populated
        assert result.economics.total_project_npv is not None
        assert result.economics.bess_npv == result.winner.bess_npv

        # Dispatch result and bill comparison populated
        assert result.dispatch_result is not None
        assert result.dispatch_result.metrics is not None
        assert result.bill_comparison is not None
        assert result.bill_comparison.bess_incremental_savings == fixed_savings

        # Write output for inspection
        output = {
            "test": "basic_sweep_with_mocked_dispatch",
            "combos_evaluated": len(result.all_combos),
            "winner_power_mw": result.winner.power_mw,
            "winner_duration_hr": result.winner.duration_hr,
            "winner_npv": round(result.winner.bess_npv, 2),
            "total_project_npv": round(result.economics.total_project_npv, 2),
        }
        (_OUTPUT_DIR / "basic_sweep.json").write_text(json.dumps(output, indent=2))

    def test_winner_is_highest_npv_not_highest_savings(self) -> None:
        """(b) Savings proportional to power, but cost grows faster.

        Smaller BESS has better NPV despite lower absolute savings because
        cost scales with power × duration (quadratic-ish effect on payback).

        Uses duration range 3.5–4.0 hr (2 points) — schema rejects min==max
        when optimization is required.
        """
        site = _optimization_site(
            bess_power_min_mw=1.0,
            bess_power_max_mw=3.0,
            bess_duration_min_hr=3.5,
            bess_duration_max_hr=4.0,
            bess_installed_cost_per_kwh=275.0,
        )

        def mock_dispatch(site_cfg, prod, rate, load, solar_bill):
            power_mw = site_cfg.bess_power_mw
            power_kw = power_mw * 1000
            duration_hr = site_cfg.bess_duration_hr
            dispatch = _make_dispatch_result(power_kw, duration_hr)
            # Savings grow linearly with power but cost grows as power × duration × $/kWh.
            # At $275/kWh with ~4hr duration: cost = power_kw * duration * 275.
            # Savings = $15,000 per MW → undersized relative to cost for large BESS.
            savings = 15000.0 * power_mw
            comparison = _make_bill_comparison(savings)
            return dispatch, comparison

        with patch(
            "src.bess.sizing_optimizer.run_bess_dispatch",
            side_effect=mock_dispatch,
        ):
            result = run_sizing_optimization(
                site, _DUMMY_PRODUCTION, _DUMMY_RATE_SCHEDULE,
                _DUMMY_LOAD_PROFILE,
                _make_bill_result(300000.0),
                _make_bill_result(600000.0),
            )

        # Find the combo with highest savings and the winner
        max_savings_combo = max(result.all_combos, key=lambda c: c.bess_incremental_savings)
        winner = result.winner

        # Winner should have highest NPV
        assert winner.bess_npv >= max(c.bess_npv for c in result.all_combos)

        # Winner should NOT be the one with highest savings (3 MW has highest savings
        # but worst NPV due to high cost vs modest savings)
        assert winner.power_mw < max_savings_combo.power_mw

        # Write output for inspection
        combos_summary = [
            {
                "power_mw": c.power_mw,
                "duration_hr": c.duration_hr,
                "savings": round(c.bess_incremental_savings, 2),
                "installed_cost": round(c.installed_cost, 2),
                "npv": round(c.bess_npv, 2),
            }
            for c in sorted(result.all_combos, key=lambda c: c.bess_npv, reverse=True)
        ]
        output = {
            "test": "winner_is_highest_npv_not_highest_savings",
            "winner_power_mw": winner.power_mw,
            "winner_npv": round(winner.bess_npv, 2),
            "max_savings_power_mw": max_savings_combo.power_mw,
            "max_savings_npv": round(max_savings_combo.bess_npv, 2),
            "all_combos": combos_summary,
        }
        (_OUTPUT_DIR / "winner_selection.json").write_text(json.dumps(output, indent=2))

    def test_failed_combo_is_skipped(self) -> None:
        """(c) One combo fails dispatch. Verify it's skipped, others succeed."""
        site = _optimization_site(
            bess_power_min_mw=0.5,
            bess_power_max_mw=0.7,
            bess_duration_min_hr=2.0,
            bess_duration_max_hr=2.5,
        )
        # 3 power points × 2 duration = 6 combos

        call_count = 0

        def mock_dispatch(site_cfg, prod, rate, load, solar_bill):
            nonlocal call_count
            call_count += 1
            # Fail on the second combo (0.6 MW)
            if abs(site_cfg.bess_power_mw - 0.6) < 0.01:
                raise RuntimeError("Simulated dispatch failure")
            power_kw = site_cfg.bess_power_mw * 1000
            duration_hr = site_cfg.bess_duration_hr
            dispatch = _make_dispatch_result(power_kw, duration_hr)
            comparison = _make_bill_comparison(20000.0)
            return dispatch, comparison

        with patch(
            "src.bess.sizing_optimizer.run_bess_dispatch",
            side_effect=mock_dispatch,
        ):
            result = run_sizing_optimization(
                site, _DUMMY_PRODUCTION, _DUMMY_RATE_SCHEDULE,
                _DUMMY_LOAD_PROFILE,
                _make_bill_result(300000.0),
                _make_bill_result(600000.0),
            )

        # 4 successful combos (0.5 and 0.7 MW × 2 durations), 0.6 MW skipped
        assert len(result.all_combos) == 4
        evaluated_powers = {c.power_mw for c in result.all_combos}
        assert 0.6 not in evaluated_powers
        assert 0.5 in evaluated_powers
        assert 0.7 in evaluated_powers

        # Winner selected from successful combos
        assert result.winner is not None

    def test_all_combos_fail_raises_error(self) -> None:
        """(d) All combos fail dispatch → ValueError raised."""
        site = _optimization_site(
            bess_power_min_mw=0.5,
            bess_power_max_mw=0.6,
            bess_duration_min_hr=2.0,
            bess_duration_max_hr=2.5,
        )
        # 2 power × 2 duration = 4 combos, all will fail

        def mock_dispatch(site_cfg, prod, rate, load, solar_bill):
            raise RuntimeError("Simulated dispatch failure")

        with patch(
            "src.bess.sizing_optimizer.run_bess_dispatch",
            side_effect=mock_dispatch,
        ):
            with pytest.raises(ValueError, match="All sizing combos failed dispatch"):
                run_sizing_optimization(
                    site, _DUMMY_PRODUCTION, _DUMMY_RATE_SCHEDULE,
                    _DUMMY_LOAD_PROFILE,
                    _make_bill_result(300000.0),
                    _make_bill_result(600000.0),
                )

    def test_dispatch_called_n_plus_one_times(self) -> None:
        """(e) Dispatch is called N times for sweep + 1 re-run for winner."""
        site = _optimization_site(
            bess_power_min_mw=0.5,
            bess_power_max_mw=0.7,
            bess_duration_min_hr=2.0,
            bess_duration_max_hr=2.5,
        )
        # 3 power × 2 duration = 6 combos

        def mock_dispatch(site_cfg, prod, rate, load, solar_bill):
            power_kw = site_cfg.bess_power_mw * 1000
            duration_hr = site_cfg.bess_duration_hr
            dispatch = _make_dispatch_result(power_kw, duration_hr)
            comparison = _make_bill_comparison(20000.0)
            return dispatch, comparison

        with patch(
            "src.bess.sizing_optimizer.run_bess_dispatch",
            side_effect=mock_dispatch,
        ) as mock_fn:
            result = run_sizing_optimization(
                site, _DUMMY_PRODUCTION, _DUMMY_RATE_SCHEDULE,
                _DUMMY_LOAD_PROFILE,
                _make_bill_result(300000.0),
                _make_bill_result(600000.0),
            )

        # 6 sweep calls + 1 winner re-run = 7
        assert mock_fn.call_count == 7
        assert len(result.all_combos) == 6
