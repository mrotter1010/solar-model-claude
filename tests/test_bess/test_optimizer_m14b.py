"""Tests for M14b optimizer features: NEM export, ITC solar-only, grid-only."""

import json
import math
from pathlib import Path

import pytest

from src.bess.models import BESSConfig
from src.bess.optimizer import MonthSolveResult, solve_month


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EFF = math.sqrt(0.88)  # ~0.9381 one-way efficiency
N_HOURS = 720  # 30-day month


def _make_config(
    strategy: str = "global",
    degradation_cost_per_kwh: float = 0.001,
) -> BESSConfig:
    """Build a 500 kW / 4hr BESSConfig with low degradation cost."""
    power_kw = 500.0
    duration_hr = 4.0
    capacity_kwh = power_kw * duration_hr  # 2000
    min_soc_pct = 10.0
    max_soc_pct = 90.0
    return BESSConfig(
        bess_power_kw=power_kw,
        bess_duration_hr=duration_hr,
        capacity_kwh=capacity_kwh,
        usable_capacity_kwh=capacity_kwh * (max_soc_pct - min_soc_pct) / 100,
        min_soc_kwh=capacity_kwh * min_soc_pct / 100,
        max_soc_kwh=capacity_kwh * max_soc_pct / 100,
        charge_efficiency=_EFF,
        discharge_efficiency=_EFF,
        degradation_cost_per_kwh=degradation_cost_per_kwh,
        strategy=strategy,
    )


def _flat_energy_rates(n_hours: int, rate: float = 0.15) -> list[float]:
    """Flat energy rate for every hour."""
    return [rate] * n_hours


def _simple_demand_setup(n_hours: int) -> tuple[dict[str, list[int]], dict[str, float], float]:
    """Simple demand structure: one TOU period covering all hours."""
    demand_periods = {"on_peak": list(range(n_hours))}
    demand_rates = {"on_peak": 10.0}
    flat_demand_rate = 5.0
    return demand_periods, demand_rates, flat_demand_rate


def _solar_profile(n_hours: int, day_kw: float = 800.0, night_kw: float = 0.0) -> list[float]:
    """Simple solar profile: day_kw during hours 8-17, night_kw otherwise."""
    daily = (
        [night_kw] * 8      # hours 0-7
        + [day_kw] * 10      # hours 8-17
        + [night_kw] * 6     # hours 18-23
    )
    n_days = n_hours // 24
    return daily * n_days


def _constant_load(n_hours: int, kw: float = 500.0) -> list[float]:
    """Constant load for every hour."""
    return [kw] * n_hours


# ---------------------------------------------------------------------------
# Baseline (no new features)
# ---------------------------------------------------------------------------


class TestBaseline:
    """Verify baseline behavior with all new parameters at defaults."""

    def test_baseline_net_demand_non_negative(self) -> None:
        """With nem=None, net_demand stays >= 0 (no export)."""
        config = _make_config()
        load = _constant_load(N_HOURS)
        prod = _solar_profile(N_HOURS)
        rates = _flat_energy_rates(N_HOURS)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)

        result = solve_month(
            load_kw=load,
            production_kw=prod,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
        )

        assert result.solver_status == "Optimal"
        # net_demand >= 0 for all hours (no export)
        assert all(v >= -0.01 for v in result.net_load_kw)
        # export_kw is empty list when NEM is not active
        assert result.export_kw == []

    def test_baseline_export_kw_empty(self) -> None:
        """export_kw defaults to empty list in MonthSolveResult."""
        result = MonthSolveResult(
            charge_kw=[0.0],
            discharge_kw=[0.0],
            soc_kwh=[0.0],
            net_load_kw=[0.0],
            curtailed_kw=[0.0],
            peak_demand_by_period={},
            peak_demand_flat=0.0,
            solver_status="Optimal",
            final_soc_kwh=0.0,
        )
        assert result.export_kw == []


# ---------------------------------------------------------------------------
# NEM export
# ---------------------------------------------------------------------------


class TestNEMExport:
    """Test NEM export behavior when nem_export_rates is provided."""

    def test_nem_enables_negative_net_demand(self) -> None:
        """With NEM and excess production, net_demand goes negative."""
        config = _make_config()
        # Production always exceeds load → always excess
        load = _constant_load(N_HOURS, kw=300.0)
        prod = _solar_profile(N_HOURS, day_kw=800.0)
        rates = _flat_energy_rates(N_HOURS)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)
        nem_rates = [0.08] * N_HOURS  # $0.08/kWh export

        result = solve_month(
            load_kw=load,
            production_kw=prod,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            nem_export_rates=nem_rates,
        )

        assert result.solver_status == "Optimal"
        # Some hours should have negative net_demand (exporting)
        negative_hours = [v for v in result.net_load_kw if v < -0.01]
        assert len(negative_hours) > 0, "Expected some negative net_demand hours"
        # export_kw should have non-zero values
        assert len(result.export_kw) == N_HOURS
        total_export = sum(result.export_kw)
        assert total_export > 0, "Expected positive total export"

    def test_nem_export_captures_negative_net_demand(self) -> None:
        """export_kw[t] >= -net_demand[t] for hours with negative net_demand."""
        config = _make_config()
        load = _constant_load(N_HOURS, kw=300.0)
        prod = _solar_profile(N_HOURS, day_kw=800.0)
        rates = _flat_energy_rates(N_HOURS)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)
        nem_rates = [0.08] * N_HOURS

        result = solve_month(
            load_kw=load,
            production_kw=prod,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            nem_export_rates=nem_rates,
        )

        assert result.solver_status == "Optimal"
        for t in range(N_HOURS):
            if result.net_load_kw[t] < -0.01:
                # export should capture the negative net_demand
                assert result.export_kw[t] >= -result.net_load_kw[t] - 0.1, (
                    f"Hour {t}: export {result.export_kw[t]:.1f} < "
                    f"-net_demand {-result.net_load_kw[t]:.1f}"
                )

    def test_nem_prefers_export_over_curtailment(self) -> None:
        """With NEM revenue, curtailment should be near zero (export is preferred)."""
        config = _make_config()
        load = _constant_load(N_HOURS, kw=300.0)
        prod = _solar_profile(N_HOURS, day_kw=800.0)
        rates = _flat_energy_rates(N_HOURS)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)
        nem_rates = [0.08] * N_HOURS

        result = solve_month(
            load_kw=load,
            production_kw=prod,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            nem_export_rates=nem_rates,
        )

        assert result.solver_status == "Optimal"
        total_curtailed = sum(result.curtailed_kw)
        total_export = sum(result.export_kw)
        # Export should dominate; curtailment should be negligible
        assert total_curtailed < 1.0, (
            f"Expected near-zero curtailment with NEM, got {total_curtailed:.1f} kWh"
        )
        assert total_export > 100.0, "Expected significant export with NEM"

    def test_nem_peak_demand_stays_non_negative(self) -> None:
        """Peak demand variables remain >= 0 even with negative net_demand."""
        config = _make_config()
        load = _constant_load(N_HOURS, kw=300.0)
        prod = _solar_profile(N_HOURS, day_kw=800.0)
        rates = _flat_energy_rates(N_HOURS)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)
        nem_rates = [0.08] * N_HOURS

        result = solve_month(
            load_kw=load,
            production_kw=prod,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            nem_export_rates=nem_rates,
        )

        assert result.solver_status == "Optimal"
        # Peak demand values must be >= 0 (variable lower bound)
        for p, val in result.peak_demand_by_period.items():
            assert val >= -0.01, f"Peak {p} went negative: {val}"
        assert result.peak_demand_flat >= -0.01


class TestNEMExportRevenueIncentive:
    """Higher export rate should incentivize more export."""

    def test_high_rate_exports_more(self) -> None:
        """Higher NEM rate leads to more total export.

        Both export rates must be <= energy rate to avoid LP arbitrage
        (buy at retail, sell at NEM). Real-world NEM rates are always
        at or below the retail energy rate.
        """
        config = _make_config()
        load = _constant_load(N_HOURS, kw=500.0)
        prod = _solar_profile(N_HOURS, day_kw=800.0)
        rates = _flat_energy_rates(N_HOURS, rate=0.15)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)

        # Low export rate ($0.02/kWh — minimal NEM value)
        result_low = solve_month(
            load_kw=load,
            production_kw=prod,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            nem_export_rates=[0.02] * N_HOURS,
        )

        # High export rate ($0.12/kWh — generous NEM, still below retail)
        result_high = solve_month(
            load_kw=load,
            production_kw=prod,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            nem_export_rates=[0.12] * N_HOURS,
        )

        assert result_low.solver_status == "Optimal"
        assert result_high.solver_status == "Optimal"

        total_export_low = sum(result_low.export_kw)
        total_export_high = sum(result_high.export_kw)

        assert total_export_high >= total_export_low, (
            f"High rate export ({total_export_high:.0f}) should be >= "
            f"low rate export ({total_export_low:.0f})"
        )


# ---------------------------------------------------------------------------
# ITC solar-only charging
# ---------------------------------------------------------------------------


class TestITCSolarOnlyCharging:
    """Battery can only charge from excess solar production."""

    def test_no_charge_when_production_below_load(self) -> None:
        """charge[t] == 0 for hours where production <= load."""
        config = _make_config()
        load = _constant_load(N_HOURS, kw=500.0)
        # Day: 800 kW (excess = 300), Night: 0 kW (no excess)
        prod = _solar_profile(N_HOURS, day_kw=800.0, night_kw=0.0)
        rates = _flat_energy_rates(N_HOURS)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)

        result = solve_month(
            load_kw=load,
            production_kw=prod,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            solar_only_charging=True,
        )

        assert result.solver_status == "Optimal"
        for t in range(N_HOURS):
            if prod[t] <= load[t]:
                assert result.charge_kw[t] < 0.1, (
                    f"Hour {t}: charge={result.charge_kw[t]:.1f} but "
                    f"production ({prod[t]}) <= load ({load[t]})"
                )

    def test_charge_bounded_by_excess_solar(self) -> None:
        """charge[t] <= max(production[t] - load[t], 0) for all hours."""
        config = _make_config()
        load = _constant_load(N_HOURS, kw=500.0)
        prod = _solar_profile(N_HOURS, day_kw=800.0)
        rates = _flat_energy_rates(N_HOURS)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)

        result = solve_month(
            load_kw=load,
            production_kw=prod,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            solar_only_charging=True,
        )

        assert result.solver_status == "Optimal"
        for t in range(N_HOURS):
            excess = max(prod[t] - load[t], 0.0)
            assert result.charge_kw[t] <= excess + 0.1, (
                f"Hour {t}: charge={result.charge_kw[t]:.1f} > "
                f"excess solar={excess:.1f}"
            )

    def test_no_charge_when_all_production_below_load(self) -> None:
        """When production always <= load, battery never charges."""
        config = _make_config()
        load = _constant_load(N_HOURS, kw=1000.0)
        # All production below load
        prod = _solar_profile(N_HOURS, day_kw=500.0, night_kw=0.0)
        rates = _flat_energy_rates(N_HOURS)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)

        result = solve_month(
            load_kw=load,
            production_kw=prod,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            solar_only_charging=True,
        )

        assert result.solver_status == "Optimal"
        total_charge = sum(result.charge_kw)
        assert total_charge < 0.1, (
            f"Expected zero total charge, got {total_charge:.1f}"
        )


# ---------------------------------------------------------------------------
# Grid-only charging
# ---------------------------------------------------------------------------


class TestGridOnlyCharging:
    """Battery charges from grid only; production is zeroed out."""

    def test_production_treated_as_zero(self) -> None:
        """With grid_only=True, result matches zero production behavior."""
        config = _make_config()
        load = _constant_load(N_HOURS, kw=500.0)
        prod = _solar_profile(N_HOURS, day_kw=800.0)
        rates = _flat_energy_rates(N_HOURS)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)

        # Grid-only with nonzero production
        result_grid = solve_month(
            load_kw=load,
            production_kw=prod,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            grid_only_charging=True,
        )

        # Explicit zero production
        result_zero = solve_month(
            load_kw=load,
            production_kw=[0.0] * N_HOURS,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
        )

        assert result_grid.solver_status == "Optimal"
        assert result_zero.solver_status == "Optimal"

        # Net load should be the same (both have zero production)
        for t in range(N_HOURS):
            assert abs(result_grid.net_load_kw[t] - result_zero.net_load_kw[t]) < 0.1, (
                f"Hour {t}: grid_only={result_grid.net_load_kw[t]:.1f} vs "
                f"zero_prod={result_zero.net_load_kw[t]:.1f}"
            )

    def test_no_solar_offset(self) -> None:
        """Grid-only: net_demand is never reduced below load (no solar offset)."""
        config = _make_config()
        load = _constant_load(N_HOURS, kw=500.0)
        prod = _solar_profile(N_HOURS, day_kw=800.0)
        rates = _flat_energy_rates(N_HOURS)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)

        result = solve_month(
            load_kw=load,
            production_kw=prod,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            grid_only_charging=True,
        )

        assert result.solver_status == "Optimal"
        # Without battery discharge, net_demand == load (500 kW).
        # Battery can reduce it via discharge, but it can't go below load - solar
        # because solar = 0 in grid-only. All net_demand >= 0.
        assert all(v >= -0.01 for v in result.net_load_kw)
        # No curtailment (no production to curtail)
        total_curtailed = sum(result.curtailed_kw)
        assert total_curtailed < 0.1


# ---------------------------------------------------------------------------
# NEM + ITC combined
# ---------------------------------------------------------------------------


class TestNEMPlusITCSolarOnly:
    """NEM export active with solar-only charging constraint."""

    def test_combined_nem_and_solar_only(self) -> None:
        """Battery charges from excess solar only AND exports are valued."""
        config = _make_config()
        load = _constant_load(N_HOURS, kw=500.0)
        prod = _solar_profile(N_HOURS, day_kw=800.0)
        rates = _flat_energy_rates(N_HOURS, rate=0.15)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)
        nem_rates = [0.08] * N_HOURS

        result = solve_month(
            load_kw=load,
            production_kw=prod,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            nem_export_rates=nem_rates,
            solar_only_charging=True,
        )

        assert result.solver_status == "Optimal"

        # Verify solar-only constraint: charge bounded by excess solar
        for t in range(N_HOURS):
            excess = max(prod[t] - load[t], 0.0)
            assert result.charge_kw[t] <= excess + 0.1, (
                f"Hour {t}: charge={result.charge_kw[t]:.1f} > excess={excess:.1f}"
            )

        # Verify NEM is active: export_kw should have values
        assert len(result.export_kw) == N_HOURS
        total_export = sum(result.export_kw)
        assert total_export > 0, "Expected export with NEM active"

        # Curtailment should be minimal since export earns revenue
        total_curtailed = sum(result.curtailed_kw)
        assert total_curtailed < 1.0


# ---------------------------------------------------------------------------
# Test output for manual inspection
# ---------------------------------------------------------------------------


class TestOutputs:
    """Write test results for manual inspection."""

    def test_write_m14b_summary(self, test_results_dir: Path) -> None:
        """Write summary of all M14b optimizer feature results."""
        output_dir = test_results_dir / "m14b_prompt5"
        output_dir.mkdir(parents=True, exist_ok=True)

        config = _make_config()
        load = _constant_load(N_HOURS, kw=500.0)
        prod = _solar_profile(N_HOURS, day_kw=800.0)
        rates = _flat_energy_rates(N_HOURS)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)

        # Baseline
        baseline = solve_month(
            load_kw=load, production_kw=prod, config=config,
            energy_rate_per_hour=rates, demand_periods=dp,
            demand_rates=dr, flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
        )

        # NEM
        nem = solve_month(
            load_kw=load, production_kw=prod, config=config,
            energy_rate_per_hour=rates, demand_periods=dp,
            demand_rates=dr, flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            nem_export_rates=[0.08] * N_HOURS,
        )

        # ITC solar-only
        itc = solve_month(
            load_kw=load, production_kw=prod, config=config,
            energy_rate_per_hour=rates, demand_periods=dp,
            demand_rates=dr, flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            solar_only_charging=True,
        )

        # Grid-only
        grid = solve_month(
            load_kw=load, production_kw=prod, config=config,
            energy_rate_per_hour=rates, demand_periods=dp,
            demand_rates=dr, flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            grid_only_charging=True,
        )

        # NEM + ITC
        nem_itc = solve_month(
            load_kw=load, production_kw=prod, config=config,
            energy_rate_per_hour=rates, demand_periods=dp,
            demand_rates=dr, flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            nem_export_rates=[0.08] * N_HOURS,
            solar_only_charging=True,
        )

        def _summary(label: str, r: MonthSolveResult) -> dict:
            return {
                "scenario": label,
                "solver_status": r.solver_status,
                "total_charge_kwh": round(sum(r.charge_kw), 1),
                "total_discharge_kwh": round(sum(r.discharge_kw), 1),
                "total_curtailed_kwh": round(sum(r.curtailed_kw), 1),
                "total_export_kwh": round(sum(r.export_kw), 1) if r.export_kw else 0.0,
                "peak_demand_flat_kw": round(r.peak_demand_flat, 1),
                "min_net_demand_kw": round(min(r.net_load_kw), 1),
                "max_net_demand_kw": round(max(r.net_load_kw), 1),
                "hours_negative_net": sum(1 for v in r.net_load_kw if v < -0.01),
            }

        summary = [
            _summary("baseline", baseline),
            _summary("nem_export", nem),
            _summary("itc_solar_only", itc),
            _summary("grid_only", grid),
            _summary("nem_plus_itc", nem_itc),
        ]

        output_path = output_dir / "optimizer_m14b_summary.json"
        output_path.write_text(json.dumps(summary, indent=2))
        assert output_path.exists()


# ---------------------------------------------------------------------------
# Export rate clamping (M14b prompt 5b)
# ---------------------------------------------------------------------------


class TestExportRateClamping:
    """Verify defensive clamp prevents LP unboundedness."""

    def test_export_rates_above_import_clamped_and_solve_succeeds(self) -> None:
        """Export rates > import rates are clamped; LP solves as Optimal."""
        config = _make_config()
        load = _constant_load(N_HOURS, kw=500.0)
        prod = _solar_profile(N_HOURS, day_kw=800.0)
        import_rates = _flat_energy_rates(N_HOURS, rate=0.15)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)
        # Export rate $0.25 > import rate $0.15 — would cause unbounded without clamp
        export_rates = [0.25] * N_HOURS

        result = solve_month(
            load_kw=load,
            production_kw=prod,
            config=config,
            energy_rate_per_hour=import_rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            nem_export_rates=export_rates,
        )

        # Should solve successfully (not Unbounded) due to clamping
        assert result.solver_status == "Optimal"
        # Export should still work (NEM is active)
        assert len(result.export_kw) == N_HOURS

    def test_clamp_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Clamping emits a warning via the logging module."""
        import logging

        config = _make_config()
        load = _constant_load(N_HOURS, kw=500.0)
        prod = _solar_profile(N_HOURS, day_kw=800.0)
        import_rates = _flat_energy_rates(N_HOURS, rate=0.15)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)
        # All 720 export rates exceed import rates
        export_rates = [0.25] * N_HOURS

        with caplog.at_level(logging.WARNING, logger="src.bess.optimizer"):
            solve_month(
                load_kw=load,
                production_kw=prod,
                config=config,
                energy_rate_per_hour=import_rates,
                demand_periods=dp,
                demand_rates=dr,
                flat_demand_rate=fdr,
                initial_soc_kwh=config.min_soc_kwh,
                nem_export_rates=export_rates,
            )

        assert any("Clamped" in r.message for r in caplog.records)
        # Check count is correct
        assert any("720" in r.message for r in caplog.records)

    def test_no_clamp_when_export_below_import(self, caplog: pytest.LogCaptureFixture) -> None:
        """No warning when export rates are all below import rates."""
        import logging

        config = _make_config()
        load = _constant_load(N_HOURS, kw=500.0)
        prod = _solar_profile(N_HOURS, day_kw=800.0)
        import_rates = _flat_energy_rates(N_HOURS, rate=0.15)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)
        export_rates = [0.08] * N_HOURS

        with caplog.at_level(logging.WARNING, logger="src.bess.optimizer"):
            result = solve_month(
                load_kw=load,
                production_kw=prod,
                config=config,
                energy_rate_per_hour=import_rates,
                demand_periods=dp,
                demand_rates=dr,
                flat_demand_rate=fdr,
                initial_soc_kwh=config.min_soc_kwh,
                nem_export_rates=export_rates,
            )

        assert result.solver_status == "Optimal"
        assert not any("Clamped" in r.message for r in caplog.records)

    def test_clamp_does_not_mutate_caller_list(self) -> None:
        """The caller's export rate list is not modified by clamping."""
        config = _make_config()
        load = _constant_load(N_HOURS, kw=500.0)
        prod = _solar_profile(N_HOURS, day_kw=800.0)
        import_rates = _flat_energy_rates(N_HOURS, rate=0.15)
        dp, dr, fdr = _simple_demand_setup(N_HOURS)
        export_rates = [0.25] * N_HOURS
        original_rates = list(export_rates)

        solve_month(
            load_kw=load,
            production_kw=prod,
            config=config,
            energy_rate_per_hour=import_rates,
            demand_periods=dp,
            demand_rates=dr,
            flat_demand_rate=fdr,
            initial_soc_kwh=config.min_soc_kwh,
            nem_export_rates=export_rates,
        )

        # Original list should be untouched
        assert export_rates == original_rates
