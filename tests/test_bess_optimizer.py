"""Tests for BESS dispatch optimizer (PuLP LP formulation)."""

import math
import time

import pytest

from src.bess.models import BESSConfig
from src.bess.optimizer import MonthSolveResult, solve_month


# ---------------------------------------------------------------------------
# Helper: build BESSConfig directly (no SiteConfig dependency)
# ---------------------------------------------------------------------------

_EFF = math.sqrt(0.88)  # ~0.9381 one-way efficiency


def _make_config(
    strategy: str = "global",
    degradation_cost_per_kwh: float = 0.001,
) -> BESSConfig:
    """Build a 500 kW / 4hr BESSConfig with defaults matching SiteConfig defaults.

    Uses a low degradation cost (0.001) by default so that test economics
    clearly favor battery dispatch.
    """
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


def _tou_energy_rates(n_hours: int) -> list[float]:
    """Build hourly energy rates with TOU spread, repeating daily.

    Off-peak (hours 0-7, 21-23): $0.10/kWh
    On-peak  (hours 8-20):       $0.30/kWh
    """
    daily = (
        [0.10] * 8    # hours 0-7
        + [0.30] * 13  # hours 8-20
        + [0.10] * 3   # hours 21-23
    )
    n_days = n_hours // 24
    return daily * n_days


# ---------------------------------------------------------------------------
# (a) Flat load, no solar, peak shaving
# ---------------------------------------------------------------------------


class TestPeakShavingFlatLoad:
    """Battery starts full and donates initial charge to shave constant peak."""

    def test_peak_demand_reduced(self) -> None:
        """Peak demand flat should be < 1000 kW with battery starting at max SOC.

        With constant 1000 kW load, the battery cannot charge without raising
        the peak. But starting at max SOC, it can spread its initial energy
        across all hours via a small uniform discharge, reducing peak slightly.
        """
        n_hours = 720  # 30-day month
        config = _make_config(strategy="peak_shaving")

        result = solve_month(
            load_kw=[1000.0] * n_hours,
            production_kw=[0.0] * n_hours,
            config=config,
            energy_rate_per_hour=[0.0] * n_hours,
            demand_periods={},
            demand_rates={},
            flat_demand_rate=10.0,
            initial_soc_kwh=config.max_soc_kwh,  # start full
        )

        assert result.solver_status == "Optimal"
        assert result.peak_demand_flat < 1000.0, (
            f"Expected peak < 1000, got {result.peak_demand_flat:.2f}"
        )
        # Battery should have discharged (at least partially)
        total_discharge = sum(result.discharge_kw)
        assert total_discharge > 0


# ---------------------------------------------------------------------------
# (b) TOU arbitrage, obvious spread
# ---------------------------------------------------------------------------


class TestTOUArbitrage:
    """Battery charges off-peak ($0.10) and discharges on-peak ($0.30)."""

    def test_energy_cost_reduced(self) -> None:
        """Total energy cost with battery < without battery."""
        n_hours = 720
        config = _make_config(strategy="tou_arbitrage")
        load = [500.0] * n_hours
        production = [0.0] * n_hours
        rates = _tou_energy_rates(n_hours)

        result = solve_month(
            load_kw=load,
            production_kw=production,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods={},
            demand_rates={},
            flat_demand_rate=0.0,
            initial_soc_kwh=config.min_soc_kwh,
        )

        assert result.solver_status == "Optimal"

        # Cost with battery
        cost_with = sum(
            result.net_load_kw[t] * rates[t] for t in range(n_hours)
        )
        # Cost without battery (baseline)
        cost_without = sum(
            max(load[t] - production[t], 0) * rates[t]
            for t in range(n_hours)
        )

        assert cost_with < cost_without, (
            f"Battery should reduce cost: with={cost_with:.2f}, "
            f"without={cost_without:.2f}"
        )

        # Store for reporting
        self._cost_with = cost_with
        self._cost_without = cost_without


# ---------------------------------------------------------------------------
# (c) SOC bounds respected
# ---------------------------------------------------------------------------


class TestSOCBounds:
    """SOC stays within [min_soc_kwh, max_soc_kwh] for every hour."""

    def test_soc_within_bounds(self) -> None:
        """Run TOU arbitrage scenario and check SOC bounds."""
        n_hours = 720
        config = _make_config(strategy="tou_arbitrage")

        result = solve_month(
            load_kw=[500.0] * n_hours,
            production_kw=[0.0] * n_hours,
            config=config,
            energy_rate_per_hour=_tou_energy_rates(n_hours),
            demand_periods={},
            demand_rates={},
            flat_demand_rate=0.0,
            initial_soc_kwh=config.min_soc_kwh,
        )

        assert result.solver_status == "Optimal"
        for t, soc_val in enumerate(result.soc_kwh):
            assert soc_val >= config.min_soc_kwh - 0.1, (
                f"Hour {t}: SOC {soc_val:.2f} < min {config.min_soc_kwh}"
            )
            assert soc_val <= config.max_soc_kwh + 0.1, (
                f"Hour {t}: SOC {soc_val:.2f} > max {config.max_soc_kwh}"
            )


# ---------------------------------------------------------------------------
# (d) Power limits respected
# ---------------------------------------------------------------------------


class TestPowerLimits:
    """Charge and discharge never exceed rated power."""

    def test_power_within_limits(self) -> None:
        """Run TOU arbitrage and verify power bounds."""
        n_hours = 720
        config = _make_config(strategy="tou_arbitrage")

        result = solve_month(
            load_kw=[500.0] * n_hours,
            production_kw=[0.0] * n_hours,
            config=config,
            energy_rate_per_hour=_tou_energy_rates(n_hours),
            demand_periods={},
            demand_rates={},
            flat_demand_rate=0.0,
            initial_soc_kwh=config.min_soc_kwh,
        )

        assert result.solver_status == "Optimal"
        for t in range(n_hours):
            assert result.charge_kw[t] <= config.bess_power_kw + 0.01, (
                f"Hour {t}: charge {result.charge_kw[t]:.2f} > "
                f"limit {config.bess_power_kw}"
            )
            assert result.discharge_kw[t] <= config.bess_power_kw + 0.01, (
                f"Hour {t}: discharge {result.discharge_kw[t]:.2f} > "
                f"limit {config.bess_power_kw}"
            )


# ---------------------------------------------------------------------------
# (e) No export (net_load >= 0)
# ---------------------------------------------------------------------------


class TestNoExport:
    """Net load never goes negative (no grid export)."""

    def test_net_load_non_negative(self) -> None:
        """Verify net_load_kw >= 0 for every hour."""
        n_hours = 720
        config = _make_config(strategy="global")

        result = solve_month(
            load_kw=[500.0] * n_hours,
            production_kw=[0.0] * n_hours,
            config=config,
            energy_rate_per_hour=_tou_energy_rates(n_hours),
            demand_periods={},
            demand_rates={},
            flat_demand_rate=5.0,
            initial_soc_kwh=config.min_soc_kwh,
        )

        assert result.solver_status == "Optimal"
        for t in range(n_hours):
            assert result.net_load_kw[t] >= -0.01, (
                f"Hour {t}: net_load {result.net_load_kw[t]:.4f} < 0"
            )


# ---------------------------------------------------------------------------
# (f) SOC continuity
# ---------------------------------------------------------------------------


class TestSOCContinuity:
    """SOC transition equation holds for each hour."""

    def test_soc_transitions(self) -> None:
        """Verify soc[t] = soc[t-1] + charge*eff - discharge/eff."""
        n_hours = 720
        config = _make_config(strategy="tou_arbitrage")

        result = solve_month(
            load_kw=[500.0] * n_hours,
            production_kw=[0.0] * n_hours,
            config=config,
            energy_rate_per_hour=_tou_energy_rates(n_hours),
            demand_periods={},
            demand_rates={},
            flat_demand_rate=0.0,
            initial_soc_kwh=config.min_soc_kwh,
        )

        assert result.solver_status == "Optimal"

        for t in range(n_hours):
            prev = config.min_soc_kwh if t == 0 else result.soc_kwh[t - 1]
            expected = (
                prev
                + result.charge_kw[t] * config.charge_efficiency
                - result.discharge_kw[t] / config.discharge_efficiency
            )
            assert abs(result.soc_kwh[t] - expected) < 0.1, (
                f"Hour {t}: SOC {result.soc_kwh[t]:.2f} != "
                f"expected {expected:.2f} "
                f"(prev={prev:.2f}, chg={result.charge_kw[t]:.2f}, "
                f"dis={result.discharge_kw[t]:.2f})"
            )


# ---------------------------------------------------------------------------
# (g) Solver status = Optimal
# ---------------------------------------------------------------------------


class TestSolverStatus:
    """All valid scenarios produce Optimal status."""

    @pytest.mark.parametrize("strategy", ["global", "peak_shaving", "tou_arbitrage"])
    def test_optimal_status(self, strategy: str) -> None:
        """Solver returns Optimal for each strategy."""
        n_hours = 720
        config = _make_config(strategy=strategy)

        result = solve_month(
            load_kw=[500.0] * n_hours,
            production_kw=[0.0] * n_hours,
            config=config,
            energy_rate_per_hour=_tou_energy_rates(n_hours),
            demand_periods={},
            demand_rates={},
            flat_demand_rate=5.0,
            initial_soc_kwh=config.min_soc_kwh,
        )

        assert result.solver_status == "Optimal"


# ---------------------------------------------------------------------------
# (h) Empty demand periods
# ---------------------------------------------------------------------------


class TestEmptyDemandPeriods:
    """No TOU demand periods and no flat demand — energy charges only."""

    def test_solves_with_empty_demand(self) -> None:
        """Solver handles empty demand_periods dict."""
        n_hours = 720
        config = _make_config(strategy="tou_arbitrage")

        result = solve_month(
            load_kw=[500.0] * n_hours,
            production_kw=[0.0] * n_hours,
            config=config,
            energy_rate_per_hour=_tou_energy_rates(n_hours),
            demand_periods={},
            demand_rates={},
            flat_demand_rate=0.0,
            initial_soc_kwh=config.min_soc_kwh,
        )

        assert result.solver_status == "Optimal"
        assert result.peak_demand_by_period == {}
        assert len(result.net_load_kw) == n_hours


# ---------------------------------------------------------------------------
# (i) Global strategy with both energy and demand
# ---------------------------------------------------------------------------


class TestGlobalStrategy:
    """Global strategy optimizes both energy cost and demand charges."""

    def test_both_energy_and_demand_reduced(self) -> None:
        """Battery reduces energy cost and TOU demand vs no-battery baseline.

        Uses TOU energy rates with spread plus TOU and flat demand charges
        to exercise both components of the objective function.

        With constant load, the battery charges off-peak (raising flat peak)
        to discharge on-peak (lowering TOU demand peak + energy cost). The
        net total bill should decrease even though flat peak may increase.
        """
        n_hours = 720
        config = _make_config(strategy="global")
        load = [1000.0] * n_hours
        production = [0.0] * n_hours
        rates = _tou_energy_rates(n_hours)

        # Build TOU demand periods: on-peak hours 8-20 each day
        on_peak_hours = []
        for day in range(30):
            for h in range(8, 21):
                on_peak_hours.append(day * 24 + h)
        demand_periods = {"on_peak": on_peak_hours}
        demand_rates = {"on_peak": 15.0}  # $15/kW TOU demand
        flat_demand_rate = 10.0

        result = solve_month(
            load_kw=load,
            production_kw=production,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods=demand_periods,
            demand_rates=demand_rates,
            flat_demand_rate=flat_demand_rate,
            initial_soc_kwh=config.max_soc_kwh,  # start full for demand shaving
        )

        assert result.solver_status == "Optimal"

        # --- Energy cost reduced ---
        cost_with = sum(
            result.net_load_kw[t] * rates[t] for t in range(n_hours)
        )
        cost_without = sum(
            max(load[t] - production[t], 0) * rates[t]
            for t in range(n_hours)
        )
        assert cost_with < cost_without, (
            f"Energy cost not reduced: with={cost_with:.2f}, "
            f"without={cost_without:.2f}"
        )

        # --- TOU demand peak reduced ---
        baseline_peak = max(
            max(load[t] - production[t], 0) for t in range(n_hours)
        )
        assert result.peak_demand_by_period["on_peak"] < baseline_peak, (
            f"TOU on-peak demand not reduced: "
            f"got {result.peak_demand_by_period['on_peak']:.2f}, "
            f"baseline {baseline_peak:.2f}"
        )

        # --- Total bill (energy + all demand) reduced ---
        bill_with = (
            cost_with
            + result.peak_demand_by_period["on_peak"] * demand_rates["on_peak"]
            + result.peak_demand_flat * flat_demand_rate
        )
        bill_without = (
            cost_without
            + baseline_peak * demand_rates["on_peak"]
            + baseline_peak * flat_demand_rate
        )
        assert bill_with < bill_without, (
            f"Total bill not reduced: with={bill_with:.2f}, "
            f"without={bill_without:.2f}"
        )


# ---------------------------------------------------------------------------
# Reporting: print key metrics for manual inspection
# ---------------------------------------------------------------------------


class TestReportMetrics:
    """Report key values for manual inspection. Always passes."""

    def test_peak_shaving_report(self) -> None:
        """Report peak_demand_flat for test (a) scenario."""
        n_hours = 720
        config = _make_config(strategy="peak_shaving")

        result = solve_month(
            load_kw=[1000.0] * n_hours,
            production_kw=[0.0] * n_hours,
            config=config,
            energy_rate_per_hour=[0.0] * n_hours,
            demand_periods={},
            demand_rates={},
            flat_demand_rate=10.0,
            initial_soc_kwh=config.max_soc_kwh,
        )

        print(f"\n[Peak Shaving] peak_demand_flat = {result.peak_demand_flat:.4f} kW")
        print(f"[Peak Shaving] total_discharge = {sum(result.discharge_kw):.2f} kW")
        print(f"[Peak Shaving] final_soc = {result.final_soc_kwh:.2f} kWh")

    def test_tou_arbitrage_report(self) -> None:
        """Report energy costs for test (b) scenario."""
        n_hours = 720
        config = _make_config(strategy="tou_arbitrage")
        load = [500.0] * n_hours
        production = [0.0] * n_hours
        rates = _tou_energy_rates(n_hours)

        result = solve_month(
            load_kw=load,
            production_kw=production,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods={},
            demand_rates={},
            flat_demand_rate=0.0,
            initial_soc_kwh=config.min_soc_kwh,
        )

        cost_with = sum(result.net_load_kw[t] * rates[t] for t in range(n_hours))
        cost_without = sum(
            max(load[t] - production[t], 0) * rates[t]
            for t in range(n_hours)
        )

        print(f"\n[TOU Arbitrage] cost_without_battery = ${cost_without:,.2f}")
        print(f"[TOU Arbitrage] cost_with_battery    = ${cost_with:,.2f}")
        print(f"[TOU Arbitrage] savings              = ${cost_without - cost_with:,.2f}")

    def test_solver_timing(self) -> None:
        """Report solve time for a 744-hour month (31 days) with full complexity."""
        n_hours = 744
        config = _make_config(strategy="global")
        load = [1000.0] * n_hours
        rates = _tou_energy_rates(n_hours)

        on_peak = []
        mid_peak = []
        for day in range(31):
            for h in range(8, 14):
                mid_peak.append(day * 24 + h)
            for h in range(14, 21):
                on_peak.append(day * 24 + h)

        t0 = time.perf_counter()
        result = solve_month(
            load_kw=load,
            production_kw=[0.0] * n_hours,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods={"on_peak": on_peak, "mid_peak": mid_peak},
            demand_rates={"on_peak": 20.0, "mid_peak": 10.0},
            flat_demand_rate=10.0,
            initial_soc_kwh=config.max_soc_kwh,
        )
        elapsed = time.perf_counter() - t0

        print(f"\n[Timing] 744-hour month with 2 TOU periods: {elapsed:.3f}s")
        print(f"[Timing] solver_status = {result.solver_status}")


# ---------------------------------------------------------------------------
# (j) Excess solar curtailment
# ---------------------------------------------------------------------------


class TestExcessSolarCurtailment:
    """Excess solar beyond load + battery capacity is curtailed explicitly."""

    def test_curtailment_when_solar_exceeds_load_and_battery(self) -> None:
        """Load=200 kW, solar=500 kW, battery=100 kW/4hr.

        Excess = 500 - 200 = 300 kW. Battery can absorb at most 100 kW.
        So at least 200 kW must be curtailed each hour (once battery is full,
        curtailment rises to 300 kW).
        """
        n_hours = 720  # 30-day month
        config = BESSConfig(
            bess_power_kw=100.0,
            bess_duration_hr=4.0,
            capacity_kwh=400.0,
            usable_capacity_kwh=320.0,  # 80% of 400
            min_soc_kwh=40.0,           # 10% of 400
            max_soc_kwh=360.0,          # 90% of 400
            charge_efficiency=_EFF,
            discharge_efficiency=_EFF,
            degradation_cost_per_kwh=0.001,
            strategy="tou_arbitrage",
        )

        result = solve_month(
            load_kw=[200.0] * n_hours,
            production_kw=[500.0] * n_hours,
            config=config,
            energy_rate_per_hour=[0.10] * n_hours,
            demand_periods={},
            demand_rates={},
            flat_demand_rate=0.0,
            initial_soc_kwh=config.min_soc_kwh,
        )

        assert result.solver_status == "Optimal"

        # Total curtailment must be significant
        total_curtailed = sum(result.curtailed_kw)
        assert total_curtailed > 0, "Expected non-zero curtailment"

        # Once battery is full, curtailment should be ~300 kW/hr
        # (500 solar - 200 load = 300 excess, battery can't absorb)
        # Battery fills in ~320 / (100 * 0.938) ≈ 3.4 hours, so most
        # hours should see ~300 kW curtailment
        hours_near_300 = sum(
            1 for c in result.curtailed_kw if c > 290.0
        )
        assert hours_near_300 > 700, (
            f"Expected most hours near 300 kW curtailment, "
            f"got {hours_near_300} hours > 290 kW"
        )

        # Net load should be 0 for all hours (solar covers load fully)
        for t in range(n_hours):
            assert result.net_load_kw[t] <= 0.01, (
                f"Hour {t}: net_load {result.net_load_kw[t]:.4f} > 0"
            )

        # Print first 24 hours for inspection
        print("\n[Curtailment] First 24 hours:")
        print(f"  {'Hour':>4} {'Charge':>8} {'Dischg':>8} {'SOC':>10} "
              f"{'NetLoad':>8} {'Curtail':>8}")
        for t in range(24):
            print(
                f"  {t:4d} {result.charge_kw[t]:8.2f} "
                f"{result.discharge_kw[t]:8.2f} {result.soc_kwh[t]:10.2f} "
                f"{result.net_load_kw[t]:8.2f} {result.curtailed_kw[t]:8.2f}"
            )
