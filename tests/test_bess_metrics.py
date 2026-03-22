"""Tests for BESS dispatch metrics and heatmap computation."""

import pytest

from src.bess.metrics import compute_battery_metrics, compute_heatmap_data
from src.bess.models import BESSConfig, DispatchResult, HourlyDispatch


def _make_config(
    bess_power_kw: float = 100.0,
    bess_duration_hr: float = 4.0,
    usable_capacity_kwh: float = 320.0,
    **overrides: object,
) -> BESSConfig:
    """Build a BESSConfig for testing with sensible defaults."""
    capacity_kwh = bess_power_kw * bess_duration_hr
    defaults = {
        "bess_power_kw": bess_power_kw,
        "bess_duration_hr": bess_duration_hr,
        "capacity_kwh": capacity_kwh,
        "usable_capacity_kwh": usable_capacity_kwh,
        "min_soc_kwh": (capacity_kwh - usable_capacity_kwh) / 2,
        "max_soc_kwh": capacity_kwh - (capacity_kwh - usable_capacity_kwh) / 2,
        "charge_efficiency": 0.938,
        "discharge_efficiency": 0.938,
        "degradation_cost_per_kwh": 0.01,
        "bess_cycles_warranty": 5000,
        "strategy": "global",
    }
    defaults.update(overrides)
    return BESSConfig(**defaults)


def _make_dispatch_result(
    charge_kw: list[float],
    discharge_kw: list[float],
    soc_kwh: list[float],
    net_load_kw: list[float],
    curtailed_kw: list[float],
    config: BESSConfig,
) -> DispatchResult:
    """Build a DispatchResult from 8760-element arrays and a BESSConfig.

    Args:
        charge_kw: 8760 hourly charge power values.
        discharge_kw: 8760 hourly discharge power values.
        soc_kwh: 8760 hourly SOC values.
        net_load_kw: 8760 hourly net load values.
        curtailed_kw: 8760 hourly curtailment values.
        config: BESS configuration.

    Returns:
        DispatchResult with hourly dispatch and placeholder monthly statuses.
    """
    hourly = [
        HourlyDispatch(
            hour=h,
            charge_kw=charge_kw[h],
            discharge_kw=discharge_kw[h],
            soc_kwh=soc_kwh[h],
            net_load_kw=net_load_kw[h],
            curtailed_kw=curtailed_kw[h],
        )
        for h in range(8760)
    ]
    return DispatchResult(
        config=config,
        hourly_dispatch=hourly,
        monthly_solve_status=["Optimal"] * 12,
    )


class TestKnownThroughput:
    """Verify metrics from a known daily discharge/charge pattern."""

    @pytest.fixture()
    def result(self):
        """Discharge 100 kW hours 0-3, charge 100 kW hours 12-15, every day."""
        config = _make_config(
            bess_power_kw=100.0,
            bess_duration_hr=4.0,
            usable_capacity_kwh=320.0,
        )

        charge = [0.0] * 8760
        discharge = [0.0] * 8760
        for day in range(365):
            base = day * 24
            for h in range(4):
                discharge[base + h] = 100.0
            for h in range(12, 16):
                charge[base + h] = 100.0

        soc = [200.0] * 8760  # placeholder
        net_load = [0.0] * 8760
        curtailed = [0.0] * 8760

        return _make_dispatch_result(charge, discharge, soc, net_load, curtailed, config)

    def test_annual_throughput(self, result):
        """Throughput = 100 kW * 4 hours * 365 days = 146,000 kWh."""
        metrics = compute_battery_metrics(result)
        assert metrics.annual_throughput_kwh == pytest.approx(146_000.0, abs=0.1)

    def test_annual_cycles(self, result):
        """Cycles = 146,000 / 320 = 456.25."""
        metrics = compute_battery_metrics(result)
        assert metrics.annual_cycles == pytest.approx(456.25, abs=0.01)

    def test_average_daily_cycles(self, result):
        """Daily cycles = 456.25 / 365 = 1.25."""
        metrics = compute_battery_metrics(result)
        assert metrics.average_daily_cycles == pytest.approx(1.25, rel=1e-3)

    def test_capacity_utilization(self, result):
        """Utilization = (146000 / (320 * 365)) * 100 = 125.0%."""
        metrics = compute_battery_metrics(result)
        assert metrics.capacity_utilization_pct == pytest.approx(125.0, rel=1e-3)

    def test_hours_charging(self, result):
        """4 charge hours per day * 365 = 1460."""
        metrics = compute_battery_metrics(result)
        assert metrics.hours_charging == 1460

    def test_hours_discharging(self, result):
        """4 discharge hours per day * 365 = 1460."""
        metrics = compute_battery_metrics(result)
        assert metrics.hours_discharging == 1460

    def test_hours_idle(self, result):
        """8760 - 1460 - 1460 = 5840."""
        metrics = compute_battery_metrics(result)
        assert metrics.hours_idle == 5840

    def test_degradation(self, result):
        """Degradation = 456.25 / 5000 * 100 = 9.125%."""
        metrics = compute_battery_metrics(result)
        assert metrics.estimated_annual_degradation_pct == pytest.approx(9.125, abs=0.01)

    def test_full_printout(self, result):
        """Print all metrics for reporting."""
        metrics = compute_battery_metrics(result)
        print(f"\n--- BatteryMetrics (test a) ---")
        print(f"  annual_throughput_kwh:           {metrics.annual_throughput_kwh:,.1f}")
        print(f"  annual_cycles:                   {metrics.annual_cycles:.2f}")
        print(f"  average_daily_cycles:            {metrics.average_daily_cycles:.4f}")
        print(f"  capacity_utilization_pct:        {metrics.capacity_utilization_pct:.2f}%")
        print(f"  average_discharge_depth_pct:     {metrics.average_discharge_depth_pct:.2f}%")
        print(f"  max_discharge_depth_pct:         {metrics.max_discharge_depth_pct:.2f}%")
        print(f"  total_curtailed_kwh:             {metrics.total_curtailed_kwh:,.1f}")
        print(f"  hours_charging:                  {metrics.hours_charging}")
        print(f"  hours_discharging:               {metrics.hours_discharging}")
        print(f"  hours_idle:                      {metrics.hours_idle}")
        print(f"  estimated_annual_degradation_pct:{metrics.estimated_annual_degradation_pct:.3f}%")


class TestDischargeEventDetection:
    """Verify per-event depth computation with multiple events."""

    @pytest.fixture()
    def result(self):
        """Two discharge events in first 24 hours, rest idle.

        Event 1: hours 2-4 at 200 kW (600 kWh)
        Event 2: hours 18-19 at 100 kW (200 kWh)
        """
        config = _make_config(
            bess_power_kw=200.0,
            bess_duration_hr=5.0,
            usable_capacity_kwh=800.0,
        )

        charge = [0.0] * 8760
        discharge = [0.0] * 8760
        # Event 1: hours 2, 3, 4
        for h in [2, 3, 4]:
            discharge[h] = 200.0
        # Event 2: hours 18, 19
        for h in [18, 19]:
            discharge[h] = 100.0

        soc = [500.0] * 8760
        net_load = [0.0] * 8760
        curtailed = [0.0] * 8760

        return _make_dispatch_result(charge, discharge, soc, net_load, curtailed, config)

    def test_average_discharge_depth(self, result):
        """Event 1: 600/800*100=75%, Event 2: 200/800*100=25%. Avg = 50%."""
        metrics = compute_battery_metrics(result)
        assert metrics.average_discharge_depth_pct == pytest.approx(50.0, abs=0.1)

    def test_max_discharge_depth(self, result):
        """Max depth = 75% (event 1)."""
        metrics = compute_battery_metrics(result)
        assert metrics.max_discharge_depth_pct == pytest.approx(75.0, abs=0.1)

    def test_throughput(self, result):
        """Total throughput = 600 + 200 = 800 kWh."""
        metrics = compute_battery_metrics(result)
        assert metrics.annual_throughput_kwh == pytest.approx(800.0, abs=0.1)


class TestHeatmapDimensions:
    """Verify heatmap output structure."""

    def test_dimensions(self):
        """Heatmap should be 12 lists of 24 floats."""
        config = _make_config()
        zeros = [0.0] * 8760
        result = _make_dispatch_result(zeros, zeros, zeros, zeros, zeros, config)
        heatmap = compute_heatmap_data(result)

        assert len(heatmap) == 12
        for month_idx, row in enumerate(heatmap):
            assert len(row) == 24, f"Month {month_idx + 1} has {len(row)} hours"


class TestHeatmapKnownPattern:
    """Verify heatmap values from a known daily pattern."""

    @pytest.fixture()
    def heatmap(self):
        """Discharge 100 kW hours 0-3, charge 100 kW hours 12-15, every day."""
        config = _make_config()

        charge = [0.0] * 8760
        discharge = [0.0] * 8760
        for day in range(365):
            base = day * 24
            for h in range(4):
                discharge[base + h] = 100.0
            for h in range(12, 16):
                charge[base + h] = 100.0

        soc = [200.0] * 8760
        net_load = [0.0] * 8760
        curtailed = [0.0] * 8760

        result = _make_dispatch_result(charge, discharge, soc, net_load, curtailed, config)
        return compute_heatmap_data(result)

    def test_discharge_hours_positive(self, heatmap):
        """Hours 0-3 should show +100 kW (discharge) for all months."""
        for month_idx in range(12):
            for h in range(4):
                assert heatmap[month_idx][h] == pytest.approx(100.0, abs=0.1), (
                    f"Month {month_idx + 1} hour {h}: expected +100, "
                    f"got {heatmap[month_idx][h]}"
                )

    def test_charge_hours_negative(self, heatmap):
        """Hours 12-15 should show -100 kW (charge) for all months."""
        for month_idx in range(12):
            for h in range(12, 16):
                assert heatmap[month_idx][h] == pytest.approx(-100.0, abs=0.1), (
                    f"Month {month_idx + 1} hour {h}: expected -100, "
                    f"got {heatmap[month_idx][h]}"
                )

    def test_idle_hours_zero(self, heatmap):
        """Hours 4-11 and 16-23 should be zero for all months."""
        idle_hours = list(range(4, 12)) + list(range(16, 24))
        for month_idx in range(12):
            for h in idle_hours:
                assert heatmap[month_idx][h] == pytest.approx(0.0, abs=0.1), (
                    f"Month {month_idx + 1} hour {h}: expected 0, "
                    f"got {heatmap[month_idx][h]}"
                )

    def test_printout_january(self, heatmap):
        """Print January heatmap values for hours 0-5 and 12-17."""
        print("\n--- Heatmap January (test d) ---")
        print("  Hours 0-5:  ", [f"{heatmap[0][h]:+.1f}" for h in range(6)])
        print("  Hours 12-17:", [f"{heatmap[0][h]:+.1f}" for h in range(12, 18)])


class TestAllIdleBattery:
    """Verify metrics when battery never charges or discharges."""

    @pytest.fixture()
    def result(self):
        """All-zero dispatch."""
        config = _make_config()
        zeros = [0.0] * 8760
        return _make_dispatch_result(zeros, zeros, zeros, zeros, zeros, config)

    def test_annual_cycles_zero(self, result):
        metrics = compute_battery_metrics(result)
        assert metrics.annual_cycles == 0.0

    def test_hours_idle_8760(self, result):
        metrics = compute_battery_metrics(result)
        assert metrics.hours_idle == 8760

    def test_capacity_utilization_zero(self, result):
        metrics = compute_battery_metrics(result)
        assert metrics.capacity_utilization_pct == 0.0

    def test_discharge_depth_zero(self, result):
        metrics = compute_battery_metrics(result)
        assert metrics.average_discharge_depth_pct == 0.0
        assert metrics.max_discharge_depth_pct == 0.0

    def test_degradation_zero(self, result):
        metrics = compute_battery_metrics(result)
        assert metrics.estimated_annual_degradation_pct == 0.0

    def test_heatmap_all_zeros(self, result):
        heatmap = compute_heatmap_data(result)
        for month_idx in range(12):
            for h in range(24):
                assert heatmap[month_idx][h] == 0.0, (
                    f"Month {month_idx + 1} hour {h}: expected 0, "
                    f"got {heatmap[month_idx][h]}"
                )


class TestCurtailmentTracking:
    """Verify total curtailment aggregation."""

    def test_total_curtailed(self):
        """50 kW curtailed for first 1000 hours = 50,000 kWh."""
        config = _make_config()
        charge = [0.0] * 8760
        discharge = [0.0] * 8760
        soc = [200.0] * 8760
        net_load = [0.0] * 8760
        curtailed = [50.0] * 1000 + [0.0] * 7760

        result = _make_dispatch_result(
            charge, discharge, soc, net_load, curtailed, config
        )
        metrics = compute_battery_metrics(result)
        assert metrics.total_curtailed_kwh == pytest.approx(50_000.0, abs=0.1)
