"""Tests for M14b metrics and report enhancements: export, charging source."""

import json
from pathlib import Path

import pytest

from src.bess.metrics import compute_battery_metrics, compute_heatmap_data
from src.bess.models import (
    BatteryMetrics,
    BESSBillComparison,
    BESSConfig,
    DispatchResult,
    HourlyDispatch,
)
from src.bess.report_section import (
    build_bess_summary_rows,
    generate_dispatch_profile_chart,
)

# ---------------------------------------------------------------------------
# Output directory
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path("outputs/test_results/m14b_prompt7")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _write_output(name: str, data: dict) -> None:
    """Write test output to JSON for inspection."""
    path = OUTPUT_DIR / f"{name}.json"
    path.write_text(json.dumps(data, indent=2, default=str))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(
    bess_power_kw: float = 100.0,
    bess_duration_hr: float = 4.0,
    usable_capacity_kwh: float = 320.0,
) -> BESSConfig:
    """Build a BESSConfig for testing."""
    capacity_kwh = bess_power_kw * bess_duration_hr
    return BESSConfig(
        bess_power_kw=bess_power_kw,
        bess_duration_hr=bess_duration_hr,
        capacity_kwh=capacity_kwh,
        usable_capacity_kwh=usable_capacity_kwh,
        min_soc_kwh=(capacity_kwh - usable_capacity_kwh) / 2,
        max_soc_kwh=capacity_kwh - (capacity_kwh - usable_capacity_kwh) / 2,
        charge_efficiency=0.938,
        discharge_efficiency=0.938,
        degradation_cost_per_kwh=0.01,
        bess_cycles_warranty=5000,
        strategy="global",
    )


def _make_dispatch_result(
    charge_kw: list[float],
    discharge_kw: list[float],
    soc_kwh: list[float],
    net_load_kw: list[float],
    curtailed_kw: list[float],
    config: BESSConfig,
    export_kw: list[float] | None = None,
) -> DispatchResult:
    """Build a DispatchResult from 8760-element arrays."""
    if export_kw is None:
        export_kw = [0.0] * 8760

    hourly = [
        HourlyDispatch(
            hour=h,
            charge_kw=charge_kw[h],
            discharge_kw=discharge_kw[h],
            soc_kwh=soc_kwh[h],
            net_load_kw=net_load_kw[h],
            curtailed_kw=curtailed_kw[h],
            export_kw=export_kw[h],
        )
        for h in range(8760)
    ]
    return DispatchResult(
        config=config,
        hourly_dispatch=hourly,
        monthly_solve_status=["Optimal"] * 12,
    )


# ---------------------------------------------------------------------------
# Test: compute_battery_metrics with export data
# ---------------------------------------------------------------------------


class TestMetricsExport:
    """Verify export metrics are computed correctly."""

    def test_export_kwh_and_hours_computed(self) -> None:
        """With nonzero export, total_export_kwh and hours are correct."""
        config = _make_config()

        # Create simple pattern: 10 hours/day of 50 kW export (hours 8-17)
        export_kw = []
        for _ in range(365):
            daily = [0.0] * 8 + [50.0] * 10 + [0.0] * 6
            export_kw.extend(daily)

        result = _make_dispatch_result(
            charge_kw=[0.0] * 8760,
            discharge_kw=[10.0] * 8760,
            soc_kwh=[200.0] * 8760,
            net_load_kw=[100.0] * 8760,
            curtailed_kw=[0.0] * 8760,
            config=config,
            export_kw=export_kw,
        )

        metrics = compute_battery_metrics(result)

        # 50 kW * 10 hours * 365 days = 182,500 kWh
        assert metrics.total_export_kwh == pytest.approx(182_500.0)
        # 10 hours/day * 365 days = 3650 hours
        assert metrics.total_export_hours == 3650

        _write_output("metrics_export", {
            "total_export_kwh": metrics.total_export_kwh,
            "total_export_hours": metrics.total_export_hours,
        })

    def test_no_export_defaults(self) -> None:
        """Without export, total_export_kwh=0 and total_export_hours=0."""
        config = _make_config()

        result = _make_dispatch_result(
            charge_kw=[0.0] * 8760,
            discharge_kw=[10.0] * 8760,
            soc_kwh=[200.0] * 8760,
            net_load_kw=[100.0] * 8760,
            curtailed_kw=[0.0] * 8760,
            config=config,
        )

        metrics = compute_battery_metrics(result)

        assert metrics.total_export_kwh == 0.0
        assert metrics.total_export_hours == 0

    def test_small_export_below_threshold_not_counted(self) -> None:
        """Export values below the activity threshold are not counted as hours."""
        config = _make_config()

        # 0.05 kW is below the 0.1 kW threshold
        result = _make_dispatch_result(
            charge_kw=[0.0] * 8760,
            discharge_kw=[0.0] * 8760,
            soc_kwh=[200.0] * 8760,
            net_load_kw=[100.0] * 8760,
            curtailed_kw=[0.0] * 8760,
            config=config,
            export_kw=[0.05] * 8760,
        )

        metrics = compute_battery_metrics(result)

        # total_export_kwh sums all values (even below threshold)
        assert metrics.total_export_kwh == pytest.approx(0.05 * 8760)
        # But hours only count values > threshold
        assert metrics.total_export_hours == 0


# ---------------------------------------------------------------------------
# Test: compute_battery_metrics with charging_source
# ---------------------------------------------------------------------------


class TestMetricsChargingSource:
    """Verify charging_source parameter passes through correctly."""

    def test_default_charging_source(self) -> None:
        """Default charging_source is 'any'."""
        config = _make_config()
        result = _make_dispatch_result(
            charge_kw=[0.0] * 8760,
            discharge_kw=[0.0] * 8760,
            soc_kwh=[200.0] * 8760,
            net_load_kw=[100.0] * 8760,
            curtailed_kw=[0.0] * 8760,
            config=config,
        )

        metrics = compute_battery_metrics(result)
        assert metrics.charging_source == "any"

    def test_solar_only_charging_source(self) -> None:
        """charging_source='solar_only' passes through."""
        config = _make_config()
        result = _make_dispatch_result(
            charge_kw=[0.0] * 8760,
            discharge_kw=[0.0] * 8760,
            soc_kwh=[200.0] * 8760,
            net_load_kw=[100.0] * 8760,
            curtailed_kw=[0.0] * 8760,
            config=config,
        )

        metrics = compute_battery_metrics(result, charging_source="solar_only")
        assert metrics.charging_source == "solar_only"

    def test_grid_only_charging_source(self) -> None:
        """charging_source='grid_only' passes through."""
        config = _make_config()
        result = _make_dispatch_result(
            charge_kw=[0.0] * 8760,
            discharge_kw=[0.0] * 8760,
            soc_kwh=[200.0] * 8760,
            net_load_kw=[100.0] * 8760,
            curtailed_kw=[0.0] * 8760,
            config=config,
        )

        metrics = compute_battery_metrics(result, charging_source="grid_only")
        assert metrics.charging_source == "grid_only"

        _write_output("charging_source_grid_only", {
            "charging_source": metrics.charging_source,
        })


# ---------------------------------------------------------------------------
# Test: BatteryMetrics model defaults
# ---------------------------------------------------------------------------


class TestBatteryMetricsDefaults:
    """Verify new fields have correct defaults."""

    def test_new_field_defaults(self) -> None:
        """New fields default to zero/any."""
        metrics = BatteryMetrics(
            annual_throughput_kwh=100.0,
            annual_cycles=1.0,
            average_daily_cycles=0.003,
            capacity_utilization_pct=10.0,
            average_discharge_depth_pct=50.0,
            max_discharge_depth_pct=80.0,
            total_curtailed_kwh=0.0,
            hours_charging=100,
            hours_discharging=100,
            hours_idle=8560,
            estimated_annual_degradation_pct=0.02,
        )

        assert metrics.total_export_kwh == 0.0
        assert metrics.total_export_hours == 0
        assert metrics.charging_source == "any"

    def test_new_fields_populated(self) -> None:
        """New fields can be explicitly set."""
        metrics = BatteryMetrics(
            annual_throughput_kwh=100.0,
            annual_cycles=1.0,
            average_daily_cycles=0.003,
            capacity_utilization_pct=10.0,
            average_discharge_depth_pct=50.0,
            max_discharge_depth_pct=80.0,
            total_curtailed_kwh=0.0,
            hours_charging=100,
            hours_discharging=100,
            hours_idle=8560,
            estimated_annual_degradation_pct=0.02,
            total_export_kwh=5000.0,
            total_export_hours=500,
            charging_source="solar_only",
        )

        assert metrics.total_export_kwh == 5000.0
        assert metrics.total_export_hours == 500
        assert metrics.charging_source == "solar_only"


# ---------------------------------------------------------------------------
# Test: BESSBillComparison with export fields
# ---------------------------------------------------------------------------


class TestBillComparisonExportFields:
    """Verify new export fields on BESSBillComparison."""

    def test_export_fields_default_to_zero(self) -> None:
        """Export fields default to 0.0 for backward compatibility."""
        comparison = BESSBillComparison(
            solar_only_annual_bill=10000.0,
            solar_plus_bess_annual_bill=8000.0,
            bess_incremental_savings=2000.0,
            bess_demand_savings=1500.0,
            bess_energy_savings=500.0,
        )

        assert comparison.solar_only_export_kwh == 0.0
        assert comparison.solar_only_export_credits == 0.0
        assert comparison.solar_plus_bess_export_kwh == 0.0
        assert comparison.solar_plus_bess_export_credits == 0.0

    def test_export_fields_populated(self) -> None:
        """Export fields can be explicitly set."""
        comparison = BESSBillComparison(
            solar_only_annual_bill=10000.0,
            solar_plus_bess_annual_bill=8000.0,
            bess_incremental_savings=2000.0,
            bess_demand_savings=1500.0,
            bess_energy_savings=500.0,
            solar_only_export_kwh=50000.0,
            solar_only_export_credits=2000.0,
            solar_plus_bess_export_kwh=45000.0,
            solar_plus_bess_export_credits=1800.0,
        )

        assert comparison.solar_only_export_kwh == 50000.0
        assert comparison.solar_only_export_credits == 2000.0
        assert comparison.solar_plus_bess_export_kwh == 45000.0
        assert comparison.solar_plus_bess_export_credits == 1800.0

        _write_output("bill_comparison_export", {
            "solar_only_export_kwh": comparison.solar_only_export_kwh,
            "solar_only_export_credits": comparison.solar_only_export_credits,
            "solar_plus_bess_export_kwh": comparison.solar_plus_bess_export_kwh,
            "solar_plus_bess_export_credits": comparison.solar_plus_bess_export_credits,
        })


# ---------------------------------------------------------------------------
# Test: build_bess_summary_rows
# ---------------------------------------------------------------------------


class TestSummaryRows:
    """Verify summary table row generation."""

    def test_standard_rows_no_export(self) -> None:
        """Standard case: no export, charging_source='any' (omitted)."""
        bd = {
            "bess_power_mw": 0.5,
            "bess_duration_hr": 4.0,
            "bess_capacity_kwh": 2000.0,
            "bess_strategy": "global",
            "solar_only_annual_bill": 10000.0,
            "solar_plus_bess_annual_bill": 8000.0,
            "bess_incremental_savings": 2000.0,
            "bess_demand_savings": 1500.0,
            "bess_energy_savings": 500.0,
            "annual_cycles": 250.0,
            "capacity_utilization_pct": 68.5,
            "estimated_annual_degradation_pct": 5.0,
            "total_curtailed_kwh": 100.0,
            "total_export_kwh": 0.0,
            "total_export_hours": 0,
            "charging_source": "any",
        }

        rows = build_bess_summary_rows(bd)

        # Should not contain "Charging Source" row (default "any")
        labels = [r[0] for r in rows]
        assert "Charging Source" not in labels

        # Should not contain export rows (total_export_kwh == 0)
        assert "Total Export" not in labels
        assert "Export Hours" not in labels

        # Verify core rows are present
        assert "Battery Size" in labels
        assert "Strategy" in labels
        assert "Annual Cycles" in labels

    def test_solar_only_charging_source_shown(self) -> None:
        """When charging_source is 'solar_only', row is included."""
        bd = {
            "bess_power_mw": 0.5,
            "bess_duration_hr": 4.0,
            "bess_capacity_kwh": 2000.0,
            "bess_strategy": "global",
            "solar_only_annual_bill": 10000.0,
            "solar_plus_bess_annual_bill": 8000.0,
            "bess_incremental_savings": 2000.0,
            "bess_demand_savings": 1500.0,
            "bess_energy_savings": 500.0,
            "annual_cycles": 250.0,
            "capacity_utilization_pct": 68.5,
            "estimated_annual_degradation_pct": 5.0,
            "total_curtailed_kwh": 100.0,
            "total_export_kwh": 0.0,
            "total_export_hours": 0,
            "charging_source": "solar_only",
        }

        rows = build_bess_summary_rows(bd)
        labels = [r[0] for r in rows]
        assert "Charging Source" in labels
        idx = labels.index("Charging Source")
        assert rows[idx][1] == "Solar Only"

    def test_export_rows_shown_when_export_present(self) -> None:
        """When total_export_kwh > 0, export rows are included."""
        bd = {
            "bess_power_mw": 0.5,
            "bess_duration_hr": 4.0,
            "bess_capacity_kwh": 2000.0,
            "bess_strategy": "global",
            "solar_only_annual_bill": 10000.0,
            "solar_plus_bess_annual_bill": 8000.0,
            "bess_incremental_savings": 2000.0,
            "bess_demand_savings": 1500.0,
            "bess_energy_savings": 500.0,
            "annual_cycles": 250.0,
            "capacity_utilization_pct": 68.5,
            "estimated_annual_degradation_pct": 5.0,
            "total_curtailed_kwh": 100.0,
            "total_export_kwh": 50000.0,
            "total_export_hours": 3650,
            "charging_source": "any",
        }

        rows = build_bess_summary_rows(bd)
        labels = [r[0] for r in rows]
        assert "Total Export" in labels
        assert "Export Hours" in labels

        export_idx = labels.index("Total Export")
        assert rows[export_idx][1] == "50,000 kWh"

        hours_idx = labels.index("Export Hours")
        assert rows[hours_idx][1] == "3,650"

        _write_output("summary_rows_with_export", {
            "rows": rows,
        })


# ---------------------------------------------------------------------------
# Test: Dispatch profile chart with export
# ---------------------------------------------------------------------------


class TestDispatchProfileChartExport:
    """Verify dispatch profile chart accepts and renders export data."""

    def test_chart_with_export_data(self) -> None:
        """Chart renders without error when export_kw is provided."""
        load = [400.0] * 24
        solar = [0.0] * 8 + [600.0] * 10 + [0.0] * 6
        battery = [0.0] * 8 + [50.0] * 5 + [-50.0] * 5 + [0.0] * 6
        export = [0.0] * 8 + [100.0] * 10 + [0.0] * 6

        output_path = OUTPUT_DIR / "dispatch_profile_with_export.png"
        result = generate_dispatch_profile_chart(
            load_kwh=load,
            solar_kwh=solar,
            battery_kw=battery,
            month_name="July",
            output_path=output_path,
            export_kw=export,
        )

        assert result == output_path
        assert output_path.exists()

    def test_chart_without_export_data(self) -> None:
        """Chart renders without error when export_kw is None (backward compat)."""
        load = [400.0] * 24
        solar = [0.0] * 8 + [600.0] * 10 + [0.0] * 6
        battery = [0.0] * 8 + [50.0] * 5 + [-50.0] * 5 + [0.0] * 6

        output_path = OUTPUT_DIR / "dispatch_profile_no_export.png"
        result = generate_dispatch_profile_chart(
            load_kwh=load,
            solar_kwh=solar,
            battery_kw=battery,
            month_name="January",
            output_path=output_path,
        )

        assert result == output_path
        assert output_path.exists()

    def test_chart_with_zero_export_no_series(self) -> None:
        """Chart renders without export series when all export values are zero."""
        load = [400.0] * 24
        solar = [0.0] * 8 + [600.0] * 10 + [0.0] * 6
        battery = [0.0] * 24
        export = [0.0] * 24

        output_path = OUTPUT_DIR / "dispatch_profile_zero_export.png"
        result = generate_dispatch_profile_chart(
            load_kwh=load,
            solar_kwh=solar,
            battery_kw=battery,
            month_name="March",
            output_path=output_path,
            export_kw=export,
        )

        assert result == output_path
        assert output_path.exists()
