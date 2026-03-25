"""Tests for BESS sizing optimization branching in pipeline.py.

Verifies that the pipeline correctly routes between the M14c sizing
optimization path and the existing M14a/b single dispatch path based
on site config flags. Uses mocks to avoid heavy dependencies (PySAM,
CEC database, climate API).
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.pipeline import SolarModelingPipeline


# ---------------------------------------------------------------------------
# Mock factories
# ---------------------------------------------------------------------------


def _make_pipeline(tmp_path: Path) -> SolarModelingPipeline:
    """Create pipeline without __init__ to skip CEC database loading."""
    pipeline = SolarModelingPipeline.__new__(SolarModelingPipeline)
    pipeline.output_dir = tmp_path
    pipeline.cec_db = MagicMock()
    pipeline.configurator = MagicMock()
    pipeline.batch_simulator = MagicMock()
    pipeline.output_writer = MagicMock()
    return pipeline


def _make_sim_result(run_name: str = "test_run") -> MagicMock:
    """Create mock SimulationResult with 8760 hourly data."""
    result = MagicMock()
    result.run_name = run_name
    result.site_name = "test_site"
    result.hourly_data = pd.DataFrame({
        "ac_gross": [100.0] * 8760,
        "dc_net": [110.0] * 8760,
    })
    result.loss_data = {"capacity_factor_ac": 0.25}
    return result


def _make_site_mock(
    *,
    bess_optimization_required: bool = False,
    bess_dispatch_required: bool = True,
    bill_calculation: bool = True,
) -> MagicMock:
    """Create mock SiteConfig with realistic scalar attribute values.

    Key attributes are set to real values to avoid MagicMock arithmetic
    issues in the pipeline's shading factor and bill savings calculations.
    """
    site = MagicMock()
    site.run_name = "test_run"
    site.site_name = "test_site"
    site.shading_percent = 2.0
    site.weather_file_path = None
    site.data_source = "nsrdb"
    site.resource_file_path = None
    site.location = (33.45, -112.07)
    site.report = False
    site.bill_calculation = bill_calculation
    site.bess_dispatch_required = bess_dispatch_required
    site.bess_optimization_required = bess_optimization_required
    site.bess_strategy = "tou_arbitrage"
    site.bess_power_mw = 2.0
    site.bess_duration_hr = 4.0
    return site


def _make_bill_calc_result() -> MagicMock:
    """Create mock BillCalculationResult with iterable monthly details.

    The pipeline iterates over monthly lists in a zip(), so these
    must be real lists of objects with .month and .total attributes.
    """
    bcr = MagicMock()

    monthly_without = []
    monthly_with = []
    for m in range(1, 13):
        wo = MagicMock()
        wo.month = m
        wo.total = 10000.0
        monthly_without.append(wo)
        wi = MagicMock()
        wi.month = m
        wi.total = 7000.0
        monthly_with.append(wi)

    bcr.bill_savings.bill_without_solar.annual_total = 120000.0
    bcr.bill_savings.bill_without_solar.monthly = monthly_without
    bcr.bill_savings.bill_with_solar.annual_total = 80000.0
    bcr.bill_savings.bill_with_solar.monthly = monthly_with
    bcr.bill_savings.bill_with_solar.annual_export_kwh = 5000.0
    bcr.bill_savings.bill_with_solar.annual_export_credits = 500.0
    bcr.bill_savings.bill_with_solar.nem_true_up_credit = 100.0
    bcr.bill_savings.annual_savings = 40000.0
    bcr.bill_savings.savings_percent = 33.3
    bcr.bill_savings.avoided_cost_per_kwh = 0.08
    bcr.bill_savings.annual_demand_savings = 10000.0
    bcr.bill_savings.monthly_savings = [3000.0] * 12

    bcr.rate_schedule = MagicMock()
    bcr.load_profile = MagicMock()
    bcr.load_profile.hourly_kwh = [500.0] * 8760

    return bcr


def _make_dispatch_return() -> tuple[MagicMock, MagicMock]:
    """Create mock (DispatchResult, BESSBillComparison) tuple.

    Provides real list values for heatmap_data and hourly_dispatch
    so the pipeline's dispatch profile computation doesn't error.
    """
    dr = MagicMock()
    dr.config.capacity_kwh = 8000.0
    dr.metrics.annual_cycles = 300.0
    dr.metrics.annual_throughput_kwh = 2400000.0
    dr.metrics.average_daily_cycles = 0.82
    dr.metrics.capacity_utilization_pct = 85.0
    dr.metrics.total_curtailed_kwh = 100.0
    dr.metrics.estimated_annual_degradation_pct = 1.5
    dr.metrics.total_export_kwh = 500.0
    dr.metrics.total_export_hours = 100
    dr.metrics.charging_source = "any"
    dr.monthly_solve_status = ["optimal"] * 12
    dr.heatmap_data = [[10.0] * 24 for _ in range(12)]

    hd = MagicMock()
    hd.discharge_kw = 50.0
    hd.charge_kw = 30.0
    hd.export_kw = 5.0
    dr.hourly_dispatch = [hd] * 8760

    bc = MagicMock()
    bc.solar_only_annual_bill = 80000.0
    bc.solar_plus_bess_annual_bill = 75000.0
    bc.bess_incremental_savings = 5000.0
    bc.bess_demand_savings = 3000.0
    bc.bess_energy_savings = 2000.0
    bc.solar_only_export_kwh = 5000.0
    bc.solar_only_export_credits = 500.0
    bc.solar_plus_bess_export_kwh = 3000.0
    bc.solar_plus_bess_export_credits = 300.0

    return (dr, bc)


def _make_sizing_result() -> MagicMock:
    """Create mock SizingResult with all nested attributes.

    Provides real values for heatmap_data and hourly_dispatch so the
    dispatch profile computation in the optimization branch succeeds.
    """
    sr = MagicMock()

    # Economics
    sr.economics.optimal_power_mw = 2.0
    sr.economics.optimal_duration_hr = 4.0
    sr.economics.optimal_capacity_kwh = 8000.0
    sr.economics.bess_npv = 50000.0
    sr.economics.total_project_npv = 150000.0
    sr.economics.system_lcoe_per_kwh = 0.05
    sr.economics.total_installed_cost = 500000.0
    sr.economics.solar_cost = 300000.0
    sr.economics.bess_cost = 200000.0
    sr.economics.total_annual_savings = 45000.0
    sr.economics.bess_incremental_savings = 5000.0
    sr.economics.annual_production_mwh = 20000.0
    sr.economics.lifetime_generation_mwh = 450000.0
    sr.economics.combos_evaluated = 12

    # Winner's dispatch result
    sr.dispatch_result.metrics.annual_cycles = 300.0
    sr.dispatch_result.metrics.annual_throughput_kwh = 2400000.0
    sr.dispatch_result.metrics.average_daily_cycles = 0.82
    sr.dispatch_result.metrics.capacity_utilization_pct = 85.0
    sr.dispatch_result.metrics.total_curtailed_kwh = 100.0
    sr.dispatch_result.metrics.estimated_annual_degradation_pct = 1.5
    sr.dispatch_result.metrics.total_export_kwh = 500.0
    sr.dispatch_result.metrics.total_export_hours = 100
    sr.dispatch_result.metrics.charging_source = "any"
    sr.dispatch_result.monthly_solve_status = ["optimal"] * 12
    sr.dispatch_result.heatmap_data = [[10.0] * 24 for _ in range(12)]

    hd = MagicMock()
    hd.discharge_kw = 50.0
    hd.charge_kw = 30.0
    hd.export_kw = 5.0
    sr.dispatch_result.hourly_dispatch = [hd] * 8760

    # Bill comparison
    sr.bill_comparison.solar_only_annual_bill = 80000.0
    sr.bill_comparison.solar_plus_bess_annual_bill = 75000.0
    sr.bill_comparison.bess_incremental_savings = 5000.0
    sr.bill_comparison.bess_demand_savings = 3000.0
    sr.bill_comparison.bess_energy_savings = 2000.0
    sr.bill_comparison.solar_only_export_kwh = 5000.0
    sr.bill_comparison.solar_only_export_credits = 500.0
    sr.bill_comparison.solar_plus_bess_export_kwh = 3000.0
    sr.bill_comparison.solar_plus_bess_export_credits = 300.0

    # All evaluated combos
    combo = MagicMock()
    combo.power_mw = 2.0
    combo.duration_hr = 4.0
    combo.capacity_kwh = 8000.0
    combo.bess_npv = 50000.0
    combo.installed_cost = 200000.0
    combo.bess_incremental_savings = 5000.0
    sr.all_combos = [combo]

    return sr


# ---------------------------------------------------------------------------
# Shared patch stack — applied to every test in the class
# ---------------------------------------------------------------------------

_PATCHES = [
    "src.pipeline.load_config",
    "src.pipeline.run_bill_calculation",
    "src.pipeline.run_bess_dispatch",
    "src.pipeline.run_sizing_optimization",
    "src.pipeline.generate_report",
    "src.pipeline.save_run_to_db",
]


class TestBESSSizingOptimizationBranching:
    """Verify pipeline branches correctly between sizing optimization
    and single dispatch based on site config flags."""

    # -- a) optimization required → run_sizing_optimization called ----------

    @patch(_PATCHES[5])
    @patch(_PATCHES[4])
    @patch(_PATCHES[3])
    @patch(_PATCHES[2])
    @patch(_PATCHES[1])
    @patch(_PATCHES[0])
    def test_optimization_branch_called_when_both_flags_true(
        self,
        mock_load_config: MagicMock,
        mock_bill_calc: MagicMock,
        mock_dispatch: MagicMock,
        mock_sizing: MagicMock,
        mock_report: MagicMock,
        mock_db: MagicMock,
        tmp_path: Path,
    ) -> None:
        """run_sizing_optimization is called when both bess_optimization_required
        and bess_dispatch_required are True."""
        # Arrange
        site = _make_site_mock(
            bess_optimization_required=True,
            bess_dispatch_required=True,
        )
        mock_load_config.return_value = [site]
        mock_bill_calc.return_value = _make_bill_calc_result()
        mock_sizing.return_value = _make_sizing_result()

        pipeline = _make_pipeline(tmp_path)
        pipeline.batch_simulator.run_batch.return_value = (
            [_make_sim_result()], []
        )
        pipeline.output_writer.write_outputs.return_value = (
            tmp_path / "ts.csv", {}
        )

        # Act
        pipeline.run(tmp_path / "input.csv", skip_climate=True)

        # Assert — optimization called, single dispatch NOT called
        mock_sizing.assert_called_once()
        mock_dispatch.assert_not_called()

    # -- b) optimization not required → run_bess_dispatch called ------------

    @patch(_PATCHES[5])
    @patch(_PATCHES[4])
    @patch(_PATCHES[3])
    @patch(_PATCHES[2])
    @patch(_PATCHES[1])
    @patch(_PATCHES[0])
    def test_single_dispatch_when_optimization_false(
        self,
        mock_load_config: MagicMock,
        mock_bill_calc: MagicMock,
        mock_dispatch: MagicMock,
        mock_sizing: MagicMock,
        mock_report: MagicMock,
        mock_db: MagicMock,
        tmp_path: Path,
    ) -> None:
        """run_bess_dispatch is called directly when bess_optimization_required
        is False and bess_dispatch_required is True (existing path)."""
        # Arrange
        site = _make_site_mock(
            bess_optimization_required=False,
            bess_dispatch_required=True,
        )
        mock_load_config.return_value = [site]
        mock_bill_calc.return_value = _make_bill_calc_result()
        mock_dispatch.return_value = _make_dispatch_return()

        pipeline = _make_pipeline(tmp_path)
        pipeline.batch_simulator.run_batch.return_value = (
            [_make_sim_result()], []
        )
        pipeline.output_writer.write_outputs.return_value = (
            tmp_path / "ts.csv", {}
        )

        # Act
        pipeline.run(tmp_path / "input.csv", skip_climate=True)

        # Assert — single dispatch called, optimization NOT called
        mock_dispatch.assert_called_once()
        mock_sizing.assert_not_called()

    # -- c) dispatch not required → neither called --------------------------

    @patch(_PATCHES[5])
    @patch(_PATCHES[4])
    @patch(_PATCHES[3])
    @patch(_PATCHES[2])
    @patch(_PATCHES[1])
    @patch(_PATCHES[0])
    def test_neither_called_when_dispatch_false(
        self,
        mock_load_config: MagicMock,
        mock_bill_calc: MagicMock,
        mock_dispatch: MagicMock,
        mock_sizing: MagicMock,
        mock_report: MagicMock,
        mock_db: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Neither BESS function is called when bess_dispatch_required=False."""
        # Arrange
        site = _make_site_mock(
            bess_optimization_required=False,
            bess_dispatch_required=False,
        )
        mock_load_config.return_value = [site]
        mock_bill_calc.return_value = _make_bill_calc_result()

        pipeline = _make_pipeline(tmp_path)
        pipeline.batch_simulator.run_batch.return_value = (
            [_make_sim_result()], []
        )
        pipeline.output_writer.write_outputs.return_value = (
            tmp_path / "ts.csv", {}
        )

        # Act
        pipeline.run(tmp_path / "input.csv", skip_climate=True)

        # Assert
        mock_dispatch.assert_not_called()
        mock_sizing.assert_not_called()

    # -- d) summary contains bess_sizing when optimization runs -------------

    @patch(_PATCHES[5])
    @patch(_PATCHES[4])
    @patch(_PATCHES[3])
    @patch(_PATCHES[2])
    @patch(_PATCHES[1])
    @patch(_PATCHES[0])
    def test_summary_contains_bess_sizing_when_optimization_runs(
        self,
        mock_load_config: MagicMock,
        mock_bill_calc: MagicMock,
        mock_dispatch: MagicMock,
        mock_sizing: MagicMock,
        mock_report: MagicMock,
        mock_db: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Summary dict contains 'bess_sizing' key with correct values
        when the optimization branch runs."""
        # Arrange
        site = _make_site_mock(
            bess_optimization_required=True,
            bess_dispatch_required=True,
        )
        mock_load_config.return_value = [site]
        mock_bill_calc.return_value = _make_bill_calc_result()
        mock_sizing.return_value = _make_sizing_result()

        pipeline = _make_pipeline(tmp_path)
        pipeline.batch_simulator.run_batch.return_value = (
            [_make_sim_result()], []
        )
        pipeline.output_writer.write_outputs.return_value = (
            tmp_path / "ts.csv", {}
        )

        # Act
        results = pipeline.run(tmp_path / "input.csv", skip_climate=True)

        # Assert — bess_sizing present with expected values
        assert len(results["summaries"]) == 1
        summary = results["summaries"][0]
        assert "bess_sizing" in summary
        sizing = summary["bess_sizing"]
        assert sizing["optimal_power_mw"] == 2.0
        assert sizing["optimal_duration_hr"] == 4.0
        assert sizing["optimal_capacity_kwh"] == 8000.0
        assert sizing["bess_npv"] == 50000.0
        assert sizing["total_project_npv"] == 150000.0
        assert sizing["combos_evaluated"] == 12
        assert len(sizing["sweep_results"]) == 1
        assert sizing["sweep_results"][0]["power_mw"] == 2.0

    # -- e) summary does NOT contain bess_sizing for single dispatch --------

    @patch(_PATCHES[5])
    @patch(_PATCHES[4])
    @patch(_PATCHES[3])
    @patch(_PATCHES[2])
    @patch(_PATCHES[1])
    @patch(_PATCHES[0])
    def test_no_bess_sizing_key_when_single_dispatch(
        self,
        mock_load_config: MagicMock,
        mock_bill_calc: MagicMock,
        mock_dispatch: MagicMock,
        mock_sizing: MagicMock,
        mock_report: MagicMock,
        mock_db: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Summary dict does NOT contain 'bess_sizing' when single dispatch
        runs (bess_optimization_required=False)."""
        # Arrange
        site = _make_site_mock(
            bess_optimization_required=False,
            bess_dispatch_required=True,
        )
        mock_load_config.return_value = [site]
        mock_bill_calc.return_value = _make_bill_calc_result()
        mock_dispatch.return_value = _make_dispatch_return()

        pipeline = _make_pipeline(tmp_path)
        pipeline.batch_simulator.run_batch.return_value = (
            [_make_sim_result()], []
        )
        pipeline.output_writer.write_outputs.return_value = (
            tmp_path / "ts.csv", {}
        )

        # Act
        results = pipeline.run(tmp_path / "input.csv", skip_climate=True)

        # Assert — bess_sizing NOT present, bess_dispatch IS present
        assert len(results["summaries"]) == 1
        summary = results["summaries"][0]
        assert "bess_sizing" not in summary
        assert "bess_dispatch" in summary
