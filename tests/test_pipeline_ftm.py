"""Tests for FTM dispatch path in pipeline.py.

Verifies that the pipeline correctly routes between the FTM wholesale
dispatch path and the existing BTM paths based on dispatch_mode. Uses
mocks to avoid heavy dependencies (PySAM, CEC database, climate API,
LMP API).
"""

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.config.schema import SiteConfig
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


def _make_ftm_site_mock(
    *,
    bill_calculation: bool = False,
    bess_optimization_required: bool = False,
) -> MagicMock:
    """Create mock SiteConfig for FTM dispatch tests.

    Sets real numeric values on attributes used by the pipeline's FTM
    branch (shading factor, BESS sizing, LMP fields).
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

    # FTM-specific
    site.dispatch_mode = "ftm"
    site.bess_dispatch_required = True
    site.bess_optimization_required = bess_optimization_required
    site.bill_calculation = bill_calculation

    # BESS fields (real values for arithmetic in pipeline)
    site.bess_power_mw = 2.0
    site.bess_duration_hr = 4.0
    site.bess_installed_cost_per_kwh = 275.0
    site.bess_opex_per_kw_year = 10.0
    site.bess_strategy = "global"
    site.bess_solar_only_charging = False

    # Financial fields
    site.ancillary_revenue_per_kw_year = 20.0
    site.rate_escalation_pct = 2.0
    site.discount_rate_pct = 8.0
    site.project_lifetime_years = 25

    # LMP fields
    site.lmp_market = "DAY_AHEAD_HOURLY"
    site.lmp_year = 2024

    return site


def _make_btm_site_mock() -> MagicMock:
    """Create mock SiteConfig for BTM dispatch tests."""
    site = MagicMock()
    site.run_name = "test_run"
    site.site_name = "test_site"
    site.shading_percent = 2.0
    site.weather_file_path = None
    site.data_source = "nsrdb"
    site.resource_file_path = None
    site.location = (33.45, -112.07)
    site.report = False
    site.dispatch_mode = "btm"
    site.bess_dispatch_required = True
    site.bess_optimization_required = False
    site.bill_calculation = True
    site.bess_power_mw = 2.0
    site.bess_duration_hr = 4.0
    site.bess_strategy = "tou_arbitrage"
    return site


def _make_bill_calc_result() -> MagicMock:
    """Create mock BillCalculationResult with iterable monthly details."""
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


def _make_pricing_zone_mock() -> MagicMock:
    """Create mock PricingZone."""
    pz = MagicMock()
    pz.iso = "caiso"
    pz.zone_name = "SP15"
    pz.zone_type = "hub"
    return pz


def _make_lmp_data_mock() -> MagicMock:
    """Create mock LMPData with 8760 prices."""
    lmp = MagicMock()
    lmp.prices = [50.0] * 8760
    lmp.year = 2024
    lmp.mean_price = 50.0
    lmp.min_price = 10.0
    lmp.max_price = 200.0
    return lmp


def _make_ftm_dispatch_result() -> MagicMock:
    """Create mock DispatchResult for FTM dispatch."""
    dr = MagicMock()
    dr.config.capacity_kwh = 8000.0
    dr.ftm_revenue = 50000.0
    dr.metrics.annual_cycles = 300.0
    dr.metrics.annual_throughput_kwh = 2400000.0
    dr.metrics.average_daily_cycles = 0.82
    dr.metrics.capacity_utilization_pct = 85.0
    dr.metrics.total_curtailed_kwh = 100.0
    dr.metrics.estimated_annual_degradation_pct = 1.5
    dr.metrics.charging_source = "any"
    dr.monthly_solve_status = ["optimal"] * 12
    dr.heatmap_data = [[10.0] * 24 for _ in range(12)]

    hd = MagicMock()
    hd.discharge_kw = 50.0
    hd.charge_kw = 30.0
    hd.solar_export_kw = 80.0
    dr.hourly_dispatch = [hd] * 8760

    return dr


def _make_ftm_project_economics_mock() -> MagicMock:
    """Create mock ProjectEconomics for FTM."""
    pe = MagicMock()
    pe.total_project_npv = 250000.0
    pe.solar_npv = 150000.0
    pe.bess_npv = 100000.0
    pe.system_lcoe_per_kwh = 0.045
    pe.total_installed_cost = 2000000.0
    return pe


def _make_btm_dispatch_return() -> tuple[MagicMock, MagicMock]:
    """Create mock (DispatchResult, BESSBillComparison) for BTM."""
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


# ---------------------------------------------------------------------------
# Shared patch targets
# ---------------------------------------------------------------------------

_COMMON_PATCHES = [
    "src.pipeline.load_config",
    "src.pipeline.run_bill_calculation",
    "src.pipeline.save_run_to_db",
    "src.pipeline.generate_report",
]

_FTM_PATCHES = [
    "src.pipeline.resolve_pricing_zone",
    "src.pipeline.fetch_lmp",
    "src.pipeline.run_ftm_dispatch",
    "src.pipeline.compute_ftm_solar_revenue",
    "src.pipeline.compute_ftm_bess_npv",
    "src.pipeline.compute_ftm_project_economics",
]

_BTM_PATCHES = [
    "src.pipeline.run_bess_dispatch",
    "src.pipeline.run_sizing_optimization",
]


# ===========================================================================
# Tests
# ===========================================================================


class TestPipelineFTMDispatch:
    """Verify pipeline FTM dispatch branching and summary output."""

    # -- a) FTM branch is entered when dispatch_mode="ftm" -----------------

    @patch(_BTM_PATCHES[1])
    @patch(_BTM_PATCHES[0])
    @patch(_FTM_PATCHES[5])
    @patch(_FTM_PATCHES[4])
    @patch(_FTM_PATCHES[3])
    @patch(_FTM_PATCHES[2])
    @patch(_FTM_PATCHES[1])
    @patch(_FTM_PATCHES[0])
    @patch(_COMMON_PATCHES[3])
    @patch(_COMMON_PATCHES[2])
    @patch(_COMMON_PATCHES[1])
    @patch(_COMMON_PATCHES[0])
    def test_pipeline_ftm_dispatch_path(
        self,
        mock_load_config: MagicMock,
        mock_bill_calc: MagicMock,
        mock_db: MagicMock,
        mock_report: MagicMock,
        mock_resolve_zone: MagicMock,
        mock_fetch_lmp: MagicMock,
        mock_ftm_dispatch: MagicMock,
        mock_ftm_solar_rev: MagicMock,
        mock_ftm_bess_npv: MagicMock,
        mock_ftm_econ: MagicMock,
        mock_btm_dispatch: MagicMock,
        mock_btm_sizing: MagicMock,
        tmp_path: Path,
    ) -> None:
        """FTM branch is entered and BTM functions are NOT called."""
        # Arrange
        site = _make_ftm_site_mock()
        mock_load_config.return_value = [site]
        mock_resolve_zone.return_value = _make_pricing_zone_mock()
        mock_fetch_lmp.return_value = _make_lmp_data_mock()
        mock_ftm_dispatch.return_value = _make_ftm_dispatch_result()
        mock_ftm_solar_rev.return_value = 30000.0
        mock_ftm_bess_npv.return_value = 100000.0
        mock_ftm_econ.return_value = _make_ftm_project_economics_mock()

        pipeline = _make_pipeline(tmp_path)
        pipeline.batch_simulator.run_batch.return_value = (
            [_make_sim_result()], []
        )
        pipeline.output_writer.write_outputs.return_value = (
            tmp_path / "ts.csv", {}
        )

        # Act
        pipeline.run(tmp_path / "input.csv", skip_climate=True)

        # Assert — FTM functions called, BTM functions NOT called
        mock_resolve_zone.assert_called_once()
        mock_fetch_lmp.assert_called_once()
        mock_ftm_dispatch.assert_called_once()
        mock_ftm_solar_rev.assert_called_once()
        mock_ftm_bess_npv.assert_called_once()
        mock_ftm_econ.assert_called_once()
        mock_btm_dispatch.assert_not_called()
        mock_btm_sizing.assert_not_called()

    # -- b) BTM branch still works when dispatch_mode="btm" ----------------

    @patch(_BTM_PATCHES[1])
    @patch(_BTM_PATCHES[0])
    @patch(_FTM_PATCHES[2])
    @patch(_FTM_PATCHES[1])
    @patch(_FTM_PATCHES[0])
    @patch(_COMMON_PATCHES[3])
    @patch(_COMMON_PATCHES[2])
    @patch(_COMMON_PATCHES[1])
    @patch(_COMMON_PATCHES[0])
    def test_pipeline_btm_unchanged(
        self,
        mock_load_config: MagicMock,
        mock_bill_calc: MagicMock,
        mock_db: MagicMock,
        mock_report: MagicMock,
        mock_resolve_zone: MagicMock,
        mock_fetch_lmp: MagicMock,
        mock_ftm_dispatch: MagicMock,
        mock_btm_dispatch: MagicMock,
        mock_btm_sizing: MagicMock,
        tmp_path: Path,
    ) -> None:
        """BTM single dispatch is called and FTM functions are NOT called."""
        # Arrange
        site = _make_btm_site_mock()
        mock_load_config.return_value = [site]
        mock_bill_calc.return_value = _make_bill_calc_result()
        mock_btm_dispatch.return_value = _make_btm_dispatch_return()

        pipeline = _make_pipeline(tmp_path)
        pipeline.batch_simulator.run_batch.return_value = (
            [_make_sim_result()], []
        )
        pipeline.output_writer.write_outputs.return_value = (
            tmp_path / "ts.csv", {}
        )

        # Act
        pipeline.run(tmp_path / "input.csv", skip_climate=True)

        # Assert — BTM called, FTM NOT called
        mock_btm_dispatch.assert_called_once()
        mock_resolve_zone.assert_not_called()
        mock_fetch_lmp.assert_not_called()
        mock_ftm_dispatch.assert_not_called()

    # -- c) FTM summary has "lmp" and "ftm_economics" keys -----------------

    @patch(_BTM_PATCHES[1])
    @patch(_BTM_PATCHES[0])
    @patch(_FTM_PATCHES[5])
    @patch(_FTM_PATCHES[4])
    @patch(_FTM_PATCHES[3])
    @patch(_FTM_PATCHES[2])
    @patch(_FTM_PATCHES[1])
    @patch(_FTM_PATCHES[0])
    @patch(_COMMON_PATCHES[3])
    @patch(_COMMON_PATCHES[2])
    @patch(_COMMON_PATCHES[1])
    @patch(_COMMON_PATCHES[0])
    def test_pipeline_ftm_summary_keys(
        self,
        mock_load_config: MagicMock,
        mock_bill_calc: MagicMock,
        mock_db: MagicMock,
        mock_report: MagicMock,
        mock_resolve_zone: MagicMock,
        mock_fetch_lmp: MagicMock,
        mock_ftm_dispatch: MagicMock,
        mock_ftm_solar_rev: MagicMock,
        mock_ftm_bess_npv: MagicMock,
        mock_ftm_econ: MagicMock,
        mock_btm_dispatch: MagicMock,
        mock_btm_sizing: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Summary dict contains 'lmp', 'ftm_economics', and 'bess_dispatch'."""
        # Arrange
        site = _make_ftm_site_mock()
        mock_load_config.return_value = [site]
        mock_resolve_zone.return_value = _make_pricing_zone_mock()
        mock_fetch_lmp.return_value = _make_lmp_data_mock()
        mock_ftm_dispatch.return_value = _make_ftm_dispatch_result()
        mock_ftm_solar_rev.return_value = 30000.0
        mock_ftm_bess_npv.return_value = 100000.0
        mock_ftm_econ.return_value = _make_ftm_project_economics_mock()

        pipeline = _make_pipeline(tmp_path)
        pipeline.batch_simulator.run_batch.return_value = (
            [_make_sim_result()], []
        )
        pipeline.output_writer.write_outputs.return_value = (
            tmp_path / "ts.csv", {}
        )

        # Act
        results = pipeline.run(tmp_path / "input.csv", skip_climate=True)

        # Assert — summary has FTM-specific keys
        assert len(results["summaries"]) == 1
        summary = results["summaries"][0]
        assert "lmp" in summary
        assert "ftm_economics" in summary
        assert "bess_dispatch" in summary

        # Verify LMP summary structure
        lmp = summary["lmp"]
        assert lmp["iso"] == "caiso"
        assert lmp["zone"] == "SP15"
        assert lmp["market"] == "DAY_AHEAD_HOURLY"
        assert lmp["year"] == 2024
        assert lmp["mean_lmp"] == 50.0

        # Verify FTM economics structure
        econ = summary["ftm_economics"]
        assert "annual_solar_revenue" in econ
        assert "annual_bess_arbitrage_revenue" in econ
        assert "annual_ancillary_revenue" in econ
        assert "annual_gross_revenue" in econ
        assert "total_project_npv" in econ
        assert "bess_npv" in econ
        assert "solar_npv" in econ
        assert "system_lcoe_per_kwh" in econ

        # Verify bess_dispatch has FTM dispatch_mode
        assert summary["bess_dispatch"]["dispatch_mode"] == "ftm"
        assert summary["bess_dispatch"]["ftm_revenue"] == 50000.0

    # -- d) FTM works without rate schedule ---------------------------------

    @patch(_BTM_PATCHES[1])
    @patch(_BTM_PATCHES[0])
    @patch(_FTM_PATCHES[5])
    @patch(_FTM_PATCHES[4])
    @patch(_FTM_PATCHES[3])
    @patch(_FTM_PATCHES[2])
    @patch(_FTM_PATCHES[1])
    @patch(_FTM_PATCHES[0])
    @patch(_COMMON_PATCHES[3])
    @patch(_COMMON_PATCHES[2])
    @patch(_COMMON_PATCHES[1])
    @patch(_COMMON_PATCHES[0])
    def test_pipeline_ftm_no_rate_schedule_required(
        self,
        mock_load_config: MagicMock,
        mock_bill_calc: MagicMock,
        mock_db: MagicMock,
        mock_report: MagicMock,
        mock_resolve_zone: MagicMock,
        mock_fetch_lmp: MagicMock,
        mock_ftm_dispatch: MagicMock,
        mock_ftm_solar_rev: MagicMock,
        mock_ftm_bess_npv: MagicMock,
        mock_ftm_econ: MagicMock,
        mock_btm_dispatch: MagicMock,
        mock_btm_sizing: MagicMock,
        tmp_path: Path,
    ) -> None:
        """FTM dispatch works without bill_calculation (no rate schedule).

        bill_calculation=False means bill_calc_result is None. The FTM
        path should proceed anyway since it uses LMP prices, not rate
        structures.
        """
        # Arrange — bill_calculation=False
        site = _make_ftm_site_mock(bill_calculation=False)
        mock_load_config.return_value = [site]
        mock_resolve_zone.return_value = _make_pricing_zone_mock()
        mock_fetch_lmp.return_value = _make_lmp_data_mock()
        mock_ftm_dispatch.return_value = _make_ftm_dispatch_result()
        mock_ftm_solar_rev.return_value = 30000.0
        mock_ftm_bess_npv.return_value = 100000.0
        mock_ftm_econ.return_value = _make_ftm_project_economics_mock()

        pipeline = _make_pipeline(tmp_path)
        pipeline.batch_simulator.run_batch.return_value = (
            [_make_sim_result()], []
        )
        pipeline.output_writer.write_outputs.return_value = (
            tmp_path / "ts.csv", {}
        )

        # Act
        results = pipeline.run(tmp_path / "input.csv", skip_climate=True)

        # Assert — FTM dispatch was called successfully
        mock_ftm_dispatch.assert_called_once()
        mock_bill_calc.assert_not_called()

        # load_kwh should be None (no bill_calc_result)
        call_kwargs = mock_ftm_dispatch.call_args
        assert call_kwargs[1].get("load_kwh") is None or (
            call_kwargs[0][3] is None if len(call_kwargs[0]) > 3 else True
        )

        # Summary still has FTM keys
        assert len(results["summaries"]) == 1
        summary = results["summaries"][0]
        assert "lmp" in summary
        assert "ftm_economics" in summary
        assert "bess_dispatch" in summary

    # -- e) FTM requires bess_dispatch_required=True (schema validation) ----

    def test_pipeline_ftm_requires_bess_dispatch(self) -> None:
        """Creating SiteConfig with dispatch_mode='ftm' and
        bess_dispatch_required=False raises a ValidationError.

        The validate_ftm_fields model validator enforces that BESS
        dispatch must be enabled for FTM mode.
        """
        with pytest.raises(
            ValueError,
            match="BESS Dispatch Required must be True when Dispatch Mode is 'ftm'",
        ):
            SiteConfig(
                run_name="FTMValidationTest",
                site_name="FTMValidationSite",
                customer="TestCustomer",
                latitude=33.45,
                longitude=-112.07,
                dc_size_mw=6.25,
                ac_installed_mw=5.0,
                ac_poi_mw=5.0,
                racking="tracker",
                tilt=55.0,
                azimuth=180.0,
                module_orientation="portrait",
                number_of_modules=1,
                ground_clearance_height_m=1.5,
                panel_model="CSI Solar Co. Ltd. CS3U-355P",
                bifacial=False,
                inverter_model="Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
                gcr=0.35,
                shading_percent=0.0,
                dc_wiring_loss_percent=2.0,
                ac_wiring_loss_percent=1.0,
                transformer_losses_percent=1.0,
                degradation_percent=0.5,
                availability_percent=2.5,
                module_mismatch_percent=1.0,
                lid_percent=1.5,
                dispatch_mode="ftm",
                bess_dispatch_required=False,
            )

    # -- f) FTM optimization runs sizing sweep (no longer a placeholder) -----

    @patch("src.pipeline.run_ftm_sizing_optimization")
    @patch(_BTM_PATCHES[1])
    @patch(_BTM_PATCHES[0])
    @patch(_FTM_PATCHES[5])
    @patch(_FTM_PATCHES[4])
    @patch(_FTM_PATCHES[3])
    @patch(_FTM_PATCHES[2])
    @patch(_FTM_PATCHES[1])
    @patch(_FTM_PATCHES[0])
    @patch(_COMMON_PATCHES[3])
    @patch(_COMMON_PATCHES[2])
    @patch(_COMMON_PATCHES[1])
    @patch(_COMMON_PATCHES[0])
    def test_pipeline_ftm_optimization_runs_sizing(
        self,
        mock_load_config: MagicMock,
        mock_bill_calc: MagicMock,
        mock_db: MagicMock,
        mock_report: MagicMock,
        mock_resolve_zone: MagicMock,
        mock_fetch_lmp: MagicMock,
        mock_ftm_dispatch: MagicMock,
        mock_ftm_solar_rev: MagicMock,
        mock_ftm_bess_npv: MagicMock,
        mock_ftm_econ: MagicMock,
        mock_btm_dispatch: MagicMock,
        mock_btm_sizing: MagicMock,
        mock_ftm_sizing: MagicMock,
        tmp_path: Path,
    ) -> None:
        """FTM with bess_optimization_required=True calls run_ftm_sizing_optimization
        and produces both bess_sizing and ftm_economics summary keys.
        """
        # Arrange — optimization flag on
        site = _make_ftm_site_mock(bess_optimization_required=True)
        mock_load_config.return_value = [site]
        mock_resolve_zone.return_value = _make_pricing_zone_mock()
        mock_fetch_lmp.return_value = _make_lmp_data_mock()

        # Build mock SizingResult
        mock_winner = MagicMock()
        mock_winner.power_mw = 2.0
        mock_winner.duration_hr = 4.0
        mock_winner.bess_npv = 100000.0
        mock_winner.installed_cost = 2200000.0
        mock_winner.capacity_kwh = 8000.0
        mock_winner.bess_incremental_savings = 25000.0

        mock_economics = MagicMock()
        mock_economics.optimal_power_mw = 2.0
        mock_economics.optimal_duration_hr = 4.0
        mock_economics.optimal_capacity_kwh = 8000.0
        mock_economics.bess_npv = 100000.0
        mock_economics.total_project_npv = 250000.0
        mock_economics.solar_npv = 150000.0
        mock_economics.system_lcoe_per_kwh = 0.045
        mock_economics.total_installed_cost = 3000000.0
        mock_economics.solar_cost = 800000.0
        mock_economics.bess_cost = 2200000.0
        mock_economics.total_annual_savings = 120000.0
        mock_economics.bess_incremental_savings = 25000.0
        mock_economics.annual_production_mwh = 15000.0
        mock_economics.lifetime_generation_mwh = 350000.0
        mock_economics.combos_evaluated = 12
        mock_economics.solar_revenue = 80000.0
        mock_economics.bess_arbitrage_revenue = 25000.0
        mock_economics.ancillary_revenue = 40000.0
        mock_economics.gross_revenue = 145000.0

        mock_sizing_result = MagicMock()
        mock_sizing_result.winner = mock_winner
        mock_sizing_result.economics = mock_economics
        mock_sizing_result.dispatch_result = _make_ftm_dispatch_result()
        mock_sizing_result.all_combos = [mock_winner]

        mock_ftm_sizing.return_value = mock_sizing_result

        pipeline = _make_pipeline(tmp_path)
        pipeline.batch_simulator.run_batch.return_value = (
            [_make_sim_result()], []
        )
        pipeline.output_writer.write_outputs.return_value = (
            tmp_path / "ts.csv", {}
        )

        # Act
        results = pipeline.run(tmp_path / "input.csv", skip_climate=True)

        # Assert — FTM sizing was called, single dispatch was NOT
        mock_ftm_sizing.assert_called_once()
        mock_ftm_dispatch.assert_not_called()
        mock_btm_sizing.assert_not_called()

        # Assert — summary has both bess_sizing and ftm_economics
        assert len(results["summaries"]) == 1
        summary = results["summaries"][0]
        assert "lmp" in summary
        assert "ftm_economics" in summary
        assert "bess_sizing" in summary
        assert "bess_dispatch" in summary

        # Verify bess_sizing structure
        sizing = summary["bess_sizing"]
        assert sizing["optimal_power_mw"] == 2.0
        assert sizing["optimal_duration_hr"] == 4.0
        assert sizing["combos_evaluated"] == 12
        assert "sweep_results" in sizing

        # Verify ftm_economics structure
        econ = summary["ftm_economics"]
        assert econ["annual_solar_revenue"] == 80000.0
        assert econ["annual_bess_arbitrage_revenue"] == 25000.0
        assert econ["total_project_npv"] == 250000.0

        # Verify bess_dispatch uses winner's power/duration
        assert summary["bess_dispatch"]["dispatch_mode"] == "ftm"
        assert summary["bess_dispatch"]["bess_power_mw"] == 2.0
