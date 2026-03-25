"""Tests for M14c database schema additions (BESS sizing optimization).

Tests ORM model column presence and writer mapping logic. Uses in-memory
SQLite for column inspection (no PostgreSQL dependency needed).
"""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import inspect as sa_inspect

from src.database.models import RunInput, RunResult


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

# The 11 new RunInputs columns for M14c sizing optimization
NEW_INPUT_COLUMNS = [
    "bess_power_min_mw",
    "bess_power_max_mw",
    "bess_duration_min_hr",
    "bess_duration_max_hr",
    "bess_opex_per_kw_year",
    "discount_rate_pct",
    "project_lifetime_years",
    "rate_escalation_pct",
    "solar_cost_per_kw_dc",
    "solar_cost_per_kw_ac",
    "solar_opex_per_kw_dc_year",
]


def _make_site_config_mock(
    bess_optimization_required: bool = True,
) -> MagicMock:
    """Build a mock SiteConfig with all fields the writer reads."""
    sc = MagicMock()
    sc.customer = "TestCo"
    sc.site_name = "TestSite"
    sc.run_name = "Run_001"
    sc.latitude = 33.0
    sc.longitude = -112.0
    sc.dc_size_mw = 10.0
    sc.ac_installed_mw = 8.0
    sc.ac_poi_mw = 8.0
    sc.racking = "tracker"
    sc.tilt = 60.0
    sc.azimuth = 180.0
    sc.module_orientation = "portrait"
    sc.number_of_modules = 2
    sc.ground_clearance_height_m = 1.8
    sc.panel_model = "TestPanel"
    sc.bifacial = True
    sc.inverter_model = "TestInverter"
    sc.gcr = 0.34
    sc.shading_percent = 1.0
    sc.dc_wiring_loss_percent = 1.5
    sc.ac_wiring_loss_percent = 1.5
    sc.transformer_losses_percent = 0.0
    sc.degradation_percent = 0.3
    sc.availability_percent = 2.0
    sc.module_mismatch_percent = 1.5
    sc.lid_percent = 1.0
    sc.bess_dispatch_required = True
    sc.bess_optimization_required = bess_optimization_required
    sc.bess_power_mw = 2.0
    sc.bess_duration_hr = 4.0
    sc.bess_rte_percent = 88.0
    sc.bess_min_soc_percent = 10.0
    sc.bess_max_soc_percent = 90.0
    sc.bess_strategy = "tou_arbitrage"
    sc.bess_installed_cost_per_kwh = 250.0
    sc.bess_cycles_warranty = 5000
    sc.bess_solar_only_charging = False
    sc.bess_grid_only_charging = False
    sc.bess_power_min_mw = 1.0
    sc.bess_power_max_mw = 3.0
    sc.bess_duration_min_hr = 2.0
    sc.bess_duration_max_hr = 5.0
    sc.bess_opex_per_kw_year = 10.0
    sc.discount_rate_pct = 7.0
    sc.project_lifetime_years = 25
    sc.rate_escalation_pct = 2.0
    sc.solar_cost_per_kw_dc = 1200.0
    sc.solar_cost_per_kw_ac = 200.0
    sc.solar_opex_per_kw_dc_year = 8.0
    sc.bill_calculation = True
    sc.rate_file_path = None
    sc.utility_name = None
    sc.tariff_name = None
    sc.load_profile_path = None
    sc.load_type = None
    sc.annual_consumption_kwh = None
    sc.peak_demand_kw = None
    sc.data_source = "nsrdb"
    sc.resource_file_path = None
    return sc


def _make_bess_sizing_dict() -> dict:
    """Build a minimal bess_sizing summary dict."""
    return {
        "optimal_power_mw": 2.5,
        "optimal_duration_hr": 4.0,
        "optimal_capacity_kwh": 10000.0,
        "bess_npv": 75000.0,
        "total_project_npv": 200000.0,
        "system_lcoe_per_kwh": 0.052,
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
        "rate_escalation_pct": 2.0,
        "sweep_results": [
            {
                "power_mw": 2.5,
                "duration_hr": 4.0,
                "capacity_kwh": 10000.0,
                "bess_npv": 75000.0,
                "installed_cost": 250000.0,
                "bess_incremental_savings": 12000.0,
            },
        ],
    }


# ---------------------------------------------------------------------------
# Model column tests
# ---------------------------------------------------------------------------


class TestRunInputColumns:
    """Verify RunInputs ORM model has all M14c columns."""

    def test_all_new_columns_present(self) -> None:
        """RunInput should have all 11 new M14c input columns."""
        mapper = sa_inspect(RunInput)
        column_names = {col.key for col in mapper.mapper.column_attrs}
        for col in NEW_INPUT_COLUMNS:
            assert col in column_names, f"Missing column: {col}"

    def test_new_columns_are_nullable(self) -> None:
        """All new input columns should be nullable (optional for non-optimization runs)."""
        mapper = sa_inspect(RunInput)
        columns_by_name = {c.name: c for c in mapper.mapper.columns}
        for col_name in NEW_INPUT_COLUMNS:
            col = columns_by_name[col_name]
            assert col.nullable is True, f"{col_name} should be nullable"


class TestRunResultColumns:
    """Verify RunResults ORM model has bess_sizing column."""

    def test_bess_sizing_column_present(self) -> None:
        """RunResult should have a bess_sizing JSON column."""
        mapper = sa_inspect(RunResult)
        column_names = {col.key for col in mapper.mapper.column_attrs}
        assert "bess_sizing" in column_names

    def test_bess_sizing_column_is_nullable(self) -> None:
        """bess_sizing should be nullable (absent for non-optimization runs)."""
        mapper = sa_inspect(RunResult)
        columns_by_name = {c.name: c for c in mapper.mapper.columns}
        assert columns_by_name["bess_sizing"].nullable is True


# ---------------------------------------------------------------------------
# Writer mapping tests
# ---------------------------------------------------------------------------


class TestWriterMapping:
    """Verify writer maps new fields correctly without hitting a real DB."""

    @patch("src.database.writer.get_session")
    def test_new_inputs_mapped_when_optimization_required(
        self, mock_get_session: MagicMock
    ) -> None:
        """When bess_optimization_required=True, all 11 new fields are mapped
        to RunInput from SiteConfig."""
        # Capture the RunInput object the writer creates
        captured_inputs: list = []

        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(
            return_value=mock_session
        )
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        # Intercept session.add to capture ORM objects
        def capture_add(obj: object) -> None:
            if isinstance(obj, RunInput):
                captured_inputs.append(obj)

        mock_session.add.side_effect = capture_add
        # Make flush/query work
        mock_session.flush.return_value = None
        mock_session.expunge.return_value = None
        customer_mock = MagicMock()
        customer_mock.id = 1
        mock_session.query.return_value.filter_by.return_value.first.return_value = (
            customer_mock
        )

        # Need to mock the Site query to return a Site-like mock
        site_mock = MagicMock()
        site_mock.id = 1
        site_mock.latitude = 33.0
        site_mock.longitude = -112.0

        # query(Customer) → customer_mock; query(Site) → site_mock
        call_count = {"n": 0}

        def query_side_effect(model: type) -> MagicMock:
            call_count["n"] += 1
            result = MagicMock()
            if model.__name__ == "Customer":
                result.filter_by.return_value.first.return_value = customer_mock
            else:
                result.filter_by.return_value.first.return_value = site_mock
            return result

        mock_session.query.side_effect = query_side_effect

        from src.database.writer import save_run_to_db

        sc = _make_site_config_mock(bess_optimization_required=True)
        summary = {"loss_data": {}, "bess_sizing": _make_bess_sizing_dict()}

        save_run_to_db(site_config=sc, summary=summary, database_url="mock://")

        assert len(captured_inputs) == 1
        ri = captured_inputs[0]

        # Verify all 11 new fields are mapped from site config
        assert ri.bess_power_min_mw == 1.0
        assert ri.bess_power_max_mw == 3.0
        assert ri.bess_duration_min_hr == 2.0
        assert ri.bess_duration_max_hr == 5.0
        assert ri.bess_opex_per_kw_year == 10.0
        assert ri.discount_rate_pct == 7.0
        assert ri.project_lifetime_years == 25
        assert ri.rate_escalation_pct == 2.0
        assert ri.solar_cost_per_kw_dc == 1200.0
        assert ri.solar_cost_per_kw_ac == 200.0
        assert ri.solar_opex_per_kw_dc_year == 8.0

    @patch("src.database.writer.get_session")
    def test_new_inputs_null_when_optimization_not_required(
        self, mock_get_session: MagicMock
    ) -> None:
        """When bess_optimization_required=False, new fields are None."""
        captured_inputs: list = []

        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(
            return_value=mock_session
        )
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        def capture_add(obj: object) -> None:
            if isinstance(obj, RunInput):
                captured_inputs.append(obj)

        mock_session.add.side_effect = capture_add
        mock_session.flush.return_value = None
        mock_session.expunge.return_value = None

        customer_mock = MagicMock()
        customer_mock.id = 1
        site_mock = MagicMock()
        site_mock.id = 1
        site_mock.latitude = 33.0
        site_mock.longitude = -112.0

        def query_side_effect(model: type) -> MagicMock:
            result = MagicMock()
            if model.__name__ == "Customer":
                result.filter_by.return_value.first.return_value = customer_mock
            else:
                result.filter_by.return_value.first.return_value = site_mock
            return result

        mock_session.query.side_effect = query_side_effect

        from src.database.writer import save_run_to_db

        sc = _make_site_config_mock(bess_optimization_required=False)
        summary = {"loss_data": {}}

        save_run_to_db(site_config=sc, summary=summary, database_url="mock://")

        assert len(captured_inputs) == 1
        ri = captured_inputs[0]
        for col in NEW_INPUT_COLUMNS:
            assert getattr(ri, col) is None, f"{col} should be None"

    @patch("src.database.writer.get_session")
    def test_bess_sizing_mapped_to_results(
        self, mock_get_session: MagicMock
    ) -> None:
        """summary['bess_sizing'] is stored as JSON in RunResult.bess_sizing."""
        captured_results: list = []

        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(
            return_value=mock_session
        )
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        def capture_add(obj: object) -> None:
            if isinstance(obj, RunResult):
                captured_results.append(obj)

        mock_session.add.side_effect = capture_add
        mock_session.flush.return_value = None
        mock_session.expunge.return_value = None

        customer_mock = MagicMock()
        customer_mock.id = 1
        site_mock = MagicMock()
        site_mock.id = 1
        site_mock.latitude = 33.0
        site_mock.longitude = -112.0

        def query_side_effect(model: type) -> MagicMock:
            result = MagicMock()
            if model.__name__ == "Customer":
                result.filter_by.return_value.first.return_value = customer_mock
            else:
                result.filter_by.return_value.first.return_value = site_mock
            return result

        mock_session.query.side_effect = query_side_effect

        from src.database.writer import save_run_to_db

        sizing_dict = _make_bess_sizing_dict()
        sc = _make_site_config_mock(bess_optimization_required=True)
        summary = {"loss_data": {}, "bess_sizing": sizing_dict}

        save_run_to_db(site_config=sc, summary=summary, database_url="mock://")

        assert len(captured_results) == 1
        rr = captured_results[0]
        assert rr.bess_sizing is sizing_dict
        assert rr.bess_sizing["optimal_power_mw"] == 2.5
        assert rr.bess_sizing["combos_evaluated"] == 24
        assert len(rr.bess_sizing["sweep_results"]) == 1

    @patch("src.database.writer.get_session")
    def test_bess_sizing_none_when_absent(
        self, mock_get_session: MagicMock
    ) -> None:
        """When summary has no 'bess_sizing', RunResult.bess_sizing is None."""
        captured_results: list = []

        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(
            return_value=mock_session
        )
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        def capture_add(obj: object) -> None:
            if isinstance(obj, RunResult):
                captured_results.append(obj)

        mock_session.add.side_effect = capture_add
        mock_session.flush.return_value = None
        mock_session.expunge.return_value = None

        customer_mock = MagicMock()
        customer_mock.id = 1
        site_mock = MagicMock()
        site_mock.id = 1
        site_mock.latitude = 33.0
        site_mock.longitude = -112.0

        def query_side_effect(model: type) -> MagicMock:
            result = MagicMock()
            if model.__name__ == "Customer":
                result.filter_by.return_value.first.return_value = customer_mock
            else:
                result.filter_by.return_value.first.return_value = site_mock
            return result

        mock_session.query.side_effect = query_side_effect

        from src.database.writer import save_run_to_db

        sc = _make_site_config_mock(bess_optimization_required=False)
        summary = {"loss_data": {}}

        save_run_to_db(site_config=sc, summary=summary, database_url="mock://")

        assert len(captured_results) == 1
        assert captured_results[0].bess_sizing is None
