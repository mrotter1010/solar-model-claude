"""Tests for M14e FTM database schema additions and writer mapping.

Tests ORM model column presence and writer mapping logic. Uses mock-based
approach (no PostgreSQL dependency needed).
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import inspect as sa_inspect

from src.database.models import RunInput, RunResult


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

# New RunInputs columns for M14e FTM dispatch
NEW_FTM_INPUT_COLUMNS = [
    "dispatch_mode",
    "iso",
    "lmp_zone",
    "lmp_market",
    "lmp_year",
    "ancillary_revenue_per_kw_year",
]

# New RunResults columns for M14e FTM dispatch
NEW_FTM_RESULT_COLUMNS = [
    "lmp_data",
    "ftm_economics",
]


def _make_site_config_mock(dispatch_mode: str = "btm") -> MagicMock:
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
    sc.bess_optimization_required = False
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
    sc.bess_power_min_mw = None
    sc.bess_power_max_mw = None
    sc.bess_duration_min_hr = None
    sc.bess_duration_max_hr = None
    sc.bess_opex_per_kw_year = None
    sc.discount_rate_pct = None
    sc.project_lifetime_years = None
    sc.rate_escalation_pct = None
    sc.solar_cost_per_kw_dc = None
    sc.solar_cost_per_kw_ac = None
    sc.solar_opex_per_kw_dc_year = None
    sc.bill_calculation = False
    sc.rate_file_path = None
    sc.utility_name = None
    sc.tariff_name = None
    sc.load_profile_path = None
    sc.load_type = None
    sc.annual_consumption_kwh = None
    sc.peak_demand_kw = None
    sc.data_source = "nsrdb"
    sc.resource_file_path = None
    # FTM fields
    sc.dispatch_mode = dispatch_mode
    sc.iso = "pjm" if dispatch_mode == "ftm" else None
    sc.lmp_zone = "PJMISO" if dispatch_mode == "ftm" else None
    sc.lmp_market = "DAY_AHEAD_HOURLY" if dispatch_mode == "ftm" else "DAY_AHEAD_HOURLY"
    sc.lmp_year = 2024 if dispatch_mode == "ftm" else None
    sc.ancillary_revenue_per_kw_year = 15.0 if dispatch_mode == "ftm" else 0.0
    return sc


def _make_ftm_lmp_dict() -> dict:
    """Build a minimal LMP summary dict."""
    return {
        "iso": "pjm",
        "zone": "PJMISO",
        "market": "DAY_AHEAD_HOURLY",
        "year": 2024,
        "mean_lmp_per_mwh": 42.5,
        "count": 8760,
    }


def _make_ftm_economics_dict() -> dict:
    """Build a minimal FTM economics summary dict."""
    return {
        "annual_revenue": 125000.0,
        "energy_revenue": 110000.0,
        "ancillary_revenue": 15000.0,
        "bess_revenue_uplift": 22000.0,
        "total_revenue_with_bess": 147000.0,
    }


def _setup_mock_session(mock_get_session: MagicMock) -> tuple[MagicMock, list, list]:
    """Set up a mock session that captures RunInput and RunResult objects.

    Returns:
        Tuple of (mock_session, captured_inputs, captured_results).
    """
    captured_inputs: list = []
    captured_results: list = []

    mock_session = MagicMock()
    mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

    def capture_add(obj: object) -> None:
        if isinstance(obj, RunInput):
            captured_inputs.append(obj)
        elif isinstance(obj, RunResult):
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

    return mock_session, captured_inputs, captured_results


# ---------------------------------------------------------------------------
# Model column tests
# ---------------------------------------------------------------------------


class TestRunInputHasFtmColumns:
    """Verify RunInputs ORM model has all M14e FTM columns."""

    def test_all_ftm_input_columns_present(self) -> None:
        """RunInput should have all 6 new FTM input columns."""
        mapper = sa_inspect(RunInput)
        column_names = {col.key for col in mapper.mapper.column_attrs}
        for col in NEW_FTM_INPUT_COLUMNS:
            assert col in column_names, f"Missing column: {col}"

    def test_ftm_input_columns_are_nullable(self) -> None:
        """All FTM input columns should be nullable (None for BTM runs)."""
        mapper = sa_inspect(RunInput)
        columns_by_name = {c.name: c for c in mapper.mapper.columns}
        for col_name in NEW_FTM_INPUT_COLUMNS:
            col = columns_by_name[col_name]
            assert col.nullable is True, f"{col_name} should be nullable"


class TestRunResultHasFtmColumns:
    """Verify RunResults ORM model has M14e FTM result columns."""

    def test_all_ftm_result_columns_present(self) -> None:
        """RunResult should have lmp_data and ftm_economics columns."""
        mapper = sa_inspect(RunResult)
        column_names = {col.key for col in mapper.mapper.column_attrs}
        for col in NEW_FTM_RESULT_COLUMNS:
            assert col in column_names, f"Missing column: {col}"

    def test_ftm_result_columns_are_nullable(self) -> None:
        """FTM result columns should be nullable (None for BTM runs)."""
        mapper = sa_inspect(RunResult)
        columns_by_name = {c.name: c for c in mapper.mapper.columns}
        for col_name in NEW_FTM_RESULT_COLUMNS:
            col = columns_by_name[col_name]
            assert col.nullable is True, f"{col_name} should be nullable"


# ---------------------------------------------------------------------------
# Writer mapping tests
# ---------------------------------------------------------------------------


class TestWriterMapsFtmInputs:
    """Verify writer maps FTM fields to RunInput."""

    @patch("src.database.writer.get_session")
    def test_ftm_inputs_mapped_when_dispatch_mode_ftm(
        self, mock_get_session: MagicMock
    ) -> None:
        """When dispatch_mode='ftm', all 6 FTM fields are mapped to RunInput."""
        _, captured_inputs, _ = _setup_mock_session(mock_get_session)

        from src.database.writer import save_run_to_db

        sc = _make_site_config_mock(dispatch_mode="ftm")
        summary = {"loss_data": {}}

        save_run_to_db(site_config=sc, summary=summary, database_url="mock://")

        assert len(captured_inputs) == 1
        ri = captured_inputs[0]

        assert ri.dispatch_mode == "ftm"
        assert ri.iso == "pjm"
        assert ri.lmp_zone == "PJMISO"
        assert ri.lmp_market == "DAY_AHEAD_HOURLY"
        assert ri.lmp_year == 2024
        assert ri.ancillary_revenue_per_kw_year == 15.0


class TestWriterMapsFtmResults:
    """Verify writer maps FTM summary data to RunResult."""

    @patch("src.database.writer.get_session")
    def test_ftm_results_mapped_as_json_strings(
        self, mock_get_session: MagicMock
    ) -> None:
        """summary['lmp'] and summary['ftm_economics'] stored as JSON strings."""
        _, _, captured_results = _setup_mock_session(mock_get_session)

        from src.database.writer import save_run_to_db

        lmp_dict = _make_ftm_lmp_dict()
        econ_dict = _make_ftm_economics_dict()
        sc = _make_site_config_mock(dispatch_mode="ftm")
        summary = {
            "loss_data": {},
            "lmp": lmp_dict,
            "ftm_economics": econ_dict,
        }

        save_run_to_db(site_config=sc, summary=summary, database_url="mock://")

        assert len(captured_results) == 1
        rr = captured_results[0]

        # Verify stored as JSON strings
        assert isinstance(rr.lmp_data, str)
        assert isinstance(rr.ftm_economics, str)

        # Verify round-trip
        parsed_lmp = json.loads(rr.lmp_data)
        assert parsed_lmp["iso"] == "pjm"
        assert parsed_lmp["mean_lmp_per_mwh"] == 42.5

        parsed_econ = json.loads(rr.ftm_economics)
        assert parsed_econ["annual_revenue"] == 125000.0
        assert parsed_econ["bess_revenue_uplift"] == 22000.0


class TestWriterBtmLeavesFtmNull:
    """Verify BTM runs leave FTM columns as None."""

    @patch("src.database.writer.get_session")
    def test_btm_inputs_ftm_fields_null(
        self, mock_get_session: MagicMock
    ) -> None:
        """BTM dispatch_mode leaves all FTM input columns as None."""
        _, captured_inputs, _ = _setup_mock_session(mock_get_session)

        from src.database.writer import save_run_to_db

        sc = _make_site_config_mock(dispatch_mode="btm")
        summary = {"loss_data": {}}

        save_run_to_db(site_config=sc, summary=summary, database_url="mock://")

        assert len(captured_inputs) == 1
        ri = captured_inputs[0]

        assert ri.dispatch_mode is None
        assert ri.iso is None
        assert ri.lmp_zone is None
        assert ri.lmp_market is None
        assert ri.lmp_year is None
        assert ri.ancillary_revenue_per_kw_year is None

    @patch("src.database.writer.get_session")
    def test_btm_results_ftm_fields_null(
        self, mock_get_session: MagicMock
    ) -> None:
        """BTM summary without lmp/ftm_economics leaves result columns as None."""
        _, _, captured_results = _setup_mock_session(mock_get_session)

        from src.database.writer import save_run_to_db

        sc = _make_site_config_mock(dispatch_mode="btm")
        summary = {"loss_data": {}}

        save_run_to_db(site_config=sc, summary=summary, database_url="mock://")

        assert len(captured_results) == 1
        rr = captured_results[0]

        assert rr.lmp_data is None
        assert rr.ftm_economics is None


# ---------------------------------------------------------------------------
# Migration script test
# ---------------------------------------------------------------------------


class TestMigrationScript:
    """Verify the M14e migration SQL script exists and has correct content."""

    def test_migration_script_exists(self) -> None:
        """scripts/migrate_m14e.sql exists."""
        script_path = Path(__file__).parent.parent / "scripts" / "migrate_m14e.sql"
        assert script_path.exists(), f"Missing migration script: {script_path}"

    def test_migration_script_has_expected_alter_statements(self) -> None:
        """Migration script contains ALTER TABLE statements for all new columns."""
        script_path = Path(__file__).parent.parent / "scripts" / "migrate_m14e.sql"
        content = script_path.read_text()

        # RunInputs columns
        for col in NEW_FTM_INPUT_COLUMNS:
            assert f"ADD COLUMN IF NOT EXISTS {col}" in content, (
                f"Missing ALTER for {col}"
            )

        # RunResults columns
        for col in NEW_FTM_RESULT_COLUMNS:
            assert f"ADD COLUMN IF NOT EXISTS {col}" in content, (
                f"Missing ALTER for {col}"
            )
