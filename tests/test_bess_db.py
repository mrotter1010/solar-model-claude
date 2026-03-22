"""Tests for BESS dispatch database persistence."""

from pathlib import Path

import pytest
from sqlalchemy import Boolean, Float, Integer, String, inspect, text
from sqlalchemy.dialects.postgresql import JSON

from src.config.schema import SiteConfig
from src.database.connection import get_engine
from src.database.models import Base, RunInput, RunResult
from src.database.writer import save_run_to_db

TEST_DB_URL = "postgresql://solar_model:solar_model@localhost:5432/solar_model_test"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _setup_tables():
    """Create all tables before each test, drop after."""
    engine = get_engine(url=TEST_DB_URL)
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)
    engine.dispose()


def _make_site_config(**overrides: object) -> SiteConfig:
    """Build a realistic SiteConfig for testing."""
    defaults = {
        "run_name": "Run_BESS_001",
        "site_name": "Phoenix BESS Site",
        "customer": "TestCustomer",
        "latitude": 33.483,
        "longitude": -112.073,
        "dc_size_mw": 13.0,
        "ac_installed_mw": 10.0,
        "ac_poi_mw": 10.0,
        "racking": "tracker",
        "tilt": 60.0,
        "azimuth": 180.0,
        "module_orientation": "portrait",
        "number_of_modules": 2,
        "ground_clearance_height_m": 1.8,
        "panel_model": "CSI Solar Co. Ltd. CS3U-355P",
        "bifacial": True,
        "inverter_model": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
        "gcr": 0.34,
        "shading_percent": 1.0,
        "dc_wiring_loss_percent": 1.5,
        "ac_wiring_loss_percent": 1.5,
        "transformer_losses_percent": 0.0,
        "degradation_percent": 0.3,
        "availability_percent": 2.0,
        "module_mismatch_percent": 1.5,
        "lid_percent": 1.0,
    }
    defaults.update(overrides)
    return SiteConfig(**defaults)


def _make_summary(**overrides: object) -> dict:
    """Build a realistic summary dict matching pipeline output."""
    summary = {
        "site_name": "Phoenix BESS Site",
        "run_name": "Run_BESS_001",
        "customer": "TestCustomer",
        "weather_year": 2024,
        "dc_size_mw": 13.0,
        "ac_installed_mw": 10.0,
        "ac_poi_mw": 10.0,
        "annual_energy_mwh": 28996.605,
        "net_capacity_factor": 0.331011,
        "specific_yield": 2230.51,
        "performance_ratio": 0.7693,
        "shading_pct_applied": 1.0,
        "loss_data": {
            "annual_dc_nominal": 36667384.56,
            "capacity_factor": 25.72,
            "capacity_factor_ac": 32.73,
            "monthly_energy": [2000000.0] * 12,
            "monthly_snow_loss": [0.0] * 12,
            "annual_dc_snow_loss_percent": 0.0,
        },
        "errors": [],
    }
    summary.update(overrides)
    return summary


def _make_bess_dispatch_dict() -> dict:
    """Build a realistic bess_dispatch summary dict."""
    return {
        "bess_power_mw": 5.0,
        "bess_duration_hr": 4,
        "bess_capacity_kwh": 20000,
        "bess_strategy": "peak_shaving",
        "solar_only_annual_bill": 285000.0,
        "solar_plus_bess_annual_bill": 242000.0,
        "bess_incremental_savings": 43000.0,
        "bess_demand_savings": 35000.0,
        "bess_energy_savings": 8000.0,
        "annual_cycles": 312.5,
        "annual_throughput_kwh": 5000000.0,
        "average_daily_cycles": 0.86,
        "capacity_utilization_pct": 85.6,
        "total_curtailed_kwh": 1250.0,
        "estimated_annual_degradation_pct": 6.25,
        "monthly_solver_status": ["Optimal"] * 12,
        "heatmap_data": [[0.0] * 24 for _ in range(12)],
    }


# ---------------------------------------------------------------------------
# Schema tests (ORM model column presence and types)
# ---------------------------------------------------------------------------


class TestRunInputBessColumns:
    """Verify all 8 new BESS columns exist on RunInput with correct types."""

    def test_bess_power_mw_column(self) -> None:
        """bess_power_mw should be a nullable Float column."""
        col = RunInput.__table__.columns["bess_power_mw"]
        assert isinstance(col.type, Float)
        assert col.nullable is True

    def test_bess_duration_hr_column(self) -> None:
        """bess_duration_hr should be a nullable Float column."""
        col = RunInput.__table__.columns["bess_duration_hr"]
        assert isinstance(col.type, Float)
        assert col.nullable is True

    def test_bess_rte_percent_column(self) -> None:
        """bess_rte_percent should be a nullable Float column."""
        col = RunInput.__table__.columns["bess_rte_percent"]
        assert isinstance(col.type, Float)
        assert col.nullable is True

    def test_bess_min_soc_percent_column(self) -> None:
        """bess_min_soc_percent should be a nullable Float column."""
        col = RunInput.__table__.columns["bess_min_soc_percent"]
        assert isinstance(col.type, Float)
        assert col.nullable is True

    def test_bess_max_soc_percent_column(self) -> None:
        """bess_max_soc_percent should be a nullable Float column."""
        col = RunInput.__table__.columns["bess_max_soc_percent"]
        assert isinstance(col.type, Float)
        assert col.nullable is True

    def test_bess_strategy_column(self) -> None:
        """bess_strategy should be a nullable String column."""
        col = RunInput.__table__.columns["bess_strategy"]
        assert isinstance(col.type, String)
        assert col.nullable is True

    def test_bess_installed_cost_per_kwh_column(self) -> None:
        """bess_installed_cost_per_kwh should be a nullable Float column."""
        col = RunInput.__table__.columns["bess_installed_cost_per_kwh"]
        assert isinstance(col.type, Float)
        assert col.nullable is True

    def test_bess_cycles_warranty_column(self) -> None:
        """bess_cycles_warranty should be a nullable Integer column."""
        col = RunInput.__table__.columns["bess_cycles_warranty"]
        assert isinstance(col.type, Integer)
        assert col.nullable is True


class TestRunResultBessColumn:
    """Verify bess_dispatch JSON column exists on RunResult."""

    def test_bess_dispatch_column_exists(self) -> None:
        """bess_dispatch should be a nullable JSON column."""
        col = RunResult.__table__.columns["bess_dispatch"]
        assert isinstance(col.type, JSON)
        assert col.nullable is True


# ---------------------------------------------------------------------------
# Writer mapping tests (DB roundtrip)
# ---------------------------------------------------------------------------


class TestWriterMapsBessInputs:
    """Verify save_run_to_db maps BESS input fields to RunInputs."""

    def test_bess_inputs_stored(self) -> None:
        """All 8 BESS input fields should be persisted when BESS is enabled."""
        site_config = _make_site_config(
            bess_dispatch_required=True,
            bess_power_mw=5.0,
            bess_duration_hr=4.0,
            bess_rte_percent=88.0,
            bess_min_soc_percent=10.0,
            bess_max_soc_percent=90.0,
            bess_strategy="peak_shaving",
            bess_installed_cost_per_kwh=275.0,
            bess_cycles_warranty=5000,
            # BESS requires bill_calculation=True
            bill_calculation=True,
            rate_file_path="tests/fixtures/rates/sample_rate.json",
            load_type="MediumOffice",
            annual_consumption_kwh=2_000_000.0,
        )
        summary = _make_summary()

        save_run_to_db(
            site_config=site_config, summary=summary, database_url=TEST_DB_URL
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM run_inputs LIMIT 1")
            ).mappings().first()

            assert row["bess_dispatch_required"] is True
            assert row["bess_power_mw"] == pytest.approx(5.0)
            assert row["bess_duration_hr"] == pytest.approx(4.0)
            assert row["bess_rte_percent"] == pytest.approx(88.0)
            assert row["bess_min_soc_percent"] == pytest.approx(10.0)
            assert row["bess_max_soc_percent"] == pytest.approx(90.0)
            assert row["bess_strategy"] == "peak_shaving"
            assert row["bess_installed_cost_per_kwh"] == pytest.approx(275.0)
            assert row["bess_cycles_warranty"] == 5000
        engine.dispose()


class TestWriterMapsBessResults:
    """Verify save_run_to_db maps bess_dispatch JSON to RunResults."""

    def test_bess_dispatch_json_stored(self) -> None:
        """bess_dispatch dict should be stored as JSON in run_results."""
        site_config = _make_site_config(
            bess_dispatch_required=True,
            bess_power_mw=5.0,
            bess_duration_hr=4.0,
            # BESS requires bill_calculation=True
            bill_calculation=True,
            rate_file_path="tests/fixtures/rates/sample_rate.json",
            load_type="MediumOffice",
            annual_consumption_kwh=2_000_000.0,
        )
        bess_dispatch = _make_bess_dispatch_dict()
        summary = _make_summary(bess_dispatch=bess_dispatch)

        save_run_to_db(
            site_config=site_config, summary=summary, database_url=TEST_DB_URL
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT bess_dispatch FROM run_results LIMIT 1")
            ).mappings().first()

            stored = row["bess_dispatch"]
            assert stored is not None
            assert stored["bess_power_mw"] == 5.0
            assert stored["bess_capacity_kwh"] == 20000
            assert stored["bess_incremental_savings"] == pytest.approx(43000.0)
            assert stored["annual_cycles"] == pytest.approx(312.5)
            assert len(stored["monthly_solver_status"]) == 12
            assert len(stored["heatmap_data"]) == 12
            assert len(stored["heatmap_data"][0]) == 24
        engine.dispose()


class TestBackwardCompatibility:
    """Verify BESS fields are None when bess_dispatch_required is False."""

    def test_bess_inputs_null_when_disabled(self) -> None:
        """All BESS-specific RunInputs fields should be None when BESS is off."""
        site_config = _make_site_config(bess_dispatch_required=False)
        summary = _make_summary()

        save_run_to_db(
            site_config=site_config, summary=summary, database_url=TEST_DB_URL
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM run_inputs LIMIT 1")
            ).mappings().first()

            assert row["bess_dispatch_required"] is False
            assert row["bess_power_mw"] is None
            assert row["bess_duration_hr"] is None
            assert row["bess_rte_percent"] is None
            assert row["bess_min_soc_percent"] is None
            assert row["bess_max_soc_percent"] is None
            assert row["bess_strategy"] is None
            assert row["bess_installed_cost_per_kwh"] is None
            assert row["bess_cycles_warranty"] is None
        engine.dispose()

    def test_bess_dispatch_null_when_disabled(self) -> None:
        """RunResults.bess_dispatch should be None when BESS is off."""
        site_config = _make_site_config(bess_dispatch_required=False)
        summary = _make_summary()  # No bess_dispatch key

        save_run_to_db(
            site_config=site_config, summary=summary, database_url=TEST_DB_URL
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT bess_dispatch FROM run_results LIMIT 1")
            ).mappings().first()
            assert row["bess_dispatch"] is None
        engine.dispose()
