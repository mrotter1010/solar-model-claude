"""Tests for M14b database schema: charging modes, export metrics, NEM fields."""

import json
from pathlib import Path

import pytest
from sqlalchemy import Boolean, Float, Integer, String, text
from sqlalchemy.dialects.postgresql import JSON

from src.config.schema import SiteConfig
from src.database.connection import get_engine
from src.database.models import Base, RunInput, RunResult
from src.database.writer import save_run_to_db

TEST_DB_URL = "postgresql://solar_model:solar_model@localhost:5432/solar_model_test"

OUTPUT_DIR = Path("outputs/test_results/m14b_prompt8")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _write_output(name: str, data: dict) -> None:
    """Write test output to JSON for inspection."""
    path = OUTPUT_DIR / f"{name}.json"
    path.write_text(json.dumps(data, indent=2, default=str))


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
    """Build a SiteConfig for testing."""
    defaults = {
        "run_name": "Run_M14b_001",
        "site_name": "M14b Test Site",
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
        "site_name": "M14b Test Site",
        "run_name": "Run_M14b_001",
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


def _make_bess_dispatch_dict(**overrides: object) -> dict:
    """Build a bess_dispatch summary dict with M14b fields."""
    bd = {
        "bess_power_mw": 5.0,
        "bess_duration_hr": 4,
        "bess_capacity_kwh": 20000,
        "bess_strategy": "global",
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
        # M14b new fields
        "total_export_kwh": 150000.0,
        "total_export_hours": 3650,
        "charging_source": "solar_only",
        "solar_only_export_kwh": 50000.0,
        "solar_only_export_credits": 2000.0,
        "solar_plus_bess_export_kwh": 45000.0,
        "solar_plus_bess_export_credits": 1800.0,
    }
    bd.update(overrides)
    return bd


# ---------------------------------------------------------------------------
# Schema tests — new RunInputs columns
# ---------------------------------------------------------------------------


class TestRunInputM14bColumns:
    """Verify M14b columns exist on RunInput with correct types and defaults."""

    def test_bess_solar_only_charging_column(self) -> None:
        """bess_solar_only_charging should be a non-nullable Boolean column."""
        col = RunInput.__table__.columns["bess_solar_only_charging"]
        assert isinstance(col.type, Boolean)
        assert col.nullable is False

    def test_bess_grid_only_charging_column(self) -> None:
        """bess_grid_only_charging should be a non-nullable Boolean column."""
        col = RunInput.__table__.columns["bess_grid_only_charging"]
        assert isinstance(col.type, Boolean)
        assert col.nullable is False

    def test_defaults_are_false(self) -> None:
        """New charging mode columns default to False in the DB."""
        site_config = _make_site_config()
        summary = _make_summary()

        save_run_to_db(
            site_config=site_config, summary=summary, database_url=TEST_DB_URL
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT bess_solar_only_charging, bess_grid_only_charging "
                     "FROM run_inputs LIMIT 1")
            ).mappings().first()
            assert row["bess_solar_only_charging"] is False
            assert row["bess_grid_only_charging"] is False
        engine.dispose()


# ---------------------------------------------------------------------------
# Writer mapping tests — charging modes
# ---------------------------------------------------------------------------


class TestWriterMapsChargingModes:
    """Verify writer maps bess_solar_only_charging and bess_grid_only_charging."""

    def test_solar_only_charging_mapped(self) -> None:
        """bess_solar_only_charging=True is persisted to run_inputs."""
        site_config = _make_site_config(
            bess_dispatch_required=True,
            bess_power_mw=5.0,
            bess_duration_hr=4.0,
            bess_solar_only_charging=True,
            bess_grid_only_charging=False,
            bill_calculation=True,
            rate_file_path="tests/fixtures/rates/sample_rate.json",
            load_type="Warehouse",
            annual_consumption_kwh=3_500_000.0,
        )
        summary = _make_summary()

        save_run_to_db(
            site_config=site_config, summary=summary, database_url=TEST_DB_URL
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT bess_solar_only_charging, bess_grid_only_charging "
                     "FROM run_inputs LIMIT 1")
            ).mappings().first()
            assert row["bess_solar_only_charging"] is True
            assert row["bess_grid_only_charging"] is False
        engine.dispose()

        _write_output("solar_only_charging_mapped", {
            "bess_solar_only_charging": True,
            "bess_grid_only_charging": False,
        })

    def test_grid_only_charging_mapped(self) -> None:
        """bess_grid_only_charging=True is persisted to run_inputs."""
        site_config = _make_site_config(
            bess_dispatch_required=True,
            bess_power_mw=5.0,
            bess_duration_hr=4.0,
            bess_solar_only_charging=False,
            bess_grid_only_charging=True,
            bill_calculation=True,
            rate_file_path="tests/fixtures/rates/sample_rate.json",
            load_type="Warehouse",
            annual_consumption_kwh=3_500_000.0,
        )
        summary = _make_summary()

        save_run_to_db(
            site_config=site_config, summary=summary, database_url=TEST_DB_URL
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT bess_solar_only_charging, bess_grid_only_charging "
                     "FROM run_inputs LIMIT 1")
            ).mappings().first()
            assert row["bess_solar_only_charging"] is False
            assert row["bess_grid_only_charging"] is True
        engine.dispose()

    def test_charging_modes_false_when_bess_disabled(self) -> None:
        """Charging modes default to False when BESS is disabled."""
        site_config = _make_site_config(bess_dispatch_required=False)
        summary = _make_summary()

        save_run_to_db(
            site_config=site_config, summary=summary, database_url=TEST_DB_URL
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT bess_solar_only_charging, bess_grid_only_charging "
                     "FROM run_inputs LIMIT 1")
            ).mappings().first()
            assert row["bess_solar_only_charging"] is False
            assert row["bess_grid_only_charging"] is False
        engine.dispose()


# ---------------------------------------------------------------------------
# Writer mapping tests — export and NEM fields in BESS JSON
# ---------------------------------------------------------------------------


class TestWriterMapsBessExportFields:
    """Verify new export/NEM fields are persisted in bess_dispatch JSON."""

    def test_export_metrics_in_bess_dispatch_json(self) -> None:
        """Export metrics (total_export_kwh, etc.) are stored in bess_dispatch JSON."""
        site_config = _make_site_config(
            bess_dispatch_required=True,
            bess_power_mw=5.0,
            bess_duration_hr=4.0,
            bess_solar_only_charging=True,
            bill_calculation=True,
            rate_file_path="tests/fixtures/rates/sample_rate.json",
            load_type="Warehouse",
            annual_consumption_kwh=3_500_000.0,
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

            # M14b export metrics
            assert stored["total_export_kwh"] == pytest.approx(150000.0)
            assert stored["total_export_hours"] == 3650
            assert stored["charging_source"] == "solar_only"

            # M14b bill comparison export fields
            assert stored["solar_only_export_kwh"] == pytest.approx(50000.0)
            assert stored["solar_only_export_credits"] == pytest.approx(2000.0)
            assert stored["solar_plus_bess_export_kwh"] == pytest.approx(45000.0)
            assert stored["solar_plus_bess_export_credits"] == pytest.approx(1800.0)
        engine.dispose()

        _write_output("export_metrics_stored", {
            "total_export_kwh": stored["total_export_kwh"],
            "total_export_hours": stored["total_export_hours"],
            "charging_source": stored["charging_source"],
            "solar_only_export_kwh": stored["solar_only_export_kwh"],
            "solar_only_export_credits": stored["solar_only_export_credits"],
            "solar_plus_bess_export_kwh": stored["solar_plus_bess_export_kwh"],
            "solar_plus_bess_export_credits": stored["solar_plus_bess_export_credits"],
        })

    def test_bess_dispatch_without_export_fields(self) -> None:
        """Pre-M14b bess_dispatch dicts (without export fields) still work."""
        site_config = _make_site_config(
            bess_dispatch_required=True,
            bess_power_mw=5.0,
            bess_duration_hr=4.0,
            bill_calculation=True,
            rate_file_path="tests/fixtures/rates/sample_rate.json",
            load_type="Warehouse",
            annual_consumption_kwh=3_500_000.0,
        )
        # Pre-M14b dict — no export fields
        bess_dispatch = {
            "bess_power_mw": 5.0,
            "bess_duration_hr": 4,
            "bess_capacity_kwh": 20000,
            "bess_strategy": "global",
            "solar_only_annual_bill": 285000.0,
            "solar_plus_bess_annual_bill": 242000.0,
            "bess_incremental_savings": 43000.0,
            "bess_demand_savings": 35000.0,
            "bess_energy_savings": 8000.0,
            "annual_cycles": 312.5,
            "monthly_solver_status": ["Optimal"] * 12,
            "heatmap_data": [[0.0] * 24 for _ in range(12)],
        }
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
            assert stored["bess_incremental_savings"] == pytest.approx(43000.0)
            # Export fields should not be present (pre-M14b)
            assert "total_export_kwh" not in stored
        engine.dispose()


class TestWriterMapsNemBillFields:
    """Verify NEM bill fields are persisted in bill_savings JSON."""

    def test_nem_fields_in_bill_savings_json(self) -> None:
        """NEM export and true-up fields flow through bill_savings JSON."""
        site_config = _make_site_config()
        bill_savings = {
            "annual_bill_without_solar": 500000.0,
            "annual_bill_with_solar": 350000.0,
            "annual_savings": 150000.0,
            "savings_percent": 30.0,
            "avoided_cost_per_kwh": 0.08,
            "annual_demand_savings": 50000.0,
            # M14b NEM fields
            "annual_export_kwh": 75000.0,
            "annual_export_credits": 3000.0,
            "nem_true_up_credit": 500.0,
        }
        summary = _make_summary(bill_savings=bill_savings)

        save_run_to_db(
            site_config=site_config, summary=summary, database_url=TEST_DB_URL
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT bill_savings FROM run_results LIMIT 1")
            ).mappings().first()

            stored = row["bill_savings"]
            assert stored is not None
            assert stored["annual_export_kwh"] == pytest.approx(75000.0)
            assert stored["annual_export_credits"] == pytest.approx(3000.0)
            assert stored["nem_true_up_credit"] == pytest.approx(500.0)
        engine.dispose()

        _write_output("nem_bill_fields_stored", {
            "annual_export_kwh": stored["annual_export_kwh"],
            "annual_export_credits": stored["annual_export_credits"],
            "nem_true_up_credit": stored["nem_true_up_credit"],
        })
