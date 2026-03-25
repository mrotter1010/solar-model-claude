"""Tests for database write service and queries using solar_model_test database."""

import json
import uuid
from pathlib import Path

import pytest
from sqlalchemy import text

from src.config.schema import SiteConfig
from src.database.connection import get_engine
from src.database.models import Base, Customer, Run, RunInput, RunResult, Site
from src.database.queries import recreate_run_input
from src.database.writer import build_data_sources, save_run_to_db
from src.utils.exceptions import SolarModelError

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
        "run_name": "Run_001",
        "site_name": "Phoenix Solar Farm",
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
        "site_name": "Phoenix Solar Farm",
        "run_name": "Run_001",
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
        "simulation_timestamp": "2026-03-07T21:34:01.229058+00:00",
        "loss_data": {
            "annual_dc_nominal": 36667384.56,
            "annual_dc_gross": 32879067.78,
            "annual_dc_net": 31423187.14,
            "annual_ac_gross": 30655220.01,
            "annual_energy": 29289500.49,
            "capacity_factor": 25.72,
            "capacity_factor_ac": 32.73,
            "kwh_per_kw": 2253.04,
            "performance_ratio": 0.777,
            "annual_dc_snow_loss_percent": 0.0,
            "annual_snow_loss": 0.0,
            "monthly_snow_loss": [0.0] * 12,
            "monthly_energy": [
                1485255.48, 1678123.17, 2421957.98, 3051744.15,
                3355456.39, 3032491.55, 3083914.57, 2782115.35,
                2690712.28, 2308281.40, 1806278.04, 1593170.14,
            ],
            "monthly_poa_eff": [0.0] * 12,
            "annual_poa_shading_loss_percent": 1.53,
            "annual_poa_soiling_loss_percent": 2.64,
            "annual_poa_cover_loss_percent": 0.68,
            "annual_dc_module_loss_percent": 10.33,
            "annual_dc_mismatch_loss_percent": 1.5,
            "annual_dc_diodes_loss_percent": 0.5,
            "annual_dc_wiring_loss_percent": 1.5,
            "annual_dc_nameplate_loss_percent": 1.0,
            "annual_dc_tracking_loss_percent": 0.0,
            "annual_bifacial_electrical_mismatch_percent": 0.17,
            "annual_ac_inv_clip_loss_percent": 0.94,
            "annual_ac_inv_eff_loss_percent": 1.42,
            "annual_ac_wiring_loss_percent": 1.5,
            "annual_xfmr_loss_percent": 0.0,
            "annual_ac_perf_adj_loss_percent": 3.0,
            "annual_poa_rear_gain_percent": 3.25,
            "avg_daytime_ghi_wm2": 465.56,
            "annual_ghi_kwh_m2": 2131.8,
        },
        "errors": [],
    }
    summary.update(overrides)
    return summary


# ---------------------------------------------------------------------------
# Full save flow
# ---------------------------------------------------------------------------


class TestSaveRunToDb:
    """Tests for the save_run_to_db function."""

    def test_creates_all_records(self) -> None:
        """A single save creates customer, site, run, inputs, and results."""
        site_config = _make_site_config()
        summary = _make_summary()

        run = save_run_to_db(
            site_config=site_config,
            summary=summary,
            timeseries_path=Path("/outputs/ts.csv"),
            report_path=Path("/outputs/report.pdf"),
            climate_path=Path("/outputs/climate.csv"),
            database_url=TEST_DB_URL,
        )

        # Verify run was created with a UUID
        assert run.id is not None
        assert run.status == "success"
        assert run.weather_year == 2024
        assert run.data_sources == "NSRDB v4.0.0, Open-Meteo ERA5"

        # Verify all records exist by querying fresh
        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM customers")).scalar() == 1
            assert conn.execute(text("SELECT count(*) FROM sites")).scalar() == 1
            assert conn.execute(text("SELECT count(*) FROM runs")).scalar() == 1
            assert conn.execute(text("SELECT count(*) FROM run_inputs")).scalar() == 1
            assert conn.execute(text("SELECT count(*) FROM run_results")).scalar() == 1
        engine.dispose()

    def test_run_inputs_match_site_config(self) -> None:
        """RunInput columns match the SiteConfig values."""
        site_config = _make_site_config()
        summary = _make_summary()

        run = save_run_to_db(
            site_config=site_config, summary=summary, database_url=TEST_DB_URL
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM run_inputs LIMIT 1")).mappings().first()
            assert row["latitude"] == pytest.approx(33.483)
            assert row["longitude"] == pytest.approx(-112.073)
            assert row["dc_size_mw"] == pytest.approx(13.0)
            assert row["racking"] == "tracker"
            assert row["bifacial"] is True
            assert row["gcr"] == pytest.approx(0.34)
            assert row["shading_pct"] == pytest.approx(1.0)
            assert row["module_mismatch_pct"] == pytest.approx(1.5)
            assert row["lid_pct"] == pytest.approx(1.0)
        engine.dispose()

    def test_run_results_match_summary(self) -> None:
        """RunResult columns match the summary dict values."""
        site_config = _make_site_config()
        summary = _make_summary()

        save_run_to_db(
            site_config=site_config, summary=summary, database_url=TEST_DB_URL
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM run_results LIMIT 1")).mappings().first()
            assert row["annual_energy_mwh"] == pytest.approx(28996.605)
            assert row["net_capacity_factor"] == pytest.approx(0.331011)
            assert row["specific_yield"] == pytest.approx(2230.51)
            assert row["capacity_factor_dc"] == pytest.approx(25.72)
            assert row["capacity_factor_ac"] == pytest.approx(32.73)
            assert row["annual_dc_nominal"] == pytest.approx(36667384.56)
            assert row["annual_snow_loss_pct"] == pytest.approx(0.0)
            # JSON fields
            assert len(row["monthly_energy"]) == 12
            assert len(row["monthly_snow_loss"]) == 12
            assert "annual_dc_nominal" in row["loss_data"]
        engine.dispose()

    def test_file_paths_stored(self) -> None:
        """File paths are stored as strings in run_results."""
        site_config = _make_site_config()
        summary = _make_summary()

        save_run_to_db(
            site_config=site_config,
            summary=summary,
            timeseries_path=Path("/out/ts.csv"),
            report_path=Path("/out/report.pdf"),
            climate_path=Path("/out/weather.csv"),
            database_url=TEST_DB_URL,
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM run_results LIMIT 1")).mappings().first()
            assert row["timeseries_file_path"] == "/out/ts.csv"
            assert row["report_file_path"] == "/out/report.pdf"
            assert row["climate_file_path"] == "/out/weather.csv"
        engine.dispose()

    def test_null_file_paths(self) -> None:
        """File paths default to None when not provided."""
        site_config = _make_site_config()
        summary = _make_summary()

        save_run_to_db(
            site_config=site_config, summary=summary, database_url=TEST_DB_URL
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM run_results LIMIT 1")).mappings().first()
            assert row["timeseries_file_path"] is None
            assert row["report_file_path"] is None
            assert row["climate_file_path"] is None
        engine.dispose()

    def test_git_hash_populated(self) -> None:
        """Git hash should be populated (we're in a git repo)."""
        site_config = _make_site_config()
        summary = _make_summary()

        run = save_run_to_db(
            site_config=site_config, summary=summary, database_url=TEST_DB_URL
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(text("SELECT git_hash FROM runs LIMIT 1")).mappings().first()
            assert row["git_hash"] is not None
            assert len(row["git_hash"]) >= 7  # short hash
        engine.dispose()

    def test_subhourly_fields_stored_when_present(self) -> None:
        """Subhourly correction fields are stored when present in summary."""
        site_config = _make_site_config()
        summary = _make_summary(
            subhourly_correction_pct=0.3,
            subhourly_model_version="v1",
            raw_annual_energy_mwh=29100.0,
        )

        save_run_to_db(
            site_config=site_config, summary=summary, database_url=TEST_DB_URL
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM run_results LIMIT 1")).mappings().first()
            assert row["subhourly_correction_pct"] == pytest.approx(0.3)
            assert row["subhourly_model_version"] == "v1"
            assert row["raw_annual_energy_mwh"] == pytest.approx(29100.0)
        engine.dispose()

    def test_subhourly_fields_null_when_absent(self) -> None:
        """Subhourly correction fields are NULL when not in summary."""
        site_config = _make_site_config()
        summary = _make_summary()  # No subhourly fields

        save_run_to_db(
            site_config=site_config, summary=summary, database_url=TEST_DB_URL
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM run_results LIMIT 1")).mappings().first()
            assert row["subhourly_correction_pct"] is None
            assert row["subhourly_model_version"] is None
            assert row["raw_annual_energy_mwh"] is None
        engine.dispose()


# ---------------------------------------------------------------------------
# Upsert behavior
# ---------------------------------------------------------------------------


class TestUpsertBehavior:
    """Tests for customer/site upsert logic."""

    def test_second_run_reuses_customer(self) -> None:
        """Two runs with same customer name should create only one customer."""
        site_config_1 = _make_site_config(run_name="Run_001")
        site_config_2 = _make_site_config(run_name="Run_002")

        save_run_to_db(
            site_config=site_config_1,
            summary=_make_summary(),
            database_url=TEST_DB_URL,
        )
        save_run_to_db(
            site_config=site_config_2,
            summary=_make_summary(),
            database_url=TEST_DB_URL,
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM customers")).scalar() == 1
            assert conn.execute(text("SELECT count(*) FROM sites")).scalar() == 1
            assert conn.execute(text("SELECT count(*) FROM runs")).scalar() == 2
        engine.dispose()

    def test_second_run_reuses_site(self) -> None:
        """Two runs with same site name should create only one site record."""
        site_config_1 = _make_site_config(run_name="Run_001")
        site_config_2 = _make_site_config(run_name="Run_002")

        save_run_to_db(
            site_config=site_config_1,
            summary=_make_summary(),
            database_url=TEST_DB_URL,
        )
        save_run_to_db(
            site_config=site_config_2,
            summary=_make_summary(),
            database_url=TEST_DB_URL,
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM sites")).scalar() == 1
            # Both runs should reference the same site_id
            site_ids = conn.execute(text("SELECT DISTINCT site_id FROM runs")).scalars().all()
            assert len(site_ids) == 1
        engine.dispose()

    def test_different_customers_create_separate_records(self) -> None:
        """Runs with different customer names create separate customer records."""
        site_config_1 = _make_site_config(
            customer="CustomerA", site_name="SiteA"
        )
        site_config_2 = _make_site_config(
            customer="CustomerB", site_name="SiteB"
        )

        save_run_to_db(
            site_config=site_config_1,
            summary=_make_summary(),
            database_url=TEST_DB_URL,
        )
        save_run_to_db(
            site_config=site_config_2,
            summary=_make_summary(),
            database_url=TEST_DB_URL,
        )

        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM customers")).scalar() == 2
        engine.dispose()

    def test_site_coordinate_mismatch_raises_error(self) -> None:
        """Reusing a site name with different coordinates raises SolarModelError."""
        site_config_1 = _make_site_config(latitude=33.483, longitude=-112.073)
        site_config_2 = _make_site_config(
            run_name="Run_002", latitude=34.0, longitude=-112.073
        )

        save_run_to_db(
            site_config=site_config_1,
            summary=_make_summary(),
            database_url=TEST_DB_URL,
        )

        with pytest.raises(SolarModelError, match="exists with coordinates") as exc_info:
            save_run_to_db(
                site_config=site_config_2,
                summary=_make_summary(),
                database_url=TEST_DB_URL,
            )

        # Verify the failed run was NOT committed (transaction rolled back)
        engine = get_engine(url=TEST_DB_URL)
        with engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM runs")).scalar() == 1
        engine.dispose()

    def test_coordinate_mismatch_includes_context(self) -> None:
        """Coordinate mismatch error includes site name and both coordinate pairs."""
        site_config_1 = _make_site_config()
        site_config_2 = _make_site_config(
            run_name="Run_002", longitude=-110.0
        )

        save_run_to_db(
            site_config=site_config_1,
            summary=_make_summary(),
            database_url=TEST_DB_URL,
        )

        with pytest.raises(SolarModelError) as exc_info:
            save_run_to_db(
                site_config=site_config_2,
                summary=_make_summary(),
                database_url=TEST_DB_URL,
            )

        assert exc_info.value.context["site_name"] == "Phoenix Solar Farm"
        assert exc_info.value.context["existing_coords"] == (33.483, -112.073)
        assert exc_info.value.context["new_coords"] == (33.483, -110.0)


# ---------------------------------------------------------------------------
# Recreate query
# ---------------------------------------------------------------------------


class TestRecreateRunInput:
    """Tests for the recreate_run_input query function."""

    def test_recreate_returns_all_28_columns(self) -> None:
        """Recreated dict has exactly the 28 original CSV columns."""
        site_config = _make_site_config()
        run = save_run_to_db(
            site_config=site_config,
            summary=_make_summary(),
            database_url=TEST_DB_URL,
        )

        result = recreate_run_input(str(run.id), database_url=TEST_DB_URL)

        expected_keys = {
            "Run Name", "Site Name", "Customer",
            "Latitude", "Longitude",
            "BESS Dispatch Required", "BESS Optimization Required",
            "DC Size (MW)", "AC Installed (MW)", "AC POI (MW)",
            "Racking", "Tilt", "Azimuth", "Module Orientation",
            "Number of Modules", "Ground Clearance Height (m)",
            "Panel Model", "Bifacial", "Inverter Model",
            "GCR", "Shading (%)", "DC Wiring Loss (%)",
            "AC Wiring Loss (%)", "Transformer Losses (%)",
            "Degradation (%)", "Availability (%)",
            "Module Mismatch (%)", "LID(%)",
        }
        assert set(result.keys()) == expected_keys

    def test_recreate_matches_original_input(self) -> None:
        """Recreated values match the original SiteConfig values."""
        site_config = _make_site_config()
        run = save_run_to_db(
            site_config=site_config,
            summary=_make_summary(),
            database_url=TEST_DB_URL,
        )

        result = recreate_run_input(str(run.id), database_url=TEST_DB_URL)

        # Relational fields from joins
        assert result["Run Name"] == "Run_001"
        assert result["Site Name"] == "Phoenix Solar Farm"
        assert result["Customer"] == "TestCustomer"

        # Numeric fields from run_inputs
        assert result["Latitude"] == pytest.approx(33.483)
        assert result["Longitude"] == pytest.approx(-112.073)
        assert result["DC Size (MW)"] == pytest.approx(13.0)
        assert result["AC Installed (MW)"] == pytest.approx(10.0)
        assert result["AC POI (MW)"] == pytest.approx(10.0)
        assert result["Tilt"] == pytest.approx(60.0)
        assert result["Azimuth"] == pytest.approx(180.0)
        assert result["GCR"] == pytest.approx(0.34)
        assert result["Number of Modules"] == 2
        assert result["Ground Clearance Height (m)"] == pytest.approx(1.8)

        # String fields
        assert result["Racking"] == "tracker"
        assert result["Module Orientation"] == "portrait"
        assert result["Panel Model"] == "CSI Solar Co. Ltd. CS3U-355P"
        assert result["Inverter Model"] == "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]"
        assert result["Bifacial"] is True

        # Loss parameters
        assert result["Shading (%)"] == pytest.approx(1.0)
        assert result["DC Wiring Loss (%)"] == pytest.approx(1.5)
        assert result["AC Wiring Loss (%)"] == pytest.approx(1.5)
        assert result["Transformer Losses (%)"] == pytest.approx(0.0)
        assert result["Degradation (%)"] == pytest.approx(0.3)
        assert result["Availability (%)"] == pytest.approx(2.0)
        assert result["Module Mismatch (%)"] == pytest.approx(1.5)
        assert result["LID(%)"] == pytest.approx(1.0)

        # Optional BESS fields
        assert result["BESS Dispatch Required"] is False
        assert result["BESS Optimization Required"] is False

    def test_nonexistent_run_id_raises_error(self) -> None:
        """Querying a nonexistent run UUID raises SolarModelError."""
        fake_id = str(uuid.uuid4())

        with pytest.raises(SolarModelError, match="Run not found"):
            recreate_run_input(fake_id, database_url=TEST_DB_URL)

    def test_invalid_run_id_format_raises_error(self) -> None:
        """Passing a non-UUID string raises SolarModelError."""
        with pytest.raises(SolarModelError, match="Invalid run ID format"):
            recreate_run_input("not-a-uuid", database_url=TEST_DB_URL)


# ---------------------------------------------------------------------------
# Data source string construction (no DB required)
# ---------------------------------------------------------------------------

SOLCAST_FIXTURE = Path(__file__).parent / "fixtures" / "synthetic_solcast_tmy_phoenix.csv"


class TestBuildDataSources:
    """Tests for the build_data_sources helper function."""

    def test_nsrdb_default(self) -> None:
        """NSRDB site produces the legacy plain-text string."""
        site = _make_site_config()
        result = build_data_sources(site)
        assert result == "NSRDB v4.0.0, Open-Meteo ERA5"

    def test_nsrdb_explicit(self) -> None:
        """Explicitly setting data_source='nsrdb' produces the legacy string."""
        site = _make_site_config(data_source="nsrdb")
        result = build_data_sources(site)
        assert result == "NSRDB v4.0.0, Open-Meteo ERA5"

    def test_solcast_structure(self) -> None:
        """Solcast site produces a JSON string with provider and filename."""
        site = _make_site_config(
            data_source="solcast",
            resource_file_path=SOLCAST_FIXTURE,
        )
        result = build_data_sources(site)
        parsed = json.loads(result)

        assert parsed["solar_resource"]["provider"] == "Solcast"
        assert parsed["solar_resource"]["file"] == "synthetic_solcast_tmy_phoenix.csv"
        assert parsed["solar_resource"]["format"] == "SAM CSV"
        assert parsed["supplementary"]["provider"] == "Open-Meteo ERA5"
        assert "precipitation" in parsed["supplementary"]["data"]

    def test_solcast_without_resource_path(self) -> None:
        """Solcast with no resource_file_path uses 'unknown' as filename."""
        site = _make_site_config(data_source="solcast")
        result = build_data_sources(site)
        parsed = json.loads(result)
        assert parsed["solar_resource"]["file"] == "unknown"
