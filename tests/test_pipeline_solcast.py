"""End-to-end integration tests for the Solcast resource file pipeline (Milestone 7).

Test 1: Full pipeline with Solcast resource file — verifies that a CSV with
    a Resource File Path column runs through climate → PySAM → outputs without
    touching NSRDB, and produces correct timeseries CSV, PDF report, and metadata.

Test 2: Backward compatibility — verifies that the standard NSRDB pipeline
    still works when no Resource File Path is present.

All output artifacts are written to test_results/m7_integration/ for inspection.
"""

import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.climate.cache_manager import CacheManager
from src.climate.nsrdb_client import NSRDBClient
from src.pipeline import SolarModelingPipeline
from tests.conftest import generate_mock_nsrdb_csv

# --- Paths ---
SOLCAST_FIXTURE = (
    Path(__file__).parent / "fixtures" / "synthetic_solcast_tmy_phoenix.csv"
)
SINGLE_ROW_CSV = Path("Energy Analytics Inputs Single Row Test - Sheet1.csv")
NSRDB_CACHE = Path("data/climate/nsrdb_33.483_-112.073_20260226.csv")

# Persistent directory for manual inspection of test outputs
M7_RESULTS = Path("test_results/m7_integration")


# --- Fixtures ---


@pytest.fixture()
def m7_results_dir() -> Path:
    """Create persistent output directory for M7 integration test artifacts."""
    M7_RESULTS.mkdir(parents=True, exist_ok=True)
    return M7_RESULTS


# --- Helpers ---


def _build_solcast_input_csv(tmp_path: Path) -> Path:
    """Build a test input CSV with Resource File Path pointing to Solcast fixture.

    Mirrors the Phoenix single-row test CSV with an additional Resource File Path
    column containing the absolute path to the synthetic Solcast TMY fixture.

    Args:
        tmp_path: Temporary directory for the generated CSV.

    Returns:
        Path to the generated CSV file.
    """
    df = pd.read_csv(SINGLE_ROW_CSV)
    df["Resource File Path"] = str(SOLCAST_FIXTURE.resolve())
    csv_path = tmp_path / "solcast_pipeline_input.csv"
    df.to_csv(csv_path, index=False)
    return csv_path


def _configure_climate_mocks(
    mock_config_cls: MagicMock,
    mock_nsrdb_cls: MagicMock,
    mock_cache_cls: MagicMock,
    mock_era5_func: MagicMock,
    tmp_path: Path,
    nsrdb_csv: str | None = None,
) -> MagicMock:
    """Configure all climate pipeline mocks with standard test values.

    Args:
        mock_config_cls: Mocked ClimateConfig class.
        mock_nsrdb_cls: Mocked NSRDBClient class.
        mock_cache_cls: Mocked CacheManager class.
        mock_era5_func: Mocked fetch_era5_land_data function.
        tmp_path: Temporary directory for cache files.
        nsrdb_csv: Raw NSRDB CSV string for mock return value.
            If None, NSRDB fetch is not configured to return data.

    Returns:
        The configured NSRDB mock instance (for call assertions).
    """
    # ClimateConfig — avoid loading real env vars
    mock_config = MagicMock()
    mock_config.api_key = "test-key"
    mock_config.api_email = "test@test.com"
    mock_config.cache_dir = tmp_path / "cache"
    mock_config.cache_max_age_days = 365
    mock_config.max_cache_distance_km = 50.0
    mock_config_cls.return_value = mock_config

    # NSRDBClient — prevent real API calls
    mock_nsrdb = MagicMock(spec=NSRDBClient)
    if nsrdb_csv is not None:
        mock_nsrdb.fetch_weather_data.return_value = nsrdb_csv
    mock_nsrdb_cls.return_value = mock_nsrdb

    # CacheManager — real instance with temp directory
    mock_cache_cls.return_value = CacheManager(cache_dir=tmp_path / "cache")

    # ERA5 — canned precipitation data (no real Open-Meteo/Zarr calls)
    mock_era5_func.return_value = {
        "snow_depth_cm": [0.0] * 8760,
        "monthly_precip_inches": [1.0] * 12,
    }

    return mock_nsrdb


# --- Tests ---


class TestSolcastFullPipeline:
    """Test 1: Full pipeline with Solcast resource file."""

    @patch("src.pipeline.save_run_to_db")
    @patch("src.pipeline.ClimateConfig")
    @patch("src.pipeline.NSRDBClient")
    @patch("src.pipeline.CacheManager")
    @patch("src.pipeline.fetch_era5_land_data")
    @patch("builtins.input", return_value="y")
    @patch("sys.stdin.isatty", return_value=True)
    def test_solcast_pipeline_produces_outputs(
        self,
        mock_isatty: MagicMock,
        mock_input: MagicMock,
        mock_era5_func: MagicMock,
        mock_cache_cls: MagicMock,
        mock_nsrdb_cls: MagicMock,
        mock_config_cls: MagicMock,
        mock_save_run: MagicMock,
        tmp_path: Path,
        m7_results_dir: Path,
    ) -> None:
        """Solcast resource file flows through full pipeline: climate -> PySAM -> outputs."""
        # Arrange — climate mocks
        mock_nsrdb = _configure_climate_mocks(
            mock_config_cls, mock_nsrdb_cls, mock_cache_cls, mock_era5_func, tmp_path
        )

        # Arrange — DB mock (pipeline catches DB errors, but mock for clean assertions)
        mock_run = MagicMock()
        mock_run.id = 1
        mock_save_run.return_value = mock_run

        # Arrange — input CSV with Resource File Path
        input_csv = _build_solcast_input_csv(tmp_path)
        output_dir = tmp_path / "pipeline_output"

        # Act — run full pipeline (real CEC database + real PySAM simulation)
        pipeline = SolarModelingPipeline(output_dir=output_dir)
        results = pipeline.run(input_csv)

        # Assert — pipeline completed successfully
        assert results["successful"] == 1, f"Expected 1 success, got {results}"
        assert results["failed"] == 0
        assert results["total_sites"] == 1

        # Assert — NSRDB was NOT called (Solcast bypasses it)
        mock_nsrdb.fetch_weather_data.assert_not_called()

        # Assert — timeseries CSV generated with 8760 rows
        assert len(results["timeseries_files"]) == 1
        ts_path = results["timeseries_files"][0]
        assert ts_path.exists()
        ts_df = pd.read_csv(ts_path)
        assert len(ts_df) == 8760, f"Expected 8760 rows, got {len(ts_df)}"

        # Assert — PDF report generated (single-row CSV has Report=TRUE)
        assert len(results["report_files"]) == 1
        report_path = results["report_files"][0]
        assert report_path.exists()
        assert report_path.suffix == ".pdf"

        # Assert — data_source and weather_file_path on site config passed to DB
        mock_save_run.assert_called_once()
        saved_site = mock_save_run.call_args.kwargs["site_config"]
        assert saved_site.data_source == "solcast"
        assert saved_site.weather_file_path == SOLCAST_FIXTURE.resolve()
        assert saved_site.solcast_metadata is not None
        assert saved_site.solcast_metadata["source"] == "Solcast"

        # Write artifacts to persistent directory for manual inspection
        solcast_out = m7_results_dir / "solcast"
        solcast_out.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ts_path, solcast_out / ts_path.name)
        shutil.copy2(report_path, solcast_out / report_path.name)


class TestNsrdbBackwardCompatibility:
    """Test 2: Standard NSRDB pipeline still works without Resource File Path."""

    @patch("src.pipeline.save_run_to_db")
    @patch("src.pipeline.ClimateConfig")
    @patch("src.pipeline.NSRDBClient")
    @patch("src.pipeline.CacheManager")
    @patch("src.pipeline.fetch_era5_land_data")
    def test_nsrdb_pipeline_backward_compatible(
        self,
        mock_era5_func: MagicMock,
        mock_cache_cls: MagicMock,
        mock_nsrdb_cls: MagicMock,
        mock_config_cls: MagicMock,
        mock_save_run: MagicMock,
        tmp_path: Path,
        m7_results_dir: Path,
    ) -> None:
        """Standard CSV without Resource File Path uses NSRDB and data_source='nsrdb'."""
        # Arrange — load cached NSRDB data (real file if available, synthetic fallback)
        if NSRDB_CACHE.exists():
            nsrdb_csv = NSRDB_CACHE.read_text()
        else:
            nsrdb_csv = generate_mock_nsrdb_csv(
                lat=33.483, lon=-112.073, year=2024, num_hours=8760
            )

        mock_nsrdb = _configure_climate_mocks(
            mock_config_cls, mock_nsrdb_cls, mock_cache_cls, mock_era5_func,
            tmp_path, nsrdb_csv=nsrdb_csv,
        )

        # Arrange — DB mock
        mock_run = MagicMock()
        mock_run.id = 2
        mock_save_run.return_value = mock_run

        output_dir = tmp_path / "pipeline_output"

        # Act — run full pipeline with standard CSV (no Resource File Path column)
        pipeline = SolarModelingPipeline(output_dir=output_dir)
        results = pipeline.run(SINGLE_ROW_CSV)

        # Assert — pipeline completed successfully
        assert results["successful"] == 1, f"Expected 1 success, got {results}"
        assert results["failed"] == 0

        # Assert — NSRDB WAS called
        mock_nsrdb.fetch_weather_data.assert_called_once()

        # Assert — data_source is "nsrdb"
        mock_save_run.assert_called_once()
        saved_site = mock_save_run.call_args.kwargs["site_config"]
        assert saved_site.data_source == "nsrdb"
        assert saved_site.solcast_metadata is None

        # Write artifacts to persistent directory for manual inspection
        nsrdb_out = m7_results_dir / "nsrdb"
        nsrdb_out.mkdir(parents=True, exist_ok=True)
        if results["timeseries_files"]:
            ts_path = results["timeseries_files"][0]
            shutil.copy2(ts_path, nsrdb_out / ts_path.name)
        if results["report_files"]:
            report_path = results["report_files"][0]
            shutil.copy2(report_path, nsrdb_out / report_path.name)
