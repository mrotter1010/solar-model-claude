"""Tests for PySAM simulation execution engine."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.config.schema import SiteConfig
from src.pysam_integration.exceptions import PySAMConfigurationError
from src.pysam_integration.model_configurator import ModelConfigurator, PySAMModelConfig
from src.pysam_integration.simulator import (
    BatchSimulator,
    PySAMSimulator,
    SimulationResult,
)


# -- Fixtures --


def _make_site_config(**overrides: object) -> SiteConfig:
    """Create a SiteConfig with sensible defaults, overridable per test."""
    defaults = {
        "run_name": "test_run",
        "site_name": "Test Site",
        "customer": "Test Customer",
        "latitude": 33.45,
        "longitude": -112.07,
        "dc_size_mw": 100.0,
        "ac_installed_mw": 80.0,
        "ac_poi_mw": 75.0,
        "racking": "tracker",
        "tilt": 60.0,
        "azimuth": 180.0,
        "module_orientation": "portrait",
        "number_of_modules": 1,
        "ground_clearance_height_m": 1.5,
        "panel_model": "Canadian Solar CS3U-355P",
        "bifacial": True,
        "inverter_model": "SMA America: Sunny Central 2500-EV-US (800V)",
        "gcr": 0.35,
        "shading_percent": 3.0,
        "dc_wiring_loss_percent": 2.0,
        "ac_wiring_loss_percent": 1.0,
        "transformer_losses_percent": 1.5,
        "degradation_percent": 0.5,
        "availability_percent": 2.0,
        "module_mismatch_percent": 1.0,
        "lid_percent": 1.5,
    }
    defaults.update(overrides)
    return SiteConfig(**defaults)


def _make_mock_model_config(
    site_name: str = "Test Site",
    weather_file_path: Path | None = None,
    gen_length: int = 8760,
) -> PySAMModelConfig:
    """Create a mock PySAMModelConfig with fake PySAM outputs."""
    site_config = _make_site_config(
        site_name=site_name,
        weather_file_path=weather_file_path,
    )

    mock_model = MagicMock()

    # Mock PySAM Outputs — 8760 hourly values
    mock_model.Outputs.gen = [100.0] * gen_length
    mock_model.Outputs.subarray1_poa_eff = [500.0] * gen_length
    mock_model.Outputs.subarray1_celltemp = [35.0] * gen_length
    mock_model.Outputs.inv_eff = [96.5] * gen_length

    return PySAMModelConfig(
        model=mock_model,
        site_config=site_config,
        module_params=MagicMock(),
        inverter_params=MagicMock(),
        inverter_count=2,
        dc_ac_ratio=1.25,
    )


# -- Test: SimulationResult --


class TestSimulationResult:
    """Test SimulationResult dataclass behavior."""

    def test_simulation_timestamp_auto_set(self) -> None:
        """Verify simulation_timestamp is set automatically in ISO 8601."""
        result = SimulationResult(
            site_name="Test",
            run_name="run1",
            customer="Customer",
            success=True,
        )
        # ISO 8601 format contains 'T' separator and timezone info
        assert "T" in result.simulation_timestamp
        assert "+" in result.simulation_timestamp or "Z" in result.simulation_timestamp


# -- Test: Successful simulation --


class TestSuccessfulSimulation:
    """Test successful PySAM simulation execution."""

    def test_successful_execution(self) -> None:
        """Mock execute + outputs, verify SimulationResult fields."""
        model_config = _make_mock_model_config()
        simulator = PySAMSimulator()

        result = simulator.execute_simulation(model_config)

        assert result.success is True
        assert result.site_name == "Test Site"
        assert result.run_name == "test_run"
        assert result.customer == "Test Customer"
        assert result.error_message is None
        assert result.hourly_data is not None
        assert len(result.hourly_data) == 8760
        model_config.model.execute.assert_called_once()

    def test_successful_with_weather_year(self, tmp_path: Path) -> None:
        """Verify weather year is extracted from weather file."""
        # Create a PySAM-format weather file (2 header rows + columns + data)
        weather_file = tmp_path / "weather.csv"
        weather_file.write_text(
            "Station ID,City,State\n"
            "12345,Phoenix,AZ\n"
            "Year,Month,Day,Hour,DNI,DHI,GHI\n"
            "2021,1,1,0,0,0,0\n"
        )

        model_config = _make_mock_model_config(weather_file_path=weather_file)
        simulator = PySAMSimulator()

        result = simulator.execute_simulation(model_config)

        assert result.success is True
        assert result.weather_year == 2021


# -- Test: Missing weather file --


class TestMissingWeatherFile:
    """Test graceful handling of missing weather file."""

    def test_missing_weather_file_returns_none_year(self) -> None:
        """weather_file_path=None should result in weather_year=None."""
        model_config = _make_mock_model_config(weather_file_path=None)
        simulator = PySAMSimulator()

        result = simulator.execute_simulation(model_config)

        assert result.success is True
        assert result.weather_year is None

    def test_nonexistent_weather_file_returns_none_year(self) -> None:
        """Nonexistent weather file path should gracefully return None year."""
        model_config = _make_mock_model_config(
            weather_file_path=Path("/nonexistent/weather.csv")
        )
        simulator = PySAMSimulator()

        result = simulator.execute_simulation(model_config)

        assert result.success is True
        assert result.weather_year is None


# -- Test: PySAM execution error --


class TestExecutionError:
    """Test handling of PySAM execution failures."""

    def test_runtime_error_returns_failure_result(self) -> None:
        """Mock execute() raising RuntimeError, verify failure result."""
        model_config = _make_mock_model_config()
        model_config.model.execute.side_effect = RuntimeError(
            "PySAM internal error"
        )
        simulator = PySAMSimulator()

        result = simulator.execute_simulation(model_config)

        assert result.success is False
        assert result.hourly_data is None
        assert "PySAM internal error" in result.error_message
        assert result.site_name == "Test Site"


# -- Test: Timeseries extraction --


class TestTimeseriesExtraction:
    """Test correct structure and types of extracted timeseries."""

    def test_timeseries_columns_and_types(self) -> None:
        """Verify correct columns, length, and data types."""
        model_config = _make_mock_model_config()
        simulator = PySAMSimulator()

        result = simulator.execute_simulation(model_config)
        df = result.hourly_data

        # Correct columns
        expected_cols = [
            "timestamp",
            "ac_gross",
            "ac_net",
            "poa_irradiance",
            "cell_temperature",
            "inverter_efficiency",
        ]
        assert list(df.columns) == expected_cols

        # Correct length
        assert len(df) == 8760

        # Correct values from mock
        assert df["ac_gross"].iloc[0] == 100.0
        assert df["ac_net"].iloc[0] == 100.0  # Same as ac_gross for now
        assert df["poa_irradiance"].iloc[0] == 500.0
        assert df["cell_temperature"].iloc[0] == 35.0
        assert df["inverter_efficiency"].iloc[0] == 96.5

        # Timestamp is datetime
        assert pd.api.types.is_datetime64_any_dtype(df["timestamp"])

    def test_timeseries_uses_weather_year(self, tmp_path: Path) -> None:
        """Verify timestamps start with the extracted weather year."""
        weather_file = tmp_path / "weather.csv"
        weather_file.write_text(
            "Station ID,City,State\n"
            "12345,Phoenix,AZ\n"
            "Year,Month,Day,Hour,DNI,DHI,GHI\n"
            "2019,1,1,0,0,0,0\n"
        )

        model_config = _make_mock_model_config(weather_file_path=weather_file)
        simulator = PySAMSimulator()

        result = simulator.execute_simulation(model_config)

        assert result.hourly_data["timestamp"].iloc[0].year == 2019


# -- Test: Batch processing --


class TestBatchProcessing:
    """Test BatchSimulator with mix of valid/invalid configs."""

    def test_batch_splits_successful_and_failed(self) -> None:
        """Mix of valid/invalid configs produces correct (successful, failed) split."""
        mock_configurator = MagicMock(spec=ModelConfigurator)
        mock_simulator = MagicMock(spec=PySAMSimulator)

        # First site: config succeeds, sim succeeds
        site1 = _make_site_config(site_name="Site A")
        mock_config1 = _make_mock_model_config(site_name="Site A")
        success_result = SimulationResult(
            site_name="Site A",
            run_name="test_run",
            customer="Test Customer",
            success=True,
            hourly_data=pd.DataFrame({"ac_gross": [1.0]}),
        )

        # Second site: config fails
        site2 = _make_site_config(site_name="Site B")

        # Third site: config succeeds, sim fails
        site3 = _make_site_config(site_name="Site C")
        mock_config3 = _make_mock_model_config(site_name="Site C")
        fail_result = SimulationResult(
            site_name="Site C",
            run_name="test_run",
            customer="Test Customer",
            success=False,
            error_message="Sim failed",
        )

        # Wire up mock behavior
        mock_configurator.configure_model.side_effect = [
            mock_config1,
            PySAMConfigurationError("Bad config for Site B"),
            mock_config3,
        ]
        mock_simulator.execute_simulation.side_effect = [
            success_result,
            fail_result,
        ]

        batch = BatchSimulator(mock_configurator, mock_simulator)
        successful, failed = batch.run_batch([site1, site2, site3])

        assert len(successful) == 1
        assert len(failed) == 2
        assert successful[0].site_name == "Site A"
        # Failed includes config failure (Site B) and sim failure (Site C)
        failed_names = [r.site_name for r in failed]
        assert "Site B" in failed_names
        assert "Site C" in failed_names

    def test_continue_on_failure(self) -> None:
        """Failed site does not prevent next site from running."""
        mock_configurator = MagicMock(spec=ModelConfigurator)
        mock_simulator = MagicMock(spec=PySAMSimulator)

        site1 = _make_site_config(site_name="Fail Site")
        site2 = _make_site_config(site_name="Pass Site")

        # First site config fails, second succeeds
        mock_config2 = _make_mock_model_config(site_name="Pass Site")
        mock_configurator.configure_model.side_effect = [
            PySAMConfigurationError("Config error"),
            mock_config2,
        ]
        mock_simulator.execute_simulation.return_value = SimulationResult(
            site_name="Pass Site",
            run_name="test_run",
            customer="Test Customer",
            success=True,
        )

        batch = BatchSimulator(mock_configurator, mock_simulator)
        successful, failed = batch.run_batch([site1, site2])

        # First site failed but second still ran
        assert len(successful) == 1
        assert len(failed) == 1
        assert successful[0].site_name == "Pass Site"
        assert failed[0].site_name == "Fail Site"


# -- Test: Weather year extraction --


class TestWeatherYearExtraction:
    """Test _extract_weather_year with PySAM-format CSV."""

    def test_extracts_year_from_pysam_csv(self, tmp_path: Path) -> None:
        """Create temp PySAM-format CSV, verify year parsed correctly."""
        weather_file = tmp_path / "test_weather.csv"
        weather_file.write_text(
            "Source,Location ID,City\n"
            "NSRDB,123456,Phoenix\n"
            "Year,Month,Day,Hour,Minute,DNI,DHI,GHI\n"
            "2020,1,1,0,0,100,50,120\n"
            "2020,1,1,1,0,200,80,250\n"
        )

        simulator = PySAMSimulator()
        year = simulator._extract_weather_year(weather_file)

        assert year == 2020

    def test_returns_none_for_invalid_csv(self, tmp_path: Path) -> None:
        """Malformed CSV returns None without crashing."""
        bad_file = tmp_path / "bad_weather.csv"
        bad_file.write_text("not,a,valid,pysam,file\n")

        simulator = PySAMSimulator()
        year = simulator._extract_weather_year(bad_file)

        assert year is None

    def test_returns_none_for_none_path(self) -> None:
        """None path returns None."""
        simulator = PySAMSimulator()
        year = simulator._extract_weather_year(None)

        assert year is None


# -- Artifact writer --


class TestArtifactWriter:
    """Write test summary to outputs/test_results/ for manual inspection."""

    def test_write_simulator_artifact(self) -> None:
        """Write a summary of simulation test results for inspection."""
        model_config = _make_mock_model_config()
        simulator = PySAMSimulator()
        result = simulator.execute_simulation(model_config)

        output_dir = Path("outputs/test_results")
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / "simulator_test.txt"
        lines = [
            "=== Simulation Engine Test Results ===",
            "",
            f"Site: {result.site_name}",
            f"Run: {result.run_name}",
            f"Customer: {result.customer}",
            f"Success: {result.success}",
            f"Timestamp: {result.simulation_timestamp}",
            f"Weather Year: {result.weather_year}",
            f"Error: {result.error_message}",
            "",
            "--- Hourly Data Summary ---",
            f"Records: {len(result.hourly_data)}",
            f"Columns: {list(result.hourly_data.columns)}",
            f"AC Gross Mean: {result.hourly_data['ac_gross'].mean():.2f}",
            f"AC Net Mean: {result.hourly_data['ac_net'].mean():.2f}",
            f"POA Mean: {result.hourly_data['poa_irradiance'].mean():.2f}",
            f"Cell Temp Mean: {result.hourly_data['cell_temperature'].mean():.2f}",
            f"Inv Eff Mean: {result.hourly_data['inverter_efficiency'].mean():.2f}",
            "",
            "=== END ===",
        ]

        output_path.write_text("\n".join(lines))
        assert output_path.exists()
