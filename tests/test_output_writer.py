"""Tests for output generation: shading haircut, metrics, file output."""

import json
from pathlib import Path

import pandas as pd
import pytest

from src.config.schema import SiteConfig
from src.outputs.output_writer import ErrorReport, OutputWriter, SummaryMetrics
from src.pysam_integration.simulator import SimulationResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_site_config(**overrides: object) -> SiteConfig:
    """Create a SiteConfig with sensible defaults, overridable per test."""
    defaults = {
        "run_name": "TestRun",
        "site_name": "Phoenix",
        "customer": "TestCo",
        "latitude": 33.45,
        "longitude": -112.07,
        "dc_size_mw": 10.0,
        "ac_installed_mw": 8.0,
        "ac_poi_mw": 8.0,
        "racking": "tracker",
        "tilt": 60.0,
        "azimuth": 180.0,
        "module_orientation": "portrait",
        "number_of_modules": 1,
        "ground_clearance_height_m": 1.5,
        "panel_model": "Canadian_Solar_CS6R_410MS",
        "bifacial": True,
        "inverter_model": "SMA_America__SHP_150_US_20__600V_",
        "gcr": 0.35,
        "shading_percent": 3.0,
        "dc_wiring_loss_percent": 2.0,
        "ac_wiring_loss_percent": 1.0,
        "transformer_losses_percent": 1.0,
        "degradation_percent": 0.5,
        "availability_percent": 2.0,
        "module_mismatch_percent": 1.0,
        "lid_percent": 1.5,
    }
    defaults.update(overrides)
    return SiteConfig(**defaults)


def _make_hourly_data(
    num_hours: int = 8760,
    ac_gross_kw: float = 5000.0,
    poa_w_per_m2: float = 200.0,
    year: int = 2023,
) -> pd.DataFrame:
    """Create synthetic hourly DataFrame matching simulator output format."""
    timestamps = pd.date_range(start=f"{year}-01-01", periods=num_hours, freq="h")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "ac_gross": [ac_gross_kw] * num_hours,
            "ac_net": [ac_gross_kw] * num_hours,
            "poa_irradiance": [poa_w_per_m2] * num_hours,
            "cell_temperature": [30.0] * num_hours,
            "inverter_efficiency": [97.5] * num_hours,
        }
    )


def _make_successful_result(**overrides: object) -> SimulationResult:
    """Create a successful SimulationResult with hourly data."""
    defaults = {
        "site_name": "Phoenix",
        "run_name": "TestRun",
        "customer": "TestCo",
        "success": True,
        "hourly_data": _make_hourly_data(),
        "weather_year": 2023,
    }
    defaults.update(overrides)
    return SimulationResult(**defaults)


def _make_failed_result(**overrides: object) -> SimulationResult:
    """Create a failed SimulationResult with no hourly data."""
    defaults = {
        "site_name": "Phoenix",
        "run_name": "TestRun",
        "customer": "TestCo",
        "success": False,
        "error_message": "PySAM execution failed: invalid configuration",
    }
    defaults.update(overrides)
    return SimulationResult(**defaults)


# ---------------------------------------------------------------------------
# Shading haircut tests
# ---------------------------------------------------------------------------


class TestShadingHaircut:
    """Tests for shading loss application."""

    def test_shading_haircut_applied_correctly(self, tmp_path: Path) -> None:
        """Shading percentage reduces ac_net by correct factor."""
        # Arrange
        writer = OutputWriter(tmp_path)
        hourly_data = _make_hourly_data(ac_gross_kw=1000.0)
        shading_pct = 3.0

        # Act
        result = writer._apply_shading_haircut(hourly_data, shading_pct)

        # Assert — ac_net = 1000 * (1 - 3/100) = 970
        assert result["ac_net"].iloc[0] == pytest.approx(970.0)
        assert result["ac_net"].iloc[-1] == pytest.approx(970.0)
        # ac_gross should be unchanged
        assert result["ac_gross"].iloc[0] == pytest.approx(1000.0)

    def test_zero_shading_leaves_ac_net_unchanged(self, tmp_path: Path) -> None:
        """Zero shading means ac_net equals ac_gross."""
        # Arrange
        writer = OutputWriter(tmp_path)
        hourly_data = _make_hourly_data(ac_gross_kw=5000.0)

        # Act
        result = writer._apply_shading_haircut(hourly_data, 0.0)

        # Assert
        pd.testing.assert_series_equal(
            result["ac_net"], result["ac_gross"], check_names=False
        )

    def test_shading_does_not_mutate_original(self, tmp_path: Path) -> None:
        """Shading returns a copy; original DataFrame is not modified."""
        # Arrange
        writer = OutputWriter(tmp_path)
        hourly_data = _make_hourly_data(ac_gross_kw=1000.0)
        original_ac_net = hourly_data["ac_net"].iloc[0]

        # Act
        writer._apply_shading_haircut(hourly_data, 10.0)

        # Assert — original unchanged
        assert hourly_data["ac_net"].iloc[0] == pytest.approx(original_ac_net)


# ---------------------------------------------------------------------------
# Metrics calculation tests
# ---------------------------------------------------------------------------


class TestMetricsCalculation:
    """Tests for production metric formulas."""

    def test_all_metrics_computed(self, tmp_path: Path) -> None:
        """All required metrics are present and correctly computed."""
        # Arrange
        writer = OutputWriter(tmp_path)
        site = _make_site_config(dc_size_mw=10.0, ac_installed_mw=8.0)
        hourly_data = _make_hourly_data(ac_gross_kw=5000.0, poa_w_per_m2=200.0)
        # Apply shading first (3%)
        hourly_data = writer._apply_shading_haircut(hourly_data, 3.0)
        result = _make_successful_result()

        # Act
        metrics = writer._calculate_metrics(hourly_data, site, result, 3.0)

        # Assert — annual energy: 5000 * 0.97 * 8760 / 1000 = 42,486 MWh
        expected_kwh = 5000.0 * 0.97 * 8760
        expected_mwh = expected_kwh / 1000
        assert metrics.annual_energy_mwh == pytest.approx(expected_mwh, rel=1e-3)

        # Net capacity factor: MWh / (8 MW * 8760 h) ≈ 0.6064
        expected_cf = expected_mwh / (8.0 * 8760)
        assert metrics.net_capacity_factor == pytest.approx(expected_cf, rel=1e-3)

        # Specific yield: kWh / (10 MW * 1000 kWp/MW)
        expected_sy = expected_kwh / (10.0 * 1000)
        assert metrics.specific_yield == pytest.approx(expected_sy, rel=1e-2)

        # Performance ratio: actual_kwh / ideal_kwh
        # ideal = (200 W/m² * 8760 h / 1000 kWh/m²) * 10000 kWp
        total_poa_kwh = 200.0 * 8760 / 1000
        ideal = total_poa_kwh * 10000
        expected_pr = expected_kwh / ideal
        assert metrics.performance_ratio == pytest.approx(expected_pr, rel=1e-3)

    def test_capacity_factor_uses_actual_hours(self, tmp_path: Path) -> None:
        """Capacity factor uses num_hours from data length (8760 vs 8784 for leap year)."""
        # Arrange
        writer = OutputWriter(tmp_path)
        site = _make_site_config(ac_installed_mw=8.0)
        # Leap year: 8784 hours
        hourly_data = _make_hourly_data(
            num_hours=8784, ac_gross_kw=5000.0, year=2024
        )
        hourly_data = writer._apply_shading_haircut(hourly_data, 0.0)
        result = _make_successful_result(weather_year=2024)

        # Act
        metrics = writer._calculate_metrics(hourly_data, site, result, 0.0)

        # Assert — uses 8784 in denominator
        expected_cf = (5000.0 * 8784 / 1000) / (8.0 * 8784)
        assert metrics.net_capacity_factor == pytest.approx(expected_cf, rel=1e-4)

    def test_metadata_fields_populated(self, tmp_path: Path) -> None:
        """Metrics contain correct site metadata and system specs."""
        # Arrange
        writer = OutputWriter(tmp_path)
        site = _make_site_config(site_name="Mesa", customer="SolarCorp")
        hourly_data = _make_hourly_data()
        hourly_data = writer._apply_shading_haircut(hourly_data, 0.0)
        result = _make_successful_result(site_name="Mesa")

        # Act
        metrics = writer._calculate_metrics(hourly_data, site, result, 0.0)

        # Assert
        assert metrics.site_name == "Mesa"
        assert metrics.customer == "SolarCorp"
        assert metrics.dc_size_mw == 10.0
        assert metrics.panel_model == "Canadian_Solar_CS6R_410MS"
        assert metrics.racking == "tracker"


# ---------------------------------------------------------------------------
# File output tests
# ---------------------------------------------------------------------------


class TestTimeseriesCSV:
    """Tests for timeseries CSV output."""

    def test_timeseries_csv_format(self, tmp_path: Path) -> None:
        """CSV has correct columns and row count."""
        # Arrange
        writer = OutputWriter(tmp_path)
        hourly_data = _make_hourly_data(num_hours=8760)

        # Act
        path = writer._write_timeseries(hourly_data, "TestRun_Phoenix")

        # Assert — file exists with correct suffix
        assert path.exists()
        assert path.name == "TestRun_Phoenix_8760.csv"
        assert path.parent == tmp_path / "timeseries"

        # Read back and verify
        df = pd.read_csv(path)
        assert len(df) == 8760
        expected_cols = {
            "timestamp", "ac_gross", "ac_net",
            "poa_irradiance", "cell_temperature", "inverter_efficiency",
        }
        assert set(df.columns) == expected_cols


class TestSummaryJSON:
    """Tests for summary JSON output."""

    def test_summary_json_has_all_fields(self, tmp_path: Path) -> None:
        """Summary JSON contains all required metric and metadata fields."""
        # Arrange
        writer = OutputWriter(tmp_path)
        metrics = SummaryMetrics(
            site_name="Phoenix",
            run_name="TestRun",
            customer="TestCo",
            weather_year=2023,
            dc_size_mw=10.0,
            ac_installed_mw=8.0,
            ac_poi_mw=8.0,
            panel_model="Canadian_Solar",
            inverter_model="SMA_150",
            racking="tracker",
            tilt=60.0,
            azimuth=180.0,
            annual_energy_mwh=42000.0,
            net_capacity_factor=0.6,
            specific_yield=1700.0,
            performance_ratio=0.82,
            shading_pct_applied=3.0,
            simulation_timestamp="2023-01-01T00:00:00+00:00",
        )

        # Act
        path = writer._write_summary(metrics, "TestRun_Phoenix")

        # Assert
        assert path.exists()
        assert path.name == "TestRun_Phoenix_summary.json"
        data = json.loads(path.read_text())
        assert data["site_name"] == "Phoenix"
        assert data["annual_energy_mwh"] == 42000.0
        assert data["net_capacity_factor"] == 0.6
        assert data["specific_yield"] == 1700.0
        assert data["performance_ratio"] == 0.82
        assert data["errors"] == []


class TestErrorJSON:
    """Tests for error JSON output."""

    def test_error_json_proper_reporting(self, tmp_path: Path) -> None:
        """Error JSON contains failure details and site config subset."""
        # Arrange
        writer = OutputWriter(tmp_path)
        result = _make_failed_result(
            error_message="PySAM execution failed: invalid module"
        )
        site = _make_site_config(latitude=33.45, longitude=-112.07, dc_size_mw=10.0)

        # Act
        path = writer._write_error(result, site, "TestRun_Phoenix")

        # Assert
        assert path.exists()
        assert path.name == "TestRun_Phoenix_error.json"
        data = json.loads(path.read_text())
        assert data["error_message"] == "PySAM execution failed: invalid module"
        assert data["latitude"] == 33.45
        assert data["dc_size_mw"] == 10.0
        assert "report_timestamp" in data


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


class TestIntegration:
    """End-to-end tests for the full output pipeline."""

    def test_successful_simulation_pipeline(self, tmp_path: Path) -> None:
        """Full pipeline: successful sim → timeseries CSV + summary JSON."""
        # Arrange
        writer = OutputWriter(tmp_path)
        site = _make_site_config()
        result = _make_successful_result()
        shading_pct = 3.0

        # Act
        ts_path, summary_path = writer.write_outputs(result, site, shading_pct)

        # Assert — both files exist
        assert ts_path is not None
        assert ts_path.exists()
        assert ts_path.suffix == ".csv"
        assert summary_path.exists()
        assert summary_path.suffix == ".json"

        # Verify timeseries has shading applied
        df = pd.read_csv(ts_path)
        assert df["ac_net"].iloc[0] < df["ac_gross"].iloc[0]

        # Verify summary metrics
        data = json.loads(summary_path.read_text())
        assert data["shading_pct_applied"] == 3.0
        assert data["annual_energy_mwh"] > 0

    def test_failed_simulation_pipeline(self, tmp_path: Path) -> None:
        """Full pipeline: failed sim → error JSON, no timeseries."""
        # Arrange
        writer = OutputWriter(tmp_path)
        site = _make_site_config()
        result = _make_failed_result()

        # Act
        ts_path, error_path = writer.write_outputs(result, site, shading_pct=3.0)

        # Assert — no timeseries, only error
        assert ts_path is None
        assert error_path.exists()
        assert "_error.json" in error_path.name
        data = json.loads(error_path.read_text())
        assert data["error_message"] == "PySAM execution failed: invalid configuration"

    def test_directory_structure_created(self, tmp_path: Path) -> None:
        """OutputWriter creates timeseries/ and results/ subdirectories."""
        # Act
        OutputWriter(tmp_path / "new_output")

        # Assert
        assert (tmp_path / "new_output" / "timeseries").is_dir()
        assert (tmp_path / "new_output" / "results").is_dir()


# ---------------------------------------------------------------------------
# Test artifact output
# ---------------------------------------------------------------------------


class TestArtifactOutput:
    """Write sample outputs to outputs/test_results/ for manual inspection."""

    def test_write_sample_outputs(self, tmp_path: Path) -> None:
        """Write representative outputs for manual review."""
        # Arrange
        writer = OutputWriter(tmp_path)
        site = _make_site_config()
        result = _make_successful_result()

        # Act — run full pipeline
        ts_path, summary_path = writer.write_outputs(result, site, shading_pct=3.0)

        # Write inspection artifact
        output_dir = Path("outputs/test_results")
        output_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = output_dir / "output_writer_test.txt"

        summary_data = json.loads(summary_path.read_text())
        ts_df = pd.read_csv(ts_path)

        lines = [
            "=== Output Writer Test Artifact ===",
            "",
            "--- Summary Metrics ---",
            json.dumps(summary_data, indent=2),
            "",
            "--- Timeseries Sample (first 5 rows) ---",
            ts_df.head().to_string(index=False),
            "",
            "--- Timeseries Sample (last 5 rows) ---",
            ts_df.tail().to_string(index=False),
            "",
            f"Total rows: {len(ts_df)}",
            f"Columns: {list(ts_df.columns)}",
            f"ac_gross[0]: {ts_df['ac_gross'].iloc[0]}",
            f"ac_net[0]: {ts_df['ac_net'].iloc[0]}",
            f"Shading applied: {summary_data['shading_pct_applied']}%",
            "",
        ]
        artifact_path.write_text("\n".join(lines))
        assert artifact_path.exists()
