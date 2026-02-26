"""Integration tests for Milestone 3: full pipeline CSV → PySAM → outputs.

All tests mock run_climate_data_pipeline and PySAMSimulator.execute_simulation
to avoid requiring real API keys or PySAM installation.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from src.config.loader import load_config
from src.config.schema import SiteConfig
from src.pipeline import SolarModelingPipeline
from src.pysam_integration.simulator import SimulationResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SINGLE_ROW_CSV = FIXTURES_DIR / "single_row_test.csv"
MULTI_ROW_CSV = FIXTURES_DIR / "multi_row_test.csv"


def _make_hourly_data(hours: int = 8760, ac_value: float = 100.0) -> pd.DataFrame:
    """Build a synthetic 8760 hourly DataFrame mimicking PySAM output."""
    timestamps = pd.date_range(start="2024-01-01", periods=hours, freq="h")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "ac_gross": [ac_value] * hours,
            "ac_net": [ac_value] * hours,
            "poa_irradiance": [500.0] * hours,
            "cell_temperature": [35.0] * hours,
            "inverter_efficiency": [96.0] * hours,
        }
    )


def _make_success_result(site: SiteConfig, ac_value: float = 100.0) -> SimulationResult:
    """Create a successful SimulationResult for a given site."""
    return SimulationResult(
        site_name=site.site_name,
        run_name=site.run_name,
        customer=site.customer,
        success=True,
        hourly_data=_make_hourly_data(ac_value=ac_value),
        weather_year=2024,
    )


def _make_failure_result(site: SiteConfig) -> SimulationResult:
    """Create a failed SimulationResult for a given site."""
    return SimulationResult(
        site_name=site.site_name,
        run_name=site.run_name,
        customer=site.customer,
        success=False,
        error_message=f"Simulated failure for {site.site_name}",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSingleSitePipeline:
    """Single-site pipeline — verify timeseries + summary output."""

    @patch("src.pipeline.run_climate_data_pipeline")
    @patch("src.pysam_integration.simulator.PySAMSimulator.execute_simulation")
    def test_single_site_produces_timeseries_and_summary(
        self, mock_sim, mock_climate, tmp_path
    ):
        # Arrange: load real site config from fixture
        sites = load_config(SINGLE_ROW_CSV)
        assert len(sites) == 1
        site = sites[0]
        site.weather_file_path = Path("/fake/weather.csv")

        mock_climate.return_value = sites
        mock_sim.return_value = _make_success_result(site)

        # Act
        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        results = pipeline.run(SINGLE_ROW_CSV, skip_climate=True)

        # Assert: counts
        assert results["total_sites"] == 1
        assert results["successful"] == 1
        assert results["failed"] == 0

        # Assert: timeseries file exists and has 8760 rows
        ts_files = results["timeseries_files"]
        assert len(ts_files) == 1
        ts_df = pd.read_csv(ts_files[0])
        assert len(ts_df) == 8760
        assert "ac_net" in ts_df.columns
        assert "ac_gross" in ts_df.columns

        # Assert: summary JSON exists with expected keys
        summary_files = results["summary_files"]
        assert len(summary_files) == 1
        summary = json.loads(summary_files[0].read_text())
        assert summary["site_name"] == site.site_name
        assert "annual_energy_mwh" in summary
        assert "net_capacity_factor" in summary


class TestMultiSitePipeline:
    """Multi-site pipeline — verify multiple outputs."""

    @patch("src.pipeline.run_climate_data_pipeline")
    @patch("src.pysam_integration.simulator.PySAMSimulator.execute_simulation")
    def test_multi_site_produces_multiple_outputs(
        self, mock_sim, mock_climate, tmp_path
    ):
        # Arrange
        sites = load_config(MULTI_ROW_CSV)
        assert len(sites) == 3
        for s in sites:
            s.weather_file_path = Path("/fake/weather.csv")

        mock_climate.return_value = sites
        mock_sim.side_effect = [_make_success_result(s) for s in sites]

        # Act
        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        results = pipeline.run(MULTI_ROW_CSV, skip_climate=True)

        # Assert
        assert results["total_sites"] == 3
        assert results["successful"] == 3
        assert results["failed"] == 0
        assert len(results["timeseries_files"]) == 3
        assert len(results["summary_files"]) == 3


class TestSkipClimateMode:
    """Skip-climate mode — pre-assign weather paths, no climate fetch."""

    @patch("src.pipeline.run_climate_data_pipeline")
    @patch("src.pysam_integration.simulator.PySAMSimulator.execute_simulation")
    def test_skip_climate_does_not_call_climate_pipeline(
        self, mock_sim, mock_climate, tmp_path
    ):
        # Arrange
        sites = load_config(SINGLE_ROW_CSV)
        sites[0].weather_file_path = Path("/fake/weather.csv")

        mock_sim.return_value = _make_success_result(sites[0])

        # Act
        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        results = pipeline.run(SINGLE_ROW_CSV, skip_climate=True)

        # Assert: climate pipeline not called
        mock_climate.assert_not_called()
        assert results["successful"] == 1


class TestFailedSimulationHandling:
    """Failed simulation handling — verify error JSON output."""

    @patch("src.pipeline.run_climate_data_pipeline")
    @patch("src.pysam_integration.simulator.PySAMSimulator.execute_simulation")
    def test_failed_site_produces_error_json(
        self, mock_sim, mock_climate, tmp_path
    ):
        # Arrange
        sites = load_config(SINGLE_ROW_CSV)
        site = sites[0]
        site.weather_file_path = Path("/fake/weather.csv")

        mock_climate.return_value = sites
        mock_sim.return_value = _make_failure_result(site)

        # Act
        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        results = pipeline.run(SINGLE_ROW_CSV, skip_climate=True)

        # Assert
        assert results["successful"] == 0
        assert results["failed"] == 1
        assert len(results["error_files"]) == 1

        # Verify error JSON content
        error_data = json.loads(results["error_files"][0].read_text())
        assert error_data["site_name"] == site.site_name
        assert "error_message" in error_data
        assert "Simulated failure" in error_data["error_message"]


class TestShadingHaircut:
    """Shading haircut applied — verify ac_net < ac_gross when shading > 0."""

    @patch("src.pipeline.run_climate_data_pipeline")
    @patch("src.pysam_integration.simulator.PySAMSimulator.execute_simulation")
    def test_shading_reduces_ac_net(
        self, mock_sim, mock_climate, tmp_path
    ):
        # Arrange: single_row_test.csv has Shading (%) = 2.0
        sites = load_config(SINGLE_ROW_CSV)
        site = sites[0]
        assert site.shading_percent == 2.0, "Test expects 2% shading from fixture"
        site.weather_file_path = Path("/fake/weather.csv")

        mock_climate.return_value = sites
        mock_sim.return_value = _make_success_result(site, ac_value=1000.0)

        # Act
        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        results = pipeline.run(SINGLE_ROW_CSV, skip_climate=True)

        # Assert: read timeseries and verify ac_net < ac_gross
        ts_df = pd.read_csv(results["timeseries_files"][0])
        # ac_gross should be 1000.0, ac_net should be 1000 * (1 - 0.02) = 980.0
        assert ts_df["ac_gross"].iloc[0] == pytest.approx(1000.0, abs=0.01)
        assert ts_df["ac_net"].iloc[0] == pytest.approx(980.0, abs=0.01)
        assert (ts_df["ac_net"] < ts_df["ac_gross"]).all()


class TestMixedSuccessFailure:
    """Mixed batch — some succeed, some fail."""

    @patch("src.pipeline.run_climate_data_pipeline")
    @patch("src.pysam_integration.simulator.PySAMSimulator.execute_simulation")
    def test_mixed_batch_reports_correct_counts(
        self, mock_sim, mock_climate, tmp_path
    ):
        # Arrange: 3 sites, first 2 succeed, third fails
        sites = load_config(MULTI_ROW_CSV)
        for s in sites:
            s.weather_file_path = Path("/fake/weather.csv")

        mock_climate.return_value = sites
        mock_sim.side_effect = [
            _make_success_result(sites[0]),
            _make_success_result(sites[1]),
            _make_failure_result(sites[2]),
        ]

        # Act
        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        results = pipeline.run(MULTI_ROW_CSV, skip_climate=True)

        # Assert
        assert results["successful"] == 2
        assert results["failed"] == 1
        assert len(results["timeseries_files"]) == 2
        assert len(results["summary_files"]) == 2
        assert len(results["error_files"]) == 1


class TestArtifactGeneration:
    """Write integration report artifact to outputs/test_results/ for inspection."""

    @patch("src.pipeline.run_climate_data_pipeline")
    @patch("src.pysam_integration.simulator.PySAMSimulator.execute_simulation")
    def test_generate_integration_report(
        self, mock_sim, mock_climate, tmp_path
    ):
        # Arrange
        sites = load_config(MULTI_ROW_CSV)
        for s in sites:
            s.weather_file_path = Path("/fake/weather.csv")

        mock_climate.return_value = sites
        mock_sim.side_effect = [
            _make_success_result(sites[0], ac_value=800.0),
            _make_success_result(sites[1], ac_value=600.0),
            _make_failure_result(sites[2]),
        ]

        # Act
        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        results = pipeline.run(MULTI_ROW_CSV, skip_climate=True)

        # Write report artifact
        report_dir = Path("outputs/test_results")
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / "integration_milestone3_report.json"

        report = {
            "test": "integration_milestone3",
            "total_sites": results["total_sites"],
            "successful": results["successful"],
            "failed": results["failed"],
            "timeseries_files": [str(p) for p in results["timeseries_files"]],
            "summary_files": [str(p) for p in results["summary_files"]],
            "error_files": [str(p) for p in results["error_files"]],
        }

        # Include summary contents for manual inspection
        summaries = []
        for sp in results["summary_files"]:
            summaries.append(json.loads(sp.read_text()))
        report["summaries"] = summaries

        errors = []
        for ep in results["error_files"]:
            errors.append(json.loads(ep.read_text()))
        report["errors"] = errors

        report_path.write_text(json.dumps(report, indent=2, default=str))

        # Assert: report written
        assert report_path.exists()
        written = json.loads(report_path.read_text())
        assert written["total_sites"] == 3
        assert written["successful"] == 2
        assert written["failed"] == 1
        assert len(written["summaries"]) == 2
        assert len(written["errors"]) == 1
