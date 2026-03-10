"""Tests for subhourly correction integration in the pipeline.

Tests the _apply_subhourly_correction method on SolarModelingPipeline and
verifies correct behavior for: successful correction, zero correction,
missing model artifact, missing weather file, and failed weather features.
"""

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from src.config.loader import load_config
from src.pipeline import SolarModelingPipeline
from src.pysam_integration.simulator import SimulationResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SINGLE_ROW_CSV = FIXTURES_DIR / "single_row_test.csv"


def _make_hourly_data(
    hours: int = 8760,
    ac_value: float = 1000.0,
    dc_value: float = 1200.0,
) -> pd.DataFrame:
    """Build synthetic hourly DataFrame with AC and DC columns."""
    timestamps = pd.date_range(start="2024-01-01", periods=hours, freq="h")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "ac_gross": [ac_value] * hours,
            "ac_net": [ac_value] * hours,
            "dc_net": [dc_value] * hours,
            "poa_irradiance": [500.0] * hours,
            "poa_nominal": [520.0] * hours,
            "cell_temperature": [35.0] * hours,
            "inverter_efficiency": [96.0] * hours,
        }
    )


def _make_loss_data() -> dict:
    """Create loss_data dict with capacity_factor_ac for correction."""
    return {
        "annual_dc_nominal": 35795452.0,
        "annual_dc_gross": 32400224.0,
        "annual_dc_net": 30965547.0,
        "annual_ac_gross": 30284794.0,
        "annual_energy": 28935577.0,
        "capacity_factor": 25.41,
        "capacity_factor_ac": 32.34,
        "kwh_per_kw": 2225.81,
        "performance_ratio": 0.7676,
        "monthly_energy": [2000000.0] * 12,
        "monthly_poa_eff": [15000000.0] * 12,
        "avg_daytime_ghi_wm2": 490.5,
        "annual_ghi_kwh_m2": 2131.8,
        "annual_poa_shading_loss_percent": 1.53,
        "annual_poa_soiling_loss_percent": 5.0,
        "annual_poa_cover_loss_percent": 0.68,
        "annual_dc_module_loss_percent": 9.48,
        "annual_dc_mismatch_loss_percent": 1.5,
        "annual_dc_diodes_loss_percent": 0.5,
        "annual_dc_wiring_loss_percent": 1.5,
        "annual_dc_nameplate_loss_percent": 1.0,
        "annual_dc_tracking_loss_percent": 0.0,
        "annual_bifacial_electrical_mismatch_percent": 0.17,
        "annual_ac_inv_clip_loss_percent": 0.69,
        "annual_ac_inv_eff_loss_percent": 1.41,
        "annual_ac_wiring_loss_percent": 1.5,
        "annual_xfmr_loss_percent": 0.0,
        "annual_ac_perf_adj_loss_percent": 3.0,
        "annual_poa_rear_gain_percent": 3.32,
        "annual_dc_snow_loss_percent": 0.0,
        "annual_snow_loss": 0.0,
        "monthly_snow_loss": [0.0] * 12,
    }


def _make_site_config():
    """Load the single-row test fixture and return the SiteConfig."""
    sites = load_config(SINGLE_ROW_CSV)
    return sites[0]


def _make_result_with_loss_data(site, ac_value=1000.0, dc_value=1200.0):
    """Create SimulationResult with loss_data for correction testing."""
    return SimulationResult(
        site_name=site.site_name,
        run_name=site.run_name,
        customer=site.customer,
        success=True,
        hourly_data=_make_hourly_data(ac_value=ac_value, dc_value=dc_value),
        loss_data=_make_loss_data(),
        weather_year=2024,
    )


# Weather features matching a Phoenix-like clear climate
PHOENIX_WEATHER_FEATURES = {
    "annual_ghi": 2131.0,
    "mean_kt": 0.6200,
    "std_kt": 0.2050,
    "ghi_cv": 0.7500,
    "mean_dni": 490.0,
    "pct_clear_hours": 55.0,
}


# ---------------------------------------------------------------------------
# _apply_subhourly_correction method tests
# ---------------------------------------------------------------------------


class TestApplySubhourlyCorrection:
    """Tests for SolarModelingPipeline._apply_subhourly_correction."""

    @patch("src.pipeline.get_model_version", return_value="v1")
    @patch("src.pipeline.predict_correction", return_value=0.3)
    @patch("src.pipeline.compute_weather_features")
    def test_positive_correction_modifies_ac_gross(
        self, mock_weather, mock_predict, mock_version, tmp_path
    ):
        """Non-zero correction reduces ac_gross total energy."""
        # Arrange
        mock_weather.return_value = PHOENIX_WEATHER_FEATURES
        site = _make_site_config()
        site.weather_file_path = Path("/fake/weather.csv")
        result = _make_result_with_loss_data(site)
        original_total = result.hourly_data["ac_gross"].sum()

        pipeline = SolarModelingPipeline(output_dir=tmp_path)

        # Act
        metadata = pipeline._apply_subhourly_correction(result, site)

        # Assert — ac_gross should be reduced by ~0.3%
        adjusted_total = result.hourly_data["ac_gross"].sum()
        expected_reduction = original_total * 0.3 / 100
        assert adjusted_total < original_total
        assert adjusted_total == pytest.approx(
            original_total - expected_reduction, rel=0.01
        )

        # Metadata should contain correction info
        assert metadata["subhourly_correction_pct"] == pytest.approx(0.3, abs=0.001)
        assert metadata["subhourly_model_version"] == "v1"
        assert "raw_annual_energy_mwh" in metadata

    @patch("src.pipeline.get_model_version", return_value="v1")
    @patch("src.pipeline.predict_correction", return_value=0.0)
    @patch("src.pipeline.compute_weather_features")
    def test_zero_correction_recorded_but_no_modification(
        self, mock_weather, mock_predict, mock_version, tmp_path
    ):
        """Zero correction is recorded in metadata but ac_gross is unchanged."""
        # Arrange
        mock_weather.return_value = PHOENIX_WEATHER_FEATURES
        site = _make_site_config()
        site.weather_file_path = Path("/fake/weather.csv")
        result = _make_result_with_loss_data(site)
        original_values = result.hourly_data["ac_gross"].values.copy()

        pipeline = SolarModelingPipeline(output_dir=tmp_path)

        # Act
        metadata = pipeline._apply_subhourly_correction(result, site)

        # Assert — values unchanged
        np.testing.assert_array_equal(
            result.hourly_data["ac_gross"].values, original_values
        )

        # But metadata is still populated
        assert metadata["subhourly_correction_pct"] == 0.0
        assert metadata["subhourly_model_version"] == "v1"

    def test_missing_weather_file_returns_empty(self, tmp_path):
        """Missing weather_file_path skips correction gracefully."""
        # Arrange
        site = _make_site_config()
        site.weather_file_path = None  # No weather file
        result = _make_result_with_loss_data(site)

        pipeline = SolarModelingPipeline(output_dir=tmp_path)

        # Act
        metadata = pipeline._apply_subhourly_correction(result, site)

        # Assert — empty metadata, no crash
        assert metadata == {}

    @patch("src.pipeline.compute_weather_features")
    def test_missing_model_artifact_returns_empty(self, mock_weather, tmp_path):
        """FileNotFoundError from model loading skips correction gracefully."""
        # Arrange
        mock_weather.side_effect = FileNotFoundError("Model artifact not found")
        site = _make_site_config()
        site.weather_file_path = Path("/fake/weather.csv")
        result = _make_result_with_loss_data(site)

        pipeline = SolarModelingPipeline(output_dir=tmp_path)

        # Act
        metadata = pipeline._apply_subhourly_correction(result, site)

        # Assert — empty metadata, pipeline continues
        assert metadata == {}

    @patch("src.pipeline.compute_weather_features")
    def test_failed_weather_features_returns_empty(self, mock_weather, tmp_path):
        """Exception in compute_weather_features skips correction gracefully."""
        # Arrange
        mock_weather.side_effect = KeyError("GHI column missing")
        site = _make_site_config()
        site.weather_file_path = Path("/fake/weather.csv")
        result = _make_result_with_loss_data(site)

        pipeline = SolarModelingPipeline(output_dir=tmp_path)

        # Act
        metadata = pipeline._apply_subhourly_correction(result, site)

        # Assert
        assert metadata == {}

    def test_missing_dc_net_column_returns_empty(self, tmp_path):
        """Missing dc_net column in hourly_data skips correction gracefully."""
        # Arrange
        site = _make_site_config()
        site.weather_file_path = Path("/fake/weather.csv")
        result = _make_result_with_loss_data(site)
        result.hourly_data = result.hourly_data.drop(columns=["dc_net"])

        pipeline = SolarModelingPipeline(output_dir=tmp_path)

        # Act
        metadata = pipeline._apply_subhourly_correction(result, site)

        # Assert
        assert metadata == {}

    def test_none_hourly_data_returns_empty(self, tmp_path):
        """None hourly_data skips correction gracefully."""
        # Arrange
        site = _make_site_config()
        site.weather_file_path = Path("/fake/weather.csv")
        result = SimulationResult(
            site_name=site.site_name,
            run_name=site.run_name,
            customer=site.customer,
            success=True,
            hourly_data=None,
            weather_year=2024,
        )

        pipeline = SolarModelingPipeline(output_dir=tmp_path)

        # Act
        metadata = pipeline._apply_subhourly_correction(result, site)

        # Assert
        assert metadata == {}

    @patch("src.pipeline.get_model_version", return_value="v1")
    @patch("src.pipeline.predict_correction", return_value=0.3)
    @patch("src.pipeline.compute_weather_features")
    def test_raw_annual_energy_reflects_pre_correction_with_shading(
        self, mock_weather, mock_predict, mock_version, tmp_path
    ):
        """raw_annual_energy_mwh accounts for shading but not correction."""
        # Arrange
        mock_weather.return_value = PHOENIX_WEATHER_FEATURES
        site = _make_site_config()
        site.weather_file_path = Path("/fake/weather.csv")
        result = _make_result_with_loss_data(site, ac_value=1000.0)

        # Raw ac_gross total: 1000 * 8760 = 8,760,000 kWh
        # With shading (2%): 8,760,000 * 0.98 = 8,584,800 kWh = 8584.8 MWh
        shading_factor = 1 - site.shading_percent / 100

        pipeline = SolarModelingPipeline(output_dir=tmp_path)

        # Act
        metadata = pipeline._apply_subhourly_correction(result, site)

        # Assert — raw_annual_energy_mwh is pre-correction, post-shading
        expected_raw_mwh = 1000.0 * 8760 * shading_factor / 1000
        assert metadata["raw_annual_energy_mwh"] == pytest.approx(
            expected_raw_mwh, rel=0.001
        )


# ---------------------------------------------------------------------------
# Full pipeline flow tests
# ---------------------------------------------------------------------------


class TestPipelineWithCorrection:
    """Tests that correction metadata flows through the full pipeline.

    These tests use skip_climate=False and mock run_climate_data_pipeline to
    return sites with weather_file_path set. This is necessary because
    pipeline.run() loads fresh site configs from CSV (with weather_file_path=None),
    and only run_climate_data_pipeline assigns weather paths.
    """

    def _setup_pipeline_mocks(
        self, mock_sim, mock_climate, mock_weather, ac_value=1000.0, dc_value=1200.0,
    ):
        """Common setup: mock climate pipeline to return sites with weather paths."""
        mock_weather.return_value = PHOENIX_WEATHER_FEATURES
        sites = load_config(SINGLE_ROW_CSV)
        site = sites[0]
        site.weather_file_path = Path("/fake/weather.csv")
        mock_climate.return_value = (sites, {})
        mock_sim.return_value = _make_result_with_loss_data(
            site, ac_value=ac_value, dc_value=dc_value,
        )
        return site

    @patch("src.pipeline.get_model_version", return_value="v1")
    @patch("src.pipeline.predict_correction", return_value=0.15)
    @patch("src.pipeline.compute_weather_features")
    @patch("src.pipeline.run_climate_data_pipeline")
    @patch("src.pysam_integration.simulator.PySAMSimulator.execute_simulation")
    def test_correction_metadata_in_summary(
        self, mock_sim, mock_climate, mock_weather, mock_predict, mock_version,
        tmp_path,
    ):
        """Summary dict includes subhourly correction metadata."""
        # Arrange
        self._setup_pipeline_mocks(mock_sim, mock_climate, mock_weather)

        # Act — skip_climate=False so run_climate_data_pipeline assigns weather paths
        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        results = pipeline.run(SINGLE_ROW_CSV)

        # Assert — summary contains correction metadata
        assert results["successful"] == 1
        summary = results["summaries"][0]
        assert "subhourly_correction_pct" in summary
        assert summary["subhourly_correction_pct"] == pytest.approx(0.15, abs=0.001)
        assert summary["subhourly_model_version"] == "v1"
        assert "raw_annual_energy_mwh" in summary

    @patch("src.pipeline.compute_weather_features")
    @patch("src.pipeline.run_climate_data_pipeline")
    @patch("src.pysam_integration.simulator.PySAMSimulator.execute_simulation")
    def test_pipeline_continues_when_weather_features_fail(
        self, mock_sim, mock_climate, mock_weather, tmp_path,
    ):
        """Pipeline succeeds even when compute_weather_features raises."""
        # Arrange — weather features fail with FileNotFoundError
        mock_weather.side_effect = FileNotFoundError("Weather file not found")
        sites = load_config(SINGLE_ROW_CSV)
        site = sites[0]
        site.weather_file_path = Path("/fake/weather.csv")
        mock_climate.return_value = (sites, {})
        mock_sim.return_value = _make_result_with_loss_data(site)

        # Act
        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        results = pipeline.run(SINGLE_ROW_CSV)

        # Assert — pipeline completed, no correction metadata in summary
        assert results["successful"] == 1
        summary = results["summaries"][0]
        assert "subhourly_correction_pct" not in summary

    @patch("src.pipeline.get_model_version", return_value="v1")
    @patch("src.pipeline.predict_correction", return_value=0.0)
    @patch("src.pipeline.compute_weather_features")
    @patch("src.pipeline.run_climate_data_pipeline")
    @patch("src.pysam_integration.simulator.PySAMSimulator.execute_simulation")
    def test_zero_correction_still_recorded_in_summary(
        self, mock_sim, mock_climate, mock_weather, mock_predict, mock_version,
        tmp_path,
    ):
        """A 0.0% correction is still recorded in the summary metadata."""
        # Arrange
        self._setup_pipeline_mocks(mock_sim, mock_climate, mock_weather)

        # Act
        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        results = pipeline.run(SINGLE_ROW_CSV)

        # Assert — zero correction IS recorded
        summary = results["summaries"][0]
        assert summary["subhourly_correction_pct"] == 0.0
        assert summary["subhourly_model_version"] == "v1"

    @patch("src.pipeline.get_model_version", return_value="v1")
    @patch("src.pipeline.predict_correction", return_value=0.5)
    @patch("src.pipeline.compute_weather_features")
    @patch("src.pipeline.run_climate_data_pipeline")
    @patch("src.pysam_integration.simulator.PySAMSimulator.execute_simulation")
    def test_adjusted_energy_less_than_raw(
        self, mock_sim, mock_climate, mock_weather, mock_predict, mock_version,
        tmp_path,
    ):
        """Adjusted annual_energy_mwh is less than raw when correction > 0."""
        # Arrange
        self._setup_pipeline_mocks(
            mock_sim, mock_climate, mock_weather, ac_value=5000.0, dc_value=6000.0,
        )

        # Act
        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        results = pipeline.run(SINGLE_ROW_CSV)

        # Assert — adjusted energy should be less than raw
        summary = results["summaries"][0]
        assert summary["annual_energy_mwh"] < summary["raw_annual_energy_mwh"]


# ---------------------------------------------------------------------------
# Output artifact test
# ---------------------------------------------------------------------------


class TestCorrectionOutputArtifact:
    """Write test results to outputs/test_results/ for manual inspection."""

    @patch("src.pipeline.get_model_version", return_value="v1")
    @patch("src.pipeline.predict_correction", return_value=0.3)
    @patch("src.pipeline.compute_weather_features")
    @patch("src.pipeline.run_climate_data_pipeline")
    @patch("src.pysam_integration.simulator.PySAMSimulator.execute_simulation")
    def test_write_correction_artifact(
        self, mock_sim, mock_climate, mock_weather, mock_predict, mock_version,
        tmp_path,
    ):
        """Write correction test results for manual inspection."""
        import json

        # Arrange
        mock_weather.return_value = PHOENIX_WEATHER_FEATURES
        sites = load_config(SINGLE_ROW_CSV)
        site = sites[0]
        site.weather_file_path = Path("/fake/weather.csv")
        mock_climate.return_value = (sites, {})
        mock_sim.return_value = _make_result_with_loss_data(site, ac_value=5000.0)

        # Act
        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        results = pipeline.run(SINGLE_ROW_CSV)

        # Write artifact
        artifact_dir = Path("outputs/test_results")
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = artifact_dir / "test_pipeline_subhourly_correction.json"

        summary = results["summaries"][0]
        artifact = {
            "site_name": summary.get("site_name"),
            "subhourly_correction_pct": summary.get("subhourly_correction_pct"),
            "subhourly_model_version": summary.get("subhourly_model_version"),
            "raw_annual_energy_mwh": summary.get("raw_annual_energy_mwh"),
            "adjusted_annual_energy_mwh": summary.get("annual_energy_mwh"),
            "net_capacity_factor": summary.get("net_capacity_factor"),
            "specific_yield": summary.get("specific_yield"),
            "shading_pct_applied": summary.get("shading_pct_applied"),
        }
        artifact_path.write_text(json.dumps(artifact, indent=2))
