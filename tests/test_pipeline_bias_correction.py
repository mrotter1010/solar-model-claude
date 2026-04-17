"""Tests for NSRDB bias correction integration in the pipeline.

Tests the _apply_nsrdb_bias_correction method on SolarModelingPipeline,
elevation lookup via Open-Meteo, Solcast skip logic, bias correction
metadata flow through summaries, and elevation caching behavior.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.climate.open_meteo_client import (
    _elevation_cache,
    get_elevation_m,
)
from src.config.loader import load_config
from src.pipeline import SolarModelingPipeline
from src.pysam_integration.simulator import SimulationResult
from src.utils.exceptions import ClimateDataError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SINGLE_ROW_CSV = FIXTURES_DIR / "single_row_test.csv"


def _make_sam_weather_csv(path: Path, n_hours: int = 8760) -> None:
    """Write a minimal SAM-format weather CSV to *path*.

    Two-row metadata header (Latitude, Longitude, Time Zone, Elevation)
    followed by hourly data rows with constant irradiance.
    """
    hours_per_month = [744, 672, 744, 720, 744, 720, 744, 744, 720, 744, 720, 744]
    rows = []
    for month_idx, n_hrs in enumerate(hours_per_month):
        month = month_idx + 1
        for h in range(n_hrs):
            day = h // 24 + 1
            hour = h % 24
            rows.append({
                "Year": 2022,
                "Month": month,
                "Day": day,
                "Hour": hour,
                "Minute": 0,
                "GHI": 500,
                "DNI": 600,
                "DHI": 100,
                "Temperature": 25,
                "Wind Speed": 3,
                "Surface Albedo": 0.2,
            })

    df = pd.DataFrame(rows)
    with path.open("w") as f:
        f.write("Latitude,Longitude,Time Zone,Elevation\n")
        f.write("33.45,-112.07,-7,340\n")
        df.to_csv(f, index=False)


def _make_loss_data() -> dict:
    """Create loss_data dict matching the output_writer expectations."""
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


def _make_hourly_data(hours: int = 8760) -> pd.DataFrame:
    """Build synthetic hourly DataFrame with AC and DC columns."""
    timestamps = pd.date_range(start="2024-01-01", periods=hours, freq="h")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "ac_gross": [1000.0] * hours,
            "ac_net": [1000.0] * hours,
            "dc_net": [1200.0] * hours,
            "poa_irradiance": [500.0] * hours,
            "poa_nominal": [520.0] * hours,
            "cell_temperature": [35.0] * hours,
            "inverter_efficiency": [96.0] * hours,
        }
    )


def _make_site_config():
    """Load the single-row test fixture and return the SiteConfig."""
    sites = load_config(SINGLE_ROW_CSV)
    return sites[0]


def _make_sim_result(site) -> SimulationResult:
    """Create a successful SimulationResult for pipeline testing."""
    return SimulationResult(
        site_name=site.site_name,
        run_name=site.run_name,
        customer=site.customer,
        success=True,
        hourly_data=_make_hourly_data(),
        loss_data=_make_loss_data(),
        weather_year=2024,
    )


# Fake correction metadata returned by the mocked apply_bias_correction
FAKE_CORRECTION_META = {
    "bias_correction_applied": True,
    "bias_correction_model_version": "1.0.0",
    "mean_ghi_correction_factor": 0.9650,
    "mean_dni_correction_factor": 0.9500,
    "monthly_ghi_factors": {str(m): 0.965 for m in range(1, 13)},
    "monthly_dni_factors": {str(m): 0.950 for m in range(1, 13)},
}


# ---------------------------------------------------------------------------
# a. NSRDB pipeline run applies bias correction
# ---------------------------------------------------------------------------


class TestNSRDBBiasCorrectionApplied:
    """Test that _apply_nsrdb_bias_correction modifies weather files."""

    @patch("src.pipeline.apply_bias_correction")
    @patch("src.pipeline.get_elevation_m", return_value=340.0)
    def test_nsrdb_site_gets_correction(
        self, mock_elev, mock_apply, tmp_path,
    ):
        """NSRDB-sourced site: weather file is read, corrected, and rewritten."""
        # Arrange — create a SAM weather file on disk
        weather_path = tmp_path / "weather.csv"
        _make_sam_weather_csv(weather_path)

        # Mock apply_bias_correction to return a modified DataFrame + metadata
        original_df = pd.read_csv(weather_path, skiprows=2)
        corrected_df = original_df.copy()
        corrected_df["GHI"] = (corrected_df["GHI"] * 0.965).astype(int)
        corrected_df["DNI"] = (corrected_df["DNI"] * 0.95).astype(int)
        mock_apply.return_value = (corrected_df, FAKE_CORRECTION_META.copy())

        site = _make_site_config()
        site.weather_file_path = weather_path
        site.data_source = "nsrdb"
        site.resource_file_path = None

        pipeline = SolarModelingPipeline(output_dir=tmp_path)

        # Act
        metadata = pipeline._apply_nsrdb_bias_correction(site)

        # Assert — metadata returned
        assert metadata is not None
        assert metadata["bias_correction_applied"] is True
        assert metadata["mean_ghi_correction_factor"] == pytest.approx(0.965, abs=0.01)

        # Assert — elevation was looked up
        mock_elev.assert_called_once_with(site.latitude, site.longitude)

        # Assert — apply_bias_correction was called with correct args
        mock_apply.assert_called_once()
        call_args = mock_apply.call_args
        assert call_args[0][1] == site.latitude  # latitude
        assert call_args[0][2] == site.longitude  # longitude
        assert call_args[0][3] == 340.0  # elevation_m

        # Assert — weather file was rewritten with corrected data
        rewritten_df = pd.read_csv(weather_path, skiprows=2)
        assert (rewritten_df["GHI"] == corrected_df["GHI"]).all()

    @patch("src.pipeline.apply_bias_correction")
    @patch("src.pipeline.get_elevation_m", return_value=340.0)
    def test_weather_file_header_preserved(
        self, mock_elev, mock_apply, tmp_path,
    ):
        """Rewritten weather file retains the 2-row SAM metadata header."""
        # Arrange
        weather_path = tmp_path / "weather.csv"
        _make_sam_weather_csv(weather_path)

        original_df = pd.read_csv(weather_path, skiprows=2)
        mock_apply.return_value = (original_df.copy(), FAKE_CORRECTION_META.copy())

        site = _make_site_config()
        site.weather_file_path = weather_path
        site.data_source = "nsrdb"
        site.resource_file_path = None

        pipeline = SolarModelingPipeline(output_dir=tmp_path)

        # Act
        pipeline._apply_nsrdb_bias_correction(site)

        # Assert — first two lines are the SAM header
        lines = weather_path.read_text().splitlines()
        assert lines[0] == "Latitude,Longitude,Time Zone,Elevation"
        parts = lines[1].split(",")
        assert float(parts[0]) == site.latitude
        assert float(parts[1]) == site.longitude

    def test_missing_weather_file_returns_none(self, tmp_path):
        """Site with no weather file skips correction gracefully."""
        site = _make_site_config()
        site.weather_file_path = None
        site.data_source = "nsrdb"

        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        result = pipeline._apply_nsrdb_bias_correction(site)
        assert result is None

    @patch("src.pipeline.get_elevation_m")
    def test_elevation_failure_returns_none(self, mock_elev, tmp_path):
        """If elevation lookup fails, correction is skipped (not fatal)."""
        mock_elev.side_effect = ClimateDataError("Elevation lookup failed")

        weather_path = tmp_path / "weather.csv"
        _make_sam_weather_csv(weather_path)

        site = _make_site_config()
        site.weather_file_path = weather_path
        site.data_source = "nsrdb"
        site.resource_file_path = None

        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        result = pipeline._apply_nsrdb_bias_correction(site)

        # Should return None (skipped), not raise
        assert result is None


# ---------------------------------------------------------------------------
# b. Solcast pipeline run skips bias correction
# ---------------------------------------------------------------------------


class TestSolcastSkipsBiasCorrection:
    """Test that Solcast-sourced sites skip bias correction entirely."""

    def test_solcast_data_source_returns_none(self, tmp_path):
        """data_source='solcast' means correction is skipped."""
        site = _make_site_config()
        site.data_source = "solcast"
        site.weather_file_path = tmp_path / "weather.csv"
        _make_sam_weather_csv(site.weather_file_path)

        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        result = pipeline._apply_nsrdb_bias_correction(site)
        assert result is None

    def test_resource_file_path_set_returns_none(self, tmp_path):
        """resource_file_path being set (Solcast file) skips correction."""
        site = _make_site_config()
        site.data_source = "nsrdb"
        site.resource_file_path = Path("/some/solcast/file.csv")
        site.weather_file_path = tmp_path / "weather.csv"
        _make_sam_weather_csv(site.weather_file_path)

        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        result = pipeline._apply_nsrdb_bias_correction(site)
        assert result is None

    @patch("src.pipeline.apply_bias_correction")
    @patch("src.pipeline.get_elevation_m")
    def test_solcast_does_not_call_models(
        self, mock_elev, mock_apply, tmp_path,
    ):
        """Solcast sites should not trigger elevation lookup or model calls."""
        site = _make_site_config()
        site.data_source = "solcast"
        site.weather_file_path = tmp_path / "weather.csv"
        _make_sam_weather_csv(site.weather_file_path)

        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        pipeline._apply_nsrdb_bias_correction(site)

        mock_elev.assert_not_called()
        mock_apply.assert_not_called()


# ---------------------------------------------------------------------------
# c. Bias correction metadata in run summaries
# ---------------------------------------------------------------------------


class TestBiasCorrectionInSummary:
    """Test that bias correction metadata flows through to pipeline summaries."""

    @patch("src.optimization.clipping_correction.get_model_version", return_value="v1")
    @patch("src.optimization.clipping_correction.predict_correction", return_value=0.0)
    @patch("src.optimization.clipping_correction.compute_weather_features")
    @patch("src.pipeline.run_climate_data_pipeline")
    @patch("src.pysam_integration.simulator.PySAMSimulator.execute_simulation")
    def test_summary_contains_bias_metadata(
        self, mock_sim, mock_climate,
        mock_weather_features, mock_predict, mock_version, tmp_path,
    ):
        """Summary dict includes bias correction keys when NSRDB is the source."""
        # Arrange — set up climate pipeline mock to return sites with weather files
        weather_path = tmp_path / "weather.csv"
        _make_sam_weather_csv(weather_path)

        sites = load_config(SINGLE_ROW_CSV)
        site = sites[0]
        site.weather_file_path = weather_path
        site.data_source = "nsrdb"
        site.resource_file_path = None
        # 3-tuple: bias correction metadata now comes from the 3rd return value
        mock_climate.return_value = (
            sites, {}, {site.site_name: FAKE_CORRECTION_META.copy()},
        )

        # Mock the PySAM simulation
        mock_weather_features.return_value = {"annual_ghi": 2131.0}
        mock_sim.return_value = _make_sim_result(site)

        # Act
        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        results = pipeline.run(SINGLE_ROW_CSV)

        # Assert — summary has bias correction metadata
        assert results["successful"] == 1
        summary = results["summaries"][0]
        assert summary["bias_correction_applied"] is True
        assert summary["bias_correction_model_version"] == "1.0.0"
        assert "mean_ghi_correction_factor" in summary
        assert "mean_dni_correction_factor" in summary

    @patch("src.optimization.clipping_correction.get_model_version", return_value="v1")
    @patch("src.optimization.clipping_correction.predict_correction", return_value=0.0)
    @patch("src.optimization.clipping_correction.compute_weather_features")
    @patch("src.pipeline.run_climate_data_pipeline")
    @patch("src.pysam_integration.simulator.PySAMSimulator.execute_simulation")
    def test_solcast_summary_has_no_bias_keys(
        self, mock_sim, mock_climate, mock_weather_features,
        mock_predict, mock_version, tmp_path,
    ):
        """Summary dict does NOT include bias correction keys for Solcast runs."""
        # Arrange
        sites = load_config(SINGLE_ROW_CSV)
        site = sites[0]
        site.weather_file_path = tmp_path / "weather.csv"
        _make_sam_weather_csv(site.weather_file_path)
        site.data_source = "solcast"
        site.resource_file_path = None
        mock_climate.return_value = (sites, {}, {})

        mock_weather_features.return_value = {"annual_ghi": 2131.0}
        mock_sim.return_value = _make_sim_result(site)

        # Act
        pipeline = SolarModelingPipeline(output_dir=tmp_path)
        results = pipeline.run(SINGLE_ROW_CSV)

        # Assert — no bias correction metadata in summary
        assert results["successful"] == 1
        summary = results["summaries"][0]
        assert "bias_correction_applied" not in summary


# ---------------------------------------------------------------------------
# d. Elevation lookup function
# ---------------------------------------------------------------------------


class TestElevationLookup:
    """Test get_elevation_m with mocked API responses."""

    def setup_method(self):
        """Clear the elevation cache before each test."""
        _elevation_cache.clear()

    @patch("src.climate.open_meteo_client.urllib.request.urlopen")
    def test_successful_elevation_lookup(self, mock_urlopen):
        """Successful API response returns elevation in meters."""
        # Arrange — mock API response
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(
            {"elevation": [340.0]}
        ).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        # Act
        elevation = get_elevation_m(33.45, -112.07)

        # Assert
        assert elevation == 340.0
        mock_urlopen.assert_called_once()

    @patch("src.climate.open_meteo_client.urllib.request.urlopen")
    def test_failed_api_raises_climate_error(self, mock_urlopen):
        """Failed API call raises ClimateDataError, not a silent default."""
        mock_urlopen.side_effect = ConnectionError("Network unreachable")

        with pytest.raises(ClimateDataError, match="Elevation lookup failed"):
            get_elevation_m(33.45, -112.07)

    @patch("src.climate.open_meteo_client.urllib.request.urlopen")
    def test_empty_elevation_response_raises(self, mock_urlopen):
        """API response with empty elevation list raises ClimateDataError."""
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(
            {"elevation": []}
        ).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        with pytest.raises(ClimateDataError, match="Elevation lookup failed"):
            get_elevation_m(33.45, -112.07)

    @patch("src.climate.open_meteo_client.urllib.request.urlopen")
    def test_missing_elevation_key_raises(self, mock_urlopen):
        """API response without 'elevation' key raises ClimateDataError."""
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(
            {"error": "Invalid request"}
        ).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        with pytest.raises(ClimateDataError, match="Elevation lookup failed"):
            get_elevation_m(33.45, -112.07)


# ---------------------------------------------------------------------------
# e. Elevation caching
# ---------------------------------------------------------------------------


class TestElevationCaching:
    """Test that elevation lookups are cached to avoid redundant API calls."""

    def setup_method(self):
        """Clear the elevation cache before each test."""
        _elevation_cache.clear()

    @patch("src.climate.open_meteo_client.urllib.request.urlopen")
    def test_second_call_uses_cache(self, mock_urlopen):
        """Second call for same lat/lon does not hit the API again."""
        # Arrange — first call returns elevation
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(
            {"elevation": [340.0]}
        ).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        # Act — two calls with same coordinates
        elev1 = get_elevation_m(33.45, -112.07)
        elev2 = get_elevation_m(33.45, -112.07)

        # Assert — API called only once, both return same value
        assert mock_urlopen.call_count == 1
        assert elev1 == 340.0
        assert elev2 == 340.0

    @patch("src.climate.open_meteo_client.urllib.request.urlopen")
    def test_different_coordinates_hit_api_separately(self, mock_urlopen):
        """Different lat/lon coordinates each make their own API call."""
        # Arrange — return different elevations based on call order
        mock_response_1 = MagicMock()
        mock_response_1.read.return_value = json.dumps(
            {"elevation": [340.0]}
        ).encode()
        mock_response_1.__enter__ = lambda s: s
        mock_response_1.__exit__ = MagicMock(return_value=False)

        mock_response_2 = MagicMock()
        mock_response_2.read.return_value = json.dumps(
            {"elevation": [50.0]}
        ).encode()
        mock_response_2.__enter__ = lambda s: s
        mock_response_2.__exit__ = MagicMock(return_value=False)

        mock_urlopen.side_effect = [mock_response_1, mock_response_2]

        # Act
        elev_phx = get_elevation_m(33.45, -112.07)
        elev_sea = get_elevation_m(47.61, -122.33)

        # Assert — two separate API calls
        assert mock_urlopen.call_count == 2
        assert elev_phx == 340.0
        assert elev_sea == 50.0

    def test_cache_populated_after_lookup(self):
        """After a successful lookup, the cache dict contains the result."""
        _elevation_cache[(33.45, -112.07)] = 340.0
        # Direct cache access without API
        elev = get_elevation_m(33.45, -112.07)
        assert elev == 340.0


# ---------------------------------------------------------------------------
# Output artifact
# ---------------------------------------------------------------------------


class TestBiasCorrectionOutputArtifact:
    """Write test results to outputs/test_results/ for manual inspection."""

    @patch("src.pipeline.apply_bias_correction")
    @patch("src.pipeline.get_elevation_m", return_value=340.0)
    def test_write_correction_artifact(
        self, mock_elev, mock_apply, tmp_path,
    ):
        """Write bias correction test results for manual inspection."""
        # Arrange
        weather_path = tmp_path / "weather.csv"
        _make_sam_weather_csv(weather_path)

        original_df = pd.read_csv(weather_path, skiprows=2)
        corrected_df = original_df.copy()
        corrected_df["GHI"] = (corrected_df["GHI"] * 0.965).astype(int)
        corrected_df["DNI"] = (corrected_df["DNI"] * 0.95).astype(int)
        mock_apply.return_value = (corrected_df, FAKE_CORRECTION_META.copy())

        site = _make_site_config()
        site.weather_file_path = weather_path
        site.data_source = "nsrdb"
        site.resource_file_path = None

        pipeline = SolarModelingPipeline(output_dir=tmp_path)

        # Act
        metadata = pipeline._apply_nsrdb_bias_correction(site)

        # Write artifact
        artifact_dir = Path("outputs/test_results")
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = artifact_dir / "test_pipeline_bias_correction.json"

        artifact = {
            "site_name": site.site_name,
            "bias_correction_applied": metadata["bias_correction_applied"],
            "model_version": metadata["bias_correction_model_version"],
            "mean_ghi_correction_factor": metadata["mean_ghi_correction_factor"],
            "mean_dni_correction_factor": metadata["mean_dni_correction_factor"],
            "original_ghi_sample": int(original_df["GHI"].iloc[0]),
            "corrected_ghi_sample": int(corrected_df["GHI"].iloc[0]),
        }
        artifact_path.write_text(json.dumps(artifact, indent=2))
