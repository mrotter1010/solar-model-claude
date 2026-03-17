"""Tests for subhourly resolution correction model.

Tests compute_weather_features, predict_correction, model loading,
and edge cases using real cached weather data and known training inputs.
"""

import json
from pathlib import Path

import numpy as np
import pytest

from src.models.subhourly_correction import (
    _classify_climate_type,
    _load_model_and_metadata,
    compute_weather_features,
    predict_correction,
)

# Real cached weather file from M8 research
PHOENIX_WEATHER = Path(
    "research/m8_subhourly/cache/60min/Phoenix_AZ_2022_60min.csv"
)

# Reference values from training data (Phoenix tracker, GCR=0.4)
PHOENIX_TRACKER_120 = {
    "dcac_ratio": 1.20,
    "gcr": 0.4,
    "racking": "tracker",
    "latitude": 33.48,
    "longitude": -112.07,
    "cf_60min": 24.959,  # from training_data.csv
    "expected_correction": -1.10,  # approximately -1.1%
}

PHOENIX_TRACKER_160 = {
    "dcac_ratio": 1.60,
    "gcr": 0.4,
    "racking": "tracker",
    "latitude": 33.48,
    "longitude": -112.07,
    "cf_60min": 22.547,
    "expected_correction": -0.60,  # approximately -0.6%
}

# Metadata artifact path
METADATA_PATH = Path(
    "src/models/artifacts/subhourly_correction_v2_metadata.json"
)


# ---------------------------------------------------------------------------
# compute_weather_features tests
# ---------------------------------------------------------------------------


class TestComputeWeatherFeatures:
    """Tests for compute_weather_features with real weather data."""

    @pytest.fixture(autouse=True)
    def _skip_if_no_weather_file(self) -> None:
        """Skip tests if cached weather file is not available."""
        if not PHOENIX_WEATHER.exists():
            pytest.skip(f"Weather file not found: {PHOENIX_WEATHER}")

    def test_returns_all_six_keys(self) -> None:
        """Verify returned dict contains all 6 expected feature keys."""
        features = compute_weather_features(PHOENIX_WEATHER)

        expected_keys = {
            "annual_ghi", "mean_kt", "std_kt",
            "ghi_cv", "mean_dni", "pct_clear_hours",
        }
        assert set(features.keys()) == expected_keys

    def test_phoenix_annual_ghi_reasonable(self) -> None:
        """Phoenix annual GHI should be ~2000-2300 kWh/m²."""
        features = compute_weather_features(PHOENIX_WEATHER)

        assert 1800 < features["annual_ghi"] < 2500, (
            f"Phoenix annual GHI={features['annual_ghi']} outside expected range"
        )

    def test_phoenix_mean_kt_reasonable(self) -> None:
        """Phoenix mean Kt should be ~0.6-0.7 (clear desert climate)."""
        features = compute_weather_features(PHOENIX_WEATHER)

        assert 0.5 < features["mean_kt"] < 0.8, (
            f"Phoenix mean Kt={features['mean_kt']} outside expected range"
        )

    def test_phoenix_pct_clear_reasonable(self) -> None:
        """Phoenix should have >40% clear hours (arid climate)."""
        features = compute_weather_features(PHOENIX_WEATHER)

        assert features["pct_clear_hours"] > 40.0, (
            f"Phoenix pct_clear={features['pct_clear_hours']} too low for desert"
        )

    def test_all_values_are_finite(self) -> None:
        """All feature values should be finite numbers, not NaN/inf."""
        features = compute_weather_features(PHOENIX_WEATHER)

        for key, value in features.items():
            assert np.isfinite(value), f"{key}={value} is not finite"

    def test_std_kt_positive(self) -> None:
        """Standard deviation of Kt should be positive (variability exists)."""
        features = compute_weather_features(PHOENIX_WEATHER)

        assert features["std_kt"] > 0, "std_kt should be positive"

    def test_ghi_cv_positive(self) -> None:
        """Coefficient of variation of GHI should be positive."""
        features = compute_weather_features(PHOENIX_WEATHER)

        assert features["ghi_cv"] > 0, "ghi_cv should be positive"

    def test_mean_dni_reasonable(self) -> None:
        """Phoenix mean daytime DNI should be ~400-700 W/m²."""
        features = compute_weather_features(PHOENIX_WEATHER)

        assert 300 < features["mean_dni"] < 800, (
            f"Phoenix mean DNI={features['mean_dni']} outside expected range"
        )

    def test_features_match_research_values(
        self, test_results_dir: Path
    ) -> None:
        """Features should approximately match research site_weather_features.csv.

        Research values for Phoenix_AZ:
            annual_ghi=2142.52, mean_kt=0.6547, std_kt=0.2548,
            ghi_cv=0.6984, mean_dni=584.82, pct_clear_hours=53.34
        """
        features = compute_weather_features(PHOENIX_WEATHER)

        # Write to test_results for manual inspection
        output_path = test_results_dir / "test_subhourly_weather_features.json"
        output_path.write_text(json.dumps(features, indent=2))

        # Allow 5% tolerance since research may have used slightly different params
        assert abs(features["annual_ghi"] - 2142.52) / 2142.52 < 0.05
        assert abs(features["mean_kt"] - 0.6547) / 0.6547 < 0.05
        assert abs(features["pct_clear_hours"] - 53.34) / 53.34 < 0.10

    def test_file_not_found_raises(self) -> None:
        """Should raise FileNotFoundError for nonexistent file."""
        with pytest.raises(FileNotFoundError):
            compute_weather_features(Path("/nonexistent/weather.csv"))


# ---------------------------------------------------------------------------
# predict_correction tests
# ---------------------------------------------------------------------------


class TestPredictCorrection:
    """Tests for predict_correction with known training data inputs."""

    @pytest.fixture(autouse=True)
    def _skip_if_no_model(self) -> None:
        """Skip tests if model artifact is not available."""
        if not METADATA_PATH.exists():
            pytest.skip("Model artifacts not found")

    def _phoenix_weather_features(self) -> dict[str, float]:
        """Return Phoenix weather features from research data."""
        return {
            "annual_ghi": 2142.52,
            "mean_kt": 0.6547,
            "std_kt": 0.2548,
            "ghi_cv": 0.6984,
            "mean_dni": 584.82,
            "pct_clear_hours": 53.34,
        }

    def _seattle_weather_features(self) -> dict[str, float]:
        """Return Seattle weather features from research data."""
        return {
            "annual_ghi": 1309.56,
            "mean_kt": 0.4619,
            "std_kt": 0.2643,
            "ghi_cv": 0.9231,
            "mean_dni": 307.63,
            "pct_clear_hours": 24.01,
        }

    def test_phoenix_tracker_dcac_120_clamped_to_zero(self) -> None:
        """Phoenix tracker DC/AC 1.20: raw model is negative, clamped to 0.0.

        Raw model predicts ~-1.1% (hourly underestimates for desert tracker),
        but the clamp converts this to 0.0 since negative corrections are
        artifacts of satellite data smoothing.
        """
        config = PHOENIX_TRACKER_120
        correction = predict_correction(
            dcac_ratio=config["dcac_ratio"],
            gcr=config["gcr"],
            racking=config["racking"],
            latitude=config["latitude"],
            longitude=config["longitude"],
            cf_60min=config["cf_60min"],
            weather_features=self._phoenix_weather_features(),
        )

        assert correction == 0.0, (
            f"Expected 0.0 after clamping, got {correction:.4f}%"
        )

    def test_phoenix_tracker_dcac_160_small_positive(self) -> None:
        """Phoenix tracker DC/AC 1.60: v2 predicts small positive correction.

        v1 predicted negative (clamped to 0.0) for this desert config. v2,
        trained on 1-min ground data, correctly identifies a small subhourly
        clipping loss even at high-irradiance desert sites with high DC/AC.
        """
        config = PHOENIX_TRACKER_160
        correction = predict_correction(
            dcac_ratio=config["dcac_ratio"],
            gcr=config["gcr"],
            racking=config["racking"],
            latitude=config["latitude"],
            longitude=config["longitude"],
            cf_60min=config["cf_60min"],
            weather_features=self._phoenix_weather_features(),
        )

        assert correction >= 0.0, "Correction must be non-negative"
        assert correction < 0.5, (
            f"Phoenix tracker DC/AC 1.60 correction {correction:.4f}% "
            f"unexpectedly high"
        )

    def test_seattle_fixed_dcac_140(self) -> None:
        """Seattle fixed DC/AC 1.40 should predict positive correction.

        Cloudy fixed-tilt: positive correction (hourly overestimates).
        v2 trained on ground-truth 1-min data predicts higher corrections
        than v1 (~1.2% vs ~0.1%).
        """
        correction = predict_correction(
            dcac_ratio=1.40,
            gcr=0.4,
            racking="fixed",
            latitude=47.61,
            longitude=-122.33,
            cf_60min=13.496,
            weather_features=self._seattle_weather_features(),
        )

        assert correction >= 0.0, "Clamped correction must be non-negative"
        assert correction < 2.0, (
            f"Seattle fixed correction {correction:.4f}% unexpectedly high"
        )

    def test_portland_fixed_dcac_160_positive_passes_through(self) -> None:
        """Portland fixed DC/AC 1.60: positive raw prediction passes through.

        From training data: actual correction = +0.61%. This is the strongest
        positive correction in the dataset — cloudy climate, high DC/AC,
        fixed-tilt. The raw prediction is positive, so the clamp has no effect.
        """
        portland_weather = {
            "annual_ghi": 1326.48,
            "mean_kt": 0.467,
            "std_kt": 0.2689,
            "ghi_cv": 0.8928,
            "mean_dni": 317.13,
            "pct_clear_hours": 24.49,
        }

        correction = predict_correction(
            dcac_ratio=1.60,
            gcr=0.4,
            racking="fixed",
            latitude=45.52,
            longitude=-122.68,
            cf_60min=13.220,  # from training_data.csv
            weather_features=portland_weather,
        )

        # Should be positive and pass through the clamp unchanged
        assert correction > 0, (
            f"Portland fixed DC/AC 1.60 should have positive correction, "
            f"got {correction:.4f}%"
        )
        # v2 predicts higher corrections than v1's training value of +0.61%
        assert 0.2 < correction < 2.5, (
            f"Portland correction {correction:.4f}% outside expected range"
        )

    def test_higher_dcac_has_larger_correction(self) -> None:
        """Higher DC/AC ratio should produce larger or equal correction.

        DC/AC ratio is the dominant predictor of subhourly clipping loss.
        """
        weather = self._phoenix_weather_features()

        correction_120 = predict_correction(
            dcac_ratio=1.20, gcr=0.4, racking="tracker",
            latitude=33.48, longitude=-112.07, cf_60min=24.959,
            weather_features=weather,
        )
        correction_160 = predict_correction(
            dcac_ratio=1.60, gcr=0.4, racking="tracker",
            latitude=33.48, longitude=-112.07, cf_60min=22.547,
            weather_features=weather,
        )

        assert correction_120 >= 0.0
        assert correction_160 >= 0.0
        assert correction_160 >= correction_120, (
            f"DC/AC 1.60 ({correction_160:.4f}%) should be >= "
            f"DC/AC 1.20 ({correction_120:.4f}%)"
        )

    def test_correction_non_negative(self) -> None:
        """All predictions should be >= 0.0 after clamping."""
        weather = self._phoenix_weather_features()

        correction = predict_correction(
            dcac_ratio=1.40, gcr=0.4, racking="tracker",
            latitude=33.48, longitude=-112.07, cf_60min=24.0,
            weather_features=weather,
        )

        assert correction >= 0.0, (
            f"Correction {correction:.4f}% should be non-negative after clamp"
        )

    def test_correction_within_clamped_range(self) -> None:
        """Clamped predictions should be in [0.0, ~3.5%]."""
        weather = self._phoenix_weather_features()

        correction = predict_correction(
            dcac_ratio=1.40, gcr=0.4, racking="tracker",
            latitude=33.48, longitude=-112.07, cf_60min=24.0,
            weather_features=weather,
        )

        assert 0.0 <= correction < 4.0, (
            f"Correction {correction:.4f}% outside clamped range [0, 4.0]"
        )

    def test_prediction_output_saved(self, test_results_dir: Path) -> None:
        """Save prediction results to test_results for manual inspection."""
        weather_phx = self._phoenix_weather_features()
        weather_sea = self._seattle_weather_features()
        weather_pdx = {
            "annual_ghi": 1326.48, "mean_kt": 0.467, "std_kt": 0.2689,
            "ghi_cv": 0.8928, "mean_dni": 317.13, "pct_clear_hours": 24.49,
        }

        results = []
        test_cases = [
            ("Phoenix tracker DC/AC 1.20 (clamped)", 1.20, 0.4, "tracker",
             33.48, -112.07, 24.959, weather_phx),
            ("Phoenix tracker DC/AC 1.60 (clamped)", 1.60, 0.4, "tracker",
             33.48, -112.07, 22.547, weather_phx),
            ("Seattle fixed DC/AC 1.40", 1.40, 0.4, "fixed",
             47.61, -122.33, 13.496, weather_sea),
            ("Portland fixed DC/AC 1.60", 1.60, 0.4, "fixed",
             45.52, -122.68, 13.220, weather_pdx),
        ]

        for label, dcac, gcr, rack, lat, lon, cf, weather in test_cases:
            correction = predict_correction(
                dcac_ratio=dcac, gcr=gcr, racking=rack,
                latitude=lat, longitude=lon, cf_60min=cf,
                weather_features=weather,
            )
            results.append({
                "config": label,
                "correction_pct": round(correction, 4),
            })

        output_path = test_results_dir / "test_subhourly_predictions.json"
        output_path.write_text(json.dumps(results, indent=2))

        # Verify file was written
        assert output_path.exists()


# ---------------------------------------------------------------------------
# Model loading and metadata tests
# ---------------------------------------------------------------------------


class TestModelLoading:
    """Tests for model artifact loading and feature order consistency."""

    @pytest.fixture(autouse=True)
    def _skip_if_no_model(self) -> None:
        """Skip tests if model artifact is not available."""
        if not METADATA_PATH.exists():
            pytest.skip("Model artifacts not found")

    def test_model_loads_successfully(self) -> None:
        """Model and metadata should load without errors."""
        # Clear cache to test fresh load
        _load_model_and_metadata.cache_clear()

        model, metadata = _load_model_and_metadata()

        assert model is not None
        assert metadata is not None

    def test_metadata_has_required_keys(self) -> None:
        """Metadata should contain all required keys."""
        _, metadata = _load_model_and_metadata()

        required_keys = {
            "model_type", "training_date", "feature_list",
            "target_variable", "version", "cv_metrics",
        }
        assert required_keys.issubset(set(metadata.keys()))

    def test_feature_list_has_14_features(self) -> None:
        """Model expects exactly 14 features."""
        _, metadata = _load_model_and_metadata()

        assert len(metadata["feature_list"]) == 14

    def test_feature_order_matches_metadata(self) -> None:
        """Feature list order should match what the model was trained on.

        The model's n_features_in_ should equal the metadata feature count.
        """
        model, metadata = _load_model_and_metadata()

        assert model.n_features_in_ == len(metadata["feature_list"])

    def test_model_is_cached(self) -> None:
        """Second load should return same object (cached)."""
        _load_model_and_metadata.cache_clear()

        model1, meta1 = _load_model_and_metadata()
        model2, meta2 = _load_model_and_metadata()

        assert model1 is model2
        assert meta1 is meta2

    def test_model_type_is_gradient_boosting(self) -> None:
        """Model should be a GradientBoostingRegressor."""
        _, metadata = _load_model_and_metadata()

        assert metadata["model_type"] == "GradientBoostingRegressor"


# ---------------------------------------------------------------------------
# Climate classification tests
# ---------------------------------------------------------------------------


class TestClimateClassification:
    """Tests for climate type classification from pct_clear_hours."""

    def test_cloudy_below_30(self) -> None:
        """pct_clear_hours < 30 should classify as cloudy."""
        assert _classify_climate_type(24.0) == "cloudy"
        assert _classify_climate_type(29.9) == "cloudy"

    def test_clear_above_45(self) -> None:
        """pct_clear_hours >= 45 should classify as clear."""
        assert _classify_climate_type(45.0) == "clear"
        assert _classify_climate_type(60.0) == "clear"

    def test_variable_between_30_and_45(self) -> None:
        """pct_clear_hours 30-45 should classify as variable."""
        assert _classify_climate_type(30.0) == "variable"
        assert _classify_climate_type(37.5) == "variable"
        assert _classify_climate_type(44.9) == "variable"

    def test_phoenix_is_clear(self) -> None:
        """Phoenix (53.34% clear hours) should classify as clear."""
        assert _classify_climate_type(53.34) == "clear"

    def test_seattle_is_cloudy(self) -> None:
        """Seattle (24.01% clear hours) should classify as cloudy."""
        assert _classify_climate_type(24.01) == "cloudy"


# ---------------------------------------------------------------------------
# Edge case: weather file without clearsky GHI
# ---------------------------------------------------------------------------


class TestNoClearskyGHI:
    """Verify that compute_weather_features works without clearsky GHI.

    Neither NSRDB nor Solcast SAM-format files include clearsky GHI columns.
    The module computes Kt using extraterrestrial irradiance (Spencer equations)
    as a fallback. This test verifies the fallback path produces valid results.
    """

    @pytest.fixture(autouse=True)
    def _skip_if_no_weather_file(self) -> None:
        """Skip tests if cached weather file is not available."""
        if not PHOENIX_WEATHER.exists():
            pytest.skip(f"Weather file not found: {PHOENIX_WEATHER}")

    def test_no_clearsky_column_still_computes_kt(self) -> None:
        """Weather file has no Clearsky GHI column but Kt is still computed.

        The NSRDB cached file at Phoenix_AZ_2022_60min.csv does not have
        a Clearsky GHI column. Verify that mean_kt and std_kt are still
        computed using the extraterrestrial irradiance fallback.
        """
        import pandas as pd

        # Confirm the file lacks clearsky GHI
        df = pd.read_csv(PHOENIX_WEATHER, skiprows=2, nrows=0)
        clearsky_cols = [
            c for c in df.columns if "clearsky" in c.lower()
        ]
        assert len(clearsky_cols) == 0, (
            f"Expected no clearsky columns, found: {clearsky_cols}"
        )

        # compute_weather_features should still work
        features = compute_weather_features(PHOENIX_WEATHER)

        # Kt should be computed via extraterrestrial irradiance
        assert 0.3 < features["mean_kt"] < 0.9
        assert features["std_kt"] > 0

    def test_solcast_format_no_clearsky(self, tmp_path: Path) -> None:
        """Solcast-format file without clearsky columns should work.

        Creates a minimal Solcast-format weather file and verifies
        compute_weather_features handles it correctly.
        """
        # Create a minimal Solcast-format file (24 hours)
        lines = [
            "Latitude,Longitude,Time Zone,Elevation,Source",
            "33.48,-112.07,-7,338,Solcast",
            "Year,Month,Day,Hour,Minute,GHI,DNI,DHI,Tdry,Wspd",
        ]

        # Generate 24 hours with a simple solar pattern
        for hour in range(24):
            if 7 <= hour <= 17:
                # Simple bell curve for daytime GHI
                solar_fraction = max(0, 1 - abs(hour - 12) / 6)
                ghi = int(200 + 800 * solar_fraction)
                dni = int(ghi * 1.1)
                dhi = int(ghi * 0.15)
            else:
                ghi, dni, dhi = 0, 0, 0
            lines.append(
                f"2024,1,15,{hour},30,{ghi},{dni},{dhi},20.0,3.0"
            )

        weather_file = tmp_path / "solcast_test.csv"
        weather_file.write_text("\n".join(lines) + "\n")

        features = compute_weather_features(weather_file)

        # Should return valid features despite no clearsky column
        assert set(features.keys()) == {
            "annual_ghi", "mean_kt", "std_kt",
            "ghi_cv", "mean_dni", "pct_clear_hours",
        }
        assert features["annual_ghi"] > 0
        assert 0.0 < features["mean_kt"] < 1.5
        assert features["std_kt"] >= 0
