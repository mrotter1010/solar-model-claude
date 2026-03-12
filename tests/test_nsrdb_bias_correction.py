"""Tests for NSRDB bias correction model.

Tests model loading, prediction, threshold/clamping logic,
and full apply_bias_correction on synthetic weather DataFrames.
"""

import json
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from src.models.nsrdb_bias_correction import (
    _CF_MAX,
    _CF_MIN,
    _load_models_and_metadata,
    apply_bias_correction,
    get_model_version,
    predict_correction_factors,
)

# Artifact paths for existence checks
ARTIFACT_DIR = Path("src/models/artifacts")
GHI_MODEL_PATH = ARTIFACT_DIR / "nsrdb_bias_correction_ghi_v1.joblib"
DNI_MODEL_PATH = ARTIFACT_DIR / "nsrdb_bias_correction_dni_v1.joblib"
METADATA_PATH = ARTIFACT_DIR / "nsrdb_bias_correction_v1_metadata.json"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def synthetic_weather_df() -> pd.DataFrame:
    """Create a synthetic 8760-row SAM-format weather DataFrame.

    Constant daytime irradiance: GHI=500, DNI=600, DHI=100.
    Implied cos(zenith) = (500-100)/600 = 0.667.
    """
    hours_per_month = [744, 672, 744, 720, 744, 720, 744, 744, 720, 744, 720, 744]
    rows = []
    for month_idx, n_hours in enumerate(hours_per_month):
        month = month_idx + 1
        for h in range(n_hours):
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
                "Snow Depth": 0,
            })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# a. Model loading
# ---------------------------------------------------------------------------


class TestModelLoading:
    """Test that model artifacts exist and load correctly."""

    def test_artifacts_exist(self) -> None:
        """Model artifact files must exist on disk."""
        assert GHI_MODEL_PATH.exists(), f"GHI model missing: {GHI_MODEL_PATH}"
        assert DNI_MODEL_PATH.exists(), f"DNI model missing: {DNI_MODEL_PATH}"
        assert METADATA_PATH.exists(), f"Metadata missing: {METADATA_PATH}"

    def test_load_models_returns_tuple(self) -> None:
        """_load_models_and_metadata returns (ghi_model, dni_model, metadata)."""
        result = _load_models_and_metadata()
        assert len(result) == 3
        ghi_model, dni_model, metadata = result
        assert hasattr(ghi_model, "predict")
        assert hasattr(dni_model, "predict")
        assert isinstance(metadata, dict)

    def test_metadata_has_required_fields(self) -> None:
        """Metadata JSON contains all expected keys."""
        _, _, metadata = _load_models_and_metadata()
        required = [
            "model_version",
            "ghi_model_type",
            "dni_model_type",
            "features",
            "n_training_stations",
            "min_correction_threshold",
        ]
        for key in required:
            assert key in metadata, f"Metadata missing key: {key}"

    def test_get_model_version(self) -> None:
        """get_model_version returns a non-empty string."""
        version = get_model_version()
        assert isinstance(version, str)
        assert len(version) > 0
        assert version == "1.0.0"

    def test_models_predict_array(self) -> None:
        """Both models accept feature arrays and return predictions."""
        ghi_model, dni_model, _ = _load_models_and_metadata()
        features = np.array([[35.0, -100.0, 300.0, 6, 5.0]])
        ghi_pred = ghi_model.predict(features)
        dni_pred = dni_model.predict(features)
        assert ghi_pred.shape == (1,)
        assert dni_pred.shape == (1,)
        assert np.isfinite(ghi_pred[0])
        assert np.isfinite(dni_pred[0])


# ---------------------------------------------------------------------------
# b. predict_correction_factors — known inputs
# ---------------------------------------------------------------------------


class TestPredictCorrectionFactors:
    """Test predict_correction_factors with various inputs."""

    def test_phoenix_returns_reasonable_factors(self) -> None:
        """Phoenix AZ should get correction factors in [0.5, 1.5]."""
        ghi_cf, dni_cf = predict_correction_factors(
            latitude=33.45, longitude=-112.07, elevation_m=340,
            month=7, nsrdb_ghi=8.5, nsrdb_dni=10.0,
        )
        assert _CF_MIN <= ghi_cf <= _CF_MAX
        assert _CF_MIN <= dni_cf <= _CF_MAX

    def test_seattle_returns_reasonable_factors(self) -> None:
        """Seattle WA (high-latitude, cloudy) should get larger corrections."""
        ghi_cf, dni_cf = predict_correction_factors(
            latitude=47.61, longitude=-122.33, elevation_m=50,
            month=1, nsrdb_ghi=1.2, nsrdb_dni=1.5,
        )
        assert _CF_MIN <= ghi_cf <= _CF_MAX
        assert _CF_MIN <= dni_cf <= _CF_MAX
        # Seattle winter should have GHI correction < 1.0 (NSRDB overestimates)
        assert ghi_cf < 1.0

    def test_bondville_returns_reasonable_factors(self) -> None:
        """Bondville IL should get moderate corrections."""
        ghi_cf, dni_cf = predict_correction_factors(
            latitude=40.05, longitude=-88.37, elevation_m=213,
            month=6, nsrdb_ghi=6.0, nsrdb_dni=6.5,
        )
        assert _CF_MIN <= ghi_cf <= _CF_MAX
        assert _CF_MIN <= dni_cf <= _CF_MAX

    def test_all_months_return_valid_factors(self) -> None:
        """Every month (1-12) produces valid correction factors."""
        for month in range(1, 13):
            ghi_cf, dni_cf = predict_correction_factors(
                latitude=40.0, longitude=-100.0, elevation_m=500,
                month=month, nsrdb_ghi=5.0, nsrdb_dni=6.0,
            )
            assert _CF_MIN <= ghi_cf <= _CF_MAX, f"GHI CF out of range for month {month}"
            assert _CF_MIN <= dni_cf <= _CF_MAX, f"DNI CF out of range for month {month}"


# ---------------------------------------------------------------------------
# c. Threshold logic — ±3% returns 1.0
# ---------------------------------------------------------------------------


class TestThresholdLogic:
    """Test the min_correction_threshold conditional application."""

    def test_factor_near_one_returns_one(self) -> None:
        """A raw prediction within ±3% of 1.0 should be set to 1.0.

        Phoenix July GHI has raw prediction ~0.978, within threshold.
        """
        ghi_cf, _ = predict_correction_factors(
            latitude=33.45, longitude=-112.07, elevation_m=340,
            month=7, nsrdb_ghi=8.5, nsrdb_dni=10.0,
        )
        # Phoenix July GHI correction is ~0.978, within ±0.03 of 1.0
        assert ghi_cf == 1.0, (
            f"Expected 1.0 (threshold skip), got {ghi_cf}"
        )

    def test_factor_far_from_one_not_thresholded(self) -> None:
        """Seattle winter correction should be far from 1.0 and not thresholded."""
        ghi_cf, _ = predict_correction_factors(
            latitude=47.61, longitude=-122.33, elevation_m=50,
            month=1, nsrdb_ghi=1.2, nsrdb_dni=1.5,
        )
        assert ghi_cf != 1.0, (
            f"Expected non-1.0 correction for Seattle winter, got {ghi_cf}"
        )
        assert ghi_cf < 0.97  # Should be well below threshold


# ---------------------------------------------------------------------------
# d. apply_bias_correction with synthetic 8760 DataFrame
# ---------------------------------------------------------------------------


class TestApplyBiasCorrection:
    """Test full apply_bias_correction on synthetic weather data."""

    def test_returns_corrected_df_and_metadata(
        self, synthetic_weather_df: pd.DataFrame,
    ) -> None:
        """apply_bias_correction returns a DataFrame and metadata dict."""
        corrected, meta = apply_bias_correction(
            synthetic_weather_df,
            latitude=40.0, longitude=-100.0, elevation_m=500,
        )
        assert isinstance(corrected, pd.DataFrame)
        assert isinstance(meta, dict)
        assert len(corrected) == 8760

    def test_ghi_values_changed(
        self, synthetic_weather_df: pd.DataFrame,
    ) -> None:
        """GHI values should be modified by bias correction."""
        corrected, _ = apply_bias_correction(
            synthetic_weather_df,
            latitude=40.0, longitude=-100.0, elevation_m=500,
        )
        # At least some months should have correction != 1.0
        assert not (corrected["GHI"] == 500).all(), (
            "GHI values unchanged — correction not applied"
        )

    def test_physical_consistency(
        self, synthetic_weather_df: pd.DataFrame,
    ) -> None:
        """All corrected irradiance values must be physically consistent."""
        corrected, _ = apply_bias_correction(
            synthetic_weather_df,
            latitude=40.0, longitude=-100.0, elevation_m=500,
        )
        # GHI >= 0
        assert (corrected["GHI"] >= 0).all(), "Negative GHI found"
        # DNI >= 0
        assert (corrected["DNI"] >= 0).all(), "Negative DNI found"
        # DHI >= 0
        assert (corrected["DHI"] >= 0).all(), "Negative DHI found"
        # GHI >= DHI
        assert (corrected["GHI"] >= corrected["DHI"]).all(), "DHI > GHI found"

    def test_original_df_not_modified(
        self, synthetic_weather_df: pd.DataFrame,
    ) -> None:
        """apply_bias_correction must not modify the input DataFrame."""
        original_ghi = synthetic_weather_df["GHI"].copy()
        apply_bias_correction(
            synthetic_weather_df,
            latitude=40.0, longitude=-100.0, elevation_m=500,
        )
        pd.testing.assert_series_equal(synthetic_weather_df["GHI"], original_ghi)

    def test_metadata_keys(
        self, synthetic_weather_df: pd.DataFrame,
    ) -> None:
        """Metadata dict has all expected keys."""
        _, meta = apply_bias_correction(
            synthetic_weather_df,
            latitude=40.0, longitude=-100.0, elevation_m=500,
        )
        assert meta["bias_correction_applied"] is True
        assert "bias_correction_model_version" in meta
        assert "mean_ghi_correction_factor" in meta
        assert "mean_dni_correction_factor" in meta
        assert "monthly_ghi_factors" in meta
        assert "monthly_dni_factors" in meta
        assert len(meta["monthly_ghi_factors"]) == 12
        assert len(meta["monthly_dni_factors"]) == 12

    def test_preserves_non_irradiance_columns(
        self, synthetic_weather_df: pd.DataFrame,
    ) -> None:
        """Non-irradiance columns (Temperature, Wind Speed, etc.) are unchanged."""
        corrected, _ = apply_bias_correction(
            synthetic_weather_df,
            latitude=40.0, longitude=-100.0, elevation_m=500,
        )
        pd.testing.assert_series_equal(
            corrected["Temperature"],
            synthetic_weather_df["Temperature"],
            check_names=False,
        )
        pd.testing.assert_series_equal(
            corrected["Wind Speed"],
            synthetic_weather_df["Wind Speed"],
            check_names=False,
        )


# ---------------------------------------------------------------------------
# e. Solcast flag test
# ---------------------------------------------------------------------------


class TestSolcastExclusion:
    """Test that bias correction is flagged for NSRDB only, not Solcast."""

    def test_metadata_indicates_nsrdb_model(self) -> None:
        """Metadata notes field should reference NSRDB specifically.

        The pipeline integration (not in this module) should check
        data_source before calling apply_bias_correction. This test
        verifies the model is trained on NSRDB data only.
        """
        _, _, metadata = _load_models_and_metadata()
        notes = metadata.get("notes", "")
        assert "NSRDB" in notes
        # Model should not claim to correct Solcast data
        assert "Solcast" not in notes


# ---------------------------------------------------------------------------
# f. Safety clamping
# ---------------------------------------------------------------------------


class TestSafetyClamping:
    """Test that extreme correction factors are clamped to [0.5, 1.5]."""

    def test_low_cf_clamped_to_minimum(self) -> None:
        """If model predicts CF=0.3, it should be clamped to 0.5."""
        with patch(
            "src.models.nsrdb_bias_correction._load_models_and_metadata"
        ) as mock_load:
            # Create mock models that return extreme values
            class MockModel:
                def predict(self, X: np.ndarray) -> np.ndarray:
                    return np.array([0.3])

            mock_load.return_value = (
                MockModel(),
                MockModel(),
                {"model_version": "test", "min_correction_threshold": 0.03},
            )

            ghi_cf, dni_cf = predict_correction_factors(
                latitude=0, longitude=0, elevation_m=0,
                month=1, nsrdb_ghi=1.0, nsrdb_dni=1.0,
            )
            assert ghi_cf == _CF_MIN
            assert dni_cf == _CF_MIN

    def test_high_cf_clamped_to_maximum(self) -> None:
        """If model predicts CF=2.0, it should be clamped to 1.5."""
        with patch(
            "src.models.nsrdb_bias_correction._load_models_and_metadata"
        ) as mock_load:
            class MockModel:
                def predict(self, X: np.ndarray) -> np.ndarray:
                    return np.array([2.0])

            mock_load.return_value = (
                MockModel(),
                MockModel(),
                {"model_version": "test", "min_correction_threshold": 0.03},
            )

            ghi_cf, dni_cf = predict_correction_factors(
                latitude=0, longitude=0, elevation_m=0,
                month=1, nsrdb_ghi=1.0, nsrdb_dni=1.0,
            )
            assert ghi_cf == _CF_MAX
            assert dni_cf == _CF_MAX

    def test_normal_cf_not_clamped(self) -> None:
        """A correction factor of 0.85 should pass through unclamped."""
        with patch(
            "src.models.nsrdb_bias_correction._load_models_and_metadata"
        ) as mock_load:
            class MockModel:
                def predict(self, X: np.ndarray) -> np.ndarray:
                    return np.array([0.85])

            mock_load.return_value = (
                MockModel(),
                MockModel(),
                {"model_version": "test", "min_correction_threshold": 0.03},
            )

            ghi_cf, dni_cf = predict_correction_factors(
                latitude=0, longitude=0, elevation_m=0,
                month=1, nsrdb_ghi=1.0, nsrdb_dni=1.0,
            )
            assert ghi_cf == 0.85
            assert dni_cf == 0.85
