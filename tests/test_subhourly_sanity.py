"""WP7 Sanity checks for the subhourly correction model.

Tests model behavior properties using the trained model directly:
  1. Correction increases (or stays flat) with DC/AC ratio
  2. Cloudy sites get higher correction than clear sites
  3. Low DC/AC produces near-zero correction after clamping
  4. Correction is never negative (20 random configs)
"""

import json
from pathlib import Path

import numpy as np
import pytest

from src.models.subhourly_correction import (
    _classify_climate_type,
    _load_model_and_metadata,
    predict_correction,
)

METADATA_PATH = Path("src/models/artifacts/subhourly_correction_v1_metadata.json")
VALIDATION_DIR = Path("outputs/test_results/wp7_validation")


# ---------------------------------------------------------------------------
# Weather feature sets
# ---------------------------------------------------------------------------

# Clear desert (Phoenix-like)
CLEAR_WEATHER = {
    "annual_ghi": 2142.52,
    "mean_kt": 0.6547,
    "std_kt": 0.2548,
    "ghi_cv": 0.6984,
    "mean_dni": 584.82,
    "pct_clear_hours": 53.34,
}

# Cloudy PNW (Seattle-like)
CLOUDY_WEATHER = {
    "annual_ghi": 1309.56,
    "mean_kt": 0.4619,
    "std_kt": 0.2643,
    "ghi_cv": 0.9231,
    "mean_dni": 307.63,
    "pct_clear_hours": 24.01,
}

# Variable Southeast (Atlanta-like)
VARIABLE_WEATHER = {
    "annual_ghi": 1650.0,
    "mean_kt": 0.5200,
    "std_kt": 0.2400,
    "ghi_cv": 0.8000,
    "mean_dni": 400.0,
    "pct_clear_hours": 35.0,
}


def _raw_predict(
    dcac_ratio: float,
    gcr: float,
    racking: str,
    latitude: float,
    longitude: float,
    cf_60min: float,
    weather_features: dict[str, float],
) -> float:
    """Get the raw (unclamped) model prediction.

    Bypasses the clamp in predict_correction() to inspect raw model output.
    """
    model, metadata = _load_model_and_metadata()
    feature_list = metadata["feature_list"]

    climate_type = _classify_climate_type(weather_features["pct_clear_hours"])

    feature_values: dict[str, float] = {
        "dcac_ratio": dcac_ratio,
        "gcr": gcr,
        "racking": 1 if racking.lower() == "tracker" else 0,
        "latitude": latitude,
        "longitude": longitude,
        "cf_60min": cf_60min,
        "annual_ghi": weather_features["annual_ghi"],
        "mean_kt": weather_features["mean_kt"],
        "std_kt": weather_features["std_kt"],
        "ghi_cv": weather_features["ghi_cv"],
        "mean_dni": weather_features["mean_dni"],
        "pct_clear_hours": weather_features["pct_clear_hours"],
        "climate_cloudy": 1 if climate_type == "cloudy" else 0,
        "climate_variable": 1 if climate_type == "variable" else 0,
    }

    X = np.array([[feature_values[f] for f in feature_list]])
    return float(model.predict(X)[0])


# ---------------------------------------------------------------------------
# Sanity check tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not METADATA_PATH.exists(),
    reason="Subhourly model artifacts not found",
)
class TestCorrectionMonotonicity:
    """Correction should increase (or stay flat) with DC/AC ratio.

    For a fixed site config, predicts correction at DC/AC 1.20, 1.40, 1.60.
    Raw predictions should trend less negative (toward positive) as DC/AC
    increases. After clamping, correction at 1.60 >= 1.40 >= 1.20.
    """

    # Fixed config: cloudy site, fixed racking (where positive corrections occur)
    BASE_CONFIG = {
        "gcr": 0.40,
        "racking": "fixed",
        "latitude": 47.61,
        "longitude": -122.33,
        "weather_features": CLOUDY_WEATHER,
    }

    # CF varies with DC/AC (higher DC/AC → more clipping → lower CF)
    DCAC_CONFIGS = [
        {"dcac": 1.20, "cf": 14.5},
        {"dcac": 1.40, "cf": 13.5},
        {"dcac": 1.60, "cf": 13.2},
    ]

    def test_raw_predictions_trend_upward_with_dcac(self) -> None:
        """Raw model predictions should become less negative as DC/AC increases."""
        raw_preds = []
        for cfg in self.DCAC_CONFIGS:
            raw = _raw_predict(
                dcac_ratio=cfg["dcac"],
                cf_60min=cfg["cf"],
                **self.BASE_CONFIG,
            )
            raw_preds.append(raw)

        # raw_preds[2] (DC/AC 1.60) should be >= raw_preds[0] (DC/AC 1.20)
        assert raw_preds[2] >= raw_preds[0], (
            f"Raw prediction at DC/AC 1.60 ({raw_preds[2]:+.4f}%) should be "
            f">= DC/AC 1.20 ({raw_preds[0]:+.4f}%)"
        )

    def test_clamped_monotonically_increasing_with_dcac(self) -> None:
        """After clamping, correction at 1.60 >= 1.40 >= 1.20."""
        clamped = []
        for cfg in self.DCAC_CONFIGS:
            corr = predict_correction(
                dcac_ratio=cfg["dcac"],
                cf_60min=cfg["cf"],
                **self.BASE_CONFIG,
            )
            clamped.append(corr)

        assert clamped[2] >= clamped[1] >= clamped[0], (
            f"Clamped corrections should be monotonically increasing: "
            f"DC/AC 1.20={clamped[0]:.4f}%, "
            f"DC/AC 1.40={clamped[1]:.4f}%, "
            f"DC/AC 1.60={clamped[2]:.4f}%"
        )

    def test_tracker_dcac_trend(self) -> None:
        """Same monotonicity check for tracker racking."""
        tracker_config = {
            "gcr": 0.40,
            "racking": "tracker",
            "latitude": 35.0,
            "longitude": -100.0,
            "weather_features": VARIABLE_WEATHER,
        }

        clamped = []
        for cfg in self.DCAC_CONFIGS:
            corr = predict_correction(
                dcac_ratio=cfg["dcac"],
                cf_60min=cfg["cf"],
                **tracker_config,
            )
            clamped.append(corr)

        assert clamped[2] >= clamped[1] >= clamped[0], (
            f"Tracker clamped corrections should be monotonically increasing: "
            f"DC/AC 1.20={clamped[0]:.4f}%, "
            f"DC/AC 1.40={clamped[1]:.4f}%, "
            f"DC/AC 1.60={clamped[2]:.4f}%"
        )

    def test_write_monotonicity_artifact(self, test_results_dir: Path) -> None:
        """Save monotonicity results to test_results for manual inspection."""
        results = []
        for racking, weather, label in [
            ("fixed", CLOUDY_WEATHER, "Cloudy fixed (Seattle)"),
            ("tracker", VARIABLE_WEATHER, "Variable tracker"),
            ("fixed", CLEAR_WEATHER, "Clear fixed (Phoenix)"),
        ]:
            config = {
                "gcr": 0.40,
                "racking": racking,
                "latitude": 35.0,
                "longitude": -100.0,
                "weather_features": weather,
            }
            for cfg in self.DCAC_CONFIGS:
                raw = _raw_predict(
                    dcac_ratio=cfg["dcac"],
                    cf_60min=cfg["cf"],
                    **config,
                )
                clamped = predict_correction(
                    dcac_ratio=cfg["dcac"],
                    cf_60min=cfg["cf"],
                    **config,
                )
                results.append({
                    "config": label,
                    "dcac_ratio": cfg["dcac"],
                    "cf_60min": cfg["cf"],
                    "raw_correction_pct": round(raw, 4),
                    "clamped_correction_pct": round(clamped, 4),
                })

        VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
        out = VALIDATION_DIR / "sanity_monotonicity.json"
        out.write_text(json.dumps(results, indent=2))
        assert out.exists()


@pytest.mark.skipif(
    not METADATA_PATH.exists(),
    reason="Subhourly model artifacts not found",
)
class TestCloudyVsClear:
    """Cloudy sites should get higher correction than clear sites.

    For a fixed system config (DC/AC 1.50, tracker), compare a cloudy site
    (high variability) vs a clear site (low variability). The cloudy site
    should have a higher clamped correction.
    """

    SYSTEM_CONFIG = {
        "dcac_ratio": 1.50,
        "gcr": 0.40,
        "racking": "fixed",
        "latitude": 40.0,
        "longitude": -100.0,
        "cf_60min": 16.0,
    }

    def test_cloudy_correction_geq_clear(self) -> None:
        """Cloudy site correction >= clear site correction (clamped)."""
        cloudy_corr = predict_correction(
            weather_features=CLOUDY_WEATHER, **self.SYSTEM_CONFIG
        )
        clear_corr = predict_correction(
            weather_features=CLEAR_WEATHER, **self.SYSTEM_CONFIG
        )

        assert cloudy_corr >= clear_corr, (
            f"Cloudy correction ({cloudy_corr:.4f}%) should be >= "
            f"clear correction ({clear_corr:.4f}%)"
        )

    def test_cloudy_raw_higher_than_clear_raw(self) -> None:
        """Raw (unclamped) cloudy prediction should be higher than clear."""
        cloudy_raw = _raw_predict(
            weather_features=CLOUDY_WEATHER, **self.SYSTEM_CONFIG
        )
        clear_raw = _raw_predict(
            weather_features=CLEAR_WEATHER, **self.SYSTEM_CONFIG
        )

        assert cloudy_raw > clear_raw, (
            f"Cloudy raw ({cloudy_raw:+.4f}%) should exceed "
            f"clear raw ({clear_raw:+.4f}%)"
        )

    def test_write_climate_comparison_artifact(
        self, test_results_dir: Path
    ) -> None:
        """Save cloudy vs clear comparison to test_results."""
        results = {}
        for climate_label, weather in [
            ("clear_desert", CLEAR_WEATHER),
            ("variable_southeast", VARIABLE_WEATHER),
            ("cloudy_pnw", CLOUDY_WEATHER),
        ]:
            raw = _raw_predict(
                weather_features=weather, **self.SYSTEM_CONFIG
            )
            clamped = predict_correction(
                weather_features=weather, **self.SYSTEM_CONFIG
            )
            results[climate_label] = {
                "raw_correction_pct": round(raw, 4),
                "clamped_correction_pct": round(clamped, 4),
                "pct_clear_hours": weather["pct_clear_hours"],
                "ghi_cv": weather["ghi_cv"],
            }

        VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
        out = VALIDATION_DIR / "sanity_cloudy_vs_clear.json"
        out.write_text(json.dumps(results, indent=2))
        assert out.exists()


@pytest.mark.skipif(
    not METADATA_PATH.exists(),
    reason="Subhourly model artifacts not found",
)
class TestLowDCAC:
    """Low DC/AC (1.10) should produce near-zero correction after clamping.

    DC/AC 1.10 is below the training range (1.20-1.60). The model should
    predict a strongly negative raw value that clamps to 0.0 or very near.
    """

    def test_dcac_110_near_zero(self) -> None:
        """DC/AC 1.10 correction should be very close to 0 after clamping."""
        correction = predict_correction(
            dcac_ratio=1.10,
            gcr=0.40,
            racking="tracker",
            latitude=33.45,
            longitude=-112.07,
            cf_60min=25.0,
            weather_features=CLEAR_WEATHER,
        )

        assert correction == pytest.approx(0.0, abs=0.05), (
            f"DC/AC 1.10 correction {correction:.4f}% should be ~0.0"
        )

    def test_dcac_110_fixed_near_zero(self) -> None:
        """DC/AC 1.10 with fixed racking should also be near zero."""
        correction = predict_correction(
            dcac_ratio=1.10,
            gcr=0.40,
            racking="fixed",
            latitude=33.45,
            longitude=-112.07,
            cf_60min=20.0,
            weather_features=CLEAR_WEATHER,
        )

        assert correction == pytest.approx(0.0, abs=0.1), (
            f"DC/AC 1.10 fixed correction {correction:.4f}% should be ~0.0"
        )

    def test_dcac_110_cloudy_near_zero(self) -> None:
        """DC/AC 1.10 even at a cloudy site should produce near-zero correction."""
        correction = predict_correction(
            dcac_ratio=1.10,
            gcr=0.40,
            racking="fixed",
            latitude=47.61,
            longitude=-122.33,
            cf_60min=13.0,
            weather_features=CLOUDY_WEATHER,
        )

        # Even cloudy sites at DC/AC 1.10 shouldn't have significant correction
        assert correction < 0.15, (
            f"DC/AC 1.10 cloudy correction {correction:.4f}% unexpectedly high"
        )


@pytest.mark.skipif(
    not METADATA_PATH.exists(),
    reason="Subhourly model artifacts not found",
)
class TestNeverNegative:
    """Clamped correction must be >= 0 for all configs.

    Tests 20 random configurations spanning the full parameter space.
    """

    def test_20_random_configs_all_non_negative(self) -> None:
        """All 20 random configs must produce clamped correction >= 0."""
        rng = np.random.default_rng(seed=42)

        failures = []
        for i in range(20):
            dcac = rng.uniform(1.10, 1.70)
            gcr = rng.uniform(0.25, 0.50)
            racking = "tracker" if rng.random() > 0.5 else "fixed"
            lat = rng.uniform(25.0, 48.0)
            lon = rng.uniform(-125.0, -70.0)
            cf = rng.uniform(10.0, 35.0)

            # Random weather features within realistic ranges
            weather = {
                "annual_ghi": rng.uniform(1100.0, 2400.0),
                "mean_kt": rng.uniform(0.35, 0.75),
                "std_kt": rng.uniform(0.15, 0.35),
                "ghi_cv": rng.uniform(0.55, 1.10),
                "mean_dni": rng.uniform(200.0, 700.0),
                "pct_clear_hours": rng.uniform(15.0, 65.0),
            }

            correction = predict_correction(
                dcac_ratio=dcac,
                gcr=gcr,
                racking=racking,
                latitude=lat,
                longitude=lon,
                cf_60min=cf,
                weather_features=weather,
            )

            if correction < 0:
                failures.append({
                    "config_idx": i,
                    "dcac": round(dcac, 3),
                    "gcr": round(gcr, 3),
                    "racking": racking,
                    "lat": round(lat, 2),
                    "lon": round(lon, 2),
                    "cf": round(cf, 2),
                    "correction": round(correction, 6),
                })

        assert len(failures) == 0, (
            f"{len(failures)} of 20 configs produced negative correction: "
            f"{failures}"
        )

    def test_edge_cases_non_negative(self) -> None:
        """Extreme parameter values should still produce non-negative corrections."""
        edge_cases = [
            # Very high DC/AC, very cloudy
            {"dcac": 1.70, "gcr": 0.50, "racking": "fixed",
             "lat": 48.0, "lon": -122.0, "cf": 10.0,
             "weather": CLOUDY_WEATHER},
            # Very low DC/AC, very clear
            {"dcac": 1.10, "gcr": 0.25, "racking": "tracker",
             "lat": 33.0, "lon": -112.0, "cf": 35.0,
             "weather": CLEAR_WEATHER},
            # Mid-range, variable climate
            {"dcac": 1.40, "gcr": 0.35, "racking": "fixed",
             "lat": 35.0, "lon": -85.0, "cf": 18.0,
             "weather": VARIABLE_WEATHER},
        ]

        for case in edge_cases:
            correction = predict_correction(
                dcac_ratio=case["dcac"],
                gcr=case["gcr"],
                racking=case["racking"],
                latitude=case["lat"],
                longitude=case["lon"],
                cf_60min=case["cf"],
                weather_features=case["weather"],
            )
            assert correction >= 0.0, (
                f"Negative correction {correction:.4f}% for {case}"
            )

    def test_write_random_configs_artifact(
        self, test_results_dir: Path
    ) -> None:
        """Save 20 random config results to test_results."""
        rng = np.random.default_rng(seed=42)
        results = []

        for i in range(20):
            dcac = rng.uniform(1.10, 1.70)
            gcr = rng.uniform(0.25, 0.50)
            racking = "tracker" if rng.random() > 0.5 else "fixed"
            lat = rng.uniform(25.0, 48.0)
            lon = rng.uniform(-125.0, -70.0)
            cf = rng.uniform(10.0, 35.0)

            weather = {
                "annual_ghi": rng.uniform(1100.0, 2400.0),
                "mean_kt": rng.uniform(0.35, 0.75),
                "std_kt": rng.uniform(0.15, 0.35),
                "ghi_cv": rng.uniform(0.55, 1.10),
                "mean_dni": rng.uniform(200.0, 700.0),
                "pct_clear_hours": rng.uniform(15.0, 65.0),
            }

            raw = _raw_predict(
                dcac_ratio=dcac, gcr=gcr, racking=racking,
                latitude=lat, longitude=lon, cf_60min=cf,
                weather_features=weather,
            )
            clamped = predict_correction(
                dcac_ratio=dcac, gcr=gcr, racking=racking,
                latitude=lat, longitude=lon, cf_60min=cf,
                weather_features=weather,
            )

            results.append({
                "idx": i,
                "dcac": round(dcac, 3),
                "gcr": round(gcr, 3),
                "racking": racking,
                "lat": round(lat, 2),
                "lon": round(lon, 2),
                "cf": round(cf, 2),
                "raw_correction_pct": round(raw, 4),
                "clamped_correction_pct": round(clamped, 4),
            })

        VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
        out = VALIDATION_DIR / "sanity_20_random_configs.json"
        out.write_text(json.dumps(results, indent=2))
        assert out.exists()
