"""NSRDB bias correction model for GHI and DNI irradiance data.

Loads trained Gradient Boosting (GHI) and Random Forest (DNI) models to
predict monthly correction factors that reduce systematic bias in NSRDB
satellite irradiance relative to ground-truth measurements.

Correction factors are applied per-month to the SAM weather DataFrame
before PySAM simulation. DHI is recomputed from the irradiance closure
relationship after GHI and DNI are corrected.

Typical correction factors range from 0.75 to 1.0 (NSRDB overestimates
at most locations, especially high-latitude cloudy sites).
"""

import json
import math
from functools import lru_cache
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

_ARTIFACT_DIR = Path(__file__).resolve().parent / "artifacts"
_GHI_MODEL_PATH = _ARTIFACT_DIR / "nsrdb_bias_correction_ghi_v1.joblib"
_DNI_MODEL_PATH = _ARTIFACT_DIR / "nsrdb_bias_correction_dni_v1.joblib"
_METADATA_PATH = _ARTIFACT_DIR / "nsrdb_bias_correction_v1_metadata.json"

# Safety clamps for correction factors
_CF_MIN = 0.5
_CF_MAX = 1.5


@lru_cache(maxsize=1)
def _load_models_and_metadata() -> tuple:
    """Load both trained models and metadata, cached after first call.

    Returns:
        Tuple of (ghi_model, dni_model, metadata_dict).

    Raises:
        FileNotFoundError: If any artifact file is missing.
    """
    for path, label in [
        (_GHI_MODEL_PATH, "GHI model"),
        (_DNI_MODEL_PATH, "DNI model"),
        (_METADATA_PATH, "metadata"),
    ]:
        if not path.exists():
            raise FileNotFoundError(
                f"NSRDB bias correction {label} not found: {path}"
            )

    ghi_model = joblib.load(_GHI_MODEL_PATH)
    dni_model = joblib.load(_DNI_MODEL_PATH)
    with open(_METADATA_PATH) as f:
        metadata = json.load(f)

    logger.debug(
        "Loaded NSRDB bias correction models v%s "
        "(GHI: %s, DNI: %s)",
        metadata["model_version"],
        type(ghi_model).__name__,
        type(dni_model).__name__,
    )
    return ghi_model, dni_model, metadata


def predict_correction_factors(
    latitude: float,
    longitude: float,
    elevation_m: float,
    month: int,
    nsrdb_ghi: float,
    nsrdb_dni: float,
) -> tuple[float, float]:
    """Predict GHI and DNI correction factors for a site-month.

    Args:
        latitude: Site latitude in degrees.
        longitude: Site longitude in degrees.
        elevation_m: Site elevation in meters.
        month: Calendar month (1-12).
        nsrdb_ghi: Monthly mean NSRDB GHI in kWh/m²/day.
        nsrdb_dni: Monthly mean NSRDB DNI in kWh/m²/day.

    Returns:
        Tuple of (ghi_correction_factor, dni_correction_factor).
        Multiply NSRDB values by these factors to get corrected values.
    """
    ghi_model, dni_model, metadata = _load_models_and_metadata()
    threshold = metadata.get("min_correction_threshold", 0.03)

    # Build feature arrays: [latitude, longitude, elevation_m, month, nsrdb_value]
    ghi_features = np.array([[latitude, longitude, elevation_m, month, nsrdb_ghi]])
    dni_features = np.array([[latitude, longitude, elevation_m, month, nsrdb_dni]])

    ghi_cf = float(ghi_model.predict(ghi_features)[0])
    dni_cf = float(dni_model.predict(dni_features)[0])

    # Apply conditional threshold: skip corrections within ±threshold of 1.0
    if abs(ghi_cf - 1.0) < threshold:
        ghi_cf = 1.0
    if abs(dni_cf - 1.0) < threshold:
        dni_cf = 1.0

    # Safety clamp
    ghi_cf = max(_CF_MIN, min(_CF_MAX, ghi_cf))
    dni_cf = max(_CF_MIN, min(_CF_MAX, dni_cf))

    return ghi_cf, dni_cf


def apply_bias_correction(
    weather_df: pd.DataFrame,
    latitude: float,
    longitude: float,
    elevation_m: float,
) -> tuple[pd.DataFrame, dict]:
    """Apply monthly bias correction to SAM-format weather DataFrame.

    Corrects GHI and DNI per-month using trained models, then recomputes
    DHI from the irradiance closure relationship.

    Args:
        weather_df: DataFrame with SAM-format columns (GHI, DNI, DHI,
            Month, etc.). Must have 8760 rows.
        latitude: Site latitude in degrees.
        longitude: Site longitude in degrees.
        elevation_m: Site elevation in meters.

    Returns:
        Tuple of (corrected DataFrame, metadata dict with correction details).
    """
    df = weather_df.copy()

    monthly_ghi_factors: dict[int, float] = {}
    monthly_dni_factors: dict[int, float] = {}

    for month in range(1, 13):
        month_mask = df["Month"] == month
        month_rows = df.loc[month_mask]

        if len(month_rows) == 0:
            continue

        # Compute monthly mean irradiance in kWh/m²/day for model input.
        # Weather data is hourly W/m², so sum W/m² hours and convert:
        # daily mean = sum(hourly W/m²) / n_days / 1000 (W→kW)
        n_days = month_rows["Day"].nunique()
        if n_days == 0:
            n_days = 1

        mean_ghi_kwh = month_rows["GHI"].sum() / n_days / 1000.0
        mean_dni_kwh = month_rows["DNI"].sum() / n_days / 1000.0

        ghi_cf, dni_cf = predict_correction_factors(
            latitude, longitude, elevation_m, month,
            mean_ghi_kwh, mean_dni_kwh,
        )

        monthly_ghi_factors[month] = ghi_cf
        monthly_dni_factors[month] = dni_cf

        # Store original GHI and DNI for DHI recomputation
        orig_ghi = df.loc[month_mask, "GHI"].values.astype(float)
        orig_dni = df.loc[month_mask, "DNI"].values.astype(float)
        orig_dhi = df.loc[month_mask, "DHI"].values.astype(float)

        # Apply correction factors
        new_ghi = orig_ghi * ghi_cf
        new_dni = orig_dni * dni_cf

        # Recompute DHI from irradiance closure: GHI = DNI*cos(z) + DHI
        # Derive cos(zenith) from original data: cos(z) = (GHI - DHI) / DNI
        with np.errstate(divide="ignore", invalid="ignore"):
            cos_zenith = np.where(
                orig_dni > 0,
                (orig_ghi - orig_dhi) / orig_dni,
                0.0,
            )
        # Clamp cos_zenith to physical range [0, 1]
        cos_zenith = np.clip(cos_zenith, 0.0, 1.0)

        new_dhi = new_ghi - new_dni * cos_zenith

        # Physical consistency: DHI >= 0 and DHI <= GHI
        new_dhi = np.clip(new_dhi, 0.0, new_ghi)

        df.loc[month_mask, "GHI"] = np.round(new_ghi).astype(int)
        df.loc[month_mask, "DNI"] = np.round(new_dni).astype(int)
        df.loc[month_mask, "DHI"] = np.round(new_dhi).astype(int)

    # Compute summary metrics
    mean_ghi_cf = (
        sum(monthly_ghi_factors.values()) / len(monthly_ghi_factors)
        if monthly_ghi_factors else 1.0
    )
    mean_dni_cf = (
        sum(monthly_dni_factors.values()) / len(monthly_dni_factors)
        if monthly_dni_factors else 1.0
    )

    _, _, metadata = _load_models_and_metadata()

    correction_metadata = {
        "bias_correction_applied": True,
        "bias_correction_model_version": metadata["model_version"],
        "mean_ghi_correction_factor": round(mean_ghi_cf, 4),
        "mean_dni_correction_factor": round(mean_dni_cf, 4),
        "monthly_ghi_factors": {
            str(k): round(v, 4) for k, v in monthly_ghi_factors.items()
        },
        "monthly_dni_factors": {
            str(k): round(v, 4) for k, v in monthly_dni_factors.items()
        },
    }

    logger.info(
        "NSRDB bias correction applied: mean GHI CF=%.4f, mean DNI CF=%.4f "
        "(model v%s)",
        mean_ghi_cf, mean_dni_cf, metadata["model_version"],
    )

    return df, correction_metadata


def get_model_version() -> str:
    """Return the loaded model's version string.

    Returns:
        Version string from the model metadata (e.g., "1.0.0").

    Raises:
        FileNotFoundError: If model metadata file is missing.
    """
    _, _, metadata = _load_models_and_metadata()
    return metadata["model_version"]
