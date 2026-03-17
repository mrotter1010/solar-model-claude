"""Subhourly resolution correction model for hourly PySAM simulations.

Loads a trained Gradient Boosting model to predict a loss-only correction for
irradiance variability effects that hourly resolution misses (e.g., inverter
clipping during brief high-irradiance periods).

v2 model is trained on 1-min ground station data (19 CONUS stations) with the
target clamped >= 0 at training time (loss-only). The prediction-time clamp is
retained as an additional safety net. Typical corrections range from 0.0% to
~3.3%, with a mean of ~0.78%.
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
_MODEL_PATH = _ARTIFACT_DIR / "subhourly_correction_v2.joblib"
_METADATA_PATH = _ARTIFACT_DIR / "subhourly_correction_v2_metadata.json"

# Solar constant (W/m²) for extraterrestrial irradiance calculation
_SOLAR_CONSTANT = 1361.0

# Minimum extraterrestrial GHI to consider "daytime" (W/m²).
# Avoids sunrise/sunset noise in clearness index calculation.
_DAYTIME_GHI_ET_THRESHOLD = 50.0


@lru_cache(maxsize=1)
def _load_model_and_metadata() -> tuple:
    """Load the trained model and metadata, cached after first call.

    Returns:
        Tuple of (sklearn model, metadata dict).

    Raises:
        FileNotFoundError: If model or metadata files are missing.
    """
    if not _MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Subhourly correction model not found: {_MODEL_PATH}"
        )
    if not _METADATA_PATH.exists():
        raise FileNotFoundError(
            f"Subhourly correction metadata not found: {_METADATA_PATH}"
        )

    model = joblib.load(_MODEL_PATH)
    with open(_METADATA_PATH) as f:
        metadata = json.load(f)

    logger.debug(
        f"Loaded subhourly correction model v{metadata['version']} "
        f"({len(metadata['feature_list'])} features)"
    )
    return model, metadata


# ---------------------------------------------------------------------------
# Solar geometry (Spencer, 1971) for extraterrestrial irradiance
# ---------------------------------------------------------------------------


def _day_of_year(year: int, month: int, day: int) -> int:
    """Compute day of year (1-366)."""
    from datetime import date

    return date(year, month, day).timetuple().tm_yday


def _solar_declination(doy: int) -> float:
    """Solar declination angle in radians (Spencer, 1971)."""
    b = 2 * math.pi * (doy - 1) / 365
    return (
        0.006918
        - 0.399912 * math.cos(b)
        + 0.070257 * math.sin(b)
        - 0.006758 * math.cos(2 * b)
        + 0.000907 * math.sin(2 * b)
        - 0.002697 * math.cos(3 * b)
        + 0.00148 * math.sin(3 * b)
    )


def _sun_earth_distance_factor(doy: int) -> float:
    """Sun-earth distance correction factor (Spencer, 1971)."""
    b = 2 * math.pi * (doy - 1) / 365
    return (
        1.00011
        + 0.034221 * math.cos(b)
        + 0.00128 * math.sin(b)
        + 0.000719 * math.cos(2 * b)
        + 0.000077 * math.sin(2 * b)
    )


def _equation_of_time(doy: int) -> float:
    """Equation of time in minutes (Spencer, 1971)."""
    b = 2 * math.pi * (doy - 1) / 365
    return 229.18 * (
        0.000075
        + 0.001868 * math.cos(b)
        - 0.032077 * math.sin(b)
        - 0.014615 * math.cos(2 * b)
        - 0.04089 * math.sin(2 * b)
    )


def _compute_ghi_extraterrestrial(
    lat: float,
    lon: float,
    tz_offset: float,
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
) -> float:
    """Compute extraterrestrial horizontal irradiance (W/m²).

    Args:
        lat: Latitude in degrees.
        lon: Longitude in degrees.
        tz_offset: Time zone offset from UTC in hours.
        year, month, day, hour, minute: Timestamp components.

    Returns:
        GHI_ET in W/m². Returns 0 if sun is below horizon.
    """
    doy = _day_of_year(year, month, day)
    decl = _solar_declination(doy)
    eot = _equation_of_time(doy)
    dist_factor = _sun_earth_distance_factor(doy)

    solar_time = (
        hour + minute / 60.0 + (eot / 60.0) + ((lon - 15 * tz_offset) / 15.0)
    )
    hour_angle = math.radians((solar_time - 12.0) * 15.0)

    lat_rad = math.radians(lat)
    cos_zenith = math.sin(lat_rad) * math.sin(decl) + math.cos(
        lat_rad
    ) * math.cos(decl) * math.cos(hour_angle)

    if cos_zenith <= 0:
        return 0.0

    return _SOLAR_CONSTANT * dist_factor * cos_zenith


# ---------------------------------------------------------------------------
# Climate classification
# ---------------------------------------------------------------------------


def _classify_climate_type(pct_clear_hours: float) -> str:
    """Classify climate type from percentage of clear hours.

    Thresholds derived from 45-site CONUS training dataset:
        - cloudy: < 30% clear hours (PNW, Great Lakes)
        - clear: >= 45% clear hours (desert Southwest, high plains)
        - variable: 30-45% (Southeast, central US, transition zones)

    Note: climate one-hot features have near-zero importance in the trained
    model (0.0004 and 0.0000), so boundary cases have negligible impact.
    """
    if pct_clear_hours < 30.0:
        return "cloudy"
    if pct_clear_hours >= 45.0:
        return "clear"
    return "variable"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_weather_features(weather_file_path: Path) -> dict[str, float]:
    """Compute weather-derived features from an hourly SAM-format weather CSV.

    Computes 6 irradiance variability metrics used as inputs to the subhourly
    correction model. Works with both NSRDB and Solcast SAM-format files.

    Clearness index (Kt) is computed as GHI / extraterrestrial GHI since
    clearsky GHI columns are not available in standard weather files.

    Args:
        weather_file_path: Path to hourly SAM-format CSV with 2-row metadata
            header. Must contain GHI, DNI, Year, Month, Day, Hour, Minute
            columns.

    Returns:
        Dict with keys: annual_ghi, mean_kt, std_kt, ghi_cv, mean_dni,
        pct_clear_hours.

    Raises:
        FileNotFoundError: If weather file does not exist.
        KeyError: If required columns are missing from the weather file.
    """
    if not weather_file_path.exists():
        raise FileNotFoundError(f"Weather file not found: {weather_file_path}")

    # Read metadata header for latitude, longitude, timezone
    meta = pd.read_csv(weather_file_path, nrows=1)

    lat_col = next(
        (c for c in meta.columns if c.strip().lower() == "latitude"), None
    )
    lon_col = next(
        (c for c in meta.columns if c.strip().lower() == "longitude"), None
    )
    tz_col = next(
        (c for c in meta.columns if "time zone" in c.lower()), None
    )

    lat = float(meta[lat_col].iloc[0]) if lat_col else 0.0
    lon = float(meta[lon_col].iloc[0]) if lon_col else 0.0
    tz_offset = float(meta[tz_col].iloc[0]) if tz_col else 0.0

    # Read weather data (skip 2 metadata rows)
    df = pd.read_csv(weather_file_path, skiprows=2)

    ghi = df["GHI"].values.astype(float)
    dni = df["DNI"].values.astype(float)

    # Compute extraterrestrial GHI for each timestep
    ghi_et = np.array([
        _compute_ghi_extraterrestrial(
            lat, lon, tz_offset,
            int(row["Year"]), int(row["Month"]), int(row["Day"]),
            int(row["Hour"]), int(row["Minute"]),
        )
        for _, row in df.iterrows()
    ])

    # Annual GHI (W/m² per hour -> kWh/m²)
    annual_ghi = ghi.sum() / 1000.0

    # Clearness index: Kt = GHI / GHI_ET (daytime only)
    daytime_mask = ghi_et > _DAYTIME_GHI_ET_THRESHOLD
    kt_valid = np.clip(
        ghi[daytime_mask] / ghi_et[daytime_mask], 0.0, 1.5
    )

    mean_kt = float(np.mean(kt_valid)) if len(kt_valid) > 0 else 0.0
    std_kt = float(np.std(kt_valid)) if len(kt_valid) > 0 else 0.0

    # GHI coefficient of variation (daytime hours where GHI > 0)
    ghi_daytime = ghi[ghi > 0]
    ghi_cv = (
        float(np.std(ghi_daytime) / np.mean(ghi_daytime))
        if len(ghi_daytime) > 0
        else 0.0
    )

    # Mean daytime DNI
    mean_dni = float(np.mean(dni[ghi > 0])) if np.sum(ghi > 0) > 0 else 0.0

    # Percentage of daytime hours where Kt > 0.7 (clear-sky proxy)
    n_daytime = int(np.sum(daytime_mask))
    n_clear = int(np.sum(kt_valid > 0.7))
    pct_clear_hours = (n_clear / n_daytime * 100) if n_daytime > 0 else 0.0

    features = {
        "annual_ghi": round(annual_ghi, 2),
        "mean_kt": round(mean_kt, 4),
        "std_kt": round(std_kt, 4),
        "ghi_cv": round(ghi_cv, 4),
        "mean_dni": round(mean_dni, 2),
        "pct_clear_hours": round(pct_clear_hours, 2),
    }

    logger.debug(
        f"Weather features from {weather_file_path.name}: "
        f"GHI={features['annual_ghi']:.0f} kWh/m², "
        f"Kt={features['mean_kt']:.3f}, "
        f"clear={features['pct_clear_hours']:.0f}%"
    )

    return features


def predict_correction(
    dcac_ratio: float,
    gcr: float,
    racking: str,
    latitude: float,
    longitude: float,
    cf_60min: float,
    weather_features: dict[str, float],
) -> float:
    """Predict subhourly resolution correction percentage (loss-only).

    Assembles a feature vector and runs the trained Gradient Boosting model.
    The raw prediction is clamped to >= 0 (loss-only) per industry practice:
    negative raw predictions are artifacts of satellite data temporal smoothing,
    not real subhourly production gains.

    Args:
        dcac_ratio: DC/AC ratio of the system.
        gcr: Ground coverage ratio.
        racking: "tracker" or "fixed".
        latitude: Site latitude in degrees.
        longitude: Site longitude in degrees.
        cf_60min: Capacity factor from the hourly PySAM simulation (as
            percentage, e.g., 25.0 for 25%).
        weather_features: Dict from compute_weather_features() with keys:
            annual_ghi, mean_kt, std_kt, ghi_cv, mean_dni, pct_clear_hours.

    Returns:
        Clamped resolution_correction_pct (>= 0). Positive means hourly
        simulation overestimates (energy should be removed as a loss).
        Returns 0.0 when the raw model predicts hourly underestimation.
    """
    model, metadata = _load_model_and_metadata()
    feature_list = metadata["feature_list"]

    # Classify climate type from weather features
    climate_type = _classify_climate_type(weather_features["pct_clear_hours"])

    # Build feature value mapping
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

    # Assemble in metadata-specified order
    X = np.array([[feature_values[f] for f in feature_list]])

    raw_correction = float(model.predict(X)[0])

    # Clamp to loss-only (>= 0) per industry practice (DNV HMC).
    # Negative raw predictions are artifacts of satellite data smoothing.
    correction_pct = max(0.0, raw_correction)

    logger.info(
        f"Subhourly correction: {correction_pct:.3f}% "
        f"(raw model: {raw_correction:+.3f}%, "
        f"clamped to loss-only per industry practice, "
        f"model v{metadata['version']})"
    )

    return correction_pct


def get_model_version() -> str:
    """Return the loaded model's version string.

    Returns:
        Version string from the model metadata (e.g., "v1").

    Raises:
        FileNotFoundError: If model metadata file is missing.
    """
    _, metadata = _load_model_and_metadata()
    return metadata["version"]
