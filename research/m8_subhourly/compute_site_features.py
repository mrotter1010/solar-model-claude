"""M8 Subhourly: Extract weather-derived site features from 60-min files.

Computes irradiance variability metrics per site for use as ML features.
Since the NSRDB cached files lack Clearsky GHI columns, clearness index Kt
is computed as GHI / extraterrestrial horizontal irradiance (GHI_ET), where
GHI_ET is derived from solar geometry (solar constant, sun-earth distance
correction, and solar zenith angle).

Usage:
    python compute_site_features.py
"""

import math
from pathlib import Path

import numpy as np
import pandas as pd

RESEARCH_DIR = Path(__file__).resolve().parent
CACHE_60MIN = RESEARCH_DIR / "cache" / "60min"
SITES_CSV = RESEARCH_DIR / "research_sites.csv"
OUTPUT_CSV = RESEARCH_DIR / "site_weather_features.csv"

# Solar constant (W/m²)
SOLAR_CONSTANT = 1361.0


def day_of_year(year: int, month: int, day: int) -> int:
    """Compute day of year (1-366)."""
    from datetime import date
    return date(year, month, day).timetuple().tm_yday


def solar_declination(doy: int) -> float:
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


def sun_earth_distance_factor(doy: int) -> float:
    """Sun-earth distance correction factor (Spencer, 1971)."""
    b = 2 * math.pi * (doy - 1) / 365
    return (
        1.00011
        + 0.034221 * math.cos(b)
        + 0.00128 * math.sin(b)
        + 0.000719 * math.cos(2 * b)
        + 0.000077 * math.sin(2 * b)
    )


def equation_of_time(doy: int) -> float:
    """Equation of time in minutes (Spencer, 1971)."""
    b = 2 * math.pi * (doy - 1) / 365
    return 229.18 * (
        0.000075
        + 0.001868 * math.cos(b)
        - 0.032077 * math.sin(b)
        - 0.014615 * math.cos(2 * b)
        - 0.04089 * math.sin(2 * b)
    )


def compute_ghi_extraterrestrial(
    lat: float, lon: float, tz_offset: float,
    year: int, month: int, day: int, hour: int, minute: int,
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
    doy = day_of_year(year, month, day)
    decl = solar_declination(doy)
    eot = equation_of_time(doy)
    dist_factor = sun_earth_distance_factor(doy)

    # Solar time
    solar_time = hour + minute / 60.0 + (eot / 60.0) + ((lon - 15 * tz_offset) / 15.0)
    hour_angle = math.radians((solar_time - 12.0) * 15.0)

    lat_rad = math.radians(lat)

    cos_zenith = (
        math.sin(lat_rad) * math.sin(decl)
        + math.cos(lat_rad) * math.cos(decl) * math.cos(hour_angle)
    )

    if cos_zenith <= 0:
        return 0.0

    return SOLAR_CONSTANT * dist_factor * cos_zenith


def extract_site_features(weather_file: Path, lat: float, lon: float) -> dict:
    """Extract irradiance variability features from a 60-min weather file.

    Args:
        weather_file: Path to PySAM-format CSV (2 header rows).
        lat: Site latitude.
        lon: Site longitude.

    Returns:
        Dict with annual_ghi, mean_kt, std_kt, ghi_cv, mean_dni, pct_clear_hours.
    """
    # Read metadata header for timezone
    meta = pd.read_csv(weather_file, nrows=1)
    tz_col = next(
        (c for c in meta.columns if "time zone" in c.lower()),
        None,
    )
    tz_offset = float(meta[tz_col].iloc[0]) if tz_col else 0.0

    # Read weather data (skip 2 header rows)
    df = pd.read_csv(weather_file, skiprows=2)

    # Compute extraterrestrial GHI for each hour
    ghi_et = np.array([
        compute_ghi_extraterrestrial(
            lat, lon, tz_offset,
            int(row["Year"]), int(row["Month"]), int(row["Day"]),
            int(row["Hour"]), int(row["Minute"]),
        )
        for _, row in df.iterrows()
    ])

    ghi = df["GHI"].values.astype(float)
    dni = df["DNI"].values.astype(float)

    # Annual GHI (W/m² per hour -> kWh/m² for the year)
    annual_ghi = ghi.sum() / 1000.0

    # Clearness index Kt = GHI / GHI_ET (daytime only: GHI_ET > 50 W/m²
    # to avoid sunrise/sunset noise)
    daytime_mask = ghi_et > 50.0
    kt = np.where(daytime_mask, ghi / ghi_et, np.nan)
    # Clamp Kt to [0, 1.5] to handle edge cases
    kt_valid = kt[daytime_mask]
    kt_valid = np.clip(kt_valid, 0.0, 1.5)

    mean_kt = float(np.nanmean(kt_valid))
    std_kt = float(np.nanstd(kt_valid))

    # GHI coefficient of variation (daytime hours where GHI > 0)
    ghi_daytime = ghi[ghi > 0]
    ghi_cv = float(np.std(ghi_daytime) / np.mean(ghi_daytime)) if len(ghi_daytime) > 0 else 0.0

    # Mean daytime DNI
    mean_dni = float(np.mean(dni[ghi > 0])) if np.sum(ghi > 0) > 0 else 0.0

    # Percentage of daytime hours where Kt > 0.7 (clear-sky fraction)
    n_daytime = int(np.sum(daytime_mask))
    n_clear = int(np.sum(kt_valid > 0.7))
    pct_clear_hours = (n_clear / n_daytime * 100) if n_daytime > 0 else 0.0

    return {
        "annual_ghi": round(annual_ghi, 2),
        "mean_kt": round(mean_kt, 4),
        "std_kt": round(std_kt, 4),
        "ghi_cv": round(ghi_cv, 4),
        "mean_dni": round(mean_dni, 2),
        "pct_clear_hours": round(pct_clear_hours, 2),
    }


def main() -> None:
    """Extract features for all sites and save to CSV."""
    sites = pd.read_csv(SITES_CSV)
    print(f"Loaded {len(sites)} sites from {SITES_CSV.name}")

    results = []
    for _, site in sites.iterrows():
        name = site["site_name"]
        lat = site["latitude"]
        lon = site["longitude"]
        weather_file = CACHE_60MIN / f"{name}_2022_60min.csv"

        if not weather_file.exists():
            print(f"  WARNING: Missing weather file for {name}")
            continue

        features = extract_site_features(weather_file, lat, lon)
        features["site_name"] = name
        results.append(features)
        print(f"  {name}: GHI={features['annual_ghi']:.0f} kWh/m², Kt={features['mean_kt']:.3f}, clear={features['pct_clear_hours']:.0f}%")

    df = pd.DataFrame(results)
    cols = ["site_name", "annual_ghi", "mean_kt", "std_kt", "ghi_cv", "mean_dni", "pct_clear_hours"]
    df = df[cols]
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nSaved {len(df)} site features to {OUTPUT_CSV.name}")


if __name__ == "__main__":
    main()
