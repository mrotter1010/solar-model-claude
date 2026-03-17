"""M11 Subhourly: Compute paired deltas and training features from 1-min ground data.

Pairs 1-min and 60-min PySAM batch results by config, computes subhourly
clipping loss percentage, and extracts weather variability features from
60-min SAM weather files. Produces a training dataset for retraining the
subhourly correction model with real ground-truth irradiance.

Usage:
    python compute_deltas_m11.py
"""

import math
from pathlib import Path

import numpy as np
import pandas as pd

RESEARCH_DIR = Path(__file__).resolve().parent
BATCH_RESULTS = RESEARCH_DIR / "batch_results.csv"
WEATHER_DIR = RESEARCH_DIR / "weather_files"
OUTPUT_CSV = RESEARCH_DIR / "training_data_m11.csv"

# Solar constant (W/m²) — matches src/models/subhourly_correction.py
_SOLAR_CONSTANT = 1361.0
_DAYTIME_GHI_ET_THRESHOLD = 50.0


# ---------------------------------------------------------------------------
# Solar geometry (Spencer, 1971) — replicated from subhourly_correction.py
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
    """Compute extraterrestrial horizontal irradiance (W/m²)."""
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
# Weather feature extraction — replicated from subhourly_correction.py
# ---------------------------------------------------------------------------


def compute_weather_features(weather_file_path: Path) -> dict[str, float]:
    """Compute weather-derived features from an hourly SAM-format weather CSV.

    Replicates src/models/subhourly_correction.py:compute_weather_features()
    exactly for standalone use in research scripts.
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

    return {
        "annual_ghi": round(annual_ghi, 2),
        "mean_kt": round(mean_kt, 4),
        "std_kt": round(std_kt, 4),
        "ghi_cv": round(ghi_cv, 4),
        "mean_dni": round(mean_dni, 2),
        "pct_clear_hours": round(pct_clear_hours, 2),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Load batch results, pair resolutions, compute deltas, save training data."""
    # --- Load and filter batch results ---
    raw = pd.read_csv(BATCH_RESULTS)
    print(f"Loaded {len(raw)} rows from {BATCH_RESULTS.name}")

    # Filter to successful runs, drop UNLV
    df = raw[raw["status"] == "success"].copy()
    n_before = len(df)
    df = df[~df["site_name"].str.contains("UnivofNevadaLV")].copy()
    n_dropped = n_before - len(df)
    print(f"Filtered to success: {n_before} rows")
    print(f"Dropped UNLV: {n_dropped} rows -> {len(df)} remaining")

    # --- Pair runs by config ---
    df["config_key"] = df["run_name"].str.replace(
        r"_(1min|60min)$", "", regex=True
    )

    df_1 = df[df["resolution"] == "1min"].copy()
    df_60 = df[df["resolution"] == "60min"].copy()

    print(f"\n  1-min runs: {len(df_1)}")
    print(f"  60-min runs: {len(df_60)}")

    # Check for unmatched configs
    configs_1 = set(df_1["config_key"])
    configs_60 = set(df_60["config_key"])

    missing_60 = configs_1 - configs_60
    missing_1 = configs_60 - configs_1

    unpaired_count = len(missing_60) + len(missing_1)
    if unpaired_count > 0:
        print(f"\nWARNING: {unpaired_count} unpaired configs found")
        if missing_60:
            print(f"  Missing 60-min counterpart ({len(missing_60)}):")
            for name in sorted(missing_60)[:10]:
                print(f"    {name}")
        if missing_1:
            print(f"  Missing 1-min counterpart ({len(missing_1)}):")
            for name in sorted(missing_1)[:10]:
                print(f"    {name}")
        if unpaired_count > 10:
            print("STOP: More than 10 unpaired configs. Investigate data.")
            return

    # Check for duplicate config keys within each resolution
    dupes_1 = df_1[df_1["config_key"].duplicated(keep=False)]
    dupes_60 = df_60[df_60["config_key"].duplicated(keep=False)]
    if len(dupes_1) > 0:
        print(f"WARNING: {len(dupes_1)} duplicate 1-min config keys")
    if len(dupes_60) > 0:
        print(f"WARNING: {len(dupes_60)} duplicate 60-min config keys")

    # Merge pairs
    paired = df_60[["config_key", "site_name", "latitude", "longitude",
                     "dcac_ratio", "gcr", "racking",
                     "annual_ac_kwh", "capacity_factor_pct"]].merge(
        df_1[["config_key", "annual_ac_kwh"]],
        on="config_key",
        suffixes=("_60min", "_1min"),
    )

    print(f"\nPaired configs: {len(paired)}")

    # --- Compute deltas ---
    paired["subhourly_loss_pct"] = (
        (paired["annual_ac_kwh_60min"] - paired["annual_ac_kwh_1min"])
        / paired["annual_ac_kwh_60min"]
        * 100
    )
    paired["cf_60min"] = paired["capacity_factor_pct"]

    # --- Compute weather features for each station ---
    stations = paired[["site_name", "latitude", "longitude"]].drop_duplicates()
    print(f"\nComputing weather features for {len(stations)} stations...")

    weather_features_map: dict[str, dict[str, float]] = {}
    for _, stn in stations.iterrows():
        site = stn["site_name"]
        weather_file = WEATHER_DIR / f"{site}_2020_60min.csv"
        if not weather_file.exists():
            print(f"  WARNING: Weather file not found: {weather_file.name}")
            continue
        features = compute_weather_features(weather_file)
        weather_features_map[site] = features
        print(
            f"  {site:30s}  GHI={features['annual_ghi']:7.1f}  "
            f"Kt={features['mean_kt']:.3f}  "
            f"clear={features['pct_clear_hours']:.1f}%"
        )

    if len(weather_features_map) != len(stations):
        missing = set(stations["site_name"]) - set(weather_features_map.keys())
        print(f"STOP: Missing weather files for: {missing}")
        return

    # --- Build training DataFrame ---
    weather_df = pd.DataFrame.from_dict(weather_features_map, orient="index")
    weather_df.index.name = "site_name"
    weather_df = weather_df.reset_index()

    training = paired.merge(weather_df, on="site_name")

    # Encode racking: tracker=1, fixed=0
    training["racking_encoded"] = (training["racking"] == "tracker").astype(int)

    # Clamped delta (for reference)
    training["subhourly_loss_pct_clamped"] = training["subhourly_loss_pct"].clip(lower=0.0)

    # Select and order output columns
    output_cols = [
        "site_name",
        "latitude",
        "longitude",
        "dcac_ratio",
        "gcr",
        "racking",
        "racking_encoded",
        "cf_60min",
        "annual_ghi",
        "mean_kt",
        "std_kt",
        "ghi_cv",
        "mean_dni",
        "pct_clear_hours",
        "subhourly_loss_pct",
        "subhourly_loss_pct_clamped",
    ]
    training = training[output_cols]

    # Save
    training.to_csv(OUTPUT_CSV, index=False)
    print(f"\nTraining dataset saved to {OUTPUT_CSV.name}: {len(training)} rows")

    # === Summary Statistics ===
    raw_delta = training["subhourly_loss_pct"]
    clamped_delta = training["subhourly_loss_pct_clamped"]

    print(f"\n{'='*70}")
    print("SUMMARY STATISTICS")
    print(f"{'='*70}")

    print(f"\nTotal paired configs: {len(training)}")

    print(f"\nRaw subhourly_loss_pct:")
    print(f"  min:    {raw_delta.min():+.4f}%")
    print(f"  max:    {raw_delta.max():+.4f}%")
    print(f"  mean:   {raw_delta.mean():+.4f}%")
    print(f"  median: {raw_delta.median():+.4f}%")
    print(f"  std:    {raw_delta.std():.4f}%")

    n_neg = (raw_delta < 0).sum()
    print(f"  negative deltas: {n_neg} / {len(raw_delta)} ({n_neg/len(raw_delta)*100:.1f}%)")

    print(f"\nClamped subhourly_loss_pct (>= 0):")
    print(f"  min:    {clamped_delta.min():+.4f}%")
    print(f"  max:    {clamped_delta.max():+.4f}%")
    print(f"  mean:   {clamped_delta.mean():+.4f}%")
    print(f"  median: {clamped_delta.median():+.4f}%")
    print(f"  std:    {clamped_delta.std():.4f}%")

    # --- Per-station mean ---
    print(f"\nPer-station mean raw delta (sorted):")
    station_means = training.groupby("site_name")["subhourly_loss_pct"].agg(
        ["mean", "std", "count"]
    ).sort_values("mean")
    for stn, row in station_means.iterrows():
        print(f"  {stn:30s}  mean={row['mean']:+.4f}%  std={row['std']:.4f}%  n={int(row['count'])}")

    # --- Per DC/AC ratio ---
    print(f"\nPer DC/AC ratio mean raw delta:")
    for ratio, group in training.sort_values("dcac_ratio").groupby("dcac_ratio"):
        g = group["subhourly_loss_pct"]
        print(f"  {ratio:.2f}: mean={g.mean():+.4f}%, std={g.std():.4f}%, n={len(g)}")

    # --- Per racking type ---
    print(f"\nPer racking type mean raw delta:")
    for racking, group in training.groupby("racking"):
        g = group["subhourly_loss_pct"]
        print(f"  {racking:8s}: mean={g.mean():+.4f}%, std={g.std():.4f}%, n={len(g)}")

    # --- Correlation ---
    corr = training["dcac_ratio"].corr(training["subhourly_loss_pct"])
    print(f"\nCorrelation(dcac_ratio, subhourly_loss_pct): {corr:.4f}")

    # --- Comparison to M8 ---
    print(f"\n{'='*70}")
    print("COMPARISON TO M8")
    print(f"{'='*70}")
    print(f"  M8 mean (5-min NSRDB satellite data): ~0.3%")
    print(f"  M11 mean (1-min ground-truth data):   {raw_delta.mean():+.4f}%")
    diff = raw_delta.mean() - 0.3
    print(f"  Difference: {diff:+.4f}%")
    if raw_delta.mean() > 0.3:
        print(f"  M11 shows LARGER subhourly losses with ground-truth 1-min data.")
        print(f"  This is expected: real irradiance peaks are higher than satellite-")
        print(f"  smoothed NSRDB data, so more clipping occurs at subhourly resolution.")
    else:
        print(f"  M11 shows SMALLER subhourly losses than M8.")


if __name__ == "__main__":
    main()
