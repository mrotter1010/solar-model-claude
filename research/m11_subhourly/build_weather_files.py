"""Build 1-minute SAM weather files from ground station data + NSRDB.

Pipeline:
  1. Parse SURFRAD/SOLRAD/MIDC raw 1-min irradiance (GHI, DNI, DHI)
  2. Apply QC (clamp negatives, cap extremes, fill short gaps)
  3. Fetch NSRDB hourly temp/wind/albedo for the station location
  4. Interpolate NSRDB to 1-min and merge with ground station irradiance
  5. Write SAM-format weather CSVs (1-min and 60-min)

Time convention: UTC throughout, with Time Zone=0 in SAM header.
This matches M8's approach where NSRDB data was fetched with utc=true.
PySAM correctly computes solar position from lat/lon + Time Zone.

Leap year handling: pipeline keeps Feb 29 (8784 hourly rows → 527,040
1-min rows). Confirmed by src/climate/weather_formatter.py tests and
NSRDB client's leap_day=true.
"""

import logging
import sys
import time
from io import StringIO
from pathlib import Path

import pandas as pd
import pvlib

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
M9_DIR = SCRIPT_DIR.parent / "m9_bias_correction"
SURFRAD_RAW = M9_DIR / "cache" / "ground_truth" / "raw"
OUTPUT_DIR = SCRIPT_DIR / "weather_files"

# Add project root for src.* imports, M9 dir for station registry
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(M9_DIR))
from stations import STATIONS  # noqa: E402

# QC thresholds (W/m²)
GHI_CAP = 1500.0
DNI_CAP = 1500.0
DHI_CAP = 800.0
MAX_FORWARD_FILL_MINUTES = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _minutes_in_year(year: int) -> int:
    """Expected 1-min records for a full year."""
    days = 366 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 365
    return days * 1440


# ---------------------------------------------------------------------------
# SURFRAD parsing
# ---------------------------------------------------------------------------
def parse_surfrad_year(station_id: str, year: int) -> pd.DataFrame:
    """Parse all cached daily .dat files for one SURFRAD station-year.

    Args:
        station_id: SURFRAD station code (e.g., "bon").
        year: Calendar year.

    Returns:
        DataFrame with columns [ghi, dni, dhi] and UTC DatetimeIndex at
        1-minute resolution. Raw values as reported (may contain NaN and
        negative nighttime readings).

    Raises:
        FileNotFoundError: If station-year directory is missing or empty.
    """
    year_dir = SURFRAD_RAW / station_id / str(year)
    if not year_dir.exists():
        raise FileNotFoundError(f"SURFRAD raw directory not found: {year_dir}")

    dat_files = sorted(year_dir.glob("*.dat"))
    if not dat_files:
        raise FileNotFoundError(f"No .dat files in {year_dir}")

    logger.info(
        "Parsing %d SURFRAD .dat files for %s %d...", len(dat_files), station_id, year,
    )

    frames: list[pd.DataFrame] = []
    read_errors = 0
    for f in dat_files:
        try:
            df, _ = pvlib.iotools.read_surfrad(str(f))
            frames.append(df[["ghi", "dni", "dhi"]])
        except Exception as e:
            read_errors += 1
            logger.warning("  Failed to read %s: %s", f.name, e)

    if not frames:
        raise RuntimeError(f"All {len(dat_files)} files failed to parse for {station_id} {year}")

    df = pd.concat(frames, axis=0).sort_index()
    # Remove any duplicate timestamps (rare overlap at day boundaries)
    df = df[~df.index.duplicated(keep="first")]

    logger.info(
        "  Parsed %d records from %d files (%d read errors)",
        len(df), len(dat_files), read_errors,
    )
    return df


# ---------------------------------------------------------------------------
# QC
# ---------------------------------------------------------------------------
def apply_irradiance_qc(df: pd.DataFrame, year: int) -> tuple[pd.DataFrame, dict]:
    """Apply QC filters to 1-min irradiance data for SAM compatibility.

    QC steps:
      1. Reindex to complete 1-min grid for the year (fills structural gaps)
      2. Forward-fill gaps up to MAX_FORWARD_FILL_MINUTES, then set NaN → 0
      3. Clamp negative values to 0 (nighttime instrument noise)
      4. Cap extreme values (GHI ≤ 1500, DNI ≤ 1500, DHI ≤ 800)

    Args:
        df: Raw irradiance DataFrame with UTC DatetimeIndex.
        year: Calendar year (for building complete time grid).

    Returns:
        Tuple of (QC'd DataFrame, summary dict with QC statistics).
    """
    n_raw = len(df)

    # Step 1: Build complete 1-min UTC time grid and reindex
    expected_minutes = _minutes_in_year(year)
    full_index = pd.date_range(
        start=f"{year}-01-01 00:00:00+00:00",
        periods=expected_minutes,
        freq="min",
    )
    df = df.reindex(full_index)

    n_missing_before_fill = int(df["ghi"].isna().sum())
    logger.info(
        "  Reindexed to %d-minute grid: %d missing rows introduced",
        expected_minutes, expected_minutes - n_raw,
    )

    # Step 2: Forward-fill short gaps (up to 10 minutes), then fill remaining NaN with 0
    df = df.ffill(limit=MAX_FORWARD_FILL_MINUTES)
    n_still_nan = int(df["ghi"].isna().sum())
    n_ffilled = n_missing_before_fill - n_still_nan
    df = df.fillna(0.0)

    logger.info(
        "  Gap fill: %d values forward-filled, %d set to 0",
        n_ffilled, n_still_nan,
    )

    # Step 3: Clamp negatives to 0
    n_neg_ghi = int((df["ghi"] < 0).sum())
    n_neg_dni = int((df["dni"] < 0).sum())
    n_neg_dhi = int((df["dhi"] < 0).sum())
    df["ghi"] = df["ghi"].clip(lower=0.0)
    df["dni"] = df["dni"].clip(lower=0.0)
    df["dhi"] = df["dhi"].clip(lower=0.0)

    logger.info(
        "  Negatives clamped to 0: GHI=%d, DNI=%d, DHI=%d",
        n_neg_ghi, n_neg_dni, n_neg_dhi,
    )

    # Step 4: Cap extreme values
    n_cap_ghi = int((df["ghi"] > GHI_CAP).sum())
    n_cap_dni = int((df["dni"] > DNI_CAP).sum())
    n_cap_dhi = int((df["dhi"] > DHI_CAP).sum())
    df["ghi"] = df["ghi"].clip(upper=GHI_CAP)
    df["dni"] = df["dni"].clip(upper=DNI_CAP)
    df["dhi"] = df["dhi"].clip(upper=DHI_CAP)

    logger.info(
        "  Values capped: GHI>%d=%d, DNI>%d=%d, DHI>%d=%d",
        GHI_CAP, n_cap_ghi, DNI_CAP, n_cap_dni, DHI_CAP, n_cap_dhi,
    )

    summary = {
        "raw_records": n_raw,
        "expected_records": expected_minutes,
        "missing_before_fill": n_missing_before_fill,
        "forward_filled": n_ffilled,
        "filled_zero": n_still_nan,
        "neg_clamped_ghi": n_neg_ghi,
        "neg_clamped_dni": n_neg_dni,
        "neg_clamped_dhi": n_neg_dhi,
        "capped_ghi": n_cap_ghi,
        "capped_dni": n_cap_dni,
        "capped_dhi": n_cap_dhi,
    }

    return df, summary


# ---------------------------------------------------------------------------
# Irradiance file output
# ---------------------------------------------------------------------------
def save_irradiance_csv(
    df: pd.DataFrame, station_name: str, state: str, year: int,
) -> Path:
    """Save QC'd 1-min irradiance data as intermediate CSV.

    Args:
        df: QC'd DataFrame with columns [ghi, dni, dhi] and UTC DatetimeIndex.
        station_name: Human-readable station name (e.g., "Bondville").
        state: State abbreviation (e.g., "IL").
        year: Calendar year.

    Returns:
        Path to the saved CSV file.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build output with explicit time columns for clarity
    out = pd.DataFrame({
        "timestamp_utc": df.index.strftime("%Y-%m-%d %H:%M:%S"),
        "ghi": df["ghi"].round(1).values,
        "dni": df["dni"].round(1).values,
        "dhi": df["dhi"].round(1).values,
    })

    filename = f"{station_name}_{state}_{year}_irradiance_1min.csv"
    filepath = OUTPUT_DIR / filename
    out.to_csv(filepath, index=False)

    logger.info("Saved irradiance file: %s (%d rows)", filepath.name, len(out))
    return filepath


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def build_station_irradiance(
    station_id: str, year: int,
) -> tuple[pd.DataFrame, dict, Path]:
    """Parse and QC one station-year of 1-min irradiance data.

    Args:
        station_id: Station code (e.g., "bon").
        year: Calendar year.

    Returns:
        Tuple of (QC'd DataFrame, QC summary dict, output file path).
    """
    # Look up station metadata
    station = next(
        (s for s in STATIONS if s["station_id"] == station_id), None,
    )
    if station is None:
        raise ValueError(f"Station {station_id} not found in registry")

    station_name = station["name"].replace(" ", "")
    state = station["state"]

    logger.info(
        "Building irradiance file for %s (%s), %s %d",
        station["name"], station_id, state, year,
    )

    # Parse raw data
    df_raw = parse_surfrad_year(station_id, year)

    # Apply QC
    df_qc, summary = apply_irradiance_qc(df_raw, year)

    # Save intermediate CSV
    filepath = save_irradiance_csv(df_qc, station_name, state, year)

    # Report
    completeness = summary["raw_records"] / summary["expected_records"] * 100
    logger.info(
        "  Completeness: %d/%d raw records (%.1f%%)",
        summary["raw_records"], summary["expected_records"], completeness,
    )

    return df_qc, summary, filepath


# ---------------------------------------------------------------------------
# NSRDB fetch and interpolation
# ---------------------------------------------------------------------------
def fetch_nsrdb_hourly(
    lat: float, lon: float, year: int,
) -> tuple[dict, pd.DataFrame]:
    """Fetch NSRDB hourly data and parse into metadata + DataFrame.

    The production NSRDBClient returns local-time data (utc=false).
    We convert to UTC by shifting timestamps using the timezone offset
    from the NSRDB header.

    Args:
        lat: Station latitude.
        lon: Station longitude.
        year: Calendar year.

    Returns:
        Tuple of (metadata dict, hourly DataFrame with UTC DatetimeIndex).
    """
    from src.climate.nsrdb_client import NSRDBClient

    logger.info("Fetching NSRDB hourly data for (%.2f, %.2f), year=%d", lat, lon, year)
    client = NSRDBClient()
    csv_str = client.fetch_weather_data(lat, lon, year)

    # Parse 3-row NSRDB header
    lines = csv_str.strip().split("\n")
    meta_labels = lines[0].split(",")
    meta_values = lines[1].split(",")
    metadata = dict(zip(meta_labels, meta_values))

    # Parse data (skip 2 header rows)
    df = pd.read_csv(StringIO(csv_str), skiprows=2)

    logger.info(
        "  NSRDB returned %d rows, columns: %s",
        len(df), list(df.columns),
    )
    logger.info(
        "  NSRDB header: Time Zone=%s, Local Time Zone=%s, Elevation=%s",
        metadata.get("Time Zone", "?"),
        metadata.get("Local Time Zone", "?"),
        metadata.get("Elevation", "?"),
    )

    # Build local-time timestamps from Year/Month/Day/Hour columns
    local_timestamps = pd.to_datetime(
        df[["Year", "Month", "Day", "Hour"]].assign(Minute=df.get("Minute", 0)),
    )

    # Convert local time to UTC.
    # NSRDB returns data in local standard time. The "Local Time Zone" header
    # gives the offset (e.g., -6 for CST). UTC = local - offset.
    local_tz = int(float(metadata.get("Local Time Zone", metadata.get("Time Zone", "0"))))
    utc_timestamps = local_timestamps - pd.Timedelta(hours=local_tz)

    weather = pd.DataFrame(
        {
            "Temperature": df["Temperature"].values,
            "Wind Speed": df["Wind Speed"].values,
            "Surface Albedo": df["Surface Albedo"].values,
        },
        index=utc_timestamps,
    )
    weather = weather.sort_index()
    # Remove any duplicate timestamps
    weather = weather[~weather.index.duplicated(keep="first")]

    logger.info(
        "  Converted to UTC: %s to %s (%d rows)",
        weather.index[0], weather.index[-1], len(weather),
    )

    return metadata, weather


def interpolate_nsrdb_to_1min(
    df_hourly: pd.DataFrame, year: int,
) -> pd.DataFrame:
    """Interpolate hourly NSRDB weather to 1-min UTC resolution.

    Args:
        df_hourly: Hourly DataFrame with UTC DatetimeIndex and columns
            [Temperature, Wind Speed, Surface Albedo].
        year: Calendar year (for building complete time grid).

    Returns:
        1-min DataFrame with same columns, linearly interpolated.
    """
    expected = _minutes_in_year(year)
    full_idx = pd.date_range(
        start=f"{year}-01-01 00:00:00",
        periods=expected,
        freq="min",
    )

    # Union the hourly index with the 1-min grid for interpolation
    combined_idx = full_idx.union(df_hourly.index)
    df = df_hourly.reindex(combined_idx)

    # Linear interpolation between hourly data points
    df = df.interpolate(method="linear")

    # Fill edges (first/last hours may be outside NSRDB coverage)
    df = df.bfill().ffill()

    # Restrict to the exact year grid
    df = df.reindex(full_idx)

    logger.info(
        "  Interpolated NSRDB to 1-min: %d rows, "
        "Temp range [%.1f, %.1f], Wind range [%.1f, %.1f]",
        len(df),
        df["Temperature"].min(), df["Temperature"].max(),
        df["Wind Speed"].min(), df["Wind Speed"].max(),
    )

    return df


# ---------------------------------------------------------------------------
# SAM weather file writing
# ---------------------------------------------------------------------------
def write_sam_csv(
    df: pd.DataFrame,
    lat: float,
    lon: float,
    elevation: float,
    filepath: Path,
    nsrdb_metadata: dict | None = None,
) -> None:
    """Write a SAM-format weather CSV with 3-row NSRDB-style header.

    Uses Time Zone=0 (UTC) since all data is in UTC. PySAM computes
    solar position from lat/lon + Time Zone, matching M8's convention.

    Args:
        df: Weather DataFrame with columns [GHI, DNI, DHI, Temperature,
            Wind Speed, Surface Albedo] and a tz-naive DatetimeIndex in UTC.
        lat: Station latitude.
        lon: Station longitude.
        elevation: Station elevation (m).
        filepath: Output path.
        nsrdb_metadata: Optional NSRDB header metadata for pass-through.
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)

    # Build SAM time columns from the DatetimeIndex
    out = pd.DataFrame({
        "Year": df.index.year,
        "Month": df.index.month,
        "Day": df.index.day,
        "Hour": df.index.hour,
        "Minute": df.index.minute,
        "GHI": df["GHI"].round(1),
        "DNI": df["DNI"].round(1),
        "DHI": df["DHI"].round(1),
        "Temperature": df["Temperature"].round(2),
        "Wind Speed": df["Wind Speed"].round(2),
        "Surface Albedo": df["Surface Albedo"].round(4),
    })

    # 3-row NSRDB-style header (matches format PySAM expects)
    local_tz = nsrdb_metadata.get("Local Time Zone", "0") if nsrdb_metadata else "0"
    header_labels = (
        "Source,Location ID,City,State,Country,"
        "Latitude,Longitude,Time Zone,Elevation,Local Time Zone"
    )
    header_values = (
        f"Ground Station,0,-,-,-,"
        f"{lat},{lon},0,{elevation},{local_tz}"
    )

    with open(filepath, "w") as f:
        f.write(header_labels + "\n")
        f.write(header_values + "\n")
        out.to_csv(f, index=False)

    logger.info("Saved SAM weather file: %s (%d data rows)", filepath.name, len(out))


def build_60min_from_1min(
    df_1min: pd.DataFrame, year: int,
) -> pd.DataFrame:
    """Aggregate 1-min SAM weather data to 60-min by averaging 60-row blocks.

    Args:
        df_1min: 1-min weather DataFrame with tz-naive UTC DatetimeIndex.
        year: Calendar year.

    Returns:
        Hourly DataFrame with same columns.
    """
    # Group by hour and average all weather columns
    hourly = df_1min.resample("h").mean()

    expected_hours = 8784 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 8760
    if len(hourly) != expected_hours:
        logger.warning(
            "  60-min aggregation: expected %d rows, got %d",
            expected_hours, len(hourly),
        )

    logger.info("  Aggregated to 60-min: %d rows", len(hourly))
    return hourly


# ---------------------------------------------------------------------------
# PySAM proof of concept
# ---------------------------------------------------------------------------
def run_pysam_poc(
    weather_file: Path,
    label: str,
    lat: float,
    lon: float,
) -> tuple[float, float, float]:
    """Run a single PySAM simulation for proof-of-concept validation.

    Uses the same config and patches as M8 batch_runner.py.

    Args:
        weather_file: Path to SAM-format weather CSV.
        label: Run label for logging (e.g., "1min", "60min").
        lat: Station latitude.
        lon: Station longitude.

    Returns:
        Tuple of (annual_energy_kwh, capacity_factor, elapsed_seconds).
    """
    from src.config.schema import SiteConfig
    from src.pysam_integration.cec_database import CECDatabase
    from src.pysam_integration.model_configurator import ModelConfigurator

    logger.info("Running PySAM POC (%s): %s", label, weather_file.name)

    site_config = SiteConfig(
        run_name=f"Bondville_IL_1.40_0.40_tracker_{label}",
        site_name="Bondville_IL",
        customer="M11_Research",
        latitude=lat,
        longitude=lon,
        dc_size_mw=28.0,
        ac_installed_mw=20.0,
        ac_poi_mw=20.0,
        racking="tracker",
        tilt=60.0,
        azimuth=180.0,
        module_orientation="portrait",
        number_of_modules=2,
        ground_clearance_height_m=1.8,
        panel_model="SunPower SPR-310-WHT-U",
        bifacial=True,
        inverter_model="Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
        gcr=0.40,
        shading_percent=1.0,
        dc_wiring_loss_percent=1.5,
        ac_wiring_loss_percent=1.5,
        transformer_losses_percent=0.0,
        degradation_percent=0.3,
        availability_percent=3.0,
        module_mismatch_percent=1.5,
        lid_percent=1.0,
        weather_file_path=weather_file,
    )

    cec_db = CECDatabase()
    configurator = ModelConfigurator(cec_database=cec_db)
    model_config = configurator.configure_model(site_config)
    model = model_config.model

    # --- M8 PySAM patches ---
    # 1. Snow model: cached weather files lack Snow Depth column
    model.Losses.en_snow_model = 0

    # 2. Bifacial ground clearance: must be positive when bifacial=1
    cec = model.CECPerformanceModelWithModuleDatabase
    if cec.cec_is_bifacial == 1 and cec.cec_bifacial_ground_clearance_height <= 0:
        cec.cec_bifacial_ground_clearance_height = (
            site_config.ground_clearance_height_m
        )

    # 3. Self-shading layout fix
    sd = model.SystemDesign
    model.Layout.subarray1_nmodx = sd.subarray1_nstrings

    # Execute
    t0 = time.time()
    model.execute()
    elapsed = time.time() - t0

    annual_energy = float(model.Outputs.annual_energy)
    capacity_factor = float(model.Outputs.capacity_factor)

    logger.info(
        "  PySAM %s: %.0f kWh, CF=%.2f%%, %.1fs",
        label, annual_energy, capacity_factor, elapsed,
    )

    return annual_energy, capacity_factor, elapsed


# ---------------------------------------------------------------------------
# Main — Prompt 2b: NSRDB merge + SAM files + PySAM POC
# ---------------------------------------------------------------------------
def main() -> None:
    """Build complete SAM weather files and run PySAM proof of concept."""
    from utils import get_timezone_offset  # noqa: E402

    station_id = "bon"
    year = 2020
    station = next(s for s in STATIONS if s["station_id"] == station_id)
    lat, lon = station["latitude"], station["longitude"]
    elevation = station["elevation_m"]
    tz_offset = get_timezone_offset(lat, lon)

    logger.info(
        "Station: %s (%s), lat=%.2f, lon=%.2f, elev=%.0f, tz=UTC%+d",
        station["name"], station_id, lat, lon, elevation, tz_offset,
    )

    # -----------------------------------------------------------------------
    # Step 1: Load QC'd irradiance from Prompt 2a (or rebuild if missing)
    # -----------------------------------------------------------------------
    irradiance_path = OUTPUT_DIR / "Bondville_IL_2020_irradiance_1min.csv"
    if irradiance_path.exists():
        logger.info("Loading cached irradiance from %s", irradiance_path.name)
        df_irr_csv = pd.read_csv(irradiance_path)
        irr_index = pd.to_datetime(df_irr_csv["timestamp_utc"])
        df_irr = pd.DataFrame(
            {
                "ghi": df_irr_csv["ghi"].values,
                "dni": df_irr_csv["dni"].values,
                "dhi": df_irr_csv["dhi"].values,
            },
            index=irr_index,
        )
    else:
        logger.info("Irradiance file not found, rebuilding...")
        df_irr, _, _ = build_station_irradiance(station_id, year)

    logger.info("Irradiance loaded: %d rows", len(df_irr))

    # -----------------------------------------------------------------------
    # Step 2: Fetch NSRDB hourly temp/wind/albedo
    # -----------------------------------------------------------------------
    nsrdb_metadata, df_nsrdb_hourly = fetch_nsrdb_hourly(lat, lon, year)

    # -----------------------------------------------------------------------
    # Step 3: Interpolate NSRDB to 1-min
    # -----------------------------------------------------------------------
    df_nsrdb_1min = interpolate_nsrdb_to_1min(df_nsrdb_hourly, year)

    # Verify row counts match
    expected = _minutes_in_year(year)
    assert len(df_irr) == expected, (
        f"Irradiance row count {len(df_irr)} != expected {expected}"
    )
    assert len(df_nsrdb_1min) == expected, (
        f"NSRDB 1-min row count {len(df_nsrdb_1min)} != expected {expected}"
    )
    logger.info("Row counts match: irradiance=%d, NSRDB=%d", len(df_irr), len(df_nsrdb_1min))

    # -----------------------------------------------------------------------
    # Step 4: Build 1-min SAM weather file
    # -----------------------------------------------------------------------
    # Combine irradiance + NSRDB weather on a tz-naive UTC index
    utc_index = pd.date_range(
        start=f"{year}-01-01 00:00:00",
        periods=expected,
        freq="min",
    )

    df_1min = pd.DataFrame(
        {
            "GHI": df_irr["ghi"].values,
            "DNI": df_irr["dni"].values,
            "DHI": df_irr["dhi"].values,
            "Temperature": df_nsrdb_1min["Temperature"].values,
            "Wind Speed": df_nsrdb_1min["Wind Speed"].values,
            "Surface Albedo": df_nsrdb_1min["Surface Albedo"].values,
        },
        index=utc_index,
    )

    sam_1min_path = OUTPUT_DIR / f"Bondville_IL_{year}_1min.csv"
    write_sam_csv(df_1min, lat, lon, elevation, sam_1min_path, nsrdb_metadata)

    # -----------------------------------------------------------------------
    # Step 5: Build 60-min SAM weather file
    # -----------------------------------------------------------------------
    df_60min = build_60min_from_1min(df_1min, year)
    sam_60min_path = OUTPUT_DIR / f"Bondville_IL_{year}_60min.csv"
    write_sam_csv(df_60min, lat, lon, elevation, sam_60min_path, nsrdb_metadata)

    # -----------------------------------------------------------------------
    # Step 6: Run PySAM proof of concept
    # -----------------------------------------------------------------------
    energy_1min, cf_1min, time_1min = run_pysam_poc(
        sam_1min_path, "1min", lat, lon,
    )
    energy_60min, cf_60min, time_60min = run_pysam_poc(
        sam_60min_path, "60min", lat, lon,
    )

    # Compute subhourly loss
    subhourly_loss_pct = (energy_60min - energy_1min) / energy_60min * 100.0

    # -----------------------------------------------------------------------
    # Report
    # -----------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("M11 BONDVILLE IL 2020 — PROOF OF CONCEPT REPORT")
    print("=" * 80)

    print(f"\nStation: {station['name']} ({station_id})")
    print(f"Location: {lat}, {lon}, elevation={elevation}m, TZ=UTC{tz_offset:+d}")

    print(f"\n--- NSRDB Fetch ---")
    print(f"Rows received: {len(df_nsrdb_hourly)} hourly")
    print(f"Columns: Temperature, Wind Speed, Surface Albedo")
    print(f"NSRDB header Time Zone: {nsrdb_metadata.get('Time Zone', '?')}")
    print(f"NSRDB header Local TZ: {nsrdb_metadata.get('Local Time Zone', '?')}")

    print(f"\n--- Interpolation ---")
    print(f"1-min NSRDB rows: {len(df_nsrdb_1min)} (matches irradiance: {len(df_irr)})")

    print(f"\n--- 1-min SAM Weather File ---")
    print(f"Path: {sam_1min_path}")
    print(f"Data rows: {len(df_1min):,}")
    print(f"First 5 rows:")
    print(df_1min.head().to_string())

    print(f"\n--- 60-min SAM Weather File ---")
    print(f"Path: {sam_60min_path}")
    print(f"Data rows: {len(df_60min):,}")
    print(f"First 5 rows:")
    print(df_60min.head().to_string())

    print(f"\n--- PySAM Results ---")
    print(f"Config: DC/AC=1.40, GCR=0.40, tracker, bifacial")
    print(f"Module: SunPower SPR-310-WHT-U")
    print(f"Inverter: Sungrow SG250HX-US [800V]")
    print(f"AC=20MW, DC=28MW")
    print(f"")
    print(f"  1-min:  {energy_1min:>14,.0f} kWh  CF={cf_1min:.2f}%  ({time_1min:.1f}s)")
    print(f"  60-min: {energy_60min:>14,.0f} kWh  CF={cf_60min:.2f}%  ({time_60min:.1f}s)")
    print(f"")
    print(f"  Subhourly clipping loss: {subhourly_loss_pct:.3f}%")
    print(f"  (positive = hourly overestimates vs 1-min)")

    print(f"\n--- Timing ---")
    print(f"  1-min PySAM: {time_1min:.1f}s")
    print(f"  60-min PySAM: {time_60min:.1f}s")
    print(f"  Ratio: {time_1min/max(time_60min, 0.01):.1f}x")
    est_total = time_1min * 1600
    print(f"  Estimated 1,600 runs at 1-min: {est_total:.0f}s ({est_total/60:.0f} min)")
    print()


if __name__ == "__main__":
    main()
