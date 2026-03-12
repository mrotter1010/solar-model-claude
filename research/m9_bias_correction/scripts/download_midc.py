"""Download, QC-filter, and aggregate MIDC ground truth irradiance data.

Uses pvlib.iotools.read_midc_raw_data_from_nrel() to fetch 1-minute data
from NREL's MIDC network, applies quality control, and aggregates to monthly
mean daily insolation (kWh/m²/day).

Reuses QC and aggregation logic from download_ground_truth.py.
"""

import logging
import sys
import time
import warnings
from pathlib import Path

import pandas as pd
import pvlib
from pvlib.iotools import read_midc_raw_data_from_nrel

# Add project root and scripts dir for imports
_project_root = Path(__file__).resolve().parent.parent
_scripts_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_scripts_dir))

from stations import STATIONS  # noqa: E402
from download_ground_truth import apply_qc, aggregate_monthly  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DIR = _project_root
CACHE_DIR = BASE_DIR / "cache" / "ground_truth"
RAW_CACHE_DIR = CACHE_DIR / "midc_raw"
OUTPUT_DIR = BASE_DIR / "outputs"

TIMEOUT = 120
MAX_FIX_ATTEMPTS = 2

# Variable maps from discovery report — maps raw MIDC column names to standard names
VARIABLE_MAPS: dict[str, dict[str, str]] = {
    "BMS": {
        "Global CMP22-2 (cor) [W/m^2]": "ghi",
        "Direct CHP1-2 [W/m^2]": "dni",
        "Diffuse CM22-2 (vent/cor) [W/m^2]": "dhi",
        "CR3000 Zen Angle [degrees]": "solar_zenith",
    },
    "UAT": {
        "Global Horiz (tracker) [W/m^2]": "ghi",
        "Direct Normal [W/m^2]": "dni",
        "Diffuse Horiz [W/m^2]": "dhi",
        "Zenith Angle [degrees]": "solar_zenith",
    },
    "UOSMRL": {
        "Global CMP22 [W/m^2]": "ghi",
        "Direct CHP1 [W/m^2]": "dni",
        "Diffuse [W/m^2]": "dhi",
        "Zenith Angle [degrees]": "solar_zenith",
    },
    "UNLV": {
        "Global Horiz [W/m^2]": "ghi",
        "Direct Normal [W/m^2]": "dni",
        "Diffuse Horiz (calc) [W/m^2]": "dhi",
        "Zenith Angle [degrees]": "solar_zenith",
    },
    "ULL": {
        "Global Horizontal [W/m^2]": "ghi",
        "Direct Normal [W/m^2]": "dni",
        "Diffuse Horizontal [W/m^2]": "dhi",
    },
    "UTPASRL": {
        "Global Horizontal [W/m^2]": "ghi",
        "Direct Normal [W/m^2]": "dni",
        "Diffuse Horizontal [W/m^2]": "dhi",
    },
}

# Year coverage per station (do NOT attempt missing years)
YEAR_COVERAGE: dict[str, list[int]] = {
    "BMS": [2018, 2019, 2020, 2021, 2022, 2023],
    "UAT": [2018, 2019, 2020, 2021, 2022, 2023],
    "UOSMRL": [2018, 2019, 2020, 2021, 2022, 2023],
    "UNLV": [2018, 2019, 2020, 2021, 2022, 2023],
    "ULL": [2019, 2020, 2021, 2022, 2023],
    "UTPASRL": [2018, 2019, 2020, 2022, 2023],
}

QUARTERS = [
    ("Q1", "01-01", "03-31"),
    ("Q2", "04-01", "06-30"),
    ("Q3", "07-01", "09-30"),
    ("Q4", "10-01", "12-31"),
]


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------
def download_midc_year(
    station_id: str, year: int, variable_map: dict[str, str],
) -> pd.DataFrame:
    """Download one year of MIDC data with caching and quarter fallback.

    Args:
        station_id: MIDC site ID (e.g. "BMS").
        year: Calendar year.
        variable_map: Maps raw MIDC column names to standard names.

    Returns:
        DataFrame with renamed columns and DatetimeIndex.
    """
    cache_path = RAW_CACHE_DIR / f"{station_id}_{year}.csv.gz"

    # Check cache
    if cache_path.exists():
        logger.info("Loading %s %d from cache: %s", station_id, year, cache_path.name)
        df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
        logger.info("  Loaded %d rows from cache", len(df))
        return df

    # Try full year first
    start = f"{year}-01-01"
    end = f"{year}-12-31"

    try:
        logger.info("Downloading %s %d (full year)...", station_id, year)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = read_midc_raw_data_from_nrel(
                site=station_id, start=start, end=end,
                variable_map=variable_map, timeout=TIMEOUT,
            )
        if len(df) > 0:
            logger.info("  Full year: %d rows", len(df))
            _cache_raw(df, cache_path)
            return df
    except Exception as e:
        logger.warning(
            "  Full year fetch failed for %s %d: %s — trying quarters",
            station_id, year, str(e)[:120],
        )

    # Fallback: quarter-by-quarter
    quarter_frames: list[pd.DataFrame] = []
    for q_label, q_start_md, q_end_md in QUARTERS:
        q_start = f"{year}-{q_start_md}"
        q_end = f"{year}-{q_end_md}"
        try:
            logger.info("  Fetching %s %d %s...", station_id, year, q_label)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                q_df = read_midc_raw_data_from_nrel(
                    site=station_id, start=q_start, end=q_end,
                    variable_map=variable_map, timeout=TIMEOUT,
                )
            if len(q_df) > 0:
                quarter_frames.append(q_df)
                logger.info("    %s: %d rows", q_label, len(q_df))
            else:
                logger.warning("    %s: empty", q_label)
        except Exception as e:
            logger.warning("    %s failed: %s", q_label, str(e)[:120])

    if not quarter_frames:
        raise RuntimeError(
            f"No data for {station_id} {year} — all quarters failed"
        )

    df = pd.concat(quarter_frames, axis=0).sort_index()
    # Remove any duplicates from overlapping quarter boundaries
    df = df[~df.index.duplicated(keep="first")]
    logger.info("  Quarters combined: %d rows", len(df))
    _cache_raw(df, cache_path)
    return df


def _cache_raw(df: pd.DataFrame, path: Path) -> None:
    """Save raw DataFrame to gzipped CSV cache."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, compression="gzip")
    logger.info("  Cached to %s", path.name)


# ---------------------------------------------------------------------------
# Solar zenith computation for stations without it
# ---------------------------------------------------------------------------
def add_solar_zenith(df: pd.DataFrame, latitude: float, longitude: float) -> pd.DataFrame:
    """Compute solar zenith angle if not present or mostly NaN.

    Some MIDC stations report zenith in some years but not others (e.g. UAT
    has all-NaN zenith before 2022). If >50% of values are NaN, we recompute.

    Args:
        df: DataFrame with DatetimeIndex (timezone-aware).
        latitude: Station latitude.
        longitude: Station longitude.

    Returns:
        DataFrame with solar_zenith column added/confirmed.
    """
    needs_compute = False

    if "solar_zenith" not in df.columns:
        needs_compute = True
    else:
        nan_frac = df["solar_zenith"].isna().mean()
        if nan_frac > 0.5:
            logger.info(
                "  solar_zenith is %.0f%% NaN — recomputing from lat/lon",
                nan_frac * 100,
            )
            needs_compute = True

    if needs_compute:
        logger.info("  Computing solar zenith for %d timestamps...", len(df))
        solpos = pvlib.solarposition.get_solarposition(
            df.index, latitude, longitude,
        )
        df = df.copy()
        df["solar_zenith"] = solpos["zenith"].values

    return df


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate_columns(df: pd.DataFrame, station_id: str) -> None:
    """Validate that required columns exist after variable mapping.

    Raises:
        RuntimeError: If ghi or dni columns are missing.
    """
    required = {"ghi", "dni"}
    actual = set(df.columns)
    missing = required - actual

    if missing:
        raise RuntimeError(
            f"STOP: {station_id} missing columns after mapping: {missing}. "
            f"Raw columns present: {sorted(actual)}"
        )


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def main() -> None:
    """Download all MIDC stations, QC, aggregate, save."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    midc_stations = [s for s in STATIONS if s["network"] == "MIDC"]
    logger.info("Processing %d MIDC stations", len(midc_stations))

    global_t0 = time.time()
    all_monthly: list[pd.DataFrame] = []
    station_summaries: list[dict] = []
    failed_items: list[str] = []

    for station in midc_stations:
        station_id = station["station_id"]
        station_name = station["name"]
        variable_map = VARIABLE_MAPS[station_id]
        years = YEAR_COVERAGE[station_id]

        station_months_total = 0
        station_months_excluded = 0
        station_ghi_values: list[float] = []
        station_dni_values: list[float] = []
        station_years_ok: list[int] = []

        logger.info(
            "\n%s\nStation: %s (%s) — %s, lat=%.2f, lon=%.2f, years=%s\n%s",
            "=" * 60, station_name, station_id, station["state"],
            station["latitude"], station["longitude"], years, "=" * 60,
        )

        for year in years:
            attempts = 0
            success = False

            while attempts < MAX_FIX_ATTEMPTS and not success:
                attempts += 1
                try:
                    # Download
                    df_raw = download_midc_year(station_id, year, variable_map)

                    # Validate required columns
                    validate_columns(df_raw, station_id)

                    # Add solar zenith if missing
                    df_raw = add_solar_zenith(
                        df_raw, station["latitude"], station["longitude"],
                    )

                    # Ensure dhi column exists (needed for QC)
                    if "dhi" not in df_raw.columns:
                        # Without DHI, skip the GHI >= DHI check by setting dhi = 0
                        df_raw["dhi"] = 0.0
                        logger.info("  No DHI column — setting dhi=0 for QC")

                    # QC (reuse from download_ground_truth)
                    df_qc = apply_qc(df_raw)

                    # Aggregate to monthly
                    monthly, n_excluded = aggregate_monthly(
                        df_qc, station_id, "MIDC",
                    )

                    # Save per-station-year CSV
                    csv_path = CACHE_DIR / f"{station_id}_{year}_monthly.csv"
                    monthly.to_csv(csv_path, index=False)
                    logger.info("Saved %s", csv_path.name)

                    all_monthly.append(monthly)
                    station_months_total += len(monthly)
                    station_months_excluded += n_excluded
                    station_ghi_values.extend(monthly["ghi_kwh_m2_day"].tolist())
                    station_dni_values.extend(monthly["dni_kwh_m2_day"].tolist())
                    station_years_ok.append(year)
                    success = True

                except RuntimeError as e:
                    if "STOP:" in str(e):
                        logger.error(str(e))
                        failed_items.append(f"{station_id} {year}: {e}")
                        break
                    if attempts >= MAX_FIX_ATTEMPTS:
                        logger.error(
                            "Failed %s %d after %d attempts: %s",
                            station_id, year, attempts, e,
                        )
                        failed_items.append(f"{station_id} {year}: {e}")
                        break
                    logger.warning(
                        "Attempt %d failed for %s %d: %s — retrying",
                        attempts, station_id, year, e,
                    )

                except Exception as e:
                    if attempts >= MAX_FIX_ATTEMPTS:
                        logger.error(
                            "Failed %s %d after %d attempts: %s",
                            station_id, year, attempts, e,
                        )
                        failed_items.append(f"{station_id} {year}: {e}")
                        break
                    logger.warning(
                        "Attempt %d failed for %s %d: %s — retrying",
                        attempts, station_id, year, e,
                    )

        # Station summary
        mean_ghi = (
            sum(station_ghi_values) / len(station_ghi_values)
            if station_ghi_values else 0.0
        )
        mean_dni = (
            sum(station_dni_values) / len(station_dni_values)
            if station_dni_values else 0.0
        )
        station_summaries.append({
            "station_id": station_id,
            "name": station_name,
            "state": station["state"],
            "years_attempted": len(years),
            "years_ok": len(station_years_ok),
            "year_range": (
                f"{min(station_years_ok)}-{max(station_years_ok)}"
                if station_years_ok else "none"
            ),
            "months_available": station_months_total,
            "months_excluded": station_months_excluded,
            "mean_ghi": round(mean_ghi, 3),
            "mean_dni": round(mean_dni, 3),
        })

    # -----------------------------------------------------------------------
    # Save master CSV
    # -----------------------------------------------------------------------
    if all_monthly:
        master = pd.concat(all_monthly, ignore_index=True)
        master_path = CACHE_DIR / "midc_all_monthly.csv"
        master.to_csv(master_path, index=False)
        logger.info("Saved master CSV: %s (%d rows)", master_path, len(master))
    else:
        master = pd.DataFrame()
        logger.warning("No MIDC monthly data to save.")

    # -----------------------------------------------------------------------
    # Summary report
    # -----------------------------------------------------------------------
    elapsed = time.time() - global_t0
    lines: list[str] = []
    lines.append("MIDC Ground Truth Download Summary")
    lines.append("=" * 80)
    lines.append(f"Total rows in midc_all_monthly.csv: {len(master)}")
    lines.append(f"Download time: {elapsed:.0f}s ({elapsed / 60:.1f} min)")
    lines.append("")
    lines.append(
        f"{'Station':<10} {'Name':<22} {'ST':<4} {'Yrs':<5} {'Range':<12} "
        f"{'Months':<8} {'Excl':<6} {'GHI':>8} {'DNI':>8}"
    )
    lines.append("-" * 90)

    for s in station_summaries:
        lines.append(
            f"{s['station_id']:<10} {s['name']:<22} {s['state']:<4} "
            f"{s['years_ok']}/{s['years_attempted']:<3} {s['year_range']:<12} "
            f"{s['months_available']:<8} {s['months_excluded']:<6} "
            f"{s['mean_ghi']:>8.3f} {s['mean_dni']:>8.3f}"
        )

    lines.append("-" * 90)

    if failed_items:
        lines.append("")
        lines.append(f"FAILURES ({len(failed_items)}):")
        for item in failed_items:
            lines.append(f"  {item}")

    if not master.empty:
        lines.append("")
        lines.append(f"Stations in master: {master['station_id'].nunique()}")
        lines.append(f"Month range: {master['year'].min()}/{master['month'].min():02d} "
                      f"to {master['year'].max()}/{master['month'].max():02d}")

    summary_text = "\n".join(lines) + "\n"
    summary_path = OUTPUT_DIR / "midc_download_summary.txt"
    summary_path.write_text(summary_text)
    logger.info("Saved summary: %s", summary_path)
    print("\n" + summary_text)


if __name__ == "__main__":
    main()
