"""Download, QC-filter, and aggregate SURFRAD + SOLRAD ground truth irradiance data.

Uses pvlib.iotools to fetch 1-minute resolution data from NOAA's SURFRAD and
SOLRAD networks, applies quality control filters, and aggregates to monthly
mean daily insolation (kWh/m²/day).

Features:
    - Raw file caching: SURFRAD .dat files, SOLRAD gzipped CSV files
    - Parallel downloads for SURFRAD via ThreadPoolExecutor (max_workers=4)
    - SOLRAD uses pvlib.iotools.get_solrad() which handles daily files internally
    - QC: GHI < DHI check relaxed at high zenith angles (>= 80°)
    - All 7 SURFRAD + 7 SOLRAD stations × years 2018–2023
"""

import logging
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import pandas as pd
import pvlib

# Add project root so we can import the station registry
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))
from stations import STATIONS  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

SURFRAD_URL_TEMPLATE = (
    "https://gml.noaa.gov/aftp/data/radiation/surfrad/"
    "{station_id}/{year}/{station_id}{yy}{doy:03d}.dat"
)

# QC thresholds (W/m²)
GHI_MAX = 1500.0
DNI_MAX = 1200.0
MONTH_COMPLETENESS_THRESHOLD = 0.90

# Parallel download settings
MAX_WORKERS = 4

# Stop conditions
MAX_RUNTIME_SECONDS = 4 * 60 * 60  # 4 hours (SURFRAD + SOLRAD)
MAX_FIX_ATTEMPTS = 2

# Years to process
YEARS = list(range(2018, 2024))


def _cache_path(raw_cache_dir: Path, station_id: str, year: int, doy: int) -> Path:
    """Build the cache path for a single daily .dat file."""
    yy = str(year)[2:]
    return raw_cache_dir / station_id / str(year) / f"{station_id}{yy}{doy:03d}.dat"


def _download_one_day(
    station_id: str,
    year: int,
    current_date: date,
    raw_cache_dir: Path,
) -> tuple[date, pd.DataFrame | None, str | None]:
    """Download (or read from cache) a single day of SURFRAD data.

    Returns:
        Tuple of (date, DataFrame or None, error_message or None).
    """
    doy = current_date.timetuple().tm_yday
    cached = _cache_path(raw_cache_dir, station_id, year, doy)

    # Check cache first
    if cached.exists():
        try:
            df_day, _ = pvlib.iotools.read_surfrad(str(cached))
            return (current_date, df_day, None)
        except Exception as e:
            # Corrupt cache file — delete and re-download
            cached.unlink(missing_ok=True)
            logger.warning("Corrupt cache file %s, re-downloading: %s", cached.name, e)

    # Download from NOAA
    yy = str(year)[2:]
    url = SURFRAD_URL_TEMPLATE.format(
        station_id=station_id, year=year, yy=yy, doy=doy,
    )

    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            raw_bytes = resp.read()

        # Save to cache
        cached.parent.mkdir(parents=True, exist_ok=True)
        cached.write_bytes(raw_bytes)

        # Parse from cached file
        df_day, _ = pvlib.iotools.read_surfrad(str(cached))
        return (current_date, df_day, None)

    except Exception as e:
        err_msg = str(e)
        if "404" in err_msg or "Not Found" in err_msg:
            return (current_date, None, f"404")
        return (current_date, None, err_msg)


def download_surfrad_year(
    station_id: str, year: int, raw_cache_dir: Path
) -> pd.DataFrame:
    """Download one year of 1-minute SURFRAD data with caching and parallelism.

    Args:
        station_id: SURFRAD station code (e.g. "bon").
        year: Calendar year to download.
        raw_cache_dir: Root directory for raw .dat file cache.

    Returns:
        DataFrame with 1-minute irradiance data, UTC datetime index.

    Raises:
        RuntimeError: If 0 days are successfully downloaded.
    """
    start_date = date(year, 1, 1)
    end_date = date(year, 12, 31)
    n_days = (end_date - start_date).days + 1

    dates = [start_date + pd.Timedelta(days=i) for i in range(n_days)]

    logger.info(
        "Downloading %s %d: %d days from SURFRAD (max_workers=%d)...",
        station_id, year, n_days, MAX_WORKERS,
    )
    t0 = time.time()

    frames: list[pd.DataFrame] = []
    failed_days: list[date] = []
    completed_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                _download_one_day, station_id, year, d, raw_cache_dir,
            ): d
            for d in dates
        }

        for future in as_completed(futures):
            current_date, df_day, error = future.result()
            completed_count += 1

            if df_day is not None:
                frames.append(df_day)
            else:
                failed_days.append(current_date)
                if error != "404":
                    logger.warning(
                        "  Day %s: %s — skipping", current_date, error
                    )

            # Progress every 50 files
            if completed_count % 50 == 0:
                elapsed = time.time() - t0
                logger.info(
                    "  %s %d: %d/%d days downloaded (%.0fs elapsed)",
                    station_id, year, completed_count, n_days, elapsed,
                )

    elapsed = time.time() - t0
    logger.info(
        "Download complete: %s %d — %d/%d days successful in %.1fs",
        station_id, year, n_days - len(failed_days), n_days, elapsed,
    )

    if not frames:
        raise RuntimeError(
            f"STOP: 0 days downloaded for {station_id} {year}. "
            f"All {n_days} days failed."
        )

    df = pd.concat(frames, axis=0)
    df = df.sort_index()
    return df


def download_solrad_year(
    station_id: str, year: int, cache_dir: Path
) -> pd.DataFrame:
    """Download one year of 1-minute SOLRAD data with compressed CSV caching.

    Uses pvlib.iotools.get_solrad() which handles daily file fetching
    internally. The full year DataFrame is cached as a gzipped CSV.

    Args:
        station_id: SOLRAD station code (e.g. "abq").
        year: Calendar year to download.
        cache_dir: Directory for cached parquet files.

    Returns:
        DataFrame with 1-minute irradiance data, UTC datetime index.

    Raises:
        RuntimeError: If get_solrad() returns no data or fails.
    """
    solrad_cache = cache_dir / "solrad_raw"
    solrad_cache.mkdir(parents=True, exist_ok=True)
    csv_cache_path = solrad_cache / f"{station_id}_{year}.csv.gz"

    # Check cache
    if csv_cache_path.exists():
        logger.info("Loading %s %d from cache: %s", station_id, year, csv_cache_path.name)
        t0 = time.time()
        df = pd.read_csv(csv_cache_path, index_col=0, parse_dates=True)
        elapsed = time.time() - t0
        logger.info("  Loaded %d rows in %.1fs", len(df), elapsed)
        return df

    # Download from NOAA via pvlib
    import warnings

    logger.info("Downloading %s %d from SOLRAD...", station_id, year)
    t0 = time.time()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # pvlib emits warnings for missing daily files
        try:
            df, meta = pvlib.iotools.get_solrad(
                station=station_id,
                start=f"{year}-01-01",
                end=f"{year}-12-31",
            )
        except ValueError as e:
            # "No objects to concatenate" = no daily files on server for this year
            if "No objects to concatenate" in str(e) or "concat" in str(e).lower():
                logger.warning(
                    "No SOLRAD data available for %s %d (no daily files on server) — skipping",
                    station_id, year,
                )
                raise RuntimeError(
                    f"No data available for {station_id} {year}: {e}"
                ) from e
            raise RuntimeError(
                f"STOP: get_solrad() failed for {station_id} {year}: {e}"
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"STOP: get_solrad() failed for {station_id} {year}: {e}"
            ) from e

    elapsed = time.time() - t0

    if df.empty:
        logger.warning(
            "get_solrad() returned 0 rows for %s %d — skipping", station_id, year,
        )
        raise RuntimeError(
            f"No data available for {station_id} {year}: empty DataFrame"
        )

    # Validate required columns
    required_cols = {"ghi", "dni", "dhi", "solar_zenith"}
    actual_cols = set(df.columns)
    missing = required_cols - actual_cols
    if missing:
        raise RuntimeError(
            f"STOP: SOLRAD {station_id} {year} missing columns: {missing}. "
            f"Actual columns: {sorted(actual_cols)}"
        )

    logger.info(
        "Download complete: %s %d — %d rows in %.1fs",
        station_id, year, len(df), elapsed,
    )

    # Save to cache (gzipped CSV — pyarrow/fastparquet not available)
    df.to_csv(csv_cache_path, compression="gzip")
    logger.info("  Cached to %s", csv_cache_path.name)

    return df


def apply_qc(df: pd.DataFrame) -> pd.DataFrame:
    """Apply quality control filters to 1-minute irradiance data.

    Filters applied in order:
        1. Remove nighttime (solar_zenith >= 90)
        2. Remove physically impossible values
        3. Remove GHI < DHI inconsistency (only when solar_zenith < 80°)
        4. Drop rows where both GHI and DNI are NaN

    Args:
        df: Raw 1-minute DataFrame with ghi, dni, dhi, solar_zenith columns.

    Returns:
        QC-filtered DataFrame (daytime only).
    """
    n_raw = len(df)
    logger.info("QC: starting with %d rows", n_raw)

    # 1. Remove nighttime
    df = df[df["solar_zenith"] < 90].copy()
    logger.info("  After nighttime removal: %d rows", len(df))

    # 2. Remove physically impossible values
    mask_possible = (
        (df["ghi"] >= 0) & (df["ghi"] <= GHI_MAX)
        & (df["dni"] >= 0) & (df["dni"] <= DNI_MAX)
        & (df["dhi"] >= 0)
    )
    df = df[mask_possible].copy()
    logger.info("  After physical limits: %d rows", len(df))

    # 3. Remove GHI < DHI inconsistency — only at low zenith angles.
    # At high zenith angles (>= 80°), instrument noise causes GHI to read
    # slightly below DHI, which is not a real quality issue.
    mask_ghi_dhi_ok = (df["solar_zenith"] >= 80) | (df["ghi"] >= df["dhi"])
    df = df[mask_ghi_dhi_ok].copy()
    logger.info("  After GHI >= DHI check (zenith < 80° only): %d rows", len(df))

    # 4. Drop rows where both GHI and DNI are NaN
    df = df.dropna(subset=["ghi", "dni"], how="all").copy()
    logger.info("  After NaN drop: %d rows (removed %d total)", len(df), n_raw - len(df))

    return df


def aggregate_monthly(
    df: pd.DataFrame, station_id: str, network: str
) -> tuple[pd.DataFrame, int]:
    """Aggregate 1-minute QC'd data to monthly mean daily insolation.

    For each day, sums 1-minute W/m² values × (1/60 h) to get Wh/m²/day,
    then converts to kWh/m²/day. Monthly values are the mean across valid days.

    Args:
        df: QC-filtered 1-minute DataFrame (daytime only).
        station_id: Station code (e.g. "bon").
        network: Network name (e.g. "SURFRAD").

    Returns:
        Tuple of (monthly DataFrame, number of months excluded for low completeness).
    """
    # Compute daily insolation: sum of (W/m² × 1/60 h) / 1000 = kWh/m²/day
    df = df.copy()
    df["date"] = df.index.date

    daily = df.groupby("date").agg(
        ghi_wh=("ghi", lambda x: x.sum() / 60.0),
        dni_wh=("dni", lambda x: x.sum() / 60.0),
        n_minutes=("ghi", "count"),
    ).reset_index()

    daily["ghi_kwh_m2_day"] = daily["ghi_wh"] / 1000.0
    daily["dni_kwh_m2_day"] = daily["dni_wh"] / 1000.0
    daily["year"] = pd.to_datetime(daily["date"]).dt.year
    daily["month"] = pd.to_datetime(daily["date"]).dt.month

    monthly = daily.groupby(["year", "month"]).agg(
        ghi_kwh_m2_day=("ghi_kwh_m2_day", "mean"),
        dni_kwh_m2_day=("dni_kwh_m2_day", "mean"),
        n_valid_days=("date", "count"),
        total_valid_minutes=("n_minutes", "sum"),
    ).reset_index()

    days_in_month_map = {
        1: 31, 2: 29 if pd.Timestamp(daily["year"].iloc[0], 1, 1).is_leap_year else 28,
        3: 31, 4: 30, 5: 31, 6: 30,
        7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31,
    }

    monthly["calendar_days"] = monthly["month"].map(days_in_month_map)
    monthly["completeness_pct"] = (
        monthly["n_valid_days"] / monthly["calendar_days"] * 100.0
    ).round(1)

    # Flag months below completeness threshold
    low_completeness = monthly["completeness_pct"] < (MONTH_COMPLETENESS_THRESHOLD * 100)
    n_excluded = low_completeness.sum()
    if low_completeness.any():
        for _, row in monthly[low_completeness].iterrows():
            logger.warning(
                "  Month %d/%d: completeness %.1f%% < %.0f%% — EXCLUDING",
                int(row["year"]), int(row["month"]),
                row["completeness_pct"], MONTH_COMPLETENESS_THRESHOLD * 100,
            )
    monthly = monthly[~low_completeness].copy()

    monthly["station_id"] = station_id
    monthly["network"] = network
    monthly["ghi_kwh_m2_day"] = monthly["ghi_kwh_m2_day"].round(3)
    monthly["dni_kwh_m2_day"] = monthly["dni_kwh_m2_day"].round(3)

    # Select and order output columns
    monthly = monthly[
        [
            "year", "month", "station_id", "network",
            "ghi_kwh_m2_day", "dni_kwh_m2_day",
            "n_valid_days", "completeness_pct",
        ]
    ].reset_index(drop=True)

    return monthly, n_excluded


def _process_network(
    network: str,
    stations: list[dict],
    cache_dir: Path,
    raw_cache_dir: Path,
    global_t0: float,
) -> tuple[list[pd.DataFrame], list[dict], bool]:
    """Process all stations for a given network.

    Args:
        network: Network name ("SURFRAD" or "SOLRAD").
        stations: List of station records for this network.
        cache_dir: Directory for monthly CSV output.
        raw_cache_dir: Directory for raw file cache (SURFRAD .dat files).
        global_t0: Start time for runtime stop condition.

    Returns:
        Tuple of (all_monthly DataFrames, station_summaries, stopped_early).
    """
    all_monthly: list[pd.DataFrame] = []
    station_summaries: list[dict] = []
    stopped_early = False

    for station in stations:
        station_id = station["station_id"]
        station_name = station["name"]
        station_months_total = 0
        station_months_excluded = 0
        station_ghi_values: list[float] = []
        station_dni_values: list[float] = []
        station_years_processed: list[int] = []

        logger.info(
            "\n%s\nStation: %s (%s) — %s, lat=%.2f, lon=%.2f\n%s",
            "=" * 60, station_name, station_id, station["state"],
            station["latitude"], station["longitude"], "=" * 60,
        )

        for year in YEARS:
            # Check runtime stop condition
            elapsed_total = time.time() - global_t0
            if elapsed_total > MAX_RUNTIME_SECONDS:
                msg = (
                    f"STOP: Runtime exceeded {MAX_RUNTIME_SECONDS/3600:.0f} hours "
                    f"({elapsed_total/3600:.1f}h elapsed). "
                    f"Last completed: {station_id} before {year}."
                )
                logger.error(msg)
                print(f"\n{msg}")
                stopped_early = True
                break

            attempts = 0
            success = False
            while attempts < MAX_FIX_ATTEMPTS and not success:
                attempts += 1
                try:
                    # Download — dispatch to network-specific function
                    if network == "SURFRAD":
                        df_raw = download_surfrad_year(station_id, year, raw_cache_dir)
                    else:
                        df_raw = download_solrad_year(station_id, year, cache_dir)

                    # QC
                    df_qc = apply_qc(df_raw)

                    # Aggregate
                    monthly, n_excluded = aggregate_monthly(df_qc, station_id, network)

                    # Save per-station-year CSV
                    csv_path = cache_dir / f"{station_id}_{year}_monthly.csv"
                    monthly.to_csv(csv_path, index=False)
                    logger.info("Saved %s", csv_path.name)

                    all_monthly.append(monthly)
                    station_months_total += len(monthly)
                    station_months_excluded += n_excluded
                    station_ghi_values.extend(monthly["ghi_kwh_m2_day"].tolist())
                    station_dni_values.extend(monthly["dni_kwh_m2_day"].tolist())
                    station_years_processed.append(year)
                    success = True

                except RuntimeError as e:
                    err_str = str(e)
                    if "STOP:" in err_str:
                        logger.error(err_str)
                        print(f"\n{err_str}")
                        stopped_early = True
                        break
                    if attempts >= MAX_FIX_ATTEMPTS:
                        logger.error(
                            "Failed %s %d after %d attempts: %s",
                            station_id, year, attempts, e,
                        )
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
                        break
                    logger.warning(
                        "Attempt %d failed for %s %d: %s — retrying",
                        attempts, station_id, year, e,
                    )

            if stopped_early:
                break

        # Station summary
        mean_ghi = sum(station_ghi_values) / len(station_ghi_values) if station_ghi_values else 0.0
        mean_dni = sum(station_dni_values) / len(station_dni_values) if station_dni_values else 0.0
        station_summaries.append({
            "station_id": station_id,
            "name": station_name,
            "state": station["state"],
            "years_processed": len(station_years_processed),
            "year_range": f"{min(station_years_processed)}-{max(station_years_processed)}" if station_years_processed else "none",
            "months_available": station_months_total,
            "months_excluded": station_months_excluded,
            "mean_ghi_kwh_m2_day": round(mean_ghi, 3),
            "mean_dni_kwh_m2_day": round(mean_dni, 3),
        })

        if stopped_early:
            break

    return all_monthly, station_summaries, stopped_early


def _save_network_results(
    network: str,
    all_monthly: list[pd.DataFrame],
    station_summaries: list[dict],
    cache_dir: Path,
    output_dir: Path,
) -> pd.DataFrame:
    """Save master CSV and summary text file for a single network.

    Returns:
        The master DataFrame (for combining across networks).
    """
    network_lower = network.lower()

    # Master CSV
    if all_monthly:
        master = pd.concat(all_monthly, ignore_index=True)
        master_path = cache_dir / f"{network_lower}_all_monthly.csv"
        master.to_csv(master_path, index=False)
        logger.info("Saved master CSV: %s (%d rows)", master_path, len(master))
    else:
        master = pd.DataFrame()
        logger.warning("No %s monthly data to save.", network)

    # Summary text
    summary_path = output_dir / f"{network_lower}_download_summary.txt"
    lines = [
        f"{network} Ground Truth Download Summary",
        "=" * 60,
        f"Total rows in {network_lower}_all_monthly.csv: {len(master)}",
        "",
        f"{'Station':<8} {'Name':<16} {'ST':<4} {'Years':<6} {'Range':<12} "
        f"{'Months':<8} {'Excl':<6} {'GHI':>8} {'DNI':>8}",
        "-" * 80,
    ]

    for s in station_summaries:
        lines.append(
            f"{s['station_id']:<8} {s['name']:<16} {s['state']:<4} "
            f"{s['years_processed']:<6} {s['year_range']:<12} "
            f"{s['months_available']:<8} {s['months_excluded']:<6} "
            f"{s['mean_ghi_kwh_m2_day']:>8.3f} {s['mean_dni_kwh_m2_day']:>8.3f}"
        )

    lines.extend(["", "-" * 80])

    # Check Jan/Feb recovery
    if not master.empty:
        jan_feb = master[master["month"].isin([1, 2])]
        n_jan_feb = len(jan_feb)
        stations_with_winter = jan_feb["station_id"].nunique()
        lines.append(
            f"Jan/Feb QC fix: {n_jan_feb} winter months recovered "
            f"across {stations_with_winter} stations"
        )
        if n_jan_feb > 0:
            lines.append("  -> Winter data successfully recovered by relaxing GHI<DHI at zenith>=80°")
        else:
            lines.append("  -> No winter months recovered (may indicate other QC issues)")

    summary_text = "\n".join(lines) + "\n"
    summary_path.write_text(summary_text)
    logger.info("Saved summary: %s", summary_path)

    # Print to console
    print("\n" + summary_text)

    return master


def main() -> None:
    """Download all SURFRAD + SOLRAD stations × 2018–2023, QC, aggregate, save."""
    base_dir = Path(__file__).resolve().parent.parent
    cache_dir = base_dir / "cache" / "ground_truth"
    raw_cache_dir = cache_dir / "raw"
    output_dir = base_dir / "outputs"
    cache_dir.mkdir(parents=True, exist_ok=True)
    raw_cache_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    global_t0 = time.time()
    all_masters: list[pd.DataFrame] = []

    # --- SURFRAD ---
    surfrad_stations = [s for s in STATIONS if s["network"] == "SURFRAD"]
    logger.info(
        "Processing %d SURFRAD stations × %d years (%d–%d)",
        len(surfrad_stations), len(YEARS), YEARS[0], YEARS[-1],
    )
    surfrad_monthly, surfrad_summaries, surfrad_stopped = _process_network(
        "SURFRAD", surfrad_stations, cache_dir, raw_cache_dir, global_t0,
    )
    surfrad_master = _save_network_results(
        "SURFRAD", surfrad_monthly, surfrad_summaries, cache_dir, output_dir,
    )
    all_masters.append(surfrad_master)

    surfrad_elapsed = time.time() - global_t0
    logger.info("SURFRAD runtime: %.1f minutes", surfrad_elapsed / 60.0)

    # --- SOLRAD ---
    if not surfrad_stopped:
        solrad_stations = [s for s in STATIONS if s["network"] == "SOLRAD"]
        logger.info(
            "\nProcessing %d SOLRAD stations × %d years (%d–%d)",
            len(solrad_stations), len(YEARS), YEARS[0], YEARS[-1],
        )
        solrad_t0 = time.time()
        solrad_monthly, solrad_summaries, solrad_stopped = _process_network(
            "SOLRAD", solrad_stations, cache_dir, raw_cache_dir, global_t0,
        )
        solrad_master = _save_network_results(
            "SOLRAD", solrad_monthly, solrad_summaries, cache_dir, output_dir,
        )
        all_masters.append(solrad_master)

        solrad_elapsed = time.time() - solrad_t0
        logger.info("SOLRAD runtime: %.1f minutes", solrad_elapsed / 60.0)

    # --- Combined summary ---
    combined = pd.concat([m for m in all_masters if not m.empty], ignore_index=True)
    print(f"\nCombined total: {len(combined)} monthly rows "
          f"(SURFRAD={len(surfrad_master)}, SOLRAD={len(combined) - len(surfrad_master)})")

    total_elapsed = time.time() - global_t0
    logger.info("Total runtime: %.1f minutes", total_elapsed / 60.0)


if __name__ == "__main__":
    main()
