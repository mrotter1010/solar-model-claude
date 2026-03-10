"""Download, QC-filter, and aggregate SURFRAD ground truth irradiance data.

Uses pvlib.iotools.read_surfrad() to fetch 1-minute resolution data from
NOAA's SURFRAD network, applies quality control filters, and aggregates
to monthly mean daily insolation (kWh/m²/day).

Proof-of-concept: Bondville (bon), 2020.
"""

import logging
import time
from datetime import date
from pathlib import Path

import pandas as pd
import pvlib

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


def download_surfrad_year(
    station_id: str, year: int, cache_dir: Path
) -> pd.DataFrame:
    """Download one year of 1-minute SURFRAD data for a single station.

    Args:
        station_id: SURFRAD station code (e.g. "bon").
        year: Calendar year to download.
        cache_dir: Directory for cached output (not used for intermediate
            files — downloads are streamed directly from NOAA).

    Returns:
        DataFrame with 1-minute irradiance data, UTC datetime index.
    """
    start_date = date(year, 1, 1)
    end_date = date(year, 12, 31)
    n_days = (end_date - start_date).days + 1
    yy = str(year)[2:]

    frames: list[pd.DataFrame] = []
    failed_days: list[date] = []

    logger.info(
        "Downloading %s %d: %d days from SURFRAD...", station_id, year, n_days
    )
    t0 = time.time()

    for day_offset in range(n_days):
        current_date = start_date + pd.Timedelta(days=day_offset)
        doy = current_date.timetuple().tm_yday
        url = SURFRAD_URL_TEMPLATE.format(
            station_id=station_id, year=year, yy=yy, doy=doy,
        )

        try:
            df_day, _ = pvlib.iotools.read_surfrad(url)
            frames.append(df_day)
        except Exception as e:
            failed_days.append(current_date)
            if "404" in str(e) or "Not Found" in str(e):
                logger.warning("  Day %s (DOY %03d): 404 — skipping", current_date, doy)
            else:
                logger.warning("  Day %s (DOY %03d): %s — skipping", current_date, doy, e)

        # Progress every 30 days
        if (day_offset + 1) % 30 == 0:
            elapsed = time.time() - t0
            logger.info(
                "  Progress: %d/%d days (%.0fs elapsed)", day_offset + 1, n_days, elapsed
            )

    elapsed = time.time() - t0
    logger.info(
        "Download complete: %d/%d days successful in %.1fs (%.1f days failed)",
        n_days - len(failed_days), n_days, elapsed, len(failed_days),
    )

    if not frames:
        raise RuntimeError(f"No data downloaded for {station_id} {year}")

    df = pd.concat(frames, axis=0)
    df = df.sort_index()
    return df


def apply_qc(df: pd.DataFrame) -> pd.DataFrame:
    """Apply quality control filters to 1-minute irradiance data.

    Filters applied in order:
        1. Remove nighttime (solar_zenith >= 90)
        2. Remove physically impossible values
        3. Remove GHI < DHI inconsistency
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

    # 3. Remove GHI < DHI inconsistency
    df = df[df["ghi"] >= df["dhi"]].copy()
    logger.info("  After GHI >= DHI check: %d rows", len(df))

    # 4. Drop rows where both GHI and DNI are NaN
    df = df.dropna(subset=["ghi", "dni"], how="all").copy()
    logger.info("  After NaN drop: %d rows (removed %d total)", len(df), n_raw - len(df))

    return df


def aggregate_monthly(
    df: pd.DataFrame, station_id: str, network: str
) -> pd.DataFrame:
    """Aggregate 1-minute QC'd data to monthly mean daily insolation.

    For each day, sums 1-minute W/m² values × (1/60 h) to get Wh/m²/day,
    then converts to kWh/m²/day. Monthly values are the mean across valid days.

    Args:
        df: QC-filtered 1-minute DataFrame (daytime only).
        station_id: Station code (e.g. "bon").
        network: Network name (e.g. "SURFRAD").

    Returns:
        DataFrame with columns: year, month, station_id, network,
        ghi_kwh_m2_day, dni_kwh_m2_day, n_valid_days, completeness_pct.
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

    # Compute expected daytime minutes per month from the raw data
    # (before QC, we need total daytime minutes — approximate from daily counts)
    # Use the actual valid minutes we have per day as-is.

    monthly = daily.groupby(["year", "month"]).agg(
        ghi_kwh_m2_day=("ghi_kwh_m2_day", "mean"),
        dni_kwh_m2_day=("dni_kwh_m2_day", "mean"),
        n_valid_days=("date", "count"),
        total_valid_minutes=("n_minutes", "sum"),
    ).reset_index()

    # Expected daytime minutes per month: approximate as n_valid_days × mean minutes
    # Better approach: use calendar days per month and typical daytime fraction
    # For completeness, compare valid minutes to calendar-day count × avg daytime
    days_in_month_map = {
        1: 31, 2: 29 if pd.Timestamp(daily["year"].iloc[0], 1, 1).is_leap_year else 28,
        3: 31, 4: 30, 5: 31, 6: 30,
        7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31,
    }

    # Estimate expected daytime minutes: use average daily valid minutes × calendar days
    # A simpler completeness: n_valid_days / calendar_days_in_month
    monthly["calendar_days"] = monthly["month"].map(days_in_month_map)
    monthly["completeness_pct"] = (
        monthly["n_valid_days"] / monthly["calendar_days"] * 100.0
    ).round(1)

    # Flag months below completeness threshold
    low_completeness = monthly["completeness_pct"] < (MONTH_COMPLETENESS_THRESHOLD * 100)
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

    return monthly


def main() -> None:
    """Proof-of-concept: download Bondville 2020, QC, aggregate, save."""
    station_id = "bon"
    network = "SURFRAD"
    year = 2020
    cache_dir = Path(__file__).resolve().parent.parent / "cache" / "ground_truth"
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Download
    df_raw = download_surfrad_year(station_id, year, cache_dir)
    logger.info("Raw data: %d rows, columns: %s", len(df_raw), list(df_raw.columns))

    # QC
    df_qc = apply_qc(df_raw)

    # Aggregate
    monthly = aggregate_monthly(df_qc, station_id, network)

    # Save
    output_path = cache_dir / f"{station_id}_{year}_monthly.csv"
    monthly.to_csv(output_path, index=False)
    logger.info("Saved monthly CSV to %s", output_path)

    # Print summary
    print("\n" + "=" * 72)
    print(f"SURFRAD {station_id.upper()} {year} — Monthly Mean Daily Insolation")
    print("=" * 72)
    print(monthly.to_string(index=False))
    print("=" * 72)
    print(f"Months included: {len(monthly)}/12")
    print()


if __name__ == "__main__":
    main()
