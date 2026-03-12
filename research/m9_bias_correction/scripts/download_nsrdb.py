"""Download NSRDB hourly data for all ground station locations and aggregate to monthly.

Fetches co-located NSRDB satellite data for the same 14 station locations
(SURFRAD + SOLRAD) and years (2018–2023) as the ground truth data, enabling
direct bias comparison.

Features:
    - Raw CSV caching (never re-download a station-year already on disk)
    - Sequential requests with 2-second delay (NSRDB rate limits)
    - Aggregation to monthly mean daily insolation (kWh/m²/day)
    - Credentials read from the production NSRDB client (not hardcoded here)
"""

import io
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import requests

# Add project root so we can import the station registry
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))
from stations import STATIONS  # noqa: E402

# Add project root so we can import the NSRDB client for credentials
# scripts/ → m9_bias_correction/ → research/ → solar-model-claude/
_src_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_src_root))
from src.climate.nsrdb_client import NSRDBClient, NSRDB_PSM_URL  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# NSRDB request parameters
NSRDB_ATTRIBUTES = "ghi,dni,dhi,solar_zenith_angle,air_temperature,wind_speed"
REQUEST_DELAY_SECONDS = 2
MAX_FIX_ATTEMPTS = 2

# Years to process
YEARS = list(range(2018, 2024))


def fetch_nsrdb_year(
    station_id: str,
    lat: float,
    lon: float,
    year: int,
    api_key: str,
    email: str,
    cache_dir: Path,
) -> pd.DataFrame:
    """Fetch one year of hourly NSRDB data for a single location.

    Args:
        station_id: Station code for cache naming.
        lat: Latitude.
        lon: Longitude.
        year: Calendar year.
        api_key: NREL API key.
        email: Email for NREL API.
        cache_dir: Directory for cached CSV files.

    Returns:
        DataFrame with hourly irradiance data.

    Raises:
        RuntimeError: If the API returns an error or unexpected data.
    """
    cache_path = cache_dir / f"{station_id}_{year}.csv"

    # Check cache
    if cache_path.exists():
        logger.info("Loading %s %d from cache", station_id, year)
        # NSRDB CSV: row 0 = metadata names, row 1 = metadata values,
        # row 2 = data column names, row 3+ = data. Skip first 2 rows.
        df = pd.read_csv(cache_path, skiprows=[0, 1])
        return df

    # Build request
    params = {
        "api_key": api_key,
        "wkt": f"POINT({lon} {lat})",
        "names": str(year),
        "attributes": NSRDB_ATTRIBUTES,
        "utc": "true",
        "leap_day": "true",
        "interval": "60",
        "email": email,
    }

    logger.info("Fetching NSRDB %s %d (lat=%.3f, lon=%.3f)...", station_id, year, lat, lon)
    t0 = time.time()

    resp = requests.get(NSRDB_PSM_URL, params=params, timeout=120)

    elapsed = time.time() - t0

    # Check for HTTP errors
    if resp.status_code != 200:
        raise RuntimeError(
            f"STOP: NSRDB API error for {station_id} {year}: "
            f"HTTP {resp.status_code}\n{resp.text[:500]}"
        )

    # Check for API-level errors (sometimes returns 200 with error JSON)
    content = resp.text
    if content.startswith("{") or '"error"' in content[:200]:
        raise RuntimeError(
            f"STOP: NSRDB API returned error for {station_id} {year}:\n{content[:500]}"
        )

    # Save raw CSV to cache
    cache_path.write_text(content)

    # Parse: row 0 = metadata names, row 1 = metadata values,
    # row 2 = data column names, row 3+ = data. Skip first 2 rows.
    df = pd.read_csv(io.StringIO(content), skiprows=[0, 1])

    # Validate required columns
    required = {"GHI", "DNI", "DHI", "Solar Zenith Angle"}
    actual = set(df.columns)
    missing = required - actual
    if missing:
        raise RuntimeError(
            f"STOP: NSRDB {station_id} {year} missing columns: {missing}. "
            f"Actual columns: {sorted(actual)}\n"
            f"First 5 rows:\n{df.head()}"
        )

    logger.info(
        "  Downloaded %s %d: %d rows in %.1fs, mean GHI=%.1f W/m²",
        station_id, year, len(df), elapsed, df["GHI"].mean(),
    )

    return df


def aggregate_nsrdb_monthly(
    df: pd.DataFrame, station_id: str, network: str, year: int,
) -> pd.DataFrame:
    """Aggregate hourly NSRDB data to monthly mean daily insolation.

    Args:
        df: Hourly NSRDB DataFrame with GHI, DNI, Solar Zenith Angle columns.
        station_id: Station code.
        network: Network name.
        year: Calendar year.

    Returns:
        Monthly DataFrame with ghi_kwh_m2_day, dni_kwh_m2_day columns.
    """
    df = df.copy()

    # Build datetime index from Year, Month, Day, Hour, Minute columns
    df["datetime"] = pd.to_datetime(
        df[["Year", "Month", "Day", "Hour", "Minute"]]
    )
    df["date"] = df["datetime"].dt.date

    # Filter to daytime only
    df = df[df["Solar Zenith Angle"] < 90].copy()

    # Daily insolation: sum of hourly W/m² × 1 h = Wh/m²/day → kWh/m²/day
    daily = df.groupby("date").agg(
        ghi_wh=("GHI", "sum"),
        dni_wh=("DNI", "sum"),
        n_hours=("GHI", "count"),
    ).reset_index()

    daily["ghi_kwh_m2_day"] = daily["ghi_wh"] / 1000.0
    daily["dni_kwh_m2_day"] = daily["dni_wh"] / 1000.0
    daily["month"] = pd.to_datetime(daily["date"]).dt.month

    # Monthly averages
    monthly = daily.groupby("month").agg(
        ghi_kwh_m2_day=("ghi_kwh_m2_day", "mean"),
        dni_kwh_m2_day=("dni_kwh_m2_day", "mean"),
        n_valid_days=("date", "count"),
    ).reset_index()

    monthly["year"] = year
    monthly["station_id"] = station_id
    monthly["network"] = network
    monthly["ghi_kwh_m2_day"] = monthly["ghi_kwh_m2_day"].round(3)
    monthly["dni_kwh_m2_day"] = monthly["dni_kwh_m2_day"].round(3)

    monthly = monthly[
        [
            "year", "month", "station_id", "network",
            "ghi_kwh_m2_day", "dni_kwh_m2_day", "n_valid_days",
        ]
    ].reset_index(drop=True)

    return monthly


def main() -> None:
    """Download NSRDB data for all 14 stations × 2018–2023."""
    base_dir = Path(__file__).resolve().parent.parent
    cache_dir = base_dir / "cache" / "nsrdb"
    output_dir = base_dir / "outputs"
    cache_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Read credentials from the production NSRDB client
    client = NSRDBClient()
    api_key = client.api_key
    email = client.email
    logger.info(
        "NSRDB credentials: api_key=%s..., email=%s",
        api_key[:4], email,
    )

    logger.info(
        "Processing %d stations × %d years (%d–%d) = %d requests",
        len(STATIONS), len(YEARS), YEARS[0], YEARS[-1],
        len(STATIONS) * len(YEARS),
    )

    global_t0 = time.time()
    all_monthly: list[pd.DataFrame] = []
    station_summaries: list[dict] = []
    failed_station_years: list[str] = []
    n_fetched = 0

    for station in STATIONS:
        station_id = station["station_id"]
        station_name = station["name"]
        network = station["network"]
        lat = station["latitude"]
        lon = station["longitude"]

        station_ghi_values: list[float] = []
        station_dni_values: list[float] = []
        station_years_ok: list[int] = []

        logger.info(
            "\n%s\n%s (%s) — %s, lat=%.3f, lon=%.3f\n%s",
            "=" * 60, station_name, station_id, station["state"],
            lat, lon, "=" * 60,
        )

        for year in YEARS:
            attempts = 0
            success = False

            while attempts < MAX_FIX_ATTEMPTS and not success:
                attempts += 1
                try:
                    # Fetch
                    df_raw = fetch_nsrdb_year(
                        station_id, lat, lon, year, api_key, email, cache_dir,
                    )

                    # Aggregate
                    monthly = aggregate_nsrdb_monthly(df_raw, station_id, network, year)

                    # Save per-station-year CSV
                    csv_path = cache_dir / f"{station_id}_{year}_monthly.csv"
                    monthly.to_csv(csv_path, index=False)

                    all_monthly.append(monthly)
                    station_ghi_values.extend(monthly["ghi_kwh_m2_day"].tolist())
                    station_dni_values.extend(monthly["dni_kwh_m2_day"].tolist())
                    station_years_ok.append(year)
                    n_fetched += 1
                    success = True

                    # Delay between requests (skip if loaded from cache)
                    cache_path = cache_dir / f"{station_id}_{year}.csv"
                    # Only delay if we actually hit the API (file was just created)
                    file_age = time.time() - cache_path.stat().st_mtime
                    if file_age < 5:
                        time.sleep(REQUEST_DELAY_SECONDS)

                except RuntimeError as e:
                    err_str = str(e)
                    if "STOP:" in err_str:
                        logger.error(err_str)
                        print(f"\n{err_str}")
                        _save_results(
                            all_monthly, station_summaries, failed_station_years,
                            n_fetched, cache_dir, output_dir, global_t0,
                        )
                        return
                    if attempts >= MAX_FIX_ATTEMPTS:
                        logger.error(
                            "Failed %s %d after %d attempts: %s",
                            station_id, year, attempts, e,
                        )
                        failed_station_years.append(f"{station_id}_{year}")
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
                        failed_station_years.append(f"{station_id}_{year}")
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
            "network": network,
            "state": station["state"],
            "years_processed": len(station_years_ok),
            "year_range": (
                f"{min(station_years_ok)}-{max(station_years_ok)}"
                if station_years_ok else "none"
            ),
            "mean_ghi_kwh_m2_day": round(mean_ghi, 3),
            "mean_dni_kwh_m2_day": round(mean_dni, 3),
        })

    _save_results(
        all_monthly, station_summaries, failed_station_years,
        n_fetched, cache_dir, output_dir, global_t0,
    )


def _save_results(
    all_monthly: list[pd.DataFrame],
    station_summaries: list[dict],
    failed_station_years: list[str],
    n_fetched: int,
    cache_dir: Path,
    output_dir: Path,
    global_t0: float,
) -> None:
    """Save master CSV and summary."""
    elapsed = time.time() - global_t0

    # Master CSV
    if all_monthly:
        master = pd.concat(all_monthly, ignore_index=True)
        master_path = cache_dir / "nsrdb_all_monthly.csv"
        master.to_csv(master_path, index=False)
        logger.info("Saved master CSV: %s (%d rows)", master_path, len(master))
    else:
        master = pd.DataFrame()

    # Summary
    total_possible = len(STATIONS) * len(YEARS)
    lines = [
        "NSRDB Download Summary",
        "=" * 60,
        f"Total download time: {elapsed/60:.1f} minutes",
        f"Station-years fetched: {n_fetched}/{total_possible}",
        f"Total rows in nsrdb_all_monthly.csv: {len(master)}",
        "",
        f"{'Station':<8} {'Name':<16} {'Net':<8} {'ST':<4} {'Years':<6} "
        f"{'Range':<12} {'GHI':>8} {'DNI':>8}",
        "-" * 80,
    ]

    for s in station_summaries:
        lines.append(
            f"{s['station_id']:<8} {s['name']:<16} {s['network']:<8} "
            f"{s['state']:<4} {s['years_processed']:<6} {s['year_range']:<12} "
            f"{s['mean_ghi_kwh_m2_day']:>8.3f} {s['mean_dni_kwh_m2_day']:>8.3f}"
        )

    if failed_station_years:
        lines.extend(["", f"Failed station-years ({len(failed_station_years)}):"])
        for f in failed_station_years:
            lines.append(f"  - {f}")

    lines.append("")
    summary_text = "\n".join(lines) + "\n"

    summary_path = output_dir / "nsrdb_download_summary.txt"
    summary_path.write_text(summary_text)
    logger.info("Saved summary: %s", summary_path)

    print("\n" + summary_text)

    total_elapsed = time.time() - global_t0
    logger.info("Total runtime: %.1f minutes", total_elapsed / 60.0)


if __name__ == "__main__":
    main()
