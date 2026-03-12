"""Download NSRDB data for 6 MIDC station locations and create combined master files.

Reuses fetch_nsrdb_year() and aggregate_nsrdb_monthly() from download_nsrdb.py.
Only fetches years with corresponding ground truth data for each station.
After download, combines SURFRAD + SOLRAD + MIDC into unified master files.
"""

import logging
import sys
import time
from pathlib import Path

import pandas as pd

# Add scripts dir and project root for imports
_scripts_dir = Path(__file__).resolve().parent
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_scripts_dir))
sys.path.insert(0, str(_project_root))

from stations import STATIONS  # noqa: E402
from download_nsrdb import (  # noqa: E402
    fetch_nsrdb_year,
    aggregate_nsrdb_monthly,
    REQUEST_DELAY_SECONDS,
    MAX_FIX_ATTEMPTS,
)

# Add src root for NSRDB credentials
_src_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_src_root))
from src.climate.nsrdb_client import NSRDBClient  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BASE_DIR = _project_root
CACHE_DIR = BASE_DIR / "cache" / "nsrdb"
GT_CACHE_DIR = BASE_DIR / "cache" / "ground_truth"
OUTPUT_DIR = BASE_DIR / "outputs"

# Year coverage per MIDC station (matching ground truth availability)
MIDC_YEAR_COVERAGE: dict[str, list[int]] = {
    "BMS": [2018, 2019, 2020, 2021, 2022, 2023],
    "UAT": [2018, 2019, 2020, 2021, 2022, 2023],
    "UOSMRL": [2018, 2019, 2020, 2021, 2022, 2023],
    "UNLV": [2018, 2019, 2020, 2021, 2022, 2023],
    "ULL": [2019, 2020, 2021, 2022, 2023],
    "UTPASRL": [2018, 2019, 2020, 2022, 2023],
}


def download_midc_nsrdb() -> None:
    """Download NSRDB data for all 6 MIDC station locations."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    midc_stations = [s for s in STATIONS if s["network"] == "MIDC"]

    # Credentials
    client = NSRDBClient()
    api_key = client.api_key
    email = client.email
    logger.info("NSRDB credentials: api_key=%s..., email=%s", api_key[:4], email)

    total_station_years = sum(len(yrs) for yrs in MIDC_YEAR_COVERAGE.values())
    logger.info(
        "Fetching NSRDB for %d MIDC stations (%d station-years)",
        len(midc_stations), total_station_years,
    )

    global_t0 = time.time()
    all_monthly: list[pd.DataFrame] = []
    station_summaries: list[dict] = []
    failed: list[str] = []
    n_fetched = 0

    for station in midc_stations:
        station_id = station["station_id"]
        years = MIDC_YEAR_COVERAGE[station_id]
        lat = station["latitude"]
        lon = station["longitude"]

        station_ghi: list[float] = []
        station_dni: list[float] = []
        station_years_ok: list[int] = []

        logger.info(
            "\n%s\n%s (%s) — %s, lat=%.3f, lon=%.3f, years=%s\n%s",
            "=" * 60, station["name"], station_id, station["state"],
            lat, lon, years, "=" * 60,
        )

        for year in years:
            attempts = 0
            success = False

            while attempts < MAX_FIX_ATTEMPTS and not success:
                attempts += 1
                try:
                    df_raw = fetch_nsrdb_year(
                        station_id, lat, lon, year, api_key, email, CACHE_DIR,
                    )
                    monthly = aggregate_nsrdb_monthly(
                        df_raw, station_id, "MIDC", year,
                    )

                    csv_path = CACHE_DIR / f"{station_id}_{year}_monthly.csv"
                    monthly.to_csv(csv_path, index=False)

                    all_monthly.append(monthly)
                    station_ghi.extend(monthly["ghi_kwh_m2_day"].tolist())
                    station_dni.extend(monthly["dni_kwh_m2_day"].tolist())
                    station_years_ok.append(year)
                    n_fetched += 1
                    success = True

                    # Rate limit delay only for fresh API hits
                    cache_path = CACHE_DIR / f"{station_id}_{year}.csv"
                    file_age = time.time() - cache_path.stat().st_mtime
                    if file_age < 5:
                        time.sleep(REQUEST_DELAY_SECONDS)

                except RuntimeError as e:
                    if "STOP:" in str(e):
                        logger.error(str(e))
                        failed.append(f"{station_id} {year}: {e}")
                        # Save what we have so far, then return
                        _save_midc_nsrdb(
                            all_monthly, station_summaries, failed, n_fetched, global_t0,
                        )
                        return
                    if attempts >= MAX_FIX_ATTEMPTS:
                        logger.error("Failed %s %d after %d attempts: %s",
                                     station_id, year, attempts, e)
                        failed.append(f"{station_id} {year}")
                        break
                    logger.warning("Attempt %d failed for %s %d: %s — retrying",
                                   attempts, station_id, year, e)

                except Exception as e:
                    if attempts >= MAX_FIX_ATTEMPTS:
                        logger.error("Failed %s %d after %d attempts: %s",
                                     station_id, year, attempts, e)
                        failed.append(f"{station_id} {year}")
                        break
                    logger.warning("Attempt %d failed for %s %d: %s — retrying",
                                   attempts, station_id, year, e)

        mean_ghi = sum(station_ghi) / len(station_ghi) if station_ghi else 0.0
        mean_dni = sum(station_dni) / len(station_dni) if station_dni else 0.0
        station_summaries.append({
            "station_id": station_id,
            "name": station["name"],
            "state": station["state"],
            "years_ok": len(station_years_ok),
            "years_attempted": len(years),
            "year_range": (
                f"{min(station_years_ok)}-{max(station_years_ok)}"
                if station_years_ok else "none"
            ),
            "mean_ghi": round(mean_ghi, 3),
            "mean_dni": round(mean_dni, 3),
        })

    _save_midc_nsrdb(all_monthly, station_summaries, failed, n_fetched, global_t0)


def _save_midc_nsrdb(
    all_monthly: list[pd.DataFrame],
    station_summaries: list[dict],
    failed: list[str],
    n_fetched: int,
    global_t0: float,
) -> None:
    """Save MIDC NSRDB master CSV and summary."""
    elapsed = time.time() - global_t0

    if all_monthly:
        master = pd.concat(all_monthly, ignore_index=True)
        master_path = CACHE_DIR / "nsrdb_midc_monthly.csv"
        master.to_csv(master_path, index=False)
        logger.info("Saved MIDC NSRDB master: %s (%d rows)", master_path, len(master))
    else:
        master = pd.DataFrame()

    total_possible = sum(len(yrs) for yrs in MIDC_YEAR_COVERAGE.values())
    lines = [
        "NSRDB Download Summary (MIDC stations)",
        "=" * 70,
        f"Download time: {elapsed:.0f}s ({elapsed / 60:.1f} min)",
        f"Station-years fetched: {n_fetched}/{total_possible}",
        f"Total rows in nsrdb_midc_monthly.csv: {len(master)}",
        "",
        f"{'Station':<10} {'Name':<22} {'ST':<4} {'Yrs':<5} {'Range':<12} "
        f"{'GHI':>8} {'DNI':>8}",
        "-" * 75,
    ]
    for s in station_summaries:
        lines.append(
            f"{s['station_id']:<10} {s['name']:<22} {s['state']:<4} "
            f"{s['years_ok']}/{s['years_attempted']:<3} {s['year_range']:<12} "
            f"{s['mean_ghi']:>8.3f} {s['mean_dni']:>8.3f}"
        )
    if failed:
        lines.extend(["", f"Failures ({len(failed)}):"])
        for f in failed:
            lines.append(f"  {f}")
    lines.append("")

    summary_text = "\n".join(lines) + "\n"
    summary_path = OUTPUT_DIR / "nsrdb_midc_download_summary.txt"
    summary_path.write_text(summary_text)
    logger.info("Saved summary: %s", summary_path)
    print("\n" + summary_text)


def create_combined_masters() -> None:
    """Create unified master files across all 20 stations (SURFRAD + SOLRAD + MIDC).

    Ground truth: concatenates surfrad_all_monthly.csv + solrad_all_monthly.csv
                  + midc_all_monthly.csv
    NSRDB: concatenates nsrdb_all_monthly.csv + nsrdb_midc_monthly.csv
    """
    logger.info("\nCreating combined master files across all 20 stations...")

    # --- Ground truth ---
    gt_files = [
        GT_CACHE_DIR / "surfrad_all_monthly.csv",
        GT_CACHE_DIR / "solrad_all_monthly.csv",
        GT_CACHE_DIR / "midc_all_monthly.csv",
    ]
    gt_frames = []
    for f in gt_files:
        if f.exists():
            df = pd.read_csv(f)
            logger.info("  Ground truth: %s — %d rows", f.name, len(df))
            gt_frames.append(df)
        else:
            logger.warning("  Missing: %s", f)

    if gt_frames:
        gt_combined = pd.concat(gt_frames, ignore_index=True)
        gt_path = GT_CACHE_DIR / "all_networks_monthly.csv"
        gt_combined.to_csv(gt_path, index=False)
        logger.info(
            "Saved combined ground truth: %s (%d rows, %d stations)",
            gt_path.name, len(gt_combined), gt_combined["station_id"].nunique(),
        )
    else:
        gt_combined = pd.DataFrame()

    # --- NSRDB ---
    nsrdb_files = [
        CACHE_DIR / "nsrdb_all_monthly.csv",
        CACHE_DIR / "nsrdb_midc_monthly.csv",
    ]
    nsrdb_frames = []
    for f in nsrdb_files:
        if f.exists():
            df = pd.read_csv(f)
            logger.info("  NSRDB: %s — %d rows", f.name, len(df))
            nsrdb_frames.append(df)
        else:
            logger.warning("  Missing: %s", f)

    if nsrdb_frames:
        nsrdb_combined = pd.concat(nsrdb_frames, ignore_index=True)
        nsrdb_path = CACHE_DIR / "nsrdb_all_stations_monthly.csv"
        nsrdb_combined.to_csv(nsrdb_path, index=False)
        logger.info(
            "Saved combined NSRDB: %s (%d rows, %d stations)",
            nsrdb_path.name, len(nsrdb_combined),
            nsrdb_combined["station_id"].nunique(),
        )
    else:
        nsrdb_combined = pd.DataFrame()

    # Summary
    print("\n" + "=" * 60)
    print("COMBINED MASTER FILES")
    print("=" * 60)
    if not gt_combined.empty:
        print(f"  Ground truth: {len(gt_combined)} rows, "
              f"{gt_combined['station_id'].nunique()} stations")
        for net in sorted(gt_combined["network"].unique()):
            n = len(gt_combined[gt_combined["network"] == net])
            print(f"    {net}: {n} rows")
    if not nsrdb_combined.empty:
        print(f"  NSRDB: {len(nsrdb_combined)} rows, "
              f"{nsrdb_combined['station_id'].nunique()} stations")
        for net in sorted(nsrdb_combined["network"].unique()):
            n = len(nsrdb_combined[nsrdb_combined["network"] == net])
            print(f"    {net}: {n} rows")
    print()


def main() -> None:
    """Download NSRDB for MIDC stations, then create combined master files."""
    download_midc_nsrdb()
    create_combined_masters()


if __name__ == "__main__":
    main()
