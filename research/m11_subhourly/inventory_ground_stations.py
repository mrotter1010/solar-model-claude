"""Inventory cached ground station 1-min data for M11 subhourly clipping loss refinement.

For each of the 20 stations (SURFRAD 7, SOLRAD 7, MIDC 6), across all
available years (2018–2023):
  - Count total 1-min records per year
  - Count records with valid GHI, DNI, and DHI (non-null, non-negative)
  - Calculate record coverage % (total records / expected minutes per year)
  - Calculate valid irradiance % (valid GHI+DNI+DHI / expected minutes)
  - Flag stations missing temperature or wind speed columns

Note: Valid irradiance ~50% is expected since roughly half the day is nighttime
where instruments report small negative values (instrument noise → fail >= 0).
Record coverage % is the primary data availability metric.

Outputs:
  - research/m11_subhourly/station_inventory.csv  (detail table)
  - Console summary with year recommendation
"""

import logging
import sys
import time
from pathlib import Path

import pandas as pd
import pvlib

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
M9_DIR = SCRIPT_DIR.parent / "m9_bias_correction"
CACHE_DIR = M9_DIR / "cache" / "ground_truth"

SURFRAD_RAW = CACHE_DIR / "raw"
SOLRAD_RAW = CACHE_DIR / "solrad_raw"
MIDC_RAW = CACHE_DIR / "midc_raw"

OUTPUT_CSV = SCRIPT_DIR / "station_inventory.csv"

# Station registry
sys.path.insert(0, str(M9_DIR))
from stations import STATIONS  # noqa: E402

YEARS = list(range(2018, 2024))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _minutes_in_year(year: int) -> int:
    """Return expected 1-min records in a calendar year."""
    days = 366 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 365
    return days * 1440


def _count_valid(series: pd.Series) -> int:
    """Count non-null, non-negative values."""
    return int(((series.notna()) & (series >= 0)).sum())


def _has_column_like(columns: list[str], keywords: list[str]) -> bool:
    """Check if any column name contains any of the keywords (case-insensitive)."""
    lower_cols = [c.lower() for c in columns]
    for kw in keywords:
        for c in lower_cols:
            if kw in c:
                return True
    return False


# ---------------------------------------------------------------------------
# SURFRAD: daily .dat files parsed with pvlib.iotools.read_surfrad
# ---------------------------------------------------------------------------
def inventory_surfrad_station_year(station_id: str, year: int) -> dict | None:
    """Read all cached .dat files for one SURFRAD station-year."""
    year_dir = SURFRAD_RAW / station_id / str(year)
    if not year_dir.exists():
        return None

    dat_files = sorted(year_dir.glob("*.dat"))
    if not dat_files:
        return None

    frames: list[pd.DataFrame] = []
    read_errors = 0
    for f in dat_files:
        try:
            df, _ = pvlib.iotools.read_surfrad(str(f))
            frames.append(df)
        except Exception:
            read_errors += 1

    if not frames:
        return None

    df = pd.concat(frames, axis=0).sort_index()
    cols = list(df.columns)

    return {
        "total_records": len(df),
        "valid_ghi": _count_valid(df["ghi"]),
        "valid_dni": _count_valid(df["dni"]),
        "valid_dhi": _count_valid(df["dhi"]),
        "valid_all": int(
            (
                (df["ghi"].notna()) & (df["ghi"] >= 0)
                & (df["dni"].notna()) & (df["dni"] >= 0)
                & (df["dhi"].notna()) & (df["dhi"] >= 0)
            ).sum()
        ),
        "has_temp": "temp_air" in cols and df["temp_air"].notna().any(),
        "has_wind": "wind_speed" in cols and df["wind_speed"].notna().any(),
        "n_files": len(dat_files),
        "read_errors": read_errors,
    }


# ---------------------------------------------------------------------------
# SOLRAD: yearly gzipped CSVs
# ---------------------------------------------------------------------------
def inventory_solrad_station_year(station_id: str, year: int) -> dict | None:
    """Read one cached SOLRAD CSV for a station-year."""
    csv_path = SOLRAD_RAW / f"{station_id}_{year}.csv.gz"
    if not csv_path.exists():
        return None

    df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    cols = list(df.columns)

    return {
        "total_records": len(df),
        "valid_ghi": _count_valid(df["ghi"]),
        "valid_dni": _count_valid(df["dni"]),
        "valid_dhi": _count_valid(df["dhi"]),
        "valid_all": int(
            (
                (df["ghi"].notna()) & (df["ghi"] >= 0)
                & (df["dni"].notna()) & (df["dni"] >= 0)
                & (df["dhi"].notna()) & (df["dhi"] >= 0)
            ).sum()
        ),
        "has_temp": _has_column_like(cols, ["temp_air", "air temp", "dry bulb"]),
        "has_wind": _has_column_like(cols, ["wind_speed", "wind speed"]),
    }


# ---------------------------------------------------------------------------
# MIDC: yearly gzipped CSVs (already have mapped ghi/dni/dhi columns)
# ---------------------------------------------------------------------------
def inventory_midc_station_year(station_id: str, year: int) -> dict | None:
    """Read one cached MIDC CSV for a station-year."""
    csv_path = MIDC_RAW / f"{station_id}_{year}.csv.gz"
    if not csv_path.exists():
        return None

    df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    cols = list(df.columns)

    has_ghi = "ghi" in cols
    has_dni = "dni" in cols
    has_dhi = "dhi" in cols

    return {
        "total_records": len(df),
        "valid_ghi": _count_valid(df["ghi"]) if has_ghi else 0,
        "valid_dni": _count_valid(df["dni"]) if has_dni else 0,
        "valid_dhi": _count_valid(df["dhi"]) if has_dhi else 0,
        "valid_all": int(
            (
                (df["ghi"].notna() if has_ghi else False)
                & (df["ghi"] >= 0 if has_ghi else False)
                & (df["dni"].notna() if has_dni else False)
                & (df["dni"] >= 0 if has_dni else False)
                & (df["dhi"].notna() if has_dhi else False)
                & (df["dhi"] >= 0 if has_dhi else False)
            ).sum()
        ) if (has_ghi and has_dni and has_dhi) else 0,
        "has_temp": _has_column_like(cols, ["air temp", "dry bulb", "deck dry bulb"]),
        "has_wind": _has_column_like(cols, ["wind speed"]),
    }


def _build_row(
    network: str, station: dict, year: int, result: dict | None,
) -> dict:
    """Build a result row dict from inventory result."""
    expected = _minutes_in_year(year)
    if result is None:
        return {
            "network": network,
            "station_id": station["station_id"],
            "station_name": station["name"],
            "state": station["state"],
            "year": year,
            "total_records": 0,
            "valid_ghi": 0,
            "valid_dni": 0,
            "valid_dhi": 0,
            "valid_all": 0,
            "expected_records": expected,
            "record_coverage_pct": 0.0,
            "valid_irradiance_pct": 0.0,
            "has_temp": False,
            "has_wind": False,
        }
    return {
        "network": network,
        "station_id": station["station_id"],
        "station_name": station["name"],
        "state": station["state"],
        "year": year,
        "total_records": result["total_records"],
        "valid_ghi": result["valid_ghi"],
        "valid_dni": result["valid_dni"],
        "valid_dhi": result["valid_dhi"],
        "valid_all": result["valid_all"],
        "expected_records": expected,
        "record_coverage_pct": round(result["total_records"] / expected * 100.0, 2),
        "valid_irradiance_pct": round(result["valid_all"] / expected * 100.0, 2),
        "has_temp": result["has_temp"],
        "has_wind": result["has_wind"],
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    """Run inventory across all 20 stations and produce summary."""
    t0 = time.time()

    # Verify raw data directories exist and are non-empty
    for label, path in [
        ("SURFRAD", SURFRAD_RAW),
        ("SOLRAD", SOLRAD_RAW),
        ("MIDC", MIDC_RAW),
    ]:
        if not path.exists():
            logger.error("STOP: %s raw directory missing: %s", label, path)
            sys.exit(1)
        contents = list(path.iterdir())
        if not contents:
            logger.error("STOP: %s raw directory is empty: %s", label, path)
            sys.exit(1)
        logger.info("%s raw dir OK: %s (%d items)", label, path, len(contents))

    rows: list[dict] = []

    # --- SURFRAD ---
    surfrad_stations = [s for s in STATIONS if s["network"] == "SURFRAD"]
    logger.info("Inventorying %d SURFRAD stations...", len(surfrad_stations))
    for station in surfrad_stations:
        sid = station["station_id"]
        for year in YEARS:
            logger.info("  SURFRAD %s %d...", sid, year)
            t_start = time.time()
            result = inventory_surfrad_station_year(sid, year)
            elapsed = time.time() - t_start
            row = _build_row("SURFRAD", station, year, result)
            rows.append(row)
            if result is None:
                logger.warning("  SURFRAD %s %d: no data found", sid, year)
            else:
                logger.info(
                    "    %s %d: %d records (%.1f%% coverage), %.1f%% valid irrad (%.1fs)",
                    sid, year, result["total_records"],
                    row["record_coverage_pct"], row["valid_irradiance_pct"], elapsed,
                )

    # --- SOLRAD ---
    solrad_stations = [s for s in STATIONS if s["network"] == "SOLRAD"]
    logger.info("Inventorying %d SOLRAD stations...", len(solrad_stations))
    for station in solrad_stations:
        sid = station["station_id"]
        for year in YEARS:
            result = inventory_solrad_station_year(sid, year)
            row = _build_row("SOLRAD", station, year, result)
            rows.append(row)
            if result is None:
                logger.warning("  SOLRAD %s %d: no cached file", sid, year)
            else:
                logger.info(
                    "  SOLRAD %s %d: %d records (%.1f%% coverage), %.1f%% valid irrad",
                    sid, year, result["total_records"],
                    row["record_coverage_pct"], row["valid_irradiance_pct"],
                )

    # --- MIDC ---
    midc_stations = [s for s in STATIONS if s["network"] == "MIDC"]
    logger.info("Inventorying %d MIDC stations...", len(midc_stations))
    for station in midc_stations:
        sid = station["station_id"]
        for year in YEARS:
            result = inventory_midc_station_year(sid, year)
            row = _build_row("MIDC", station, year, result)
            rows.append(row)
            if result is None:
                logger.warning("  MIDC %s %d: no cached file", sid, year)
            else:
                logger.info(
                    "  MIDC %s %d: %d records (%.1f%% coverage), %.1f%% valid irrad",
                    sid, year, result["total_records"],
                    row["record_coverage_pct"], row["valid_irradiance_pct"],
                )

    # --- Build summary ---
    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_CSV, index=False)
    logger.info("Saved inventory to %s", OUTPUT_CSV)

    elapsed = time.time() - t0
    logger.info("Total inventory time: %.1f minutes", elapsed / 60)

    # --- Print summary tables ---
    print("\n" + "=" * 120)
    print("STATION INVENTORY SUMMARY — 1-minute ground station data")
    print("=" * 120)

    # Pivot: record coverage % by station × year
    pivot_coverage = df.pivot_table(
        index=["network", "station_id", "station_name", "state"],
        columns="year",
        values="record_coverage_pct",
        aggfunc="first",
    )

    # Pivot: valid irradiance % by station × year
    pivot_valid = df.pivot_table(
        index=["network", "station_id", "station_name", "state"],
        columns="year",
        values="valid_irradiance_pct",
        aggfunc="first",
    )

    # Temperature and wind availability (any year)
    temp_wind = df.groupby(["network", "station_id"]).agg(
        has_temp=("has_temp", "any"),
        has_wind=("has_wind", "any"),
    )

    # --- Table 1: Record Coverage ---
    print("\nTable 1: Record Coverage % (total 1-min records / expected minutes per year):\n")
    header = (
        f"{'Network':<10} {'Station':<10} {'Name':<22} {'ST':<4} "
        + "".join(f"{y:>8}" for y in YEARS)
        + f"  {'Temp':>4} {'Wind':>4}"
    )
    print(header)
    print("-" * len(header))

    for (network, sid, name, state), row in pivot_coverage.iterrows():
        tw = temp_wind.loc[(network, sid)]
        line = f"{network:<10} {sid:<10} {name:<22} {state:<4} "
        for y in YEARS:
            val = row.get(y, 0.0)
            if pd.isna(val) or val == 0:
                line += f"{'---':>8}"
            elif val < 90:
                line += f"{val:>7.1f}*"
            else:
                line += f"{val:>8.1f}"
        line += f"  {'Y' if tw['has_temp'] else 'N':>4} {'Y' if tw['has_wind'] else 'N':>4}"
        print(line)

    print("-" * len(header))
    print("* = below 90% record coverage\n")

    # --- Table 2: Valid Irradiance ---
    print("Table 2: Valid Irradiance % (records with GHI>=0 & DNI>=0 & DHI>=0 / expected):")
    print("(~50% is normal — nighttime instrument readings are negative)\n")
    header2 = (
        f"{'Network':<10} {'Station':<10} {'Name':<22} {'ST':<4} "
        + "".join(f"{y:>8}" for y in YEARS)
    )
    print(header2)
    print("-" * len(header2))

    for (network, sid, name, state), row in pivot_valid.iterrows():
        line = f"{network:<10} {sid:<10} {name:<22} {state:<4} "
        for y in YEARS:
            val = row.get(y, 0.0)
            if pd.isna(val) or val == 0:
                line += f"{'---':>8}"
            elif val < 30:
                line += f"{val:>7.1f}!"
            else:
                line += f"{val:>8.1f}"
        print(line)

    print("-" * len(header2))
    print("! = notably low valid irradiance (< 30%)\n")

    # --- Year recommendation ---
    print("=" * 100)
    print("YEAR RECOMMENDATION (based on record coverage %)")
    print("=" * 100)

    for year in YEARS:
        year_data = df[df["year"] == year]
        n_stations = len(year_data)
        n_with_data = (year_data["total_records"] > 0).sum()
        min_cov = year_data["record_coverage_pct"].min()
        median_cov = year_data["record_coverage_pct"].median()
        n_above_90 = (year_data["record_coverage_pct"] >= 90).sum()
        min_valid = year_data[year_data["total_records"] > 0]["valid_irradiance_pct"].min()
        print(
            f"  {year}: {n_with_data}/{n_stations} stations, "
            f"coverage min={min_cov:.1f}% median={median_cov:.1f}%, "
            f"{n_above_90}/{n_stations} >= 90% coverage, "
            f"min valid irrad={min_valid:.1f}%"
        )

    # Find best year: highest minimum record_coverage across all 20 stations
    best_year = None
    best_min_cov = -1.0
    for year in YEARS:
        year_data = df[df["year"] == year]
        if (year_data["total_records"] == 0).any():
            continue
        min_cov = year_data["record_coverage_pct"].min()
        if min_cov > best_min_cov:
            best_min_cov = min_cov
            best_year = year

    print()
    if best_year is not None and best_min_cov >= 90:
        print(f"RECOMMENDED YEAR: {best_year} (min record coverage = {best_min_cov:.1f}%)")
        year_data = df[df["year"] == best_year]
        below_90 = year_data[year_data["record_coverage_pct"] < 90]
        if not below_90.empty:
            print(f"\n  Stations below 90% coverage in {best_year}:")
            for _, r in below_90.iterrows():
                print(
                    f"    {r['network']} {r['station_id']} ({r['station_name']}): "
                    f"{r['record_coverage_pct']:.1f}%"
                )
    else:
        if best_year is not None:
            print(
                f"Best available year with all 20 stations: {best_year} "
                f"(min record coverage = {best_min_cov:.1f}%)"
            )
        else:
            print("WARNING: No single year has data for all 20 stations!")

        # Show which stations are missing per year
        for year in YEARS:
            year_data = df[df["year"] == year]
            missing = year_data[year_data["total_records"] == 0]
            if not missing.empty:
                stations_str = ", ".join(
                    f"{r['station_id']}({r['network']})" for _, r in missing.iterrows()
                )
                print(f"  {year} missing: {stations_str}")

        # Stations below 90% in the best year
        if best_year is not None:
            year_data = df[df["year"] == best_year]
            below_90 = year_data[year_data["record_coverage_pct"] < 90]
            if not below_90.empty:
                print(f"\n  Stations below 90% record coverage in {best_year}:")
                for _, r in below_90.iterrows():
                    print(
                        f"    {r['network']} {r['station_id']} ({r['station_name']}): "
                        f"{r['record_coverage_pct']:.1f}%"
                    )

        # Alternative analysis: what if we drop problematic stations?
        print("\n--- ALTERNATIVE: Drop stations with missing years ---")
        missing_stations: set[str] = set()
        for sid in df["station_id"].unique():
            station_data = df[df["station_id"] == sid]
            if (station_data["total_records"] == 0).any():
                net = station_data["network"].iloc[0]
                name = station_data["station_name"].iloc[0]
                missing_years = station_data[station_data["total_records"] == 0]["year"].tolist()
                missing_stations.add(sid)
                print(f"  {net} {sid} ({name}): missing years {missing_years}")

        # Find best year if we drop those stations
        df_clean = df[~df["station_id"].isin(missing_stations)]
        n_remaining = df_clean["station_id"].nunique()
        print(f"\n  With {len(missing_stations)} station(s) dropped, {n_remaining} remain:")
        for year in YEARS:
            year_data = df_clean[df_clean["year"] == year]
            if year_data.empty:
                continue
            min_cov = year_data["record_coverage_pct"].min()
            n_above_90 = (year_data["record_coverage_pct"] >= 90).sum()
            n_total = len(year_data)
            below_90_stations = year_data[year_data["record_coverage_pct"] < 90]
            below_list = ""
            if not below_90_stations.empty:
                below_list = " — below 90%: " + ", ".join(
                    f"{r['station_id']}({r['record_coverage_pct']:.0f}%)"
                    for _, r in below_90_stations.iterrows()
                )
            print(
                f"    {year}: min={min_cov:.1f}%, {n_above_90}/{n_total} >= 90%{below_list}"
            )

    # --- Stations missing temp or wind ---
    print("\n" + "=" * 80)
    print("STATIONS MISSING TEMPERATURE OR WIND SPEED")
    print("=" * 80)
    no_temp = temp_wind[~temp_wind["has_temp"]]
    no_wind = temp_wind[~temp_wind["has_wind"]]

    if no_temp.empty and no_wind.empty:
        print("  All stations have both temperature and wind speed data.")
    else:
        if not no_temp.empty:
            print("  Missing temperature:")
            for (net, sid), _ in no_temp.iterrows():
                name = df[(df["network"] == net) & (df["station_id"] == sid)]["station_name"].iloc[0]
                print(f"    {net} {sid} ({name})")
        if not no_wind.empty:
            print("  Missing wind speed:")
            for (net, sid), _ in no_wind.iterrows():
                name = df[(df["network"] == net) & (df["station_id"] == sid)]["station_name"].iloc[0]
                print(f"    {net} {sid} ({name})")

    print()


if __name__ == "__main__":
    main()
