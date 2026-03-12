"""Probe candidate ground truth stations for viability.

Tests 4 candidate stations across two APIs (MIDC + SRML) to assess
data availability, column naming, resolution, and year coverage.
Exploratory only — no data is saved to cache.
"""

import logging
import sys
import traceback
import warnings
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
from pvlib.iotools import read_midc_raw_data_from_nrel, get_srml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "outputs"

TIMEOUT = 120

# -----------------------------------------------------------------------
# Candidate station definitions
# -----------------------------------------------------------------------
CANDIDATES = [
    {
        "station_id": "ORNL",
        "name": "Oak Ridge National Lab",
        "state": "TN",
        "latitude": 35.93,
        "longitude": -84.33,
        "elevation_m": 330,
        "api": "MIDC",
        "gap_filled": "Southeast",
    },
    {
        "station_id": "UFL",
        "name": "University of Florida",
        "state": "FL",
        "latitude": 29.68,
        "longitude": -82.27,
        "elevation_m": 29,
        "api": "MIDC",
        "gap_filled": "Southeast/Florida",
    },
    {
        "station_id": "OSUBUR",
        "name": "Oregon State Burns",
        "state": "OR",
        "latitude": 43.52,
        "longitude": -119.53,
        "elevation_m": 1265,
        "api": "MIDC",
        "gap_filled": "Inland PNW / high desert",
    },
    {
        "station_id": "CY",
        "name": "Cheney",
        "state": "WA",
        "latitude": 47.49,
        "longitude": -117.58,
        "elevation_m": 785,
        "api": "SRML",
        "gap_filled": "Inland PNW / eastern WA",
    },
    {
        "station_id": "BU",
        "name": "Burns",
        "state": "OR",
        "latitude": 43.52,
        "longitude": -119.53,
        "elevation_m": 1265,
        "api": "SRML",
        "gap_filled": "Inland PNW / high desert",
    },
    {
        "station_id": "HE",
        "name": "Hermiston",
        "state": "OR",
        "latitude": 45.82,
        "longitude": -119.28,
        "elevation_m": 180,
        "api": "SRML",
        "gap_filled": "Inland PNW / Columbia Basin",
    },
    {
        "station_id": "SL",
        "name": "Silver Lake",
        "state": "OR",
        "latitude": 43.12,
        "longitude": -121.06,
        "elevation_m": 1340,
        "api": "SRML",
        "gap_filled": "Inland PNW / high desert",
    },
]

# Months to try (in order)
TRIAL_MONTHS = [
    ("2022-06-01", "2022-06-30"),
    ("2021-06-01", "2021-06-30"),
]

# Years for coverage check (1-day pulls)
COVERAGE_CHECK_YEARS = [2018, 2020, 2022, 2023]


@dataclass
class StationResult:
    """Result of probing a single station."""

    station_id: str
    name: str
    state: str
    api: str
    gap_filled: str
    status: str = "UNKNOWN"
    ghi_col: str = ""
    dni_col: str = ""
    resolution: str = ""
    years_available: str = ""
    mean_june_ghi: float = 0.0
    mean_june_dni: float = 0.0
    notes: str = ""
    all_columns: list[str] = field(default_factory=list)


# -----------------------------------------------------------------------
# Fetch helpers
# -----------------------------------------------------------------------
def fetch_midc(station_id: str, start: str, end: str) -> pd.DataFrame:
    """Fetch MIDC data, suppressing warnings."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return read_midc_raw_data_from_nrel(
            site=station_id, start=start, end=end, timeout=TIMEOUT,
        )


def fetch_srml(station_id: str, start: str, end: str) -> tuple[pd.DataFrame, dict]:
    """Fetch SRML data, suppressing warnings."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return get_srml(
            station=station_id, start=start, end=end,
            url="http://solardata.uoregon.edu/download/Archive/",
        )


def try_fetch(candidate: dict) -> tuple[pd.DataFrame | None, str]:
    """Try fetching trial month data. Returns (df, notes) or (None, error_notes)."""
    api = candidate["api"]
    sid = candidate["station_id"]
    notes_parts: list[str] = []

    for start, end in TRIAL_MONTHS:
        month_label = start[:7]
        try:
            logger.info("  Trying %s %s...", sid, month_label)
            if api == "MIDC":
                df = fetch_midc(sid, start, end)
            else:
                df, _meta = fetch_srml(sid, start, end)

            if df is not None and len(df) > 0:
                logger.info("  Success: %d rows", len(df))
                return df, f"Fetched {month_label}"
            else:
                notes_parts.append(f"{month_label}: empty")
                logger.warning("  %s %s: empty response", sid, month_label)

        except Exception as e:
            err_msg = str(e)[:200]
            notes_parts.append(f"{month_label}: {err_msg}")
            logger.warning("  %s %s failed: %s", sid, month_label, err_msg)
            # Print full traceback for diagnostics
            logger.debug(traceback.format_exc())

    return None, "; ".join(notes_parts)


# -----------------------------------------------------------------------
# Column identification
# -----------------------------------------------------------------------
def identify_columns_midc(columns: list[str]) -> tuple[str, str]:
    """Identify GHI and DNI columns from MIDC column names."""
    ghi_col = ""
    dni_col = ""

    for col in columns:
        lower = col.lower()
        # Skip columns that are clearly not irradiance
        if "w/m^2" not in lower and "w/m²" not in lower:
            continue

        if ("global" in lower or "ghi" in lower) and not ghi_col:
            ghi_col = col
        elif "direct normal" in lower and not dni_col:
            dni_col = col
        elif "direct" in lower and "normal" not in lower:
            # Skip "direct" without "normal" — could be direct horizontal
            pass

    return ghi_col, dni_col


def identify_columns_srml(columns: list[str]) -> tuple[str, str]:
    """Identify GHI and DNI columns from SRML column names."""
    ghi_col = ""
    dni_col = ""

    for col in columns:
        lower = col.lower()
        if lower in ("ghi", "ghi_0") or col == "ghi_0":
            ghi_col = col
        elif lower in ("dni", "dni_0") or col == "dni_0":
            dni_col = col

    return ghi_col, dni_col


# -----------------------------------------------------------------------
# Data quality assessment
# -----------------------------------------------------------------------
def assess_data(
    df: pd.DataFrame, ghi_col: str, dni_col: str,
) -> tuple[float, float, str, int]:
    """Compute mean daytime GHI/DNI, resolution, and valid row count.

    Returns:
        (mean_ghi, mean_dni, resolution_str, n_valid_rows)
    """
    # Infer resolution from index
    if hasattr(df.index, "freq") and df.index.freq is not None:
        resolution = str(df.index.freq)
    elif len(df) > 2:
        diffs = pd.Series(df.index).diff().dropna()
        median_diff = diffs.median()
        total_seconds = median_diff.total_seconds()
        if total_seconds <= 61:
            resolution = "1-min"
        elif total_seconds <= 901:
            resolution = f"{int(total_seconds / 60)}-min"
        elif total_seconds <= 3601:
            resolution = "1-hr"
        else:
            resolution = f"{int(total_seconds)}s"
    else:
        resolution = "unknown"

    # Filter to daytime (GHI > 10 W/m²)
    if ghi_col and ghi_col in df.columns:
        daytime = df[df[ghi_col] > 10].copy()
    else:
        daytime = df

    n_valid = len(daytime)

    mean_ghi = daytime[ghi_col].mean() if ghi_col and ghi_col in df.columns else 0.0
    mean_dni = daytime[dni_col].mean() if dni_col and dni_col in df.columns else 0.0

    return float(mean_ghi), float(mean_dni), resolution, n_valid


# -----------------------------------------------------------------------
# Year coverage check
# -----------------------------------------------------------------------
def check_year_coverage(candidate: dict) -> list[int]:
    """Check which years have data via 1-day pulls on Jan 1."""
    api = candidate["api"]
    sid = candidate["station_id"]
    available: list[int] = []

    for year in COVERAGE_CHECK_YEARS:
        start = f"{year}-01-01"
        end = f"{year}-01-01"
        try:
            if api == "MIDC":
                df = fetch_midc(sid, start, end)
            else:
                # SRML returns full months, so Jan 1 pull gets all of January
                df, _ = fetch_srml(sid, start, end)

            if df is not None and len(df) > 0:
                available.append(year)
                logger.info("    Year %d: OK (%d rows)", year, len(df))
            else:
                logger.info("    Year %d: empty", year)
        except Exception as e:
            logger.info("    Year %d: failed (%s)", year, str(e)[:80])

    return available


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------
def main() -> None:
    """Probe all candidate stations and generate report."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    results: list[StationResult] = []
    # Track unique stations per (api, error_type) pair
    error_stations: dict[str, set[str]] = {}
    blocked_apis: set[str] = set()

    logger.info("Probing %d candidate stations\n", len(CANDIDATES))

    for candidate in CANDIDATES:
        sid = candidate["station_id"]
        api = candidate["api"]

        # Skip stations whose API has a systemic failure
        if api in blocked_apis:
            logger.info("Skipping %s (%s) — %s API has systemic failure", sid, candidate["name"], api)
            result = StationResult(
                station_id=sid, name=candidate["name"], state=candidate["state"],
                api=api, gap_filled=candidate["gap_filled"],
                status="SKIPPED", notes=f"Skipped — {api} API systemic failure",
            )
            results.append(result)
            continue

        logger.info(
            "%s\n%s (%s) — %s %s, lat=%.2f, lon=%.2f, API=%s\n%s",
            "=" * 60, candidate["name"], sid, candidate["state"],
            candidate["gap_filled"], candidate["latitude"],
            candidate["longitude"], api, "=" * 60,
        )

        result = StationResult(
            station_id=sid,
            name=candidate["name"],
            state=candidate["state"],
            api=api,
            gap_filled=candidate["gap_filled"],
        )

        # Step a: fetch trial month
        df, fetch_notes = try_fetch(candidate)

        if df is None:
            result.status = "UNAVAILABLE"
            result.notes = fetch_notes

            # Track unique stations per (api, error_type) pair
            for note_part in fetch_notes.split("; "):
                if ":" in note_part:
                    err_type = note_part.split(": ", 1)[-1][:50]
                    key = f"{api}:{err_type}"
                    if key not in error_stations:
                        error_stations[key] = set()
                    error_stations[key].add(sid)

            # Check: more than 2 unique stations with same error on same API
            for key, station_set in error_stations.items():
                if len(station_set) > 2 and key.startswith(f"{api}:"):
                    logger.error(
                        "SYSTEMIC FAILURE: %d stations failed with: %s (%s)",
                        len(station_set), key, ", ".join(sorted(station_set)),
                    )
                    logger.error(
                        "Skipping remaining %s stations. Continuing with other APIs.",
                        api,
                    )
                    blocked_apis.add(api)

            results.append(result)
            logger.info("  -> UNAVAILABLE: %s\n", fetch_notes)
            continue

        # Step b: print full column list
        cols = list(df.columns)
        result.all_columns = cols
        logger.info("  Columns (%d): %s", len(cols), cols)

        # Step c: identify GHI and DNI columns
        if api == "MIDC":
            ghi_col, dni_col = identify_columns_midc(cols)
        else:
            ghi_col, dni_col = identify_columns_srml(cols)

        result.ghi_col = ghi_col
        result.dni_col = dni_col
        logger.info("  GHI column: %s", ghi_col or "(not found)")
        logger.info("  DNI column: %s", dni_col or "(not found)")

        if not ghi_col and not dni_col:
            result.status = "NO_GHI_DNI"
            result.notes = f"{fetch_notes}; columns: {cols}"
            results.append(result)
            logger.info("  -> NO GHI/DNI columns identified\n")
            continue

        # Step d: compute data quality metrics
        mean_ghi, mean_dni, resolution, n_valid = assess_data(df, ghi_col, dni_col)
        result.mean_june_ghi = round(mean_ghi, 1)
        result.mean_june_dni = round(mean_dni, 1)
        result.resolution = resolution
        logger.info(
            "  Mean daytime GHI=%.1f W/m², DNI=%.1f W/m², resolution=%s, valid rows=%d",
            mean_ghi, mean_dni, resolution, n_valid,
        )

        # Step e: year coverage check
        logger.info("  Checking year coverage...")
        years_ok = check_year_coverage(candidate)
        result.years_available = (
            ", ".join(str(y) for y in years_ok) if years_ok else "none"
        )
        logger.info("  Years available: %s", result.years_available)

        # Determine status
        has_ghi = bool(ghi_col)
        has_dni = bool(dni_col)
        has_years = len(years_ok) >= 2

        if has_ghi and has_dni and has_years:
            result.status = "VIABLE"
            result.notes = fetch_notes
        elif has_ghi and has_dni:
            result.status = "PARTIAL"
            result.notes = f"{fetch_notes}; limited year coverage ({len(years_ok)} years)"
        elif has_ghi or has_dni:
            missing = "DNI" if not has_dni else "GHI"
            result.status = "PARTIAL"
            result.notes = f"{fetch_notes}; missing {missing}"
        else:
            result.status = "NO_GHI_DNI"
            result.notes = fetch_notes

        results.append(result)
        logger.info("  -> %s\n", result.status)

    _save_report(results)


def _save_report(results: list[StationResult]) -> None:
    """Build and save the discovery report."""
    lines: list[str] = []
    lines.append("NEW STATION DISCOVERY REPORT")
    lines.append("=" * 100)
    lines.append("")

    # Summary counts
    viable = [r for r in results if r.status == "VIABLE"]
    partial = [r for r in results if r.status == "PARTIAL"]
    unavailable = [r for r in results if r.status in ("UNAVAILABLE", "NO_GHI_DNI")]
    skipped = [r for r in results if r.status == "SKIPPED"]
    lines.append(f"Stations probed: {len(results)}")
    lines.append(f"  VIABLE: {len(viable)}")
    lines.append(f"  PARTIAL: {len(partial)}")
    lines.append(f"  UNAVAILABLE: {len(unavailable)}")
    if skipped:
        lines.append(f"  SKIPPED (systemic API failure): {len(skipped)}")
    lines.append("")

    # Discovery table
    hdr = (
        f"{'Station':<10} {'API':<6} {'Location':<28} {'Status':<13} "
        f"{'GHI Column':<30} {'DNI Column':<30} {'Res':<6} "
        f"{'Years':<22} {'June GHI':>9} {'June DNI':>9} {'Notes'}"
    )
    lines.append(hdr)
    lines.append("-" * 180)

    for r in results:
        location = f"{r.name}, {r.state}"
        ghi_col = r.ghi_col[:28] if r.ghi_col else "-"
        dni_col = r.dni_col[:28] if r.dni_col else "-"
        ghi_str = f"{r.mean_june_ghi:>7.1f}" if r.mean_june_ghi else "      -"
        dni_str = f"{r.mean_june_dni:>7.1f}" if r.mean_june_dni else "      -"
        lines.append(
            f"{r.station_id:<10} {r.api:<6} {location:<28} {r.status:<13} "
            f"{ghi_col:<30} {dni_col:<30} {r.resolution:<6} "
            f"{r.years_available:<22} {ghi_str}  {dni_str}  {r.notes}"
        )

    lines.append("")

    # Column listings for each station
    lines.append("FULL COLUMN LISTINGS")
    lines.append("-" * 80)
    for r in results:
        if r.all_columns:
            lines.append(f"\n{r.station_id} ({r.name}):")
            for i, col in enumerate(r.all_columns):
                lines.append(f"  [{i:>2}] {col}")
    lines.append("")

    # Geographic gap analysis
    lines.append("GEOGRAPHIC GAP ANALYSIS")
    lines.append("-" * 80)
    for r in viable:
        lines.append(f"  {r.station_id} ({r.name}, {r.state}) -> fills: {r.gap_filled}")
    for r in partial:
        lines.append(f"  {r.station_id} ({r.name}, {r.state}) -> fills: {r.gap_filled} [PARTIAL]")
    if not viable and not partial:
        lines.append("  No viable stations found.")
    lines.append("")

    report_text = "\n".join(lines) + "\n"
    report_path = OUTPUT_DIR / "new_station_discovery_report.txt"
    report_path.write_text(report_text)
    logger.info("Saved report: %s", report_path)
    print("\n" + report_text)


if __name__ == "__main__":
    main()
