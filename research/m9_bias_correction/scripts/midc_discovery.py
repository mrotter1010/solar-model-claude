"""Probe candidate MIDC stations for data availability and column structure.

Fetches sample data from each station, identifies GHI/DNI/DHI columns,
checks year coverage (2018-2023), and produces a viability report.
"""

import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import requests
from pvlib.iotools import read_midc_raw_data_from_nrel
from pvlib.iotools.midc import MIDC_VARIABLE_MAP

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TIMEOUT = 60

# ---------------------------------------------------------------------------
# Candidate stations
# ---------------------------------------------------------------------------
@dataclass
class CandidateStation:
    site_id: str
    name: str
    state: str
    latitude: float
    longitude: float
    elevation_m: float


CANDIDATES = [
    CandidateStation("BMS", "NREL SRRL", "CO", 39.74, -105.18, 1829.0),
    CandidateStation("UAT", "Univ of Arizona OASIS", "AZ", 32.23, -110.95, 786.0),
    CandidateStation("UFL", "Univ of Florida", "FL", 29.68, -82.27, 29.0),
    CandidateStation("UOSMRL", "Univ of Oregon SRML", "OR", 44.05, -123.07, 150.0),
    CandidateStation("HSU", "Cal Poly Humboldt SoRMS", "CA", 40.87, -124.08, 57.0),
    CandidateStation("UNLV", "Univ of Nevada LV", "NV", 36.11, -115.14, 615.0),
    CandidateStation("ULL", "Univ of Louisiana Laf", "LA", 30.21, -92.02, 12.0),
    CandidateStation("UTPASRL", "UT Rio Grande Valley", "TX", 26.31, -98.17, 30.0),
    CandidateStation("STAC", "SolarTAC", "CO", 39.75, -104.62, 1672.0),
]


# ---------------------------------------------------------------------------
# Discovery result
# ---------------------------------------------------------------------------
@dataclass
class StationResult:
    station: CandidateStation
    status: str = "UNKNOWN"
    all_columns: list[str] = field(default_factory=list)
    ghi_col: str = ""
    dni_col: str = ""
    dhi_col: str = ""
    zenith_col: str = ""
    resolution_min: float = 0.0
    n_valid_rows: int = 0
    mean_daytime_ghi: float = 0.0
    mean_daytime_dni: float = 0.0
    years_available: list[int] = field(default_factory=list)
    variable_map: dict[str, str] = field(default_factory=dict)
    notes: str = ""


# ---------------------------------------------------------------------------
# Column identification
# ---------------------------------------------------------------------------
GHI_KEYWORDS = ["global horiz", "global cmp", "global horizontal"]
DNI_KEYWORDS = ["direct normal", "direct chp", "direct nip"]
DHI_KEYWORDS = ["diffuse horiz", "diffuse cm", "diffuse horizontal"]
ZENITH_KEYWORDS = ["zenith", "zen angle", "solar zen"]


def identify_columns(columns: list[str]) -> dict[str, str]:
    """Map raw column names to standard irradiance variables.

    Returns:
        Dict with keys 'ghi', 'dni', 'dhi', 'zenith' mapped to raw column names.
        Only includes keys where a match was found.
    """
    mapping: dict[str, str] = {}
    cols_lower = {c: c.lower() for c in columns}

    # GHI — prefer ventilated/corrected instruments
    for col, lower in cols_lower.items():
        if any(kw in lower for kw in GHI_KEYWORDS) and "w/m^2" in lower:
            # Prefer vent/cor versions
            if "vent" in lower or "cor" in lower or "ghi" not in mapping:
                mapping["ghi"] = col

    # DNI — prefer CHP1 over NIP, prefer non-calculated
    for col, lower in cols_lower.items():
        if any(kw in lower for kw in DNI_KEYWORDS) and "w/m^2" in lower:
            if "calc" not in lower:  # Prefer measured over calculated
                if "chp1" in lower or "dni" not in mapping:
                    mapping["dni"] = col
    # Fallback: accept calculated DNI if no measured found
    if "dni" not in mapping:
        for col, lower in cols_lower.items():
            if any(kw in lower for kw in DNI_KEYWORDS) and "w/m^2" in lower:
                mapping["dni"] = col

    # DHI
    for col, lower in cols_lower.items():
        if any(kw in lower for kw in DHI_KEYWORDS) and "w/m^2" in lower:
            if "vent" in lower or "cor" in lower or "dhi" not in mapping:
                mapping["dhi"] = col

    # Zenith — exclude columns already matched as irradiance
    already_matched = set(mapping.values())
    for col, lower in cols_lower.items():
        if col in already_matched:
            continue
        if any(kw in lower for kw in ZENITH_KEYWORDS) and "w/m^2" not in lower:
            mapping["zenith"] = col
            break

    return mapping


def detect_resolution(df: pd.DataFrame) -> float:
    """Detect data resolution in minutes from DatetimeIndex."""
    if len(df) < 2:
        return 0.0
    diffs = pd.Series(df.index).diff().dropna()
    median_diff = diffs.median()
    return median_diff.total_seconds() / 60.0


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------
def fetch_sample(site_id: str) -> pd.DataFrame | None:
    """Fetch one month of sample data. Try June 2022, fall back to Jan 2022.

    Returns:
        DataFrame or None if both attempts fail.
    """
    for period_label, start, end in [
        ("June 2022", "2022-06-01", "2022-06-30"),
        ("Jan 2022", "2022-01-01", "2022-01-31"),
    ]:
        try:
            logger.info("  Fetching %s for %s...", period_label, site_id)
            df = read_midc_raw_data_from_nrel(
                site=site_id, start=start, end=end, timeout=TIMEOUT,
            )
            if len(df) > 0:
                logger.info("  -> Got %d rows", len(df))
                return df
            logger.warning("  -> Empty DataFrame for %s", period_label)
        except requests.Timeout:
            logger.warning("  -> TIMEOUT fetching %s", period_label)
        except Exception as e:
            logger.warning("  -> Error fetching %s: %s", period_label, e)
    return None


def check_year_coverage(site_id: str) -> list[int]:
    """Check which years 2018-2023 have data by fetching Jan 1 of each year."""
    available: list[int] = []
    for year in range(2018, 2024):
        date_str = f"{year}-01-01"
        try:
            df = read_midc_raw_data_from_nrel(
                site=site_id, start=date_str, end=date_str, timeout=30,
            )
            if len(df) > 0:
                available.append(year)
                logger.info("    Year %d: OK (%d rows)", year, len(df))
            else:
                logger.info("    Year %d: empty", year)
        except requests.Timeout:
            logger.info("    Year %d: TIMEOUT", year)
        except Exception as e:
            logger.info("    Year %d: error (%s)", year, str(e)[:80])
    return available


# ---------------------------------------------------------------------------
# Main probe
# ---------------------------------------------------------------------------
def probe_station(station: CandidateStation) -> StationResult:
    """Probe a single MIDC station for data availability and structure."""
    result = StationResult(station=station)
    logger.info("=" * 60)
    logger.info("Probing %s (%s)", station.site_id, station.name)

    # Fetch sample month
    df = fetch_sample(station.site_id)
    if df is None:
        result.status = "UNAVAILABLE"
        result.notes = "Could not fetch sample data (both June and Jan 2022 failed)"
        return result

    # Record columns
    result.all_columns = list(df.columns)
    logger.info("  Columns (%d): %s", len(df.columns), result.all_columns)

    # Identify irradiance columns
    col_map = identify_columns(result.all_columns)
    logger.info("  Identified mapping: %s", col_map)

    result.ghi_col = col_map.get("ghi", "")
    result.dni_col = col_map.get("dni", "")
    result.dhi_col = col_map.get("dhi", "")
    result.zenith_col = col_map.get("zenith", "")

    if not result.ghi_col or not result.dni_col:
        result.status = "NO_GHI_DNI"
        result.notes = f"Missing: {'GHI' if not result.ghi_col else ''} {'DNI' if not result.dni_col else ''}".strip()
        return result

    # Data resolution
    result.resolution_min = detect_resolution(df)
    logger.info("  Resolution: %.0f min", result.resolution_min)

    # Quick daytime stats (solar zenith < 85° proxy: GHI > 10 W/m²)
    ghi_series = df[result.ghi_col]
    dni_series = df[result.dni_col]
    daytime = ghi_series > 10
    result.n_valid_rows = int(daytime.sum())
    if result.n_valid_rows > 0:
        result.mean_daytime_ghi = float(ghi_series[daytime].mean())
        result.mean_daytime_dni = float(dni_series[daytime].mean())
    logger.info(
        "  Daytime stats: %d rows, mean GHI=%.1f W/m², mean DNI=%.1f W/m²",
        result.n_valid_rows, result.mean_daytime_ghi, result.mean_daytime_dni,
    )

    # Flag suspicious DNI (calculated values can be garbage)
    if result.mean_daytime_dni < 0:
        logger.warning("  WARNING: negative mean daytime DNI — calculated column may be unreliable")

    # Check year coverage
    logger.info("  Checking year coverage 2018-2023...")
    result.years_available = check_year_coverage(station.site_id)
    logger.info("  Years available: %s", result.years_available)

    # Build variable map (guard against same column matching two variables)
    result.variable_map = {}
    used_cols: set[str] = set()
    for col, std_name in [
        (result.ghi_col, "ghi"),
        (result.dni_col, "dni"),
        (result.dhi_col, "dhi"),
        (result.zenith_col, "solar_zenith"),
    ]:
        if col and col not in used_cols:
            result.variable_map[col] = std_name
            used_cols.add(col)

    # Check pvlib built-in map
    has_pvlib_map = station.site_id in MIDC_VARIABLE_MAP

    # Determine status
    full_coverage = all(y in result.years_available for y in range(2018, 2024))
    if full_coverage:
        result.status = "VIABLE"
        result.notes = "Full 2018-2023 coverage"
    elif len(result.years_available) >= 4:
        result.status = "PARTIAL"
        missing = [y for y in range(2018, 2024) if y not in result.years_available]
        result.notes = f"Missing years: {missing}"
    elif len(result.years_available) > 0:
        result.status = "LIMITED"
        result.notes = f"Only {len(result.years_available)} years available"
    else:
        result.status = "NO_YEAR_DATA"
        result.notes = "Sample month worked but no Jan 1 probes succeeded"

    if has_pvlib_map:
        result.notes += " | pvlib has built-in variable_map"

    return result


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def format_report(results: list[StationResult]) -> str:
    """Format the discovery report as a text table and variable maps."""
    lines: list[str] = []
    lines.append("MIDC STATION DISCOVERY REPORT")
    lines.append("=" * 120)
    lines.append(f"Date: probe run against MIDC API")
    lines.append(f"Stations probed: {len(results)}")
    lines.append("")

    # Summary counts
    viable = [r for r in results if r.status == "VIABLE"]
    partial = [r for r in results if r.status == "PARTIAL"]
    failed = [r for r in results if r.status in ("UNAVAILABLE", "NO_GHI_DNI", "NO_YEAR_DATA", "LIMITED")]
    lines.append(f"Viable (full 2018-2023): {len(viable)}")
    lines.append(f"Partial (4+ years):      {len(partial)}")
    lines.append(f"Failed/Limited:          {len(failed)}")
    lines.append("")

    # Discovery table
    lines.append("DISCOVERY TABLE")
    lines.append("-" * 120)
    header = (
        f"{'ID':<10} {'Name':<24} {'ST':>2} {'Lat':>7} {'Lon':>8} "
        f"{'Status':<12} {'GHI Column':<35} {'DNI Column':<30} {'Res':>5} "
        f"{'Years':>6}"
    )
    lines.append(header)
    lines.append("-" * 120)

    for r in results:
        s = r.station
        years_str = ",".join(str(y)[-2:] for y in r.years_available) if r.years_available else "none"
        res_str = f"{r.resolution_min:.0f}m" if r.resolution_min > 0 else "—"
        lines.append(
            f"{s.site_id:<10} {s.name:<24} {s.state:>2} {s.latitude:>7.2f} {s.longitude:>8.2f} "
            f"{r.status:<12} {r.ghi_col:<35} {r.dni_col:<30} {res_str:>5} "
            f"{years_str:>6}"
        )

    lines.append("-" * 120)
    lines.append("")

    # Detailed per-station results
    lines.append("DETAILED STATION RESULTS")
    lines.append("=" * 120)

    for r in results:
        s = r.station
        lines.append("")
        lines.append(f"--- {s.site_id}: {s.name} ({s.state}) ---")
        lines.append(f"  Location: {s.latitude}, {s.longitude}, elev {s.elevation_m}m")
        lines.append(f"  Status: {r.status}")
        lines.append(f"  Notes: {r.notes}")

        if r.all_columns:
            lines.append(f"  All columns ({len(r.all_columns)}):")
            for col in r.all_columns:
                tag = ""
                if col == r.ghi_col:
                    tag = " <- GHI"
                elif col == r.dni_col:
                    tag = " <- DNI"
                elif col == r.dhi_col:
                    tag = " <- DHI"
                elif col == r.zenith_col:
                    tag = " <- ZENITH"
                lines.append(f"    {col}{tag}")

        if r.ghi_col:
            lines.append(f"  Resolution: {r.resolution_min:.0f} min")
            lines.append(f"  Daytime rows: {r.n_valid_rows}")
            lines.append(f"  Mean daytime GHI: {r.mean_daytime_ghi:.1f} W/m²")
            lines.append(f"  Mean daytime DNI: {r.mean_daytime_dni:.1f} W/m²")

        if r.years_available:
            lines.append(f"  Years available: {r.years_available}")

    # Variable maps for viable/partial stations
    usable = [r for r in results if r.status in ("VIABLE", "PARTIAL") and r.variable_map]
    if usable:
        lines.append("")
        lines.append("")
        lines.append("PROPOSED VARIABLE MAPS")
        lines.append("=" * 80)
        lines.append("(For use with pvlib.iotools.read_midc_raw_data_from_nrel)")
        lines.append("")

        for r in usable:
            has_builtin = r.station.site_id in MIDC_VARIABLE_MAP
            label = " (pvlib built-in available)" if has_builtin else " (custom)"
            lines.append(f'# {r.station.site_id}: {r.station.name}{label}')
            lines.append(f'"{r.station.site_id}": {{')
            for raw_col, std_name in r.variable_map.items():
                lines.append(f'    {raw_col!r}: {std_name!r},')
            lines.append("}")
            lines.append("")

    # Recommendations
    lines.append("")
    lines.append("RECOMMENDATIONS")
    lines.append("=" * 80)

    if viable:
        lines.append(f"Add {len(viable)} fully viable stations immediately:")
        for r in viable:
            lines.append(
                f"  {r.station.site_id} ({r.station.name}, {r.station.state}) "
                f"— {r.station.latitude}N, {r.resolution_min:.0f}-min data"
            )

    if partial:
        lines.append(f"\n{len(partial)} stations with partial coverage (may still be useful):")
        for r in partial:
            lines.append(f"  {r.station.site_id} ({r.station.name}) — {r.notes}")

    if failed:
        lines.append(f"\n{len(failed)} stations to skip:")
        for r in failed:
            lines.append(f"  {r.station.site_id} ({r.station.name}) — {r.status}: {r.notes}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    """Probe all candidate MIDC stations and produce discovery report."""
    logger.info("MIDC Station Discovery — probing %d candidates", len(CANDIDATES))
    start_time = time.time()

    results: list[StationResult] = []
    for station in CANDIDATES:
        result = probe_station(station)
        results.append(result)
        logger.info("  => %s: %s", station.site_id, result.status)

    elapsed = time.time() - start_time
    logger.info("Discovery complete in %.0f seconds", elapsed)

    # Format and save report
    report = format_report(results)
    print("\n" + report)

    report_path = OUTPUT_DIR / "midc_discovery_report.txt"
    report_path.write_text(report)
    logger.info("Saved report to %s", report_path)


if __name__ == "__main__":
    main()
