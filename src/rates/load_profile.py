"""Load profile loading and scaling for typical buildings and customer data."""

import json
import logging
import math
from pathlib import Path

from src.rates.models import LoadProfile

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/load_profiles/typical")

ZONE_COORDS: dict[str, tuple[float, float]] = {
    "1A": (25.76, -80.19),
    "2A": (29.76, -95.37),
    "2B": (33.45, -112.07),
    "3A": (33.75, -84.39),
    "3B": (36.17, -115.14),
    "3C": (37.77, -122.42),
    "4A": (39.29, -76.61),
    "4B": (35.08, -106.65),
    "4C": (47.61, -122.33),
    "5A": (41.88, -87.63),
    "5B": (40.01, -105.27),
    "6A": (44.98, -93.27),
    "6B": (46.60, -112.04),
    "7": (46.78, -92.10),
    "8": (64.84, -147.72),
}


def get_ashrae_climate_zone(latitude: float, longitude: float) -> str:
    """Find the nearest ASHRAE climate zone by Euclidean distance.

    Args:
        latitude: Site latitude in decimal degrees.
        longitude: Site longitude in decimal degrees.

    Returns:
        Zone code string (e.g., "2B").
    """
    best_zone = ""
    best_dist = float("inf")
    for zone, (lat, lon) in ZONE_COORDS.items():
        dist = math.sqrt((latitude - lat) ** 2 + (longitude - lon) ** 2)
        if dist < best_dist:
            best_dist = dist
            best_zone = zone
    return best_zone


def get_available_building_types() -> list[str]:
    """Return the list of available DOE reference building types.

    Returns:
        List of canonical building type names from the cache metadata.

    Raises:
        FileNotFoundError: If climate_zones.json is missing.
    """
    meta_path = CACHE_DIR / "climate_zones.json"
    if not meta_path.exists():
        raise FileNotFoundError(
            f"Load profile metadata not found at {meta_path}. "
            "Run the NREL load profile cache builder first."
        )
    with open(meta_path) as f:
        meta = json.load(f)
    return meta["building_types"]


def load_typical_profile(
    building_type: str,
    latitude: float,
    longitude: float,
    annual_consumption_kwh: float,
) -> LoadProfile:
    """Load and scale a typical building load profile for a given location.

    Args:
        building_type: DOE reference building type (e.g., "MediumOffice").
        latitude: Site latitude in decimal degrees.
        longitude: Site longitude in decimal degrees.
        annual_consumption_kwh: Target annual consumption to scale profile to.

    Returns:
        LoadProfile with scaled hourly values summing to annual_consumption_kwh.

    Raises:
        ValueError: If building_type is not recognized.
        FileNotFoundError: If profile CSV or metadata is missing.
    """
    available = get_available_building_types()
    if building_type not in available:
        raise ValueError(
            f"Unknown building type '{building_type}'. "
            f"Available types: {available}"
        )

    zone = get_ashrae_climate_zone(latitude, longitude)

    meta_path = CACHE_DIR / "climate_zones.json"
    with open(meta_path) as f:
        meta = json.load(f)

    folder = meta["zones"][zone]["folder"]
    csv_path = CACHE_DIR / "profiles" / folder / f"{building_type}.csv"

    if not csv_path.exists():
        raise FileNotFoundError(
            f"Profile not found at {csv_path} for zone {zone}, "
            f"building type {building_type}"
        )

    raw_values = []
    with open(csv_path) as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                raw_values.append(float(stripped))

    if len(raw_values) != 8760:
        raise ValueError(
            f"Expected 8760 hourly values in {csv_path}, got {len(raw_values)}"
        )

    typical_annual = sum(raw_values)
    scale_factor = annual_consumption_kwh / typical_annual
    scaled = [v * scale_factor for v in raw_values]

    logger.info(
        "Loaded typical profile: zone=%s, type=%s, "
        "typical_annual=%.0f kWh, scale_factor=%.4f",
        zone,
        building_type,
        typical_annual,
        scale_factor,
    )

    return LoadProfile(
        hourly_kwh=scaled,
        source="typical",
        building_type=building_type,
        scale_factor=scale_factor,
    )


def load_customer_profile(path: Path) -> LoadProfile:
    """Load an 8760 hourly load profile from a customer-provided CSV.

    Accepts single-column or multi-column CSVs. For multi-column files,
    uses the column named "kwh" or "kWh" if present, otherwise the first
    numeric column. Handles optional header row.

    Args:
        path: Path to the CSV file.

    Returns:
        LoadProfile with source="customer".

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file doesn't contain exactly 8760 numeric values.
    """
    if not path.exists():
        raise FileNotFoundError(f"Customer profile not found at {path}")

    lines = path.read_text().strip().splitlines()
    if not lines:
        raise ValueError(f"Customer profile at {path} is empty")

    # Determine delimiter
    first_line = lines[0]
    if "\t" in first_line:
        delimiter = "\t"
    elif "," in first_line:
        delimiter = ","
    else:
        delimiter = None  # single column

    # Parse all rows into columns
    def split_row(line: str) -> list[str]:
        if delimiter:
            return [c.strip() for c in line.split(delimiter)]
        return [line.strip()]

    # Check if first row is a header (contains non-numeric value)
    first_fields = split_row(lines[0])
    has_header = False
    for field in first_fields:
        try:
            float(field)
        except ValueError:
            has_header = True
            break

    if has_header:
        header = [h.lower() for h in first_fields]
        data_lines = lines[1:]
    else:
        header = None
        data_lines = lines

    # Determine which column index to use
    col_idx = 0
    if header:
        for i, h in enumerate(header):
            if h in ("kwh", "kw"):
                col_idx = i
                break

    # Extract values
    values: list[float] = []
    for line_num, line in enumerate(data_lines, start=2 if has_header else 1):
        fields = split_row(line)
        if not fields or all(f == "" for f in fields):
            continue
        if col_idx >= len(fields):
            raise ValueError(
                f"Row {line_num}: expected at least {col_idx + 1} columns, "
                f"got {len(fields)}"
            )
        try:
            values.append(float(fields[col_idx]))
        except ValueError:
            raise ValueError(
                f"Row {line_num}: non-numeric value '{fields[col_idx]}' "
                f"in column {col_idx}"
            )

    if len(values) != 8760:
        raise ValueError(
            f"Customer profile must have exactly 8760 hourly values, "
            f"got {len(values)} from {path}"
        )

    return LoadProfile(hourly_kwh=values, source="customer")
