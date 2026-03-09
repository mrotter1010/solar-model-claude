"""Parser and validator for Solcast TMY SAM CSV weather files.

Validates file structure, metadata, and required columns. Does NOT reformat
or rewrite the file — PySAM reads SAM CSV files directly and accepts multiple
column name aliases (e.g. Tdry, Temperature, Dry Bulb are all valid).
"""

import os
from pathlib import Path

from src.utils.exceptions import ClimateDataError
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# SAM column aliases — each tuple lists accepted names (matched case-insensitively)
REQUIRED_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "GHI": ("ghi", "gh", "global horizontal"),
    "DNI": ("dni", "dn", "direct normal"),
    "DHI": ("dhi", "df", "diffuse horizontal"),
    "Temperature": ("tdry", "temperature", "dry bulb", "temp_air", "ambient"),
    "Wind Speed": ("wspd", "wind speed", "wind_speed"),
}

OPTIONAL_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "Snow Depth": ("snow depth", "snow", "snow cover"),
    "Albedo": ("albedo", "alb", "surface albedo"),
}

# Valid SAM data row counts: hourly through 1-minute resolution for a single year
VALID_ROW_COUNTS = {8760, 17520, 35040, 105120, 525600}

# Required metadata labels in row 1 of the SAM CSV header
REQUIRED_HEADER_LABELS = {"latitude", "longitude", "time zone", "elevation"}


def parse_solcast_file(
    file_path: Path,
    expected_lat: float,
    expected_lon: float,
    lat_lon_tolerance: float = 0.1,
) -> dict:
    """Validate a Solcast TMY SAM CSV file and return metadata.

    Checks file structure, header metadata, required columns, row count,
    and lat/lon proximity. The file itself is not modified — PySAM will
    read it directly using SAM's built-in column alias support.

    Args:
        file_path: Path to the Solcast SAM CSV file.
        expected_lat: Expected site latitude for proximity check.
        expected_lon: Expected site longitude for proximity check.
        lat_lon_tolerance: Maximum allowed distance in degrees between
            file coordinates and expected coordinates.

    Returns:
        Dictionary with validated file path, metadata, and optional
        column flags.

    Raises:
        ClimateDataError: If any validation check fails.
    """
    file_path = Path(file_path)

    # 1. File exists and is readable
    _validate_file_accessible(file_path)

    # 2-3. Parse and validate header metadata (rows 1-2)
    metadata = _parse_header_metadata(file_path)

    # 4. Lat/lon proximity check
    _validate_location(
        metadata["latitude"],
        metadata["longitude"],
        expected_lat,
        expected_lon,
        lat_lon_tolerance,
    )

    # 5-7. Validate columns and row count, detect optional columns
    has_snow_depth, has_albedo = _validate_columns_and_rows(file_path)

    logger.info(
        f"Solcast file validated: {file_path.name} "
        f"({metadata['latitude']}, {metadata['longitude']}), "
        f"snow_depth={has_snow_depth}, albedo={has_albedo}"
    )

    return {
        "weather_file": file_path,
        "metadata": metadata,
        "has_snow_depth": has_snow_depth,
        "has_albedo": has_albedo,
    }


def _validate_file_accessible(file_path: Path) -> None:
    """Check that the file exists and is readable.

    Args:
        file_path: Path to validate.

    Raises:
        ClimateDataError: If file does not exist or is not readable.
    """
    if not file_path.exists():
        raise ClimateDataError(
            f"Solcast file not found: {file_path}",
            context={"path": str(file_path)},
        )
    if not file_path.is_file():
        raise ClimateDataError(
            f"Solcast path is not a file: {file_path}",
            context={"path": str(file_path)},
        )
    if not os.access(file_path, os.R_OK):
        raise ClimateDataError(
            f"Solcast file is not readable: {file_path}",
            context={"path": str(file_path)},
        )


def _parse_header_metadata(file_path: Path) -> dict:
    """Parse the 2-row SAM CSV header for location metadata.

    Row 1 contains comma-separated labels (e.g. Latitude,Longitude,...).
    Row 2 contains the corresponding values.

    Args:
        file_path: Path to the Solcast SAM CSV file.

    Returns:
        Metadata dictionary with latitude, longitude, time_zone, elevation,
        and source fields.

    Raises:
        ClimateDataError: If header labels or values are missing/invalid.
    """
    with open(file_path) as f:
        row1 = f.readline().strip()
        row2 = f.readline().strip()

    if not row1 or not row2:
        raise ClimateDataError(
            "Solcast file header is empty or incomplete",
            context={"path": str(file_path)},
        )

    labels = [label.strip().lower() for label in row1.split(",")]
    values = [val.strip() for val in row2.split(",")]

    # Validate required header labels are present
    missing_labels = REQUIRED_HEADER_LABELS - set(labels)
    if missing_labels:
        raise ClimateDataError(
            f"Solcast file header missing required labels: {sorted(missing_labels)}",
            context={"path": str(file_path), "found_labels": labels},
        )

    # Build label→value mapping
    header_map = dict(zip(labels, values))

    # Parse numeric values
    try:
        latitude = float(header_map["latitude"])
        longitude = float(header_map["longitude"])
        time_zone = float(header_map["time zone"])
        elevation = float(header_map["elevation"])
    except (ValueError, KeyError) as e:
        raise ClimateDataError(
            f"Solcast file header has invalid numeric values: {e}",
            context={"path": str(file_path), "header_values": header_map},
        )

    source = header_map.get("source", "Solcast")

    return {
        "latitude": latitude,
        "longitude": longitude,
        "time_zone": time_zone,
        "elevation": elevation,
        "source": source,
    }


def _validate_location(
    file_lat: float,
    file_lon: float,
    expected_lat: float,
    expected_lon: float,
    tolerance: float,
) -> None:
    """Validate that file coordinates are within tolerance of expected location.

    Args:
        file_lat: Latitude from the file header.
        file_lon: Longitude from the file header.
        expected_lat: Expected site latitude.
        expected_lon: Expected site longitude.
        tolerance: Maximum allowed distance in degrees.

    Raises:
        ClimateDataError: If the distance exceeds tolerance.
    """
    distance = max(abs(file_lat - expected_lat), abs(file_lon - expected_lon))
    if distance > tolerance:
        raise ClimateDataError(
            f"Solcast file location ({file_lat}, {file_lon}) is {distance:.2f}° "
            f"from expected site location ({expected_lat}, {expected_lon}). "
            f"Maximum tolerance is {tolerance}°.",
            context={
                "file_lat": file_lat,
                "file_lon": file_lon,
                "expected_lat": expected_lat,
                "expected_lon": expected_lon,
                "distance": distance,
                "tolerance": tolerance,
            },
        )


def _validate_columns_and_rows(file_path: Path) -> tuple[bool, bool]:
    """Validate column headers and data row count.

    Args:
        file_path: Path to the Solcast SAM CSV file.

    Returns:
        Tuple of (has_snow_depth, has_albedo) booleans.

    Raises:
        ClimateDataError: If required columns are missing or row count is invalid.
    """
    with open(file_path) as f:
        f.readline()  # skip row 1 (metadata labels)
        f.readline()  # skip row 2 (metadata values)
        column_header = f.readline().strip()
        data_row_count = sum(1 for line in f if line.strip())

    columns_lower = [col.strip().lower() for col in column_header.split(",")]

    # Check required columns
    missing = []
    for display_name, aliases in REQUIRED_COLUMN_ALIASES.items():
        if not any(alias in columns_lower for alias in aliases):
            missing.append(display_name)

    if missing:
        raise ClimateDataError(
            f"Solcast file missing required columns: {missing}. "
            f"Found: {[col.strip() for col in column_header.split(',')]}",
            context={
                "path": str(file_path),
                "missing_columns": missing,
                "found_columns": [col.strip() for col in column_header.split(",")],
            },
        )

    # Validate row count
    if data_row_count not in VALID_ROW_COUNTS:
        raise ClimateDataError(
            f"Expected 8760 (or valid subhourly count) data rows, "
            f"found {data_row_count}. SAM requires single-year weather files.",
            context={
                "path": str(file_path),
                "data_row_count": data_row_count,
                "valid_counts": sorted(VALID_ROW_COUNTS),
            },
        )

    # Check optional columns
    has_snow_depth = any(
        alias in columns_lower for alias in OPTIONAL_COLUMN_ALIASES["Snow Depth"]
    )
    has_albedo = any(
        alias in columns_lower for alias in OPTIONAL_COLUMN_ALIASES["Albedo"]
    )

    return has_snow_depth, has_albedo
