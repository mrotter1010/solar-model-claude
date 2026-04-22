"""Auto-detect uploaded file types based on filename extension and content inspection."""

import csv
import io
import json

from pydantic import BaseModel


class DetectionResult(BaseModel):
    """Result of file type auto-detection.

    Args:
        detected_type: One of "kmz", "load_profile", "rate_schedule", or "unknown".
        confidence: "high" if detection is certain, "low" otherwise.
        message: Human-readable explanation of the detection result.
    """

    detected_type: str
    confidence: str
    message: str


_RATE_REQUIRED_KEYS = {
    "utility_name",
    "tariff_name",
    "energyratestructure",
    "energyweekdayschedule",
    "energyweekendschedule",
}

# Acceptable 8760 row counts: standard year, leap year, and +1 for possible
# double-header or trailing blank in common exports.
_MIN_LOAD_ROWS = 8_760
_MAX_LOAD_ROWS = 8_784

# Batch input detection: must have analysis_type + lat + lon columns
_BATCH_REQUIRED_COLUMN = "analysis_type"
_BATCH_LAT_COLUMNS = frozenset({"latitude", "lat"})
_BATCH_LON_COLUMNS = frozenset({"longitude", "lon"})


def detect_file_type(filename: str, content: bytes) -> DetectionResult:
    """Detect the type of an uploaded file from its name and content.

    Detection is conservative — files are only classified when the signal is
    unambiguous.  Anything uncertain falls through to ``"unknown"``.

    Args:
        filename: Original client-side filename (used for extension check).
        content: Raw file bytes.

    Returns:
        DetectionResult with detected_type, confidence, and a human-readable
        message.
    """
    lower = filename.lower()

    # 1. KMZ/KML — extension alone is sufficient
    if lower.endswith(".kmz") or lower.endswith(".kml"):
        return DetectionResult(
            detected_type="kmz",
            confidence="high",
            message=f"Detected as KMZ/KML boundary file: '{filename}'",
        )

    # 2. CSV — check if it looks like an 8760 hourly load profile or batch input
    if lower.endswith(".csv"):
        result = _check_load_profile_csv(filename, content)
        if result is not None:
            return result
        result = _check_batch_input_csv(filename, content)
        if result is not None:
            return result
        # Not a load profile or batch input — fall through to unknown
        return DetectionResult(
            detected_type="unknown",
            confidence="low",
            message=(
                f"CSV file '{filename}' does not match load profile format "
                f"(expected {_MIN_LOAD_ROWS:,}-{_MAX_LOAD_ROWS:,} data rows)"
            ),
        )

    # 3. JSON — check for RateSchedule structure
    if lower.endswith(".json"):
        result = _check_rate_schedule_json(filename, content)
        if result is not None:
            return result
        return DetectionResult(
            detected_type="unknown",
            confidence="low",
            message=f"JSON file '{filename}' does not match rate schedule structure",
        )

    # 4. Excel — check if it looks like a batch input file
    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        result = _check_batch_input_excel(filename, content)
        if result is not None:
            return result
        return DetectionResult(
            detected_type="unknown",
            confidence="low",
            message=f"Excel file '{filename}' does not match batch input format",
        )

    # 5. Everything else
    return DetectionResult(
        detected_type="unknown",
        confidence="low",
        message="File type not automatically recognized",
    )


def _check_load_profile_csv(
    filename: str, content: bytes
) -> DetectionResult | None:
    """Return a DetectionResult if content looks like an 8760 load profile CSV."""
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return None

    try:
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
    except csv.Error:
        return None

    if not rows:
        return None

    # Exclude the header row (first row)
    data_rows = len(rows) - 1

    if _MIN_LOAD_ROWS <= data_rows <= _MAX_LOAD_ROWS:
        return DetectionResult(
            detected_type="load_profile",
            confidence="high",
            message=(
                f"Detected as load profile: CSV '{filename}' with "
                f"{data_rows:,} data rows of hourly data"
            ),
        )

    return None


def _check_batch_input_headers(
    filename: str, headers: list[str]
) -> DetectionResult | None:
    """Return a DetectionResult if headers match batch input template columns.

    Requires ``analysis_type`` AND at least one latitude column AND at least
    one longitude column.
    """
    lower_headers = {h.strip().lower() for h in headers}

    if _BATCH_REQUIRED_COLUMN not in lower_headers:
        return None
    if not lower_headers & _BATCH_LAT_COLUMNS:
        return None
    if not lower_headers & _BATCH_LON_COLUMNS:
        return None

    return DetectionResult(
        detected_type="batch_input",
        confidence="high",
        message=f"Batch input file detected: '{filename}'",
    )


def _check_batch_input_csv(
    filename: str, content: bytes
) -> DetectionResult | None:
    """Return a DetectionResult if CSV has batch input template columns."""
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return None

    try:
        reader = csv.reader(io.StringIO(text))
        headers = next(reader, None)
    except csv.Error:
        return None

    if not headers:
        return None

    return _check_batch_input_headers(filename, headers)


def _check_batch_input_excel(
    filename: str, content: bytes
) -> DetectionResult | None:
    """Return a DetectionResult if Excel file has batch input template columns."""
    try:
        import openpyxl
    except ImportError:
        return None

    try:
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True)
        ws = wb.active
        headers = [str(cell.value or "") for cell in next(ws.iter_rows(max_row=1))]
        wb.close()
    except Exception:
        return None

    return _check_batch_input_headers(filename, headers)


def _check_rate_schedule_json(
    filename: str, content: bytes
) -> DetectionResult | None:
    """Return a DetectionResult if content looks like a RateSchedule JSON."""
    try:
        data = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    if not isinstance(data, dict):
        return None

    if _RATE_REQUIRED_KEYS.issubset(data.keys()):
        return DetectionResult(
            detected_type="rate_schedule",
            confidence="high",
            message=(
                f"Detected as rate schedule: JSON '{filename}' with "
                f"utility '{data.get('utility_name')}' / "
                f"tariff '{data.get('tariff_name')}'"
            ),
        )

    return None
