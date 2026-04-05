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

    # 2. CSV — check if it looks like an 8760 hourly load profile
    if lower.endswith(".csv"):
        result = _check_load_profile_csv(filename, content)
        if result is not None:
            return result
        # Not a load profile shape — fall through to unknown
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

    # 4. Everything else
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
