"""Utility functions for M11 subhourly clipping loss refinement."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from timezonefinder import TimezoneFinder


# Module-level singleton to avoid repeated initialization (~100ms)
_TZ_FINDER: TimezoneFinder | None = None


def _get_tz_finder() -> TimezoneFinder:
    """Lazily initialize the TimezoneFinder singleton."""
    global _TZ_FINDER
    if _TZ_FINDER is None:
        _TZ_FINDER = TimezoneFinder()
    return _TZ_FINDER


def get_timezone_offset(latitude: float, longitude: float) -> int:
    """Return standard (non-DST) UTC offset as integer for SAM header.

    Uses timezonefinder to get the IANA timezone string, then zoneinfo
    to get the standard UTC offset. SAM expects the standard offset
    (e.g., -6 for US/Central, not -5 during DST).

    Args:
        latitude: Station latitude.
        longitude: Station longitude.

    Returns:
        Standard UTC offset as integer (e.g., -6 for US/Central).

    Raises:
        ValueError: If timezone cannot be determined for the given coordinates.
    """
    tf = _get_tz_finder()
    tz_name = tf.timezone_at(lat=latitude, lng=longitude)

    if tz_name is None:
        raise ValueError(
            f"Could not determine timezone for lat={latitude}, lon={longitude}"
        )

    # Get standard (non-DST) offset using January 1 (always standard time)
    tz = ZoneInfo(tz_name)
    jan1 = datetime(2020, 1, 1, 0, 0, 0, tzinfo=tz)
    offset_seconds = jan1.utcoffset().total_seconds()
    offset_hours = int(offset_seconds / 3600)

    return offset_hours
