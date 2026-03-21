"""Rate file parser: load and validate electricity rate JSON files."""

import json
from pathlib import Path

from pydantic import ValidationError

from src.rates.models import RateSchedule
from src.utils.exceptions import SolarModelError
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class RateFileError(SolarModelError):
    """Raised when a rate file cannot be loaded or validated.

    Example context:
        - path: File path that failed
        - error: Underlying error message
    """


def load_rate_file(path: Path) -> RateSchedule:
    """Load and validate a rate schedule from a JSON file.

    Args:
        path: Path to a JSON file containing the rate structure.

    Returns:
        Validated RateSchedule model.

    Raises:
        RateFileError: If the file cannot be read, parsed, or validated.
    """
    if not path.exists():
        raise RateFileError(
            f"Rate file not found: {path}",
            context={"path": str(path)},
        )

    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RateFileError(
            f"Failed to read rate file: {path}",
            context={"path": str(path), "error": str(exc)},
        )

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RateFileError(
            f"Invalid JSON in rate file: {path}",
            context={"path": str(path), "error": str(exc)},
        )

    return validate_rate_schedule(data, source_path=path)


def validate_rate_schedule(
    data: dict, source_path: Path | None = None
) -> RateSchedule:
    """Validate a dict as a RateSchedule.

    Useful for validating data from API responses or programmatic
    construction, not just file loads.

    Args:
        data: Dictionary with rate schedule fields.
        source_path: Optional source file path for error context.

    Returns:
        Validated RateSchedule model.

    Raises:
        RateFileError: If validation fails.
    """
    try:
        schedule = RateSchedule(**data)
        logger.info(
            f"Validated rate schedule: {schedule.utility_name} / "
            f"{schedule.tariff_name} ({schedule.source})"
        )
        return schedule
    except ValidationError as exc:
        context: dict = {"error": str(exc)}
        if source_path is not None:
            context["path"] = str(source_path)
        raise RateFileError(
            f"Rate schedule validation failed: {exc.error_count()} errors",
            context=context,
        )
