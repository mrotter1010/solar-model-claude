"""Monthly soiling loss calculator based on precipitation thresholds."""

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def calculate_monthly_soiling(monthly_precip_inches: list[float]) -> list[float]:
    """Convert 12 monthly precipitation totals (inches) to 12 monthly soiling loss percentages.

    Threshold table:
        >= 2.0 inches  -> 1.0%
        1.5 - 1.99     -> 1.5%
        1.0 - 1.49     -> 2.0%
        0.5 - 0.99     -> 2.5%
        < 0.5          -> 3.0%

    Args:
        monthly_precip_inches: list of 12 floats, monthly precipitation in inches.

    Returns:
        list of 12 floats, monthly soiling loss percentages.

    Raises:
        ValueError: If input is not exactly 12 elements.
    """
    if len(monthly_precip_inches) != 12:
        raise ValueError(
            f"Expected 12 monthly values, got {len(monthly_precip_inches)}"
        )

    soiling: list[float] = []
    for precip in monthly_precip_inches:
        if precip >= 2.0:
            loss = 1.0
        elif precip >= 1.5:
            loss = 1.5
        elif precip >= 1.0:
            loss = 2.0
        elif precip >= 0.5:
            loss = 2.5
        else:
            loss = 3.0
        soiling.append(loss)

    logger.debug(f"Monthly soiling losses: {soiling}")
    return soiling
