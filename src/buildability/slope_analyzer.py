"""Terrain slope analysis for buildability assessment.

Computes slope statistics and distribution from a slope raster,
classifying terrain suitability for tracker and fixed-tilt solar arrays.
"""

import numpy as np

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

_DEFAULT_THRESHOLDS = [5.0, 10.0, 15.0]
_TRACKER_SLOPE_LIMIT = 10.0
_FIXED_TILT_SLOPE_LIMIT = 20.0


def analyze_slope(
    slope_array: np.ndarray, thresholds: list[float] | None = None
) -> dict:
    """Analyze terrain slope distribution and suitability.

    Computes summary statistics, a bucketed distribution based on
    configurable thresholds, and percentage of area suitable for
    tracker and fixed-tilt solar installations.

    NaN values (pixels outside the analysis polygon) are excluded
    from all statistics.

    Args:
        slope_array: 2D numpy array of slope values in degrees.
            NaN values are treated as nodata (outside polygon).
        thresholds: Slope bucket boundaries in degrees. Defaults
            to [5.0, 10.0, 15.0]. Creates buckets: 0-5, 5-10,
            10-15, 15+.

    Returns:
        Dict with min/max/mean/median slope, bucketed distribution,
        and tracker/fixed-tilt suitability percentages.
    """
    if thresholds is None:
        thresholds = _DEFAULT_THRESHOLDS

    thresholds = sorted(thresholds)

    # Exclude NaN pixels (outside analysis polygon)
    valid_slopes = slope_array[~np.isnan(slope_array)]
    total_pixels = valid_slopes.size

    if total_pixels == 0:
        return {
            "min_degrees": 0.0,
            "max_degrees": 0.0,
            "mean_degrees": 0.0,
            "median_degrees": 0.0,
            "distribution": [],
            "pct_below_tracker_limit": 0.0,
            "pct_below_fixed_tilt_limit": 0.0,
        }

    # Summary statistics (over valid pixels only)
    min_deg = float(np.min(valid_slopes))
    max_deg = float(np.max(valid_slopes))
    mean_deg = float(np.mean(valid_slopes))
    median_deg = float(np.median(valid_slopes))

    # Bucketed distribution
    distribution = _compute_distribution(valid_slopes, thresholds, total_pixels)

    # Suitability percentages
    pct_tracker = float(np.sum(valid_slopes < _TRACKER_SLOPE_LIMIT) / total_pixels * 100)
    pct_fixed = float(np.sum(valid_slopes < _FIXED_TILT_SLOPE_LIMIT) / total_pixels * 100)

    result = {
        "min_degrees": round(min_deg, 2),
        "max_degrees": round(max_deg, 2),
        "mean_degrees": round(mean_deg, 2),
        "median_degrees": round(median_deg, 2),
        "distribution": distribution,
        "pct_below_tracker_limit": round(pct_tracker, 1),
        "pct_below_fixed_tilt_limit": round(pct_fixed, 1),
    }

    logger.info(
        f"Slope analysis: mean={mean_deg:.1f}°, "
        f"tracker-suitable={pct_tracker:.1f}%, "
        f"fixed-suitable={pct_fixed:.1f}%"
    )
    return result


def _compute_distribution(
    slope_array: np.ndarray,
    thresholds: list[float],
    total_pixels: int,
) -> list[dict]:
    """Compute bucketed slope distribution.

    Args:
        slope_array: 2D array of slope values in degrees.
        thresholds: Sorted list of bucket boundaries.
        total_pixels: Total pixel count for percentage calculation.

    Returns:
        List of dicts with 'range', 'pixels', and 'percent' keys.
    """
    distribution = []

    # First bucket: 0 to first threshold
    lower = 0.0
    for upper in thresholds:
        mask = (slope_array >= lower) & (slope_array < upper)
        count = int(np.sum(mask))
        distribution.append({
            "range": f"{lower:.0f}\u00b0 - {upper:.0f}\u00b0",
            "pixels": count,
            "percent": round(count / total_pixels * 100, 1),
        })
        lower = upper

    # Last bucket: above last threshold
    mask = slope_array >= thresholds[-1]
    count = int(np.sum(mask))
    distribution.append({
        "range": f"{thresholds[-1]:.0f}\u00b0+",
        "pixels": count,
        "percent": round(count / total_pixels * 100, 1),
    })

    return distribution
