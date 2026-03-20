"""Land cover classification for buildability analysis.

Classifies NLCD land cover pixels into buildable, soft exclusion,
and hard exclusion categories and computes area statistics.
"""

import math

import numpy as np

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Conversion factor: 1 acre = 4046.86 square meters
_SQ_M_PER_ACRE = 4046.86

# Meters per degree of latitude (approximate, constant)
_METERS_PER_DEG_LAT = 111_000.0

# Land cover categories
HARD_EXCLUSIONS: dict[int, str] = {
    11: "Open Water",
    12: "Perennial Ice/Snow",
    23: "Developed, Medium Intensity",
    24: "Developed, High Intensity",
    90: "Woody Wetlands",
    95: "Emergent Herbaceous Wetlands",
}

SOFT_EXCLUSIONS: dict[int, str] = {
    22: "Developed, Low Intensity",
    41: "Deciduous Forest",
    42: "Evergreen Forest",
    43: "Mixed Forest",
}

BUILDABLE: dict[int, str] = {
    21: "Developed, Open Space",
    31: "Barren Land",
    52: "Shrub/Scrub",
    71: "Grassland/Herbaceous",
    81: "Pasture/Hay",
    82: "Cultivated Crops",
}

# Combined lookup for category classification
_CODE_TO_CATEGORY: dict[int, str] = {}
for code in HARD_EXCLUSIONS:
    _CODE_TO_CATEGORY[code] = "hard_exclusion"
for code in SOFT_EXCLUSIONS:
    _CODE_TO_CATEGORY[code] = "soft_exclusion"
for code in BUILDABLE:
    _CODE_TO_CATEGORY[code] = "buildable"

# Combined lookup for class names
_CODE_TO_NAME: dict[int, str] = {
    **HARD_EXCLUSIONS,
    **SOFT_EXCLUSIONS,
    **BUILDABLE,
}


def classify_land_cover(
    nlcd_array: np.ndarray, pixel_area_sq_m: float
) -> dict:
    """Classify NLCD land cover pixels into buildability categories.

    Args:
        nlcd_array: 2D uint8 array of NLCD land cover class codes.
        pixel_area_sq_m: Area of each pixel in square meters.

    Returns:
        Dict with total/buildable/soft/hard/unclassified pixel counts
        and acres, per-class breakdown, and summary percentages.
    """
    total_pixels = int(nlcd_array.size)
    total_acres = total_pixels * pixel_area_sq_m / _SQ_M_PER_ACRE

    # Count pixels per category
    unique, counts = np.unique(nlcd_array, return_counts=True)

    buildable_pixels = 0
    soft_exclusion_pixels = 0
    hard_exclusion_pixels = 0
    unclassified_pixels = 0
    class_breakdown = []

    for code, count in zip(unique, counts):
        code_int = int(code)
        count_int = int(count)
        category = _CODE_TO_CATEGORY.get(code_int, "unclassified")
        name = _CODE_TO_NAME.get(code_int, f"Unknown ({code_int})")
        acres = count_int * pixel_area_sq_m / _SQ_M_PER_ACRE
        pct = count_int / total_pixels * 100

        if category == "buildable":
            buildable_pixels += count_int
        elif category == "soft_exclusion":
            soft_exclusion_pixels += count_int
        elif category == "hard_exclusion":
            hard_exclusion_pixels += count_int
        else:
            unclassified_pixels += count_int

        class_breakdown.append({
            "code": code_int,
            "name": name,
            "category": category,
            "pixels": count_int,
            "acres": round(acres, 2),
            "percent": round(pct, 1),
        })

    # Sort by pixel count descending
    class_breakdown.sort(key=lambda x: x["pixels"], reverse=True)

    result = {
        "total_pixels": total_pixels,
        "total_area_acres": round(total_acres, 2),
        "buildable_pixels": buildable_pixels,
        "buildable_acres": round(buildable_pixels * pixel_area_sq_m / _SQ_M_PER_ACRE, 2),
        "soft_exclusion_pixels": soft_exclusion_pixels,
        "soft_exclusion_acres": round(soft_exclusion_pixels * pixel_area_sq_m / _SQ_M_PER_ACRE, 2),
        "hard_exclusion_pixels": hard_exclusion_pixels,
        "hard_exclusion_acres": round(hard_exclusion_pixels * pixel_area_sq_m / _SQ_M_PER_ACRE, 2),
        "unclassified_pixels": unclassified_pixels,
        "unclassified_acres": round(unclassified_pixels * pixel_area_sq_m / _SQ_M_PER_ACRE, 2),
        "class_breakdown": class_breakdown,
        "summary_pct": {
            "buildable": round(buildable_pixels / total_pixels * 100, 1),
            "soft_exclusion": round(soft_exclusion_pixels / total_pixels * 100, 1),
            "hard_exclusion": round(hard_exclusion_pixels / total_pixels * 100, 1),
            "unclassified": round(unclassified_pixels / total_pixels * 100, 1),
        },
    }

    logger.info(
        f"Land cover classification: {total_pixels} pixels, "
        f"{result['buildable_acres']:.1f} buildable acres "
        f"({result['summary_pct']['buildable']}%)"
    )
    return result


def get_pixel_area_sq_m(metadata: dict) -> float:
    """Compute the area of a single pixel in square meters.

    Handles both geographic (EPSG:4326) and projected (EPSG:3857)
    coordinate systems.

    Args:
        metadata: Raster metadata dict with 'transform' and 'crs' keys.

    Returns:
        Pixel area in square meters.
    """
    transform = metadata["transform"]
    crs = metadata["crs"]
    x_res = abs(transform.a)
    y_res = abs(transform.e)

    if "4326" in str(crs) or "CRS:84" in str(crs):
        # Geographic CRS: pixel size is in degrees
        n_rows = metadata["height"]
        center_lat = transform.f + transform.e * (n_rows / 2)
        cos_lat = math.cos(math.radians(center_lat))
        x_m = x_res * _METERS_PER_DEG_LAT * cos_lat
        y_m = y_res * _METERS_PER_DEG_LAT
        area = x_m * y_m
    else:
        # Projected CRS: pixel size is already in meters
        area = x_res * y_res

    logger.debug(f"Pixel area: {area:.2f} sq m (CRS={crs})")
    return area
