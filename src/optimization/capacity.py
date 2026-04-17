"""Convert buildable land area and design parameters into achievable DC capacity."""

import math


_SQ_M_PER_ACRE = 4046.86


def capacity_from_acreage(
    buildable_acres: float,
    gcr: float,
    module_area_m2: float,
    module_power_w: float,
    utilization_factor: float = 0.75,
) -> dict:
    """Estimate DC capacity achievable on a given land area.

    Args:
        buildable_acres: Buildable land area in acres.
        gcr: Ground coverage ratio (0 < gcr <= 1).
        module_area_m2: Single module area in square metres.
        module_power_w: Single module STC power in watts.
        utilization_factor: Fraction of buildable area actually used for
            modules (accounts for roads, setbacks, substations). Must be
            > 0 and <= 1.0.

    Returns:
        Dict with capacity metrics: mw_dc, kw_dc, num_modules,
        usable_acres, modules_per_mw, acres_per_mw.

    Raises:
        ValueError: If any input is out of valid range.
    """
    if buildable_acres <= 0:
        raise ValueError(f"buildable_acres must be > 0, got {buildable_acres}")
    if gcr <= 0:
        raise ValueError(f"gcr must be > 0, got {gcr}")
    if module_area_m2 <= 0:
        raise ValueError(f"module_area_m2 must be > 0, got {module_area_m2}")
    if module_power_w <= 0:
        raise ValueError(f"module_power_w must be > 0, got {module_power_w}")
    if utilization_factor <= 0 or utilization_factor > 1.0:
        raise ValueError(
            f"utilization_factor must be > 0 and <= 1.0, got {utilization_factor}"
        )

    usable_area_m2 = buildable_acres * _SQ_M_PER_ACRE * utilization_factor
    module_footprint_m2 = module_area_m2 / gcr
    num_modules = math.floor(usable_area_m2 / module_footprint_m2)
    capacity_kw_dc = num_modules * module_power_w / 1000
    capacity_mw_dc = capacity_kw_dc / 1000

    usable_acres = buildable_acres * utilization_factor
    modules_per_mw = 1_000_000 / module_power_w
    acres_per_mw = round(usable_acres / capacity_mw_dc, 2) if capacity_mw_dc > 0 else 0

    return {
        "mw_dc": round(capacity_mw_dc, 3),
        "kw_dc": round(capacity_kw_dc, 1),
        "num_modules": num_modules,
        "usable_acres": round(usable_acres, 2),
        "modules_per_mw": round(modules_per_mw),
        "acres_per_mw": acres_per_mw,
    }
