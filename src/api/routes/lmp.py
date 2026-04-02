"""LMP price query endpoint — standalone access to LMP data."""

import statistics
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from src.lmp.client import fetch_lmp
from src.lmp.zone_mapper import (
    get_caiso_zone,
    get_ercot_zone,
    get_iso_from_latlon,
    get_pjm_zone,
)
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

SUPPORTED_ISOS = {"pjm", "ercot", "caiso"}

router = APIRouter(prefix="/lmp", tags=["lmp"])


def _resolve_zone(iso: str, lat: float, lon: float) -> str:
    """Auto-detect pricing zone from coordinates and ISO.

    Args:
        iso: Validated ISO identifier (lowercase).
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.

    Returns:
        Zone name string.

    Raises:
        HTTPException: If zone cannot be determined.
    """
    if iso == "pjm":
        zone = get_pjm_zone(lat, lon)
        if zone is None:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Could not auto-detect PJM zone for ({lat}, {lon}). "
                    f"Provide the 'zone' parameter explicitly."
                ),
            )
        return zone
    elif iso == "ercot":
        return get_ercot_zone(lat, lon)
    elif iso == "caiso":
        return get_caiso_zone(lat, lon)

    raise HTTPException(status_code=422, detail=f"Unsupported ISO: '{iso}'.")


@router.get("/prices")
def get_lmp_prices(
    iso: str = Query(..., description="ISO/RTO: pjm, ercot, or caiso"),
    zone: str | None = Query(
        default=None,
        description="Pricing zone (e.g., AEP, LZ_HOUSTON, NP15). "
        "Required if lat/lon not provided.",
    ),
    lat: float | None = Query(
        default=None, ge=-90, le=90,
        description="Latitude for zone auto-detection",
    ),
    lon: float | None = Query(
        default=None, ge=-180, le=180,
        description="Longitude for zone auto-detection",
    ),
    market: str = Query(
        default="DAY_AHEAD_HOURLY",
        description="Market type",
    ),
    year: int | None = Query(
        default=None,
        description="Calendar year (default: previous year)",
    ),
) -> dict:
    """Fetch LMP prices for a given ISO and zone.

    Provide either `zone` directly, or `lat`+`lon` for auto-detection.

    Returns summary statistics, monthly averages, and the full 8760
    hourly price series.
    """
    # Validate ISO
    iso_lower = iso.strip().lower()
    if iso_lower not in SUPPORTED_ISOS:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Invalid ISO '{iso}'. "
                f"Must be one of: {sorted(SUPPORTED_ISOS)}"
            ),
        )

    # Resolve zone
    if zone is not None:
        resolved_zone = zone.strip()
    elif lat is not None and lon is not None:
        resolved_zone = _resolve_zone(iso_lower, lat, lon)
    else:
        raise HTTPException(
            status_code=422,
            detail=(
                "Either 'zone' or both 'lat' and 'lon' must be provided. "
                "Use zone for a direct lookup, or lat+lon for auto-detection."
            ),
        )

    # Fetch LMP data
    try:
        lmp_data = fetch_lmp(
            iso=iso_lower,
            zone=resolved_zone,
            market=market,
            year=year,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("LMP fetch failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch LMP data: {exc}",
        ) from exc

    # Compute summary stats
    prices = lmp_data.prices
    mean_price = statistics.mean(prices)
    median_price = statistics.median(prices)
    min_price = min(prices)
    max_price = max(prices)
    std_price = statistics.stdev(prices)

    # Compute monthly averages (8760 = 12 months of varying length)
    monthly_hours = [
        744, 672, 744, 720, 744, 720,  # Jan-Jun
        744, 744, 720, 744, 720, 744,  # Jul-Dec
    ]
    monthly_averages: dict[str, float] = {}
    month_names = [
        "jan", "feb", "mar", "apr", "may", "jun",
        "jul", "aug", "sep", "oct", "nov", "dec",
    ]
    offset = 0
    for name, hours in zip(month_names, monthly_hours):
        month_prices = prices[offset : offset + hours]
        monthly_averages[name] = round(statistics.mean(month_prices), 2)
        offset += hours

    # Format timestamps as ISO strings if available
    timestamps_iso: list[str] | None = None
    if lmp_data.timestamps is not None:
        timestamps_iso = [
            t.isoformat() if isinstance(t, datetime) else str(t)
            for t in lmp_data.timestamps
        ]

    return {
        "iso": lmp_data.iso,
        "zone": lmp_data.zone,
        "market": lmp_data.market,
        "year": lmp_data.year,
        "currency": lmp_data.currency,
        "summary": {
            "mean_price": round(mean_price, 2),
            "median_price": round(median_price, 2),
            "min_price": round(min_price, 2),
            "max_price": round(max_price, 2),
            "std_price": round(std_price, 2),
            "count": len(prices),
        },
        "monthly_averages": monthly_averages,
        "prices": prices,
        "timestamps": timestamps_iso,
    }
