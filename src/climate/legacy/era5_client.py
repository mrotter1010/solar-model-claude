"""ERA5 client for fetching snow depth and precipitation data.

Uses Google's ARCO ERA5 Zarr store on Google Cloud Storage for fast,
queue-free access to ERA5 reanalysis data.
"""

import calendar
import json
from datetime import datetime
from pathlib import Path

import gcsfs
import numpy as np
import zarr

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

ARCO_ZARR_URL = (
    "gcp-public-data-arco-era5/ar/"
    "full_37-1h-0p25deg-chunk-1.zarr-v3"
)


def fetch_era5_land_data(
    lat: float,
    lon: float,
    year: int,
    cache_dir: Path = Path("data/climate/era5"),
) -> dict[str, list[float]] | None:
    """Fetch snow depth and total precipitation from ERA5 via ARCO Zarr.

    Reads data directly from Google's ARCO ERA5 cloud-optimized Zarr store.
    Results are cached locally as JSON for subsequent runs.
    Best-effort function: any failure returns None and the caller falls back
    to defaults. Never raises exceptions.

    Args:
        lat: Site latitude in degrees.
        lon: Site longitude in degrees (-180 to 180 or 0 to 360).
        year: Data year to retrieve.
        cache_dir: Directory for caching extracted JSON results.

    Returns:
        Dict with keys:
            - "snow_depth_cm": list of 8760 hourly values in centimeters.
            - "monthly_precip_inches": list of 12 monthly totals in inches.
        Returns None if retrieval fails.
    """
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)

        # Check JSON cache first
        cache_path = cache_dir / f"era5_arco_{lat}_{lon}_{year}.json"
        if cache_path.exists():
            logger.debug(f"ERA5 ARCO cache hit: {cache_path.name}")
            return _load_cache(cache_path)

        logger.info(
            f"Fetching ERA5 data from ARCO Zarr for ({lat}, {lon}), "
            f"year {year}"
        )

        result = _fetch_from_zarr(lat, lon, year)

        _save_cache(cache_path, result)
        logger.info(
            f"ERA5 ARCO processed: snow_depth {len(result['snow_depth_cm'])} "
            f"hours, precip {len(result['monthly_precip_inches'])} months"
        )

        return result

    except Exception:
        logger.warning(
            f"Unexpected error fetching ERA5 data for ({lat}, {lon})",
            exc_info=True,
        )
        return None


def _fetch_from_zarr(
    lat: float, lon: float, year: int
) -> dict[str, list[float]]:
    """Fetch snow depth and precipitation from the ARCO ERA5 Zarr store.

    Uses gcsfs + zarr to calculate exact array indices and read only the
    needed bytes, avoiding a full metadata scan of the 2PB dataset.

    Args:
        lat: Site latitude in degrees.
        lon: Site longitude in degrees.
        year: Data year to retrieve.

    Returns:
        Dict with "snow_depth_cm" (8760/8784 floats) and
        "monthly_precip_inches" (12 floats).
    """
    # 1. Calculate array indices directly (no metadata scan)
    arco_lon = lon % 360
    lat_idx = round((90.0 - lat) / 0.25)  # 90.0=idx0, -90.0=idx720
    lon_idx = round(arco_lon / 0.25)  # 0.0=idx0, 359.75=idx1439

    # Time index: hours since 1900-01-01
    start = datetime(year, 1, 1)
    epoch = datetime(1900, 1, 1)
    start_idx = int((start - epoch).total_seconds() / 3600)
    hours_in_year = 8784 if calendar.isleap(year) else 8760
    end_idx = start_idx + hours_in_year

    # 2. Open each variable array directly via gcsfs (anonymous access).
    #    Opening the root store reads metadata for ALL ~100 variables;
    #    opening each variable path reads only that array's metadata (~KB).
    fs = gcsfs.GCSFileSystem(token="anon")

    snow_store = zarr.open(
        fs.get_mapper(f"{ARCO_ZARR_URL}/snow_depth"), mode="r"
    )
    snow_raw = snow_store[start_idx:end_idx, lat_idx, lon_idx]

    precip_store = zarr.open(
        fs.get_mapper(f"{ARCO_ZARR_URL}/total_precipitation"), mode="r"
    )
    precip_raw = precip_store[start_idx:end_idx, lat_idx, lon_idx]

    # 4. Process snow depth: meters water equiv -> cm, NaN -> 0
    snow_arr = np.array(snow_raw, dtype=float)
    snow_arr = np.nan_to_num(snow_arr, nan=0.0) * 100.0
    snow_arr = np.clip(snow_arr, 0.0, None)

    # 5. Process precipitation: sum by month, meters -> inches
    precip_arr = np.array(precip_raw, dtype=float)
    precip_arr = np.nan_to_num(precip_arr, nan=0.0)
    precip_arr = np.clip(precip_arr, 0.0, None)

    monthly_precip: list[float] = []
    hour_offset = 0
    for month in range(1, 13):
        days_in_month = calendar.monthrange(year, month)[1]
        hours_in_month = days_in_month * 24
        month_total = float(
            np.sum(precip_arr[hour_offset : hour_offset + hours_in_month])
        )
        monthly_precip.append(month_total * 39.3701)  # meters to inches
        hour_offset += hours_in_month

    logger.debug(
        f"ARCO nearest grid point: lat_idx={lat_idx} "
        f"(~{90.0 - lat_idx * 0.25}°), lon_idx={lon_idx} "
        f"(~{lon_idx * 0.25}°)"
    )

    return {
        "snow_depth_cm": snow_arr.tolist(),
        "monthly_precip_inches": monthly_precip,
    }


def _load_cache(cache_path: Path) -> dict[str, list[float]]:
    """Load cached ERA5 results from a JSON file.

    Args:
        cache_path: Path to the JSON cache file.

    Returns:
        Dict with "snow_depth_cm" and "monthly_precip_inches".
    """
    with cache_path.open("r") as f:
        return json.load(f)


def _save_cache(
    cache_path: Path, data: dict[str, list[float]]
) -> None:
    """Save ERA5 results to a JSON cache file.

    Args:
        cache_path: Path to write the JSON cache file.
        data: Dict with "snow_depth_cm" and "monthly_precip_inches".
    """
    with cache_path.open("w") as f:
        json.dump(data, f)
    logger.debug(f"ERA5 ARCO cache saved: {cache_path.name}")
