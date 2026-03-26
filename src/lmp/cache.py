"""LMP data caching layer — simple CSV file cache.

Caches fetched LMP price data to avoid redundant API calls.
Filename pattern: {iso}_{zone}_{market}_{year}.csv
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

CACHE_DIR = Path("data/lmp/cache")


def _cache_filename(iso: str, zone: str, market: str, year: int) -> str:
    """Build a normalized cache filename.

    Args:
        iso: ISO identifier (e.g., "pjm").
        zone: Pricing zone name (e.g., "AEP").
        market: Market type (e.g., "DAY_AHEAD_HOURLY").
        year: Calendar year.

    Returns:
        Filename string like "pjm_aep_day_ahead_hourly_2025.csv".
    """
    parts = [iso, zone, market, str(year)]
    normalized = "_".join(p.lower().replace(" ", "_") for p in parts)
    return f"{normalized}.csv"


def get_cached_lmp(
    iso: str, zone: str, market: str, year: int
) -> pd.DataFrame | None:
    """Read cached LMP data if available.

    Args:
        iso: ISO identifier.
        zone: Pricing zone name.
        market: Market type.
        year: Calendar year.

    Returns:
        DataFrame with columns ["timestamp", "lmp"], or None if not cached.
    """
    filepath = CACHE_DIR / _cache_filename(iso, zone, market, year)
    if not filepath.exists():
        logger.debug("Cache miss: %s", filepath)
        return None

    logger.info("Cache hit: %s", filepath)
    return pd.read_csv(filepath, parse_dates=["timestamp"])


def save_lmp_cache(
    iso: str, zone: str, market: str, year: int, df: pd.DataFrame
) -> Path:
    """Save LMP data to the cache.

    Args:
        iso: ISO identifier.
        zone: Pricing zone name.
        market: Market type.
        year: Calendar year.
        df: DataFrame with columns ["timestamp", "lmp"].

    Returns:
        Path to the saved cache file.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    filepath = CACHE_DIR / _cache_filename(iso, zone, market, year)
    df.to_csv(filepath, index=False)
    logger.info("Saved LMP cache: %s (%d rows)", filepath, len(df))
    return filepath
