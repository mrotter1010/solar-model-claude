"""LMP data fetching client — PJM, ERCOT, and CAISO via gridstatus.

Fetches day-ahead hourly LMP/SPP prices and caches results.
"""

import logging
import os
from datetime import datetime

import gridstatus
import pandas as pd

from src.lmp.cache import get_cached_lmp, save_lmp_cache
from src.lmp.models import LMPData

logger = logging.getLogger(__name__)

DEFAULT_PJM_API_KEY = "577dd812c69c47759874dc6a19bbe239"
PJM_API_KEY = os.getenv("PJM_API_KEY", DEFAULT_PJM_API_KEY)

# All 23 PJM pricing zones (Location Short Name values)
PJM_ZONES: list[str] = [
    "AEP",
    "AECO",
    "APS",
    "ATSI",
    "BGE",
    "COMED",
    "DAY",
    "DEOK",
    "DOM",
    "DPL",
    "DUQ",
    "EKPC",
    "JCPL",
    "METED",
    "MID-ATL/APS",
    "OVEC",
    "PECO",
    "PENELEC",
    "PEPCO",
    "PJM-RTO",
    "PPL",
    "PSEG",
    "RECO",
]

# ERCOT settlement point locations: 8 load zones + 7 trading hubs
ERCOT_LOCATIONS: list[str] = [
    "HB_BUSAVG",
    "HB_HOUSTON",
    "HB_HUBAVG",
    "HB_NORTH",
    "HB_PAN",
    "HB_SOUTH",
    "HB_WEST",
    "LZ_AEN",
    "LZ_CPS",
    "LZ_HOUSTON",
    "LZ_LCRA",
    "LZ_NORTH",
    "LZ_RAYBN",
    "LZ_SOUTH",
    "LZ_WEST",
]

# CAISO trading hub mapping: short name → full gridstatus location name
CAISO_HUB_MAP: dict[str, str] = {
    "NP15": "TH_NP15_GEN-APND",
    "SP15": "TH_SP15_GEN-APND",
    "ZP26": "TH_ZP26_GEN-APND",
}
CAISO_LOCATIONS: list[str] = list(CAISO_HUB_MAP.keys())


def fetch_pjm_lmp(
    zone: str,
    market: str = "DAY_AHEAD_HOURLY",
    year: int | None = None,
) -> LMPData:
    """Fetch PJM day-ahead hourly LMP data for a zone.

    Args:
        zone: PJM zone short name (e.g., "AEP", "PECO"). Case-insensitive.
        market: Market type. Default "DAY_AHEAD_HOURLY".
        year: Calendar year to fetch. Defaults to previous full calendar year.

    Returns:
        LMPData with 8760 hourly prices.

    Raises:
        ValueError: If zone is not a valid PJM zone.
    """
    # Normalize and validate zone
    zone = zone.upper()
    if zone not in PJM_ZONES:
        raise ValueError(
            f"Invalid PJM zone '{zone}'. Must be one of: {PJM_ZONES}"
        )

    # Default to previous full calendar year
    if year is None:
        year = datetime.now().year - 1

    # Check cache first
    cached_df = get_cached_lmp("pjm", zone, market, year)
    if cached_df is not None:
        logger.info("Using cached LMP data for PJM/%s/%d", zone, year)
        return LMPData(
            iso="pjm",
            zone=zone,
            market=market,
            year=year,
            prices=cached_df["lmp"].tolist(),
            timestamps=pd.to_datetime(cached_df["timestamp"]).tolist(),
        )

    # Fetch from PJM via gridstatus
    logger.info("Fetching PJM LMP data: zone=%s, market=%s, year=%d", zone, market, year)
    pjm = gridstatus.PJM(api_key=PJM_API_KEY)
    df = pjm.get_lmp(
        date=f"{year}-01-01",
        end=f"{year + 1}-01-01",
        market=market,
        location_type="ZONE",
    )

    # Filter to requested zone
    df = df[df["Location Short Name"] == zone].copy()
    if df.empty:
        raise ValueError(
            f"No LMP data returned for zone '{zone}' in {year}. "
            f"Check zone name and data availability."
        )

    # Extract timestamps and prices, sort by time
    df = df.sort_values("Interval Start").reset_index(drop=True)
    timestamps = pd.to_datetime(df["Interval Start"])
    prices = df["LMP"].astype(float)

    # Convert to UTC
    if timestamps.dt.tz is not None:
        timestamps = timestamps.dt.tz_convert("UTC")
    else:
        timestamps = timestamps.dt.tz_localize("UTC")

    # Build hourly series and handle gaps
    timestamps, prices = _fill_gaps(timestamps, prices)

    # Handle leap year: drop Feb 29 to get 8760
    if len(timestamps) == 8784:
        logger.info("Leap year detected (%d), dropping Feb 29 rows", year)
        mask = ~((timestamps.dt.month == 2) & (timestamps.dt.day == 29))
        timestamps = timestamps[mask].reset_index(drop=True)
        prices = prices[mask].reset_index(drop=True)

    if len(prices) != 8760:
        logger.warning(
            "PJM/%s/%d: expected 8760 prices, got %d",
            zone,
            year,
            len(prices),
        )

    # Build cache DataFrame and save
    cache_df = pd.DataFrame(
        {"timestamp": timestamps, "lmp": prices}
    )
    save_lmp_cache("pjm", zone, market, year, cache_df)

    return LMPData(
        iso="pjm",
        zone=zone,
        market=market,
        year=year,
        prices=prices.tolist(),
        timestamps=timestamps.tolist(),
    )


def fetch_ercot_lmp(
    zone: str,
    market: str = "DAY_AHEAD_HOURLY",
    year: int | None = None,
) -> LMPData:
    """Fetch ERCOT day-ahead Settlement Point Prices for a zone or hub.

    Args:
        zone: ERCOT location name (e.g., "LZ_HOUSTON", "HB_NORTH").
            Case-insensitive.
        market: Market type. Only "DAY_AHEAD_HOURLY" is supported via
            get_dam_spp. Other values trigger a warning and DAM data
            is returned.
        year: Calendar year to fetch. Defaults to previous full calendar year.

    Returns:
        LMPData with 8760 hourly prices.

    Raises:
        ValueError: If zone is not a valid ERCOT location.
    """
    # Normalize and validate zone
    zone = zone.upper()
    if zone not in ERCOT_LOCATIONS:
        raise ValueError(
            f"Invalid ERCOT location '{zone}'. Must be one of: {ERCOT_LOCATIONS}"
        )

    if market != "DAY_AHEAD_HOURLY":
        logger.warning(
            "ERCOT get_dam_spp only returns DAY_AHEAD_HOURLY data. "
            "Requested market '%s' ignored; returning DAM data.",
            market,
        )

    # Default to previous full calendar year
    if year is None:
        year = datetime.now().year - 1

    # Check cache first
    cached_df = get_cached_lmp("ercot", zone, market, year)
    if cached_df is not None:
        logger.info("Using cached SPP data for ERCOT/%s/%d", zone, year)
        return LMPData(
            iso="ercot",
            zone=zone,
            market=market,
            year=year,
            prices=cached_df["lmp"].tolist(),
            timestamps=pd.to_datetime(cached_df["timestamp"]).tolist(),
        )

    # Fetch from ERCOT via gridstatus
    logger.info("Fetching ERCOT SPP data: zone=%s, year=%d", zone, year)
    ercot = gridstatus.Ercot()
    df = ercot.get_dam_spp(year=year)

    # Filter to requested location
    df = df[df["Location"] == zone].copy()
    if df.empty:
        raise ValueError(
            f"No SPP data returned for location '{zone}' in {year}. "
            f"Check location name and data availability."
        )

    # Extract timestamps and prices, sort by time
    df = df.sort_values("Interval Start").reset_index(drop=True)
    timestamps = pd.to_datetime(df["Interval Start"])
    prices = df["SPP"].astype(float)

    # Convert to UTC
    if timestamps.dt.tz is not None:
        timestamps = timestamps.dt.tz_convert("UTC")
    else:
        timestamps = timestamps.dt.tz_localize("UTC")

    # Build hourly series and handle gaps
    timestamps, prices = _fill_gaps(timestamps, prices)

    # Handle leap year: drop Feb 29 to get 8760
    if len(timestamps) == 8784:
        logger.info("Leap year detected (%d), dropping Feb 29 rows", year)
        mask = ~((timestamps.dt.month == 2) & (timestamps.dt.day == 29))
        timestamps = timestamps[mask].reset_index(drop=True)
        prices = prices[mask].reset_index(drop=True)

    if len(prices) != 8760:
        logger.warning(
            "ERCOT/%s/%d: expected 8760 prices, got %d",
            zone,
            year,
            len(prices),
        )

    # Build cache DataFrame and save
    cache_df = pd.DataFrame(
        {"timestamp": timestamps, "lmp": prices}
    )
    save_lmp_cache("ercot", zone, market, year, cache_df)

    return LMPData(
        iso="ercot",
        zone=zone,
        market=market,
        year=year,
        prices=prices.tolist(),
        timestamps=timestamps.tolist(),
    )


def fetch_caiso_lmp(
    zone: str,
    market: str = "DAY_AHEAD_HOURLY",
    year: int | None = None,
) -> LMPData:
    """Fetch CAISO day-ahead hourly LMP data for a trading hub.

    Args:
        zone: CAISO hub short name ("NP15", "SP15", "ZP26").
            Case-insensitive.
        market: Market type. Default "DAY_AHEAD_HOURLY".
        year: Calendar year to fetch. Defaults to previous full calendar year.

    Returns:
        LMPData with 8760 hourly prices.

    Raises:
        ValueError: If zone is not a valid CAISO hub.
    """
    # Normalize and validate zone
    zone = zone.upper()
    if zone not in CAISO_HUB_MAP:
        raise ValueError(
            f"Invalid CAISO hub '{zone}'. Must be one of: {CAISO_LOCATIONS}"
        )
    full_hub_name = CAISO_HUB_MAP[zone]

    # Default to previous full calendar year
    if year is None:
        year = datetime.now().year - 1

    # Check cache first (use short name in cache key)
    cached_df = get_cached_lmp("caiso", zone, market, year)
    if cached_df is not None:
        logger.info("Using cached LMP data for CAISO/%s/%d", zone, year)
        return LMPData(
            iso="caiso",
            zone=zone,
            market=market,
            year=year,
            prices=cached_df["lmp"].tolist(),
            timestamps=pd.to_datetime(cached_df["timestamp"]).tolist(),
        )

    # Fetch from CAISO via gridstatus
    logger.info(
        "Fetching CAISO LMP data: zone=%s (%s), market=%s, year=%d",
        zone, full_hub_name, market, year,
    )
    caiso = gridstatus.CAISO()
    df = caiso.get_lmp(
        date=f"{year}-01-01",
        end=f"{year + 1}-01-01",
        market=market,
        locations=[full_hub_name],
    )

    # Filter to requested hub (should already be filtered, but be safe)
    df = df[df["Location"] == full_hub_name].copy()
    if df.empty:
        raise ValueError(
            f"No LMP data returned for hub '{zone}' ({full_hub_name}) in {year}. "
            f"Check hub name and data availability."
        )

    # Extract timestamps and prices, sort by time
    df = df.sort_values("Interval Start").reset_index(drop=True)
    timestamps = pd.to_datetime(df["Interval Start"])
    prices = df["LMP"].astype(float)

    # Convert to UTC
    if timestamps.dt.tz is not None:
        timestamps = timestamps.dt.tz_convert("UTC")
    else:
        timestamps = timestamps.dt.tz_localize("UTC")

    # Build hourly series and handle gaps
    timestamps, prices = _fill_gaps(timestamps, prices)

    # Handle leap year: drop Feb 29 to get 8760
    if len(timestamps) == 8784:
        logger.info("Leap year detected (%d), dropping Feb 29 rows", year)
        mask = ~((timestamps.dt.month == 2) & (timestamps.dt.day == 29))
        timestamps = timestamps[mask].reset_index(drop=True)
        prices = prices[mask].reset_index(drop=True)

    if len(prices) != 8760:
        logger.warning(
            "CAISO/%s/%d: expected 8760 prices, got %d",
            zone,
            year,
            len(prices),
        )

    # Build cache DataFrame and save (use short name)
    cache_df = pd.DataFrame(
        {"timestamp": timestamps, "lmp": prices}
    )
    save_lmp_cache("caiso", zone, market, year, cache_df)

    return LMPData(
        iso="caiso",
        zone=zone,
        market=market,
        year=year,
        prices=prices.tolist(),
        timestamps=timestamps.tolist(),
    )


def _fill_gaps(
    timestamps: pd.Series, prices: pd.Series
) -> tuple[pd.Series, pd.Series]:
    """Forward-fill small gaps in hourly LMP data.

    Gaps of 4 hours or less are forward-filled. Larger gaps are logged
    as warnings (data quality varies by year/zone).

    Args:
        timestamps: Sorted UTC timestamps.
        prices: Corresponding LMP prices.

    Returns:
        Tuple of (timestamps, prices) with gaps filled.
    """
    # Build an hourly index spanning the data range
    full_index = pd.date_range(
        start=timestamps.iloc[0],
        end=timestamps.iloc[-1],
        freq="h",
    )

    # Reindex to expose gaps
    series = pd.Series(prices.values, index=pd.DatetimeIndex(timestamps))
    series = series[~series.index.duplicated(keep="first")]
    series = series.reindex(full_index)

    # Identify and handle gaps
    missing = series.isna()
    if missing.any():
        # Find consecutive gap lengths
        gap_groups = (~missing).cumsum()
        gap_lengths = missing.groupby(gap_groups).transform("sum")
        large_gaps = (gap_lengths > 4) & missing

        if large_gaps.any():
            n_large = large_gaps.sum()
            logger.warning(
                "LMP data has %d hours in gaps > 4 hours. Forward-filling all gaps.",
                n_large,
            )

        series = series.ffill()

    result_timestamps = pd.Series(series.index)
    result_prices = pd.Series(series.values, dtype=float)
    return result_timestamps, result_prices


def fetch_lmp(
    iso: str,
    zone: str,
    market: str = "DAY_AHEAD_HOURLY",
    year: int | None = None,
) -> LMPData:
    """Route LMP fetch to the appropriate ISO-specific client.

    Args:
        iso: ISO/RTO identifier ("pjm", "ercot", "caiso").
        zone: Pricing zone name.
        market: Market type. Default "DAY_AHEAD_HOURLY".
        year: Calendar year. Defaults to previous full calendar year.

    Returns:
        LMPData with 8760 hourly prices.

    Raises:
        NotImplementedError: If the ISO is not yet supported.
        ValueError: If the ISO is unrecognized.
    """
    iso_lower = iso.lower()

    if iso_lower == "pjm":
        return fetch_pjm_lmp(zone=zone, market=market, year=year)
    elif iso_lower == "ercot":
        return fetch_ercot_lmp(zone=zone, market=market, year=year)
    elif iso_lower == "caiso":
        return fetch_caiso_lmp(zone=zone, market=market, year=year)
    else:
        raise ValueError(f"Unrecognized ISO: '{iso}'.")
