"""NOAA NCEI client for fetching hourly precipitation data (HPCP)."""

import time

import pandas as pd
import requests

from src.climate.cache_manager import _calculate_distance
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

NCEI_BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"
HPCP_DATASET_ID = "PRECIP_HLY"
HPCP_DATATYPE_ID = "HPCP"
PAGE_SIZE = 1000
MIN_REQUEST_INTERVAL = 0.2  # 200ms = 5 req/sec


class PrecipitationClient:
    """Fetches hourly precipitation data from NOAA NCEI API.

    Best-effort client: any failure returns None and the caller falls back
    to zeros. Never raises exceptions.

    Args:
        api_token: NOAA NCEI API token for authentication.
    """

    def __init__(self, api_token: str = "WewidNCeiBHMUnnVbgNyjKxxHCSXXCad") -> None:
        self.api_token = api_token
        self._last_request_time: float = 0.0

    def fetch_precipitation(
        self,
        lat: float,
        lon: float,
        year: int,
        max_distance_km: float = 100.0,
    ) -> pd.Series | None:
        """Fetch hourly precipitation for a location and year.

        Orchestrates station search, data fetch, and alignment to a complete
        hourly series. Returns None on any failure.

        Args:
            lat: Site latitude in degrees.
            lon: Site longitude in degrees.
            year: Data year to retrieve.
            max_distance_km: Maximum station distance in kilometers.

        Returns:
            pd.Series of hourly precipitation (8760 or 8784 values), or None.
        """
        try:
            station_id = self._find_nearest_station(lat, lon, max_distance_km)
            if station_id is None:
                logger.warning(
                    f"No NCEI station within {max_distance_km} km of ({lat}, {lon})"
                )
                return None

            raw_data = self._fetch_hpcp_data(station_id, year)
            if raw_data is None:
                logger.warning(
                    f"No HPCP data from station {station_id} for year {year}"
                )
                return None

            series = self._align_to_hourly(raw_data, year)
            logger.info(
                f"Precipitation data fetched for ({lat}, {lon}): "
                f"station {station_id}, {len(series)} hours"
            )
            return series

        except Exception:
            logger.warning(
                f"Unexpected error fetching precipitation for ({lat}, {lon})",
                exc_info=True,
            )
            return None

    def _find_nearest_station(
        self, lat: float, lon: float, max_distance_km: float
    ) -> str | None:
        """Find the nearest NCEI station with HPCP data.

        Uses a bounding box search via the NCEI /stations endpoint,
        then picks the closest station using Haversine distance.

        Args:
            lat: Target latitude.
            lon: Target longitude.
            max_distance_km: Maximum acceptable distance.

        Returns:
            Station ID string, or None if no station found.
        """
        # Convert km to approximate degree offset (1 degree ≈ 111 km)
        degree_offset = max_distance_km / 111.0
        extent = (
            f"{lat - degree_offset:.4f},{lon - degree_offset:.4f},"
            f"{lat + degree_offset:.4f},{lon + degree_offset:.4f}"
        )

        self._rate_limit()
        try:
            response = requests.get(
                f"{NCEI_BASE_URL}/stations",
                headers={"token": self.api_token},
                params={
                    "datasetid": HPCP_DATASET_ID,
                    "datatypeid": HPCP_DATATYPE_ID,
                    "extent": extent,
                    "limit": 1000,
                },
                timeout=30,
            )
            response.raise_for_status()
        except requests.RequestException:
            logger.warning(
                f"NCEI station search failed for ({lat}, {lon})",
                exc_info=True,
            )
            return None

        data = response.json()
        results = data.get("results", [])
        if not results:
            return None

        # Find closest station using Haversine
        best_id: str | None = None
        best_dist = float("inf")

        for station in results:
            station_lat = station.get("latitude")
            station_lon = station.get("longitude")
            if station_lat is None or station_lon is None:
                continue

            dist = _calculate_distance(lat, lon, station_lat, station_lon)
            if dist < best_dist and dist <= max_distance_km:
                best_dist = dist
                best_id = station["id"]

        if best_id is not None:
            logger.debug(
                f"Nearest NCEI station: {best_id} ({best_dist:.1f} km from ({lat}, {lon}))"
            )

        return best_id

    def _fetch_hpcp_data(
        self, station_id: str, year: int
    ) -> pd.DataFrame | None:
        """Fetch raw HPCP data for a station and year with pagination.

        Args:
            station_id: NCEI station identifier.
            year: Data year to retrieve.

        Returns:
            DataFrame with 'date' and 'value' columns, or None.
        """
        all_results: list[dict] = []
        offset = 1

        while True:
            self._rate_limit()
            try:
                response = requests.get(
                    f"{NCEI_BASE_URL}/data",
                    headers={"token": self.api_token},
                    params={
                        "datasetid": HPCP_DATASET_ID,
                        "datatypeid": HPCP_DATATYPE_ID,
                        "stationid": station_id,
                        "startdate": f"{year}-01-01",
                        "enddate": f"{year}-12-31",
                        "units": "metric",
                        "limit": PAGE_SIZE,
                        "offset": offset,
                    },
                    timeout=30,
                )
                response.raise_for_status()
            except requests.RequestException:
                logger.warning(
                    f"NCEI data fetch failed for station {station_id}",
                    exc_info=True,
                )
                return None

            data = response.json()
            results = data.get("results", [])
            if not results:
                break

            all_results.extend(results)

            # Check if there are more pages
            metadata = data.get("metadata", {}).get("resultset", {})
            total_count = metadata.get("count", 0)
            if offset + PAGE_SIZE > total_count:
                break
            offset += PAGE_SIZE

        if not all_results:
            return None

        df = pd.DataFrame(all_results)
        return df[["date", "value"]]

    def _align_to_hourly(self, raw_data: pd.DataFrame, year: int) -> pd.Series:
        """Align sparse NCEI timestamps to a complete hourly series.

        Maps NCEI precipitation records to an 8760 (non-leap) or 8784 (leap)
        hourly index, filling gaps with 0.0.

        Args:
            raw_data: DataFrame with 'date' and 'value' columns.
            year: Year for the hourly index.

        Returns:
            pd.Series with complete hourly precipitation values.
        """
        # Create complete hourly index for the year
        start = pd.Timestamp(f"{year}-01-01")
        end = pd.Timestamp(f"{year}-12-31 23:00:00")
        hourly_index = pd.date_range(start=start, end=end, freq="h")

        # Parse NCEI timestamps and set as index
        raw_data = raw_data.copy()
        raw_data["date"] = pd.to_datetime(raw_data["date"])
        raw_data = raw_data.set_index("date")

        # Reindex to complete hourly series, fill gaps with 0
        aligned = raw_data["value"].reindex(hourly_index, fill_value=0.0)
        aligned.name = "Precipitation"

        return aligned

    def _rate_limit(self) -> None:
        """Enforce minimum interval between API requests."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.time()
