"""NSRDB API client for retrieving solar resource weather data."""

import requests

from src.utils.exceptions import ClimateDataError
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

NSRDB_PSM_URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/nsrdb-GOES-aggregated-v4-0-0-download.csv"

WEATHER_ATTRIBUTES = [
    "ghi",
    "dni",
    "dhi",
    "air_temperature",
    "wind_speed",
    "surface_albedo",
]


class NSRDBClient:
    """Client for the NREL NSRDB GOES Aggregated v4.0.0 API.

    Updated 2026-03-07 from PSM v3.2.2 (deprecated, returns 404) to GOES
    Aggregated v4.0.0. Old endpoint URL was:
    https://developer.nrel.gov/api/nsrdb/v2/solar/psm3-2-2-download.csv

    If NSRDB starts returning 404 again, check:
    1. Has the endpoint been deprecated? See https://developer.nrel.gov/docs/solar/nsrdb/
    2. Are credentials valid? Test at https://developer.nrel.gov/api/nsrdb/

    Credentials default to hardcoded values here and in ClimateConfig. Override
    with env vars NSRDB_API_KEY and NSRDB_API_EMAIL.

    Args:
        api_key: NREL API key.
        email: Email associated with NREL API account.
    """

    def __init__(
        self,
        api_key: str = "y1zAp5Hghami0SWXdi0xhc6kcvfWZhpliZoApVzB",
        email: str = "rotter.mich@gmail.com",
    ) -> None:
        self.api_key = api_key
        self.email = email

    def fetch_weather_data(self, lat: float, lon: float, year: int = 2024) -> str:
        """Fetch hourly weather data from NSRDB for a given location and year.

        Args:
            lat: Latitude of the site.
            lon: Longitude of the site.
            year: Data year to retrieve (default: 2024).

        Returns:
            Raw CSV string from the NSRDB API response.

        Raises:
            ClimateDataError: On HTTP errors, timeouts, or unexpected responses.
        """
        # NSRDB uses WKT format: POINT(lon lat) — longitude first
        wkt = f"POINT({lon} {lat})"

        params = {
            "api_key": self.api_key,
            "email": self.email,
            "wkt": wkt,
            "names": str(year),
            "attributes": ",".join(WEATHER_ATTRIBUTES),
            "interval": "60",
            "leap_day": "true",
            "utc": "false",
        }

        logger.info(f"Fetching NSRDB data for ({lat}, {lon}), year={year}")

        try:
            response = requests.get(NSRDB_PSM_URL, params=params, timeout=120)
            response.raise_for_status()
        except requests.exceptions.Timeout as e:
            logger.error(f"NSRDB request timed out for ({lat}, {lon})")
            raise ClimateDataError(
                "NSRDB API request timed out",
                context={
                    "location": (lat, lon),
                    "api": "NSRDB",
                    "year": year,
                },
            ) from e
        except requests.exceptions.HTTPError as e:
            logger.error(
                f"NSRDB HTTP error {response.status_code} for ({lat}, {lon})"
            )
            raise ClimateDataError(
                f"NSRDB API returned HTTP {response.status_code}",
                context={
                    "location": (lat, lon),
                    "api": "NSRDB",
                    "status_code": response.status_code,
                    "response": response.text[:500],
                },
            ) from e
        except requests.exceptions.RequestException as e:
            logger.error(f"NSRDB request failed for ({lat}, {lon}): {e}")
            raise ClimateDataError(
                "NSRDB API request failed",
                context={
                    "location": (lat, lon),
                    "api": "NSRDB",
                    "error": str(e),
                },
            ) from e

        logger.info(f"Successfully retrieved NSRDB data for ({lat}, {lon})")
        return response.text
