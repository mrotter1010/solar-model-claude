"""Tests for NSRDB API client."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.climate.nsrdb_client import NSRDB_PSM_URL, NSRDBClient, WEATHER_ATTRIBUTES
from src.utils.exceptions import ClimateDataError

# Realistic NSRDB CSV response (2-row header + column names + data)
SAMPLE_NSRDB_CSV = """Source,Location ID,City,State,Country,Latitude,Longitude,Time Zone,Elevation,Local Time Zone,Clearsky DHI Units,Clearsky DNI Units,Clearsky GHI Units,Dew Point Units,DHI Units,DNI Units,GHI Units,Solar Zenith Angle Units,Temperature Units,Pressure Units,Relative Humidity Units,Precipitable Water Units,Wind Direction Units,Wind Speed Units,Cloud Type -15,Cloud Type 0,Cloud Type 1,Cloud Type 2,Cloud Type 3,Cloud Type 4,Cloud Type 5,Cloud Type 6,Cloud Type 7,Cloud Type 8,Cloud Type 9,Cloud Type 10,Cloud Type 11,Cloud Type 12,Fill Flag 0,Fill Flag 1,Fill Flag 2,Fill Flag 3,Fill Flag 4,Fill Flag 5,Surface Albedo Units
NSRDB,123456,Phoenix,AZ,United States,33.45,-111.98,-7,337,
Year,Month,Day,Hour,Minute,GHI,DNI,DHI,Temperature,Wind Speed,Surface Albedo
2024,1,1,0,0,0,0,0,8.5,2.1,0.18
2024,1,1,1,0,0,0,0,7.9,1.8,0.18
2024,1,1,2,0,0,0,0,7.2,1.5,0.18
"""


class TestNSRDBClientInit:
    """Tests for NSRDBClient initialization."""

    def test_default_params(self) -> None:
        """Client uses demo defaults when no params provided."""
        client = NSRDBClient()
        assert client.api_key == "DEMO_KEY"
        assert client.email == "demo@example.com"

    def test_custom_params(self) -> None:
        """Client stores custom API key and email."""
        client = NSRDBClient(api_key="my_key", email="user@test.com")
        assert client.api_key == "my_key"
        assert client.email == "user@test.com"


class TestFetchWeatherData:
    """Tests for the fetch_weather_data method."""

    @patch("src.climate.nsrdb_client.requests.get")
    def test_successful_fetch(self, mock_get: MagicMock) -> None:
        """Successful API call returns raw CSV text."""
        # Arrange
        mock_response = MagicMock()
        mock_response.text = SAMPLE_NSRDB_CSV
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = NSRDBClient()

        # Act
        result = client.fetch_weather_data(lat=33.45, lon=-111.98)

        # Assert — returns the raw CSV
        assert result == SAMPLE_NSRDB_CSV
        mock_get.assert_called_once()

    @patch("src.climate.nsrdb_client.requests.get")
    def test_wkt_format_lon_lat_order(self, mock_get: MagicMock) -> None:
        """WKT parameter uses POINT(lon lat) order — longitude first."""
        mock_response = MagicMock()
        mock_response.text = SAMPLE_NSRDB_CSV
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = NSRDBClient()
        client.fetch_weather_data(lat=33.45, lon=-111.98, year=2023)

        # Verify the WKT in the call params
        call_kwargs = mock_get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert params["wkt"] == "POINT(-111.98 33.45)"

    @patch("src.climate.nsrdb_client.requests.get")
    def test_query_params(self, mock_get: MagicMock) -> None:
        """All required query parameters are sent correctly."""
        mock_response = MagicMock()
        mock_response.text = SAMPLE_NSRDB_CSV
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = NSRDBClient(api_key="test_key", email="test@test.com")
        client.fetch_weather_data(lat=33.45, lon=-111.98, year=2023)

        call_kwargs = mock_get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")

        # Verify all expected params
        assert params["api_key"] == "test_key"
        assert params["email"] == "test@test.com"
        assert params["names"] == "2023"
        assert params["attributes"] == ",".join(WEATHER_ATTRIBUTES)
        assert params["interval"] == "60"
        assert params["leap_day"] == "true"
        assert params["utc"] == "false"

    @patch("src.climate.nsrdb_client.requests.get")
    def test_correct_url(self, mock_get: MagicMock) -> None:
        """Request is made to the PSM v3.2.2 endpoint."""
        mock_response = MagicMock()
        mock_response.text = SAMPLE_NSRDB_CSV
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = NSRDBClient()
        client.fetch_weather_data(lat=33.45, lon=-111.98)

        call_args = mock_get.call_args
        assert call_args[0][0] == NSRDB_PSM_URL

    @patch("src.climate.nsrdb_client.requests.get")
    def test_http_error_raises_climate_data_error(
        self, mock_get: MagicMock
    ) -> None:
        """HTTP errors are wrapped in ClimateDataError with context."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )
        mock_get.return_value = mock_response

        client = NSRDBClient()

        with pytest.raises(ClimateDataError) as exc_info:
            client.fetch_weather_data(lat=33.45, lon=-111.98)

        assert exc_info.value.context["status_code"] == 404
        assert exc_info.value.context["api"] == "NSRDB"

    @patch("src.climate.nsrdb_client.requests.get")
    def test_server_error_500(self, mock_get: MagicMock) -> None:
        """500 errors include status code and response snippet in context."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )
        mock_get.return_value = mock_response

        client = NSRDBClient()

        with pytest.raises(ClimateDataError) as exc_info:
            client.fetch_weather_data(lat=33.45, lon=-111.98)

        assert exc_info.value.context["status_code"] == 500
        assert "response" in exc_info.value.context

    @patch("src.climate.nsrdb_client.requests.get")
    def test_timeout_raises_climate_data_error(
        self, mock_get: MagicMock
    ) -> None:
        """Timeout is caught and wrapped with appropriate context."""
        mock_get.side_effect = requests.exceptions.Timeout("Connection timed out")

        client = NSRDBClient()

        with pytest.raises(ClimateDataError) as exc_info:
            client.fetch_weather_data(lat=33.45, lon=-111.98)

        assert "timed out" in exc_info.value.message.lower()
        assert exc_info.value.context["location"] == (33.45, -111.98)

    @patch("src.climate.nsrdb_client.requests.get")
    def test_connection_error_raises_climate_data_error(
        self, mock_get: MagicMock
    ) -> None:
        """Connection errors are wrapped in ClimateDataError."""
        mock_get.side_effect = requests.exceptions.ConnectionError("DNS failed")

        client = NSRDBClient()

        with pytest.raises(ClimateDataError):
            client.fetch_weather_data(lat=33.45, lon=-111.98)
