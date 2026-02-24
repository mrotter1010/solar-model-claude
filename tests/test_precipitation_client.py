"""Tests for the NOAA NCEI precipitation client."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.climate.precipitation_client import (
    HPCP_DATASET_ID,
    HPCP_DATATYPE_ID,
    NCEI_BASE_URL,
    PAGE_SIZE,
    PrecipitationClient,
)

# Sample NCEI station search response
SAMPLE_STATIONS_RESPONSE = {
    "metadata": {"resultset": {"count": 2}},
    "results": [
        {
            "id": "COOP:026481",
            "name": "PHOENIX AIRPORT",
            "latitude": 33.43,
            "longitude": -112.02,
        },
        {
            "id": "COOP:026482",
            "name": "MESA",
            "latitude": 33.42,
            "longitude": -111.83,
        },
    ],
}

# Sample NCEI HPCP data response (single page)
SAMPLE_HPCP_RESPONSE = {
    "metadata": {"resultset": {"count": 3, "offset": 1, "limit": 1000}},
    "results": [
        {"date": "2023-01-01T06:00:00", "datatype": "HPCP", "value": 0.5},
        {"date": "2023-01-01T12:00:00", "datatype": "HPCP", "value": 1.2},
        {"date": "2023-07-15T18:00:00", "datatype": "HPCP", "value": 3.8},
    ],
}


class TestPrecipitationClientInit:
    """Tests for PrecipitationClient initialization."""

    def test_default_token(self) -> None:
        """Client uses default NCEI token when none provided."""
        client = PrecipitationClient()
        assert client.api_token == "WewidNCeiBHMUnnVbgNyjKxxHCSXXCad"

    def test_custom_token(self) -> None:
        """Client stores a custom API token."""
        client = PrecipitationClient(api_token="custom-token")
        assert client.api_token == "custom-token"


class TestFindNearestStation:
    """Tests for _find_nearest_station method."""

    @patch("src.climate.precipitation_client.requests.get")
    @patch("src.climate.precipitation_client.time.sleep")
    def test_finds_nearest_station(
        self, mock_sleep: MagicMock, mock_get: MagicMock
    ) -> None:
        """Returns the closest station ID when multiple stations found."""
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_STATIONS_RESPONSE
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = PrecipitationClient()
        station_id = client._find_nearest_station(33.45, -112.07, 100.0)

        # Phoenix Airport (33.43, -112.02) is closer than Mesa (33.42, -111.83)
        assert station_id == "COOP:026481"

    @patch("src.climate.precipitation_client.requests.get")
    @patch("src.climate.precipitation_client.time.sleep")
    def test_no_stations_returns_none(
        self, mock_sleep: MagicMock, mock_get: MagicMock
    ) -> None:
        """Returns None when no stations found in bounding box."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"metadata": {}, "results": []}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = PrecipitationClient()
        assert client._find_nearest_station(33.45, -112.07, 100.0) is None

    @patch("src.climate.precipitation_client.requests.get")
    @patch("src.climate.precipitation_client.time.sleep")
    def test_api_error_returns_none(
        self, mock_sleep: MagicMock, mock_get: MagicMock
    ) -> None:
        """Returns None when API call fails."""
        import requests as req

        mock_get.side_effect = req.exceptions.ConnectionError("DNS failed")

        client = PrecipitationClient()
        assert client._find_nearest_station(33.45, -112.07, 100.0) is None

    @patch("src.climate.precipitation_client.requests.get")
    @patch("src.climate.precipitation_client.time.sleep")
    def test_all_stations_too_far_returns_none(
        self, mock_sleep: MagicMock, mock_get: MagicMock
    ) -> None:
        """Returns None when all stations exceed max_distance_km."""
        # Station very far away
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "metadata": {"resultset": {"count": 1}},
            "results": [
                {
                    "id": "COOP:099999",
                    "name": "FAR AWAY",
                    "latitude": 40.0,
                    "longitude": -100.0,
                },
            ],
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = PrecipitationClient()
        # Max 50km, station is >1000km away
        assert client._find_nearest_station(33.45, -112.07, 50.0) is None

    @patch("src.climate.precipitation_client.requests.get")
    @patch("src.climate.precipitation_client.time.sleep")
    def test_station_search_params(
        self, mock_sleep: MagicMock, mock_get: MagicMock
    ) -> None:
        """Verify correct API parameters are sent for station search."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"metadata": {}, "results": []}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = PrecipitationClient(api_token="test-token")
        client._find_nearest_station(33.45, -112.07, 100.0)

        call_kwargs = mock_get.call_args
        assert call_kwargs[1]["headers"] == {"token": "test-token"}
        params = call_kwargs[1]["params"]
        assert params["datasetid"] == HPCP_DATASET_ID
        assert params["datatypeid"] == HPCP_DATATYPE_ID


class TestFetchHpcpData:
    """Tests for _fetch_hpcp_data method."""

    @patch("src.climate.precipitation_client.requests.get")
    @patch("src.climate.precipitation_client.time.sleep")
    def test_single_page_fetch(
        self, mock_sleep: MagicMock, mock_get: MagicMock
    ) -> None:
        """Returns DataFrame with date and value columns for single page."""
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_HPCP_RESPONSE
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = PrecipitationClient()
        df = client._fetch_hpcp_data("COOP:026481", 2023)

        assert df is not None
        assert len(df) == 3
        assert list(df.columns) == ["date", "value"]
        assert df.iloc[0]["value"] == 0.5

    @patch("src.climate.precipitation_client.requests.get")
    @patch("src.climate.precipitation_client.time.sleep")
    def test_multi_page_pagination(
        self, mock_sleep: MagicMock, mock_get: MagicMock
    ) -> None:
        """Fetches multiple pages when total count exceeds page size."""
        # Page 1: 1000 records (more to fetch)
        page1_results = [
            {"date": f"2023-01-01T{i % 24:02d}:00:00", "datatype": "HPCP", "value": 0.1}
            for i in range(PAGE_SIZE)
        ]
        page1_response = MagicMock()
        page1_response.json.return_value = {
            "metadata": {"resultset": {"count": 1500, "offset": 1, "limit": PAGE_SIZE}},
            "results": page1_results,
        }
        page1_response.raise_for_status = MagicMock()

        # Page 2: 500 records (last page)
        page2_results = [
            {"date": f"2023-06-01T{i % 24:02d}:00:00", "datatype": "HPCP", "value": 0.2}
            for i in range(500)
        ]
        page2_response = MagicMock()
        page2_response.json.return_value = {
            "metadata": {"resultset": {"count": 1500, "offset": 1001, "limit": PAGE_SIZE}},
            "results": page2_results,
        }
        page2_response.raise_for_status = MagicMock()

        mock_get.side_effect = [page1_response, page2_response]

        client = PrecipitationClient()
        df = client._fetch_hpcp_data("COOP:026481", 2023)

        assert df is not None
        assert len(df) == 1500
        assert mock_get.call_count == 2

    @patch("src.climate.precipitation_client.requests.get")
    @patch("src.climate.precipitation_client.time.sleep")
    def test_empty_results_returns_none(
        self, mock_sleep: MagicMock, mock_get: MagicMock
    ) -> None:
        """Returns None when API returns no data records."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"metadata": {}, "results": []}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = PrecipitationClient()
        assert client._fetch_hpcp_data("COOP:026481", 2023) is None

    @patch("src.climate.precipitation_client.requests.get")
    @patch("src.climate.precipitation_client.time.sleep")
    def test_api_error_returns_none(
        self, mock_sleep: MagicMock, mock_get: MagicMock
    ) -> None:
        """Returns None when API call fails."""
        import requests as req

        mock_get.side_effect = req.exceptions.Timeout("timeout")

        client = PrecipitationClient()
        assert client._fetch_hpcp_data("COOP:026481", 2023) is None


class TestAlignToHourly:
    """Tests for _align_to_hourly method."""

    def test_non_leap_year_8760(self) -> None:
        """Non-leap year produces 8760-hour series."""
        raw = pd.DataFrame({
            "date": ["2023-01-01T06:00:00", "2023-06-15T12:00:00"],
            "value": [1.5, 2.3],
        })

        client = PrecipitationClient()
        series = client._align_to_hourly(raw, 2023)

        assert len(series) == 8760
        assert series.name == "Precipitation"

    def test_leap_year_8784(self) -> None:
        """Leap year produces 8784-hour series."""
        raw = pd.DataFrame({
            "date": ["2024-02-29T12:00:00"],
            "value": [0.8],
        })

        client = PrecipitationClient()
        series = client._align_to_hourly(raw, 2024)

        assert len(series) == 8784

    def test_sparse_data_fills_zeros(self) -> None:
        """Gaps between data points are filled with 0.0."""
        raw = pd.DataFrame({
            "date": ["2023-01-01T00:00:00", "2023-01-01T12:00:00"],
            "value": [1.0, 2.0],
        })

        client = PrecipitationClient()
        series = client._align_to_hourly(raw, 2023)

        # Provided values should be present
        assert series.iloc[0] == 1.0  # Hour 0
        assert series.iloc[12] == 2.0  # Hour 12
        # Gaps should be zero
        assert series.iloc[1] == 0.0
        assert series.iloc[6] == 0.0

    def test_values_preserved(self, climate_results_dir: Path) -> None:
        """Specific precipitation values are correctly placed in the output."""
        raw = pd.DataFrame({
            "date": [
                "2023-03-15T08:00:00",
                "2023-03-15T09:00:00",
                "2023-07-04T14:00:00",
            ],
            "value": [5.0, 3.2, 10.1],
        })

        client = PrecipitationClient()
        series = client._align_to_hourly(raw, 2023)

        # Verify non-zero values sum
        assert series.sum() == pytest.approx(18.3)

        # Save test artifact
        output_path = climate_results_dir / "test_precip_aligned_hourly.json"
        output_path.write_text(json.dumps({
            "total_hours": len(series),
            "non_zero_hours": int((series > 0).sum()),
            "total_precipitation_mm": round(float(series.sum()), 2),
        }, indent=2))


class TestFetchPrecipitation:
    """Tests for the end-to-end fetch_precipitation method."""

    @patch("src.climate.precipitation_client.requests.get")
    @patch("src.climate.precipitation_client.time.sleep")
    def test_end_to_end_success(
        self, mock_sleep: MagicMock, mock_get: MagicMock
    ) -> None:
        """Successful station search + data fetch returns 8760 series."""
        # First call: station search
        station_response = MagicMock()
        station_response.json.return_value = SAMPLE_STATIONS_RESPONSE
        station_response.raise_for_status = MagicMock()

        # Second call: HPCP data fetch
        data_response = MagicMock()
        data_response.json.return_value = SAMPLE_HPCP_RESPONSE
        data_response.raise_for_status = MagicMock()

        mock_get.side_effect = [station_response, data_response]

        client = PrecipitationClient()
        result = client.fetch_precipitation(33.45, -112.07, 2023)

        assert result is not None
        assert len(result) == 8760
        assert result.sum() > 0

    @patch("src.climate.precipitation_client.requests.get")
    @patch("src.climate.precipitation_client.time.sleep")
    def test_no_station_returns_none(
        self, mock_sleep: MagicMock, mock_get: MagicMock
    ) -> None:
        """Returns None when no nearby station is found."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"metadata": {}, "results": []}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = PrecipitationClient()
        assert client.fetch_precipitation(33.45, -112.07, 2023) is None

    @patch("src.climate.precipitation_client.requests.get")
    @patch("src.climate.precipitation_client.time.sleep")
    def test_data_fetch_failure_returns_none(
        self, mock_sleep: MagicMock, mock_get: MagicMock
    ) -> None:
        """Returns None when station found but data fetch fails."""
        import requests as req

        # Station search succeeds
        station_response = MagicMock()
        station_response.json.return_value = SAMPLE_STATIONS_RESPONSE
        station_response.raise_for_status = MagicMock()

        # Data fetch fails
        data_response = MagicMock()
        data_response.raise_for_status.side_effect = req.exceptions.HTTPError(
            response=MagicMock(status_code=500)
        )

        mock_get.side_effect = [station_response, data_response]

        client = PrecipitationClient()
        assert client.fetch_precipitation(33.45, -112.07, 2023) is None


class TestRateLimiting:
    """Tests for rate limiting behavior."""

    @patch("src.climate.precipitation_client.time.sleep")
    @patch("src.climate.precipitation_client.time.time")
    def test_sleep_called_when_requests_fast(
        self, mock_time: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """Rate limiter sleeps when requests come faster than 200ms apart."""
        # First call: time returns 100.0, sets _last_request_time
        # Second call: time returns 100.05 (only 50ms elapsed)
        mock_time.side_effect = [100.0, 100.0, 100.05, 100.25]

        client = PrecipitationClient()
        client._rate_limit()  # First call, sets baseline
        client._rate_limit()  # Second call, should sleep

        # sleep should have been called on the second invocation
        assert mock_sleep.call_count >= 1
        # Sleep duration should be ~0.15s (0.2 - 0.05)
        sleep_arg = mock_sleep.call_args[0][0]
        assert 0.1 < sleep_arg < 0.2
