"""Tests for the Open-Meteo Historical Weather API client.

Tests fetch_era5_land_data which is re-exported from era5_client.py
but implemented in open_meteo_client.py. Mocks target the implementation
module (open_meteo_client).
"""

import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.climate.era5_client import fetch_era5_land_data

LAT, LON, YEAR = 33.45, -112.07, 2023


def _make_api_response(
    n_hours: int = 8760,
    snow_value: float = 0.05,
    precip_value: float = 0.1,
    year: int = 2023,
    include_nulls: bool = False,
) -> bytes:
    """Create a fake Open-Meteo API JSON response.

    Args:
        n_hours: Number of hourly values (8760 or 8784).
        snow_value: Constant snow depth in meters.
        precip_value: Constant precipitation in mm per hour.
        year: Year for timestamp generation.
        include_nulls: If True, set first 24 values of each variable to None.

    Returns:
        JSON-encoded bytes matching the Open-Meteo response format.
    """
    start = datetime(year, 1, 1)
    timestamps = [
        (start + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M")
        for i in range(n_hours)
    ]

    snow_values: list[float | None] = [snow_value] * n_hours
    precip_values: list[float | None] = [precip_value] * n_hours

    if include_nulls:
        for i in range(24):
            snow_values[i] = None
            precip_values[i] = None

    data = {
        "hourly": {
            "time": timestamps,
            "snow_depth": snow_values,
            "precipitation": precip_values,
        }
    }
    return json.dumps(data).encode()


def _mock_urlopen(response_bytes: bytes) -> MagicMock:
    """Create a mock for urllib.request.urlopen as a context manager.

    Args:
        response_bytes: The bytes that mock_resp.read() should return.

    Returns:
        MagicMock configured as a context manager returning a readable response.
    """
    mock_resp = MagicMock()
    mock_resp.read.return_value = response_bytes
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


@patch("src.climate.open_meteo_client.urllib.request.urlopen")
def test_successful_fetch_returns_correct_structure(
    mock_urlopen: MagicMock,
    tmp_path: Path,
) -> None:
    """Open-Meteo returns valid data; verify return dict structure and values."""
    # Arrange: snow=0.05m, precip=0.1mm/hr for 8760 hours
    mock_urlopen.return_value = _mock_urlopen(_make_api_response())

    # Act
    result = fetch_era5_land_data(LAT, LON, YEAR, cache_dir=tmp_path)

    # Assert: correct keys and lengths
    assert result is not None
    assert set(result.keys()) == {"snow_depth_cm", "monthly_precip_inches"}
    assert len(result["snow_depth_cm"]) == 8760
    assert len(result["monthly_precip_inches"]) == 12

    # Assert: snow depth converted correctly (0.05 m -> 5.0 cm)
    assert all(v == 5.0 for v in result["snow_depth_cm"])

    # Assert: precipitation is positive for all months
    assert all(v > 0 for v in result["monthly_precip_inches"])

    # Assert: January precipitation is correct
    # 31 days * 24 hours * 0.1 mm = 74.4 mm / 25.4 = 2.929... inches
    jan_precip = result["monthly_precip_inches"][0]
    assert abs(jan_precip - 74.4 / 25.4) < 0.01

    # Assert: urlopen was called once
    mock_urlopen.assert_called_once()


@patch("src.climate.open_meteo_client.urllib.request.urlopen")
def test_cache_hit_skips_api_call(
    mock_urlopen: MagicMock,
    tmp_path: Path,
) -> None:
    """When JSON cache file exists, returns data without calling the API."""
    # Arrange: write a valid JSON cache file with new naming convention
    cache_data = {
        "snow_depth_cm": [0.0] * 8760,
        "monthly_precip_inches": [1.5] * 12,
    }
    cache_path = tmp_path / f"era5_{LAT}_{LON}_{YEAR}.json"
    with cache_path.open("w") as f:
        json.dump(cache_data, f)

    # Act
    result = fetch_era5_land_data(LAT, LON, YEAR, cache_dir=tmp_path)

    # Assert: data returned from cache, no API call
    assert result is not None
    assert result["snow_depth_cm"] == [0.0] * 8760
    assert result["monthly_precip_inches"] == [1.5] * 12
    mock_urlopen.assert_not_called()


@patch("src.climate.open_meteo_client.urllib.request.urlopen")
def test_api_failure_returns_none(
    mock_urlopen: MagicMock,
    tmp_path: Path,
) -> None:
    """HTTP error is caught and function returns None."""
    # Arrange: urlopen raises
    mock_urlopen.side_effect = Exception("Network unreachable")

    # Act
    result = fetch_era5_land_data(LAT, LON, YEAR, cache_dir=tmp_path)

    # Assert
    assert result is None


@patch("src.climate.open_meteo_client.urllib.request.urlopen")
def test_none_values_replaced_with_zero(
    mock_urlopen: MagicMock,
    tmp_path: Path,
) -> None:
    """None values in API response are replaced with 0.0."""
    # Arrange: first 24 values are None
    mock_urlopen.return_value = _mock_urlopen(
        _make_api_response(include_nulls=True)
    )

    # Act
    result = fetch_era5_land_data(LAT, LON, YEAR, cache_dir=tmp_path)

    # Assert
    assert result is not None

    # First 24 snow values should be 0.0 (None -> 0.0 * 100 = 0.0)
    assert all(v == 0.0 for v in result["snow_depth_cm"][:24])
    # Remaining should be 5.0 cm (0.05 m * 100)
    assert all(v == 5.0 for v in result["snow_depth_cm"][24:])

    # All values should be floats with no NaN
    for v in result["snow_depth_cm"]:
        assert isinstance(v, float)
        assert not math.isnan(v)
    for v in result["monthly_precip_inches"]:
        assert isinstance(v, float)
        assert not math.isnan(v)


@patch("src.climate.open_meteo_client.urllib.request.urlopen")
def test_leap_year_returns_8784_hours(
    mock_urlopen: MagicMock,
    tmp_path: Path,
) -> None:
    """Leap year (2024) returns 8784 hourly snow depth values."""
    # Arrange: 8784 hours for leap year 2024
    leap_year = 2024
    mock_urlopen.return_value = _mock_urlopen(
        _make_api_response(n_hours=8784, year=leap_year)
    )

    # Act
    result = fetch_era5_land_data(LAT, LON, leap_year, cache_dir=tmp_path)

    # Assert
    assert result is not None
    assert len(result["snow_depth_cm"]) == 8784
    assert len(result["monthly_precip_inches"]) == 12

    # Feb should have 29 days * 24 hours * 0.1 mm = 69.6 mm
    feb_precip_mm = 29 * 24 * 0.1
    feb_precip_inches = feb_precip_mm / 25.4
    assert abs(result["monthly_precip_inches"][1] - feb_precip_inches) < 0.01


@patch("src.climate.open_meteo_client.urllib.request.urlopen")
def test_cache_file_created_after_fetch(
    mock_urlopen: MagicMock,
    tmp_path: Path,
) -> None:
    """Successful fetch creates a JSON cache file for subsequent runs."""
    # Arrange
    mock_urlopen.return_value = _mock_urlopen(_make_api_response())

    # Act
    fetch_era5_land_data(LAT, LON, YEAR, cache_dir=tmp_path)

    # Assert: cache file exists with correct structure
    cache_path = tmp_path / f"era5_{LAT}_{LON}_{YEAR}.json"
    assert cache_path.exists()

    with cache_path.open("r") as f:
        cached = json.load(f)
    assert len(cached["snow_depth_cm"]) == 8760
    assert len(cached["monthly_precip_inches"]) == 12


@patch("src.climate.open_meteo_client.urllib.request.urlopen")
def test_all_values_are_floats_no_nan(
    mock_urlopen: MagicMock,
    tmp_path: Path,
) -> None:
    """All returned values are Python floats with no NaN."""
    # Arrange
    mock_urlopen.return_value = _mock_urlopen(_make_api_response())

    # Act
    result = fetch_era5_land_data(LAT, LON, YEAR, cache_dir=tmp_path)

    # Assert
    assert result is not None

    for v in result["snow_depth_cm"]:
        assert isinstance(v, float)
        assert not math.isnan(v)

    for v in result["monthly_precip_inches"]:
        assert isinstance(v, float)
        assert not math.isnan(v)
