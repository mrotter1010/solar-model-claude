"""Tests for the GET /lmp/prices endpoint."""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from src.api.app import create_app
from src.lmp.models import LMPData


@pytest.fixture()
def client() -> TestClient:
    """Create a TestClient for the FastAPI app."""
    app = create_app()
    return TestClient(app)


def _make_lmp_data(
    iso: str = "pjm",
    zone: str = "AEP",
    market: str = "DAY_AHEAD_HOURLY",
    year: int = 2025,
) -> LMPData:
    """Build a mock LMPData with deterministic 8760 prices."""
    # Create a simple price pattern: base 30 + hour-of-day variation
    prices = [30.0 + (i % 24) * 2.0 for i in range(8760)]
    timestamps = [
        datetime(year, 1, 1, tzinfo=timezone.utc)
    ] * 8760  # simplified
    return LMPData(
        iso=iso,
        zone=zone,
        market=market,
        year=year,
        prices=prices,
        timestamps=timestamps,
    )


# -----------------------------------------------------------------------
# Valid requests
# -----------------------------------------------------------------------


@patch("src.api.routes.lmp.fetch_lmp")
def test_get_prices_with_iso_and_zone(mock_fetch: object, client: TestClient) -> None:
    """Valid request with iso + zone returns 200 with expected structure."""
    mock_fetch.return_value = _make_lmp_data()

    resp = client.get("/lmp/prices", params={"iso": "pjm", "zone": "AEP"})

    assert resp.status_code == 200
    data = resp.json()
    assert data["iso"] == "pjm"
    assert data["zone"] == "AEP"
    assert data["market"] == "DAY_AHEAD_HOURLY"
    assert data["year"] == 2025
    assert data["currency"] == "USD"
    assert len(data["prices"]) == 8760

    mock_fetch.assert_called_once_with(
        iso="pjm", zone="AEP", market="DAY_AHEAD_HOURLY", year=None,
    )


@patch("src.api.routes.lmp.fetch_lmp")
def test_get_prices_with_explicit_year_and_market(
    mock_fetch: object, client: TestClient,
) -> None:
    """Optional year and market params are forwarded to fetch_lmp."""
    mock_fetch.return_value = _make_lmp_data(year=2024, market="REAL_TIME_HOURLY")

    resp = client.get(
        "/lmp/prices",
        params={
            "iso": "ercot",
            "zone": "LZ_HOUSTON",
            "year": 2024,
            "market": "REAL_TIME_HOURLY",
        },
    )

    assert resp.status_code == 200
    mock_fetch.assert_called_once_with(
        iso="ercot",
        zone="LZ_HOUSTON",
        market="REAL_TIME_HOURLY",
        year=2024,
    )


@patch("src.api.routes.lmp.fetch_lmp")
@patch("src.api.routes.lmp.get_ercot_zone", return_value="LZ_HOUSTON")
def test_get_prices_with_lat_lon_auto_detect(
    mock_zone: object, mock_fetch: object, client: TestClient,
) -> None:
    """Valid request with iso + lat + lon uses zone auto-detection."""
    mock_fetch.return_value = _make_lmp_data(iso="ercot", zone="LZ_HOUSTON")

    resp = client.get(
        "/lmp/prices",
        params={"iso": "ercot", "lat": 29.76, "lon": -95.37},
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["zone"] == "LZ_HOUSTON"
    mock_zone.assert_called_once_with(29.76, -95.37)
    mock_fetch.assert_called_once_with(
        iso="ercot",
        zone="LZ_HOUSTON",
        market="DAY_AHEAD_HOURLY",
        year=None,
    )


@patch("src.api.routes.lmp.fetch_lmp")
@patch("src.api.routes.lmp.get_caiso_zone", return_value="SP15")
def test_get_prices_caiso_auto_detect(
    mock_zone: object, mock_fetch: object, client: TestClient,
) -> None:
    """CAISO lat/lon auto-detection returns correct hub."""
    mock_fetch.return_value = _make_lmp_data(iso="caiso", zone="SP15")

    resp = client.get(
        "/lmp/prices",
        params={"iso": "caiso", "lat": 34.05, "lon": -118.24},
    )

    assert resp.status_code == 200
    assert resp.json()["zone"] == "SP15"
    mock_zone.assert_called_once_with(34.05, -118.24)


# -----------------------------------------------------------------------
# Response structure
# -----------------------------------------------------------------------


@patch("src.api.routes.lmp.fetch_lmp")
def test_response_has_summary_stats(mock_fetch: object, client: TestClient) -> None:
    """Response includes summary with mean, median, min, max, std, count."""
    mock_fetch.return_value = _make_lmp_data()

    resp = client.get("/lmp/prices", params={"iso": "pjm", "zone": "AEP"})

    data = resp.json()
    summary = data["summary"]
    assert "mean_price" in summary
    assert "median_price" in summary
    assert "min_price" in summary
    assert "max_price" in summary
    assert "std_price" in summary
    assert summary["count"] == 8760

    # Verify numeric values are reasonable for our test data
    assert summary["min_price"] == 30.0
    assert summary["max_price"] == 76.0  # 30 + 23*2


@patch("src.api.routes.lmp.fetch_lmp")
def test_response_has_monthly_averages(mock_fetch: object, client: TestClient) -> None:
    """Response includes monthly_averages dict with 12 month keys."""
    mock_fetch.return_value = _make_lmp_data()

    resp = client.get("/lmp/prices", params={"iso": "pjm", "zone": "AEP"})

    monthly = resp.json()["monthly_averages"]
    assert len(monthly) == 12
    expected_months = [
        "jan", "feb", "mar", "apr", "may", "jun",
        "jul", "aug", "sep", "oct", "nov", "dec",
    ]
    for month in expected_months:
        assert month in monthly
        assert isinstance(monthly[month], float)


@patch("src.api.routes.lmp.fetch_lmp")
def test_response_has_timestamps(mock_fetch: object, client: TestClient) -> None:
    """Response includes timestamps array when LMPData has them."""
    mock_fetch.return_value = _make_lmp_data()

    resp = client.get("/lmp/prices", params={"iso": "pjm", "zone": "AEP"})

    data = resp.json()
    assert data["timestamps"] is not None
    assert len(data["timestamps"]) == 8760


# -----------------------------------------------------------------------
# Validation errors (422)
# -----------------------------------------------------------------------


def test_missing_zone_and_latlon_returns_422(client: TestClient) -> None:
    """Omitting both zone and lat/lon returns 422 with clear message."""
    resp = client.get("/lmp/prices", params={"iso": "pjm"})

    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert "zone" in detail.lower()
    assert "lat" in detail.lower()


def test_missing_lon_with_lat_returns_422(client: TestClient) -> None:
    """Providing lat without lon returns 422."""
    resp = client.get("/lmp/prices", params={"iso": "pjm", "lat": 40.0})

    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert "zone" in detail.lower() or "lat" in detail.lower()


def test_invalid_iso_returns_422(client: TestClient) -> None:
    """Invalid ISO value returns 422."""
    resp = client.get(
        "/lmp/prices", params={"iso": "nyiso", "zone": "NYC"},
    )

    assert resp.status_code == 422
    assert "nyiso" in resp.json()["detail"].lower()


def test_case_insensitive_iso(client: TestClient) -> None:
    """ISO parameter is case-insensitive."""
    with patch("src.api.routes.lmp.fetch_lmp") as mock_fetch:
        mock_fetch.return_value = _make_lmp_data()
        resp = client.get(
            "/lmp/prices", params={"iso": "PJM", "zone": "AEP"},
        )
        assert resp.status_code == 200
        mock_fetch.assert_called_once_with(
            iso="pjm", zone="AEP", market="DAY_AHEAD_HOURLY", year=None,
        )


# -----------------------------------------------------------------------
# Error handling
# -----------------------------------------------------------------------


@patch("src.api.routes.lmp.fetch_lmp")
def test_invalid_zone_returns_422(mock_fetch: object, client: TestClient) -> None:
    """Invalid zone for an ISO returns 422 from fetch_lmp ValueError."""
    mock_fetch.side_effect = ValueError("Invalid PJM zone 'FAKE'")

    resp = client.get(
        "/lmp/prices", params={"iso": "pjm", "zone": "FAKE"},
    )

    assert resp.status_code == 422
    assert "FAKE" in resp.json()["detail"]


@patch("src.api.routes.lmp.fetch_lmp")
def test_fetch_failure_returns_502(mock_fetch: object, client: TestClient) -> None:
    """External API failure returns 502."""
    mock_fetch.side_effect = RuntimeError("gridstatus connection timeout")

    resp = client.get(
        "/lmp/prices", params={"iso": "pjm", "zone": "AEP"},
    )

    assert resp.status_code == 502
    assert "gridstatus" in resp.json()["detail"].lower()


@patch("src.api.routes.lmp.get_pjm_zone", return_value=None)
def test_pjm_auto_detect_failure_returns_422(
    mock_zone: object, client: TestClient,
) -> None:
    """PJM zone auto-detection failure returns 422."""
    resp = client.get(
        "/lmp/prices",
        params={"iso": "pjm", "lat": 45.0, "lon": -70.0},
    )

    assert resp.status_code == 422
    assert "auto-detect" in resp.json()["detail"].lower()
