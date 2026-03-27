"""Tests for CEC equipment lookup endpoints.

These tests hit the real CECDatabase (read-only from static CSVs).
"""

import pytest
from starlette.testclient import TestClient

from src.api.app import create_app


@pytest.fixture()
def client() -> TestClient:
    """Create a TestClient for the FastAPI app."""
    app = create_app()
    return TestClient(app)


class TestListModules:
    """GET /analyses/equipment/modules."""

    def test_unfiltered_returns_modules(self, client: TestClient) -> None:
        resp = client.get("/analyses/equipment/modules")
        assert resp.status_code == 200
        data = resp.json()
        assert "count" in data
        assert "modules" in data
        assert data["count"] > 0
        assert len(data["modules"]) == data["count"]

    def test_search_sunpower(self, client: TestClient) -> None:
        resp = client.get("/analyses/equipment/modules", params={"search": "SunPower"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] > 0
        for name in data["modules"]:
            assert "sunpower" in name.lower()

    def test_search_nonexistent(self, client: TestClient) -> None:
        resp = client.get(
            "/analyses/equipment/modules", params={"search": "xyznonexistent"}
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["modules"] == []

    def test_known_module_spr_310(self, client: TestClient) -> None:
        """SPR-310-WHT search returns the known SunPower module."""
        resp = client.get(
            "/analyses/equipment/modules", params={"search": "SPR-310-WHT"}
        )
        data = resp.json()
        assert data["count"] > 0
        names = data["modules"]
        assert any("SunPower" in n and "SPR-310-WHT" in n for n in names)


class TestListInverters:
    """GET /analyses/equipment/inverters."""

    def test_unfiltered_returns_inverters(self, client: TestClient) -> None:
        resp = client.get("/analyses/equipment/inverters")
        assert resp.status_code == 200
        data = resp.json()
        assert "count" in data
        assert "inverters" in data
        assert data["count"] > 0
        assert len(data["inverters"]) == data["count"]

    def test_search_sungrow(self, client: TestClient) -> None:
        resp = client.get(
            "/analyses/equipment/inverters", params={"search": "Sungrow"}
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] > 0

    def test_search_nonexistent(self, client: TestClient) -> None:
        resp = client.get(
            "/analyses/equipment/inverters", params={"search": "xyznonexistent"}
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["inverters"] == []

    def test_known_inverter_sg250hx(self, client: TestClient) -> None:
        """SG250HX search returns the known Sungrow inverter."""
        resp = client.get(
            "/analyses/equipment/inverters", params={"search": "SG250HX"}
        )
        data = resp.json()
        assert data["count"] > 0
        names = data["inverters"]
        assert any("SG250HX" in n for n in names)
