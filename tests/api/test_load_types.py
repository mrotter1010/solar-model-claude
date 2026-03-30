"""Tests for the GET /analyses/load-types endpoint."""

import pytest
from starlette.testclient import TestClient

from src.api.app import create_app


@pytest.fixture()
def client() -> TestClient:
    """Create a TestClient for the FastAPI app."""
    app = create_app()
    return TestClient(app)


class TestListLoadTypes:
    """GET /analyses/load-types."""

    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/analyses/load-types")
        assert resp.status_code == 200

    def test_response_has_count_and_load_types(self, client: TestClient) -> None:
        data = client.get("/analyses/load-types").json()
        assert "count" in data
        assert "load_types" in data

    def test_count_matches_list_length(self, client: TestClient) -> None:
        data = client.get("/analyses/load-types").json()
        assert data["count"] == len(data["load_types"])

    def test_contains_known_building_types(self, client: TestClient) -> None:
        data = client.get("/analyses/load-types").json()
        types = data["load_types"]
        assert "SmallOffice" in types
        assert "LargeHotel" in types
        assert "Warehouse" in types

    def test_returns_16_building_types(self, client: TestClient) -> None:
        data = client.get("/analyses/load-types").json()
        assert data["count"] == 16
