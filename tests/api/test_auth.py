"""Tests for API key authentication middleware."""

import os

import pytest
from starlette.testclient import TestClient

from src.api.app import create_app


@pytest.fixture()
def client() -> TestClient:
    """Create a TestClient for the FastAPI app."""
    return TestClient(create_app())


# ---------------------------------------------------------------------------
# Auth disabled (API_KEY env var unset)
# ---------------------------------------------------------------------------


class TestAuthDisabled:
    """When API_KEY is not set, auth is disabled — all requests pass."""

    def test_health_no_key_env(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """GET /health works without auth when API_KEY is unset."""
        monkeypatch.delenv("API_KEY", raising=False)

        resp = client.get("/health")

        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_protected_endpoint_no_key_env(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Protected endpoint works without header when API_KEY is unset."""
        monkeypatch.delenv("API_KEY", raising=False)

        resp = client.get("/analyses/load-types")

        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Auth enabled (API_KEY env var set)
# ---------------------------------------------------------------------------


class TestAuthEnabled:
    """When API_KEY is set, all endpoints except /health require the header."""

    def test_health_exempt(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """GET /health works without auth header even when API_KEY is set."""
        monkeypatch.setenv("API_KEY", "secret-test-key")

        resp = client.get("/health")

        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_missing_header_returns_401(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Protected endpoint without X-API-Key header returns 401."""
        monkeypatch.setenv("API_KEY", "secret-test-key")

        resp = client.get("/analyses/load-types")

        assert resp.status_code == 401
        assert resp.json()["detail"] == "Missing API key. Provide X-API-Key header."

    def test_wrong_key_returns_403(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Protected endpoint with wrong key returns 403."""
        monkeypatch.setenv("API_KEY", "secret-test-key")

        resp = client.get(
            "/analyses/load-types",
            headers={"X-API-Key": "wrong-key"},
        )

        assert resp.status_code == 403
        assert resp.json()["detail"] == "Invalid API key."

    def test_correct_key_returns_200(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Protected endpoint with correct key returns 200."""
        monkeypatch.setenv("API_KEY", "secret-test-key")

        resp = client.get(
            "/analyses/load-types",
            headers={"X-API-Key": "secret-test-key"},
        )

        assert resp.status_code == 200
