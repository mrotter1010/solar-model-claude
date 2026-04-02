"""Tests for equipment endpoint numeric filters and tokenized search.

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


# ---------------------------------------------------------------------------
# Module endpoint — tokenized search
# ---------------------------------------------------------------------------


class TestModuleTokenizedSearch:
    """GET /analyses/equipment/modules with multi-token search."""

    def test_multi_token_trina_550(self, client: TestClient) -> None:
        """'Trina Solar 550' returns Trina 550W modules."""
        resp = client.get(
            "/analyses/equipment/modules",
            params={"search": "Trina Solar 550"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] > 0
        for name in data["modules"]:
            name_lower = name.lower()
            assert "trina" in name_lower
            assert "550" in name_lower

    def test_multi_token_csi_cs6w_550ms(self, client: TestClient) -> None:
        """'CSI Solar CS6W-550MS' matches despite 'Co. Ltd.' gap."""
        resp = client.get(
            "/analyses/equipment/modules",
            params={"search": "CSI Solar CS6W-550MS"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] > 0
        for name in data["modules"]:
            assert "CS6W-550MS" in name


# ---------------------------------------------------------------------------
# Module endpoint — STC filters
# ---------------------------------------------------------------------------


class TestModuleSTCFilters:
    """GET /analyses/equipment/modules with min_stc/max_stc params."""

    def test_min_stc(self, client: TestClient) -> None:
        resp = client.get(
            "/analyses/equipment/modules",
            params={"min_stc": 600},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] > 0

    def test_stc_range(self, client: TestClient) -> None:
        resp = client.get(
            "/analyses/equipment/modules",
            params={"min_stc": 540, "max_stc": 560},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] > 0

    def test_combined_search_and_stc(self, client: TestClient) -> None:
        """search=Trina&min_stc=540&max_stc=560 returns Trina ~550W modules."""
        resp = client.get(
            "/analyses/equipment/modules",
            params={"search": "Trina", "min_stc": 540, "max_stc": 560},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] > 0
        for name in data["modules"]:
            assert "trina" in name.lower()


# ---------------------------------------------------------------------------
# Inverter endpoint — tokenized search
# ---------------------------------------------------------------------------


class TestInverterTokenizedSearch:
    """GET /analyses/equipment/inverters with multi-token search."""

    def test_multi_token_sungrow_250(self, client: TestClient) -> None:
        """'Sungrow 250' matches Sungrow 250kW inverters."""
        resp = client.get(
            "/analyses/equipment/inverters",
            params={"search": "Sungrow 250"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] > 0
        for name in data["inverters"]:
            name_lower = name.lower()
            assert "sungrow" in name_lower
            assert "250" in name_lower


# ---------------------------------------------------------------------------
# Inverter endpoint — Paco filters
# ---------------------------------------------------------------------------


class TestInverterPacoFilters:
    """GET /analyses/equipment/inverters with min_paco/max_paco params."""

    def test_min_paco(self, client: TestClient) -> None:
        resp = client.get(
            "/analyses/equipment/inverters",
            params={"min_paco": 200000},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] > 0

    def test_paco_range(self, client: TestClient) -> None:
        resp = client.get(
            "/analyses/equipment/inverters",
            params={"min_paco": 200000, "max_paco": 300000},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] > 0

    def test_combined_search_and_paco(self, client: TestClient) -> None:
        """search=Sungrow&min_paco=200000 returns large Sungrow inverters."""
        resp = client.get(
            "/analyses/equipment/inverters",
            params={"search": "Sungrow", "min_paco": 200000},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] > 0
        for name in data["inverters"]:
            assert "sungrow" in name.lower()
