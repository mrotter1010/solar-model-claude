"""Tests for result retrieval endpoints (GET analyses/{run_id}/...)."""

import json
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from src.api.app import create_app
from src.api.routes import results as results_module

RUN_ID = "TestRun_TestSite_20240101"


@pytest.fixture()
def fake_outputs(tmp_path: Path) -> Path:
    """Create a fake output directory structure matching pipeline output.

    Structure:
        tmp_path/
            {RUN_ID}/
                results.json
                reports/TestSite_report.pdf
                timeseries/TestRun_TestSite_8760.csv
    """
    run_dir = tmp_path / RUN_ID
    run_dir.mkdir()

    # Fake results.json
    results_data = {"status": "success", "site_name": "TestSite", "energy_mwh": 24500.0}
    (run_dir / "results.json").write_text(json.dumps(results_data), encoding="utf-8")

    # Fake PDF report (arbitrary bytes)
    reports_dir = run_dir / "reports"
    reports_dir.mkdir()
    (reports_dir / "TestSite_report.pdf").write_bytes(b"%PDF-1.4 fake content")

    # Fake 8760 CSV
    ts_dir = run_dir / "timeseries"
    ts_dir.mkdir()
    (ts_dir / "TestRun_TestSite_8760.csv").write_text(
        "timestamp,energy_kwh\n2024-01-01 00:00,100\n", encoding="utf-8"
    )

    return tmp_path


@pytest.fixture()
def client(fake_outputs: Path) -> TestClient:
    """Create a TestClient with OUTPUT_BASE_DIR patched to the temp directory."""
    original = results_module.OUTPUT_BASE_DIR
    results_module.OUTPUT_BASE_DIR = fake_outputs
    app = create_app()
    yield TestClient(app)
    results_module.OUTPUT_BASE_DIR = original


class TestGetResults:
    """GET /analyses/{run_id}/results."""

    def test_returns_json(self, client: TestClient) -> None:
        """Existing run → 200 with matching JSON content."""
        resp = client.get(f"/analyses/{RUN_ID}/results")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["site_name"] == "TestSite"
        assert data["energy_mwh"] == 24500.0

    def test_not_found(self, client: TestClient) -> None:
        """Nonexistent run → 404."""
        resp = client.get("/analyses/nonexistent/results")

        assert resp.status_code == 404
        assert resp.json()["detail"] == "Run not found"


class TestGetReport:
    """GET /analyses/{run_id}/report."""

    def test_returns_pdf(self, client: TestClient) -> None:
        """Existing run → 200 with PDF content type."""
        resp = client.get(f"/analyses/{RUN_ID}/report")

        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/pdf"
        assert resp.content.startswith(b"%PDF")

    def test_not_found(self, client: TestClient) -> None:
        """Nonexistent run → 404."""
        resp = client.get("/analyses/nonexistent/report")

        assert resp.status_code == 404
        assert resp.json()["detail"] == "Report not found"


class TestGetTimeseries:
    """GET /analyses/{run_id}/timeseries."""

    def test_returns_csv(self, client: TestClient) -> None:
        """Existing run → 200 with CSV content type."""
        resp = client.get(f"/analyses/{RUN_ID}/timeseries")

        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]
        assert b"timestamp,energy_kwh" in resp.content

    def test_not_found(self, client: TestClient) -> None:
        """Nonexistent run → 404."""
        resp = client.get("/analyses/nonexistent/timeseries")

        assert resp.status_code == 404
        assert resp.json()["detail"] == "Timeseries not found"
