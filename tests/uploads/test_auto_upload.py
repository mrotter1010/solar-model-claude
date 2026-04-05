"""Tests for the POST /uploads auto-detect endpoint."""

import json
from io import BytesIO
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from src.api.app import create_app


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Create a TestClient with UPLOAD_DIR pointing to tmp_path."""
    monkeypatch.setattr("src.api.routes.uploads.UPLOAD_DIR", str(tmp_path))
    app = create_app()
    return TestClient(app)


def _schedule_12x24(value: int = 0) -> list[list[int]]:
    return [[value] * 24 for _ in range(12)]


def _valid_rate_json() -> bytes:
    data = {
        "utility_name": "Test Utility",
        "tariff_name": "Test Tariff",
        "energyratestructure": [[{"rate": 0.10}]],
        "energyweekdayschedule": _schedule_12x24(0),
        "energyweekendschedule": _schedule_12x24(0),
    }
    return json.dumps(data).encode()


def _csv_8760() -> bytes:
    lines = ["hour,kwh"]
    for i in range(8_760):
        lines.append(f"{i},{100.0 + i}")
    return "\n".join(lines).encode("utf-8")


# ---------------------------------------------------------------------------
# KMZ auto-detection
# ---------------------------------------------------------------------------


class TestAutoDetectKmz:
    """POST /uploads with .kmz files auto-detects as KMZ."""

    def test_kmz_detected(self, client: TestClient) -> None:
        resp = client.post(
            "/uploads",
            files={"file": ("site.kmz", BytesIO(b"fake kmz"), "application/octet-stream")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["file_type"] == "kmz"
        assert data["detected"] is True
        assert data["confidence"] == "high"
        assert data["extracted_text"] is None

    def test_kmz_saved_to_kmz_dir(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        resp = client.post(
            "/uploads",
            files={"file": ("boundary.kmz", BytesIO(b"kmz bytes"), "application/octet-stream")},
        )
        assert resp.status_code == 200
        saved = Path(resp.json()["path"])
        assert saved.exists()
        assert "kmz" in str(saved)


# ---------------------------------------------------------------------------
# Load profile auto-detection
# ---------------------------------------------------------------------------


class TestAutoDetectLoadProfile:
    """POST /uploads with 8760-row CSV auto-detects as load profile."""

    def test_8760_csv_detected(self, client: TestClient) -> None:
        resp = client.post(
            "/uploads",
            files={"file": ("load.csv", BytesIO(_csv_8760()), "text/csv")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["file_type"] == "load-profile"
        assert data["detected"] is True
        assert data["confidence"] == "high"

    def test_short_csv_is_unknown(self, client: TestClient) -> None:
        """CSV with <8760 rows is treated as unknown, not a validation error."""
        csv_content = b"hour,kwh\n0,100\n1,200\n"
        resp = client.post(
            "/uploads",
            files={"file": ("small.csv", BytesIO(csv_content), "text/csv")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["file_type"] == "unknown"
        assert data["detected"] is True
        # Short CSV text is extracted
        assert data["extracted_text"] is not None


# ---------------------------------------------------------------------------
# Rate schedule auto-detection
# ---------------------------------------------------------------------------


class TestAutoDetectRate:
    """POST /uploads with rate JSON auto-detects as rate schedule."""

    def test_valid_rate_detected(self, client: TestClient) -> None:
        resp = client.post(
            "/uploads",
            files={"file": ("rate.json", BytesIO(_valid_rate_json()), "application/json")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["file_type"] == "rate"
        assert data["detected"] is True
        assert data["confidence"] == "high"

    def test_invalid_rate_json_returns_422(self, client: TestClient) -> None:
        """JSON with rate keys but invalid RateSchedule data returns 422."""
        # Has required keys but energyratestructure is wrong type
        bad = {
            "utility_name": "U",
            "tariff_name": "T",
            "energyratestructure": "not a list",
            "energyweekdayschedule": "wrong",
            "energyweekendschedule": "wrong",
        }
        resp = client.post(
            "/uploads",
            files={"file": ("bad_rate.json", BytesIO(json.dumps(bad).encode()), "application/json")},
        )
        assert resp.status_code == 422

    def test_non_rate_json_is_unknown(self, client: TestClient) -> None:
        """JSON without rate keys is unknown."""
        content = json.dumps({"name": "test"}).encode()
        resp = client.post(
            "/uploads",
            files={"file": ("config.json", BytesIO(content), "application/json")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["file_type"] == "unknown"
        assert data["extracted_text"] is not None


# ---------------------------------------------------------------------------
# Unknown files
# ---------------------------------------------------------------------------


class TestAutoDetectUnknown:
    """POST /uploads with unrecognized files stores in general/ with text extraction."""

    def test_txt_file_unknown_with_text(self, client: TestClient) -> None:
        content = b"This is a project description document."
        resp = client.post(
            "/uploads",
            files={"file": ("notes.txt", BytesIO(content), "text/plain")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["file_type"] == "unknown"
        assert data["detected"] is True
        assert data["confidence"] == "low"
        assert data["extracted_text"] == "This is a project description document."

    def test_pdf_unknown_no_text(self, client: TestClient) -> None:
        """PDF files are saved but text extraction returns None (not supported)."""
        resp = client.post(
            "/uploads",
            files={"file": ("report.pdf", BytesIO(b"%PDF-1.4 fake"), "application/pdf")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["file_type"] == "unknown"
        assert data["extracted_text"] is None

    def test_unknown_saved_to_general_dir(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        resp = client.post(
            "/uploads",
            files={"file": ("doc.docx", BytesIO(b"docx bytes"), "application/octet-stream")},
        )
        assert resp.status_code == 200
        saved = Path(resp.json()["path"])
        assert saved.exists()
        assert "general" in str(saved)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestAutoDetectEdgeCases:
    """Edge cases for the auto-detect endpoint."""

    def test_empty_file_returns_422(self, client: TestClient) -> None:
        resp = client.post(
            "/uploads",
            files={"file": ("empty.txt", BytesIO(b""), "text/plain")},
        )
        assert resp.status_code == 422
        assert "Empty file" in resp.json()["detail"]

    def test_old_endpoint_still_works(self, client: TestClient) -> None:
        """The old POST /uploads/{file_type} endpoint remains functional."""
        resp = client.post(
            "/uploads/rate",
            files={"file": ("r.json", BytesIO(_valid_rate_json()), "application/json")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["file_type"] == "rate"
        # Old endpoint should NOT have detected=True
        assert data["detected"] is False

    def test_old_endpoint_response_has_new_fields_with_defaults(
        self, client: TestClient
    ) -> None:
        """Old endpoint response includes new fields with default values."""
        resp = client.post(
            "/uploads/kmz",
            files={"file": ("site.kmz", BytesIO(b"kmz"), "application/octet-stream")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["detected"] is False
        assert data["confidence"] is None
        assert data["message"] is None
        assert data["extracted_text"] is None
