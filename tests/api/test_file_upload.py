"""Tests for the POST /uploads/{file_type} API endpoint."""

import json
from io import BytesIO
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from src.api.app import create_app


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Create a TestClient with UPLOAD_DIR pointing to tmp_path."""
    # Patch the module-level UPLOAD_DIR before any requests
    monkeypatch.setattr("src.api.routes.uploads.UPLOAD_DIR", str(tmp_path))
    app = create_app()
    return TestClient(app)


def _schedule_12x24(value: int = 0) -> list[list[int]]:
    """Build a uniform 12x24 schedule matrix filled with a single value."""
    return [[value] * 24 for _ in range(12)]


def _valid_rate_json() -> bytes:
    """Return valid RateSchedule JSON as bytes."""
    data = {
        "utility_name": "Test Utility",
        "tariff_name": "Test Tariff",
        "energyratestructure": [[{"rate": 0.10}]],
        "energyweekdayschedule": _schedule_12x24(0),
        "energyweekendschedule": _schedule_12x24(0),
    }
    return json.dumps(data).encode()


# ---------------------------------------------------------------------------
# Rate file uploads
# ---------------------------------------------------------------------------


class TestRateUpload:
    """POST /uploads/rate with rate JSON files."""

    def test_valid_rate_returns_200(self, client: TestClient) -> None:
        """Valid RateSchedule JSON uploads successfully."""
        resp = client.post(
            "/uploads/rate",
            files={"file": ("my_rate.json", BytesIO(_valid_rate_json()), "application/json")},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["file_type"] == "rate"
        assert data["filename"] == "my_rate.json"
        assert data["size_bytes"] > 0

    def test_invalid_json_returns_422(self, client: TestClient) -> None:
        """Non-JSON content returns 422."""
        resp = client.post(
            "/uploads/rate",
            files={"file": ("bad.json", BytesIO(b"not json {{{"), "application/json")},
        )

        assert resp.status_code == 422
        assert "not valid JSON" in resp.json()["detail"]

    def test_valid_json_invalid_rate_returns_422(self, client: TestClient) -> None:
        """Valid JSON that doesn't match RateSchedule returns 422."""
        # Missing required fields like energyratestructure
        data = json.dumps({"utility_name": "X", "tariff_name": "Y"}).encode()
        resp = client.post(
            "/uploads/rate",
            files={"file": ("partial.json", BytesIO(data), "application/json")},
        )

        assert resp.status_code == 422
        assert "RateSchedule validation" in resp.json()["detail"]

    def test_rate_file_exists_on_disk(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        """Uploaded rate file is written to disk at the returned path."""
        resp = client.post(
            "/uploads/rate",
            files={"file": ("disk_rate.json", BytesIO(_valid_rate_json()), "application/json")},
        )

        assert resp.status_code == 200
        saved_path = Path(resp.json()["path"])
        assert saved_path.exists()
        content = json.loads(saved_path.read_text())
        assert content["utility_name"] == "Test Utility"


# ---------------------------------------------------------------------------
# KMZ file uploads
# ---------------------------------------------------------------------------


class TestKmzUpload:
    """POST /uploads/kmz with KMZ files."""

    def test_valid_kmz_returns_200(self, client: TestClient) -> None:
        """File with .kmz extension uploads successfully."""
        resp = client.post(
            "/uploads/kmz",
            files={"file": ("site.kmz", BytesIO(b"fake kmz content"), "application/octet-stream")},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["file_type"] == "kmz"
        assert data["filename"] == "site.kmz"

    def test_wrong_extension_returns_422(self, client: TestClient) -> None:
        """File without .kmz extension returns 422."""
        resp = client.post(
            "/uploads/kmz",
            files={"file": ("site.zip", BytesIO(b"fake content"), "application/octet-stream")},
        )

        assert resp.status_code == 422
        assert ".kmz extension" in resp.json()["detail"]

    def test_kmz_file_exists_on_disk(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        """Uploaded KMZ file is written to disk at the returned path."""
        resp = client.post(
            "/uploads/kmz",
            files={"file": ("boundary.kmz", BytesIO(b"kmz bytes"), "application/octet-stream")},
        )

        assert resp.status_code == 200
        saved_path = Path(resp.json()["path"])
        assert saved_path.exists()
        assert saved_path.read_bytes() == b"kmz bytes"


# ---------------------------------------------------------------------------
# Load profile uploads
# ---------------------------------------------------------------------------


class TestLoadProfileUpload:
    """POST /uploads/load-profile with CSV files."""

    def test_valid_csv_returns_200(self, client: TestClient) -> None:
        """File with .csv extension uploads successfully."""
        csv_content = b"hour,kwh\n1,100\n2,200\n"
        resp = client.post(
            "/uploads/load-profile",
            files={"file": ("load.csv", BytesIO(csv_content), "text/csv")},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["file_type"] == "load-profile"
        assert data["filename"] == "load.csv"

    def test_wrong_extension_returns_422(self, client: TestClient) -> None:
        """File without .csv extension returns 422."""
        resp = client.post(
            "/uploads/load-profile",
            files={"file": ("load.xlsx", BytesIO(b"data"), "application/octet-stream")},
        )

        assert resp.status_code == 422
        assert ".csv extension" in resp.json()["detail"]

    def test_load_profile_file_exists_on_disk(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        """Uploaded load profile is written to disk at the returned path."""
        csv_content = b"hour,kwh\n1,100\n"
        resp = client.post(
            "/uploads/load-profile",
            files={"file": ("profile.csv", BytesIO(csv_content), "text/csv")},
        )

        assert resp.status_code == 200
        saved_path = Path(resp.json()["path"])
        assert saved_path.exists()
        assert saved_path.read_bytes() == csv_content


# ---------------------------------------------------------------------------
# Invalid file_type and empty files
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge cases: invalid file_type and empty files."""

    def test_invalid_file_type_returns_422(self, client: TestClient) -> None:
        """Unrecognized file_type path param returns 422."""
        resp = client.post(
            "/uploads/bogus",
            files={"file": ("f.txt", BytesIO(b"data"), "text/plain")},
        )

        assert resp.status_code == 422

    def test_empty_rate_file_returns_422(self, client: TestClient) -> None:
        """Empty rate file returns 422."""
        resp = client.post(
            "/uploads/rate",
            files={"file": ("empty.json", BytesIO(b""), "application/json")},
        )

        assert resp.status_code == 422
        assert "Empty file" in resp.json()["detail"]

    def test_empty_kmz_file_returns_422(self, client: TestClient) -> None:
        """Empty KMZ file returns 422."""
        resp = client.post(
            "/uploads/kmz",
            files={"file": ("empty.kmz", BytesIO(b""), "application/octet-stream")},
        )

        assert resp.status_code == 422
        assert "Empty file" in resp.json()["detail"]

    def test_empty_load_profile_returns_422(self, client: TestClient) -> None:
        """Empty CSV file returns 422."""
        resp = client.post(
            "/uploads/load-profile",
            files={"file": ("empty.csv", BytesIO(b""), "text/csv")},
        )

        assert resp.status_code == 422
        assert "Empty file" in resp.json()["detail"]
