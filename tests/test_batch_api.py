"""Tests for batch processing API routes.

API contract tests using FastAPI TestClient. The batch execution layer
is mocked — these test HTTP request/response contracts, not analysis logic.
"""

import io
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.api.app import create_app
from src.batch.executor import BatchResult, RowResult
from src.batch.validator import ValidationError, ValidationResult


@pytest.fixture
def client() -> TestClient:
    """Create a FastAPI TestClient."""
    app = create_app()
    return TestClient(app)


# ── GET /analyses/batch/template ───────────────────────────────────────


class TestGetBatchTemplate:
    """Tests for GET /analyses/batch/template."""

    def test_returns_xlsx_by_default(self, client: TestClient) -> None:
        """Default format returns an Excel template file."""
        response = client.get("/analyses/batch/template")

        assert response.status_code == 200
        assert "spreadsheetml" in response.headers["content-type"]
        assert "batch_template.xlsx" in response.headers.get(
            "content-disposition", ""
        )
        # Should have non-trivial content (real xlsx file)
        assert len(response.content) > 100

    def test_returns_csv_format(self, client: TestClient) -> None:
        """Explicit format=csv returns a CSV template file."""
        response = client.get("/analyses/batch/template?format=csv")

        assert response.status_code == 200
        assert "text/csv" in response.headers["content-type"]
        assert "batch_template.csv" in response.headers.get(
            "content-disposition", ""
        )
        # CSV should contain the header column names
        content = response.content.decode("utf-8")
        assert "name" in content
        assert "latitude" in content
        assert "analysis_type" in content

    def test_invalid_format_returns_400(self, client: TestClient) -> None:
        """Unsupported format should return 400."""
        response = client.get("/analyses/batch/template?format=pdf")

        assert response.status_code == 400
        assert "Unsupported format" in response.json()["detail"]


# ── POST /analyses/batch ───────────────────────────────────────────────


class TestPostBatch:
    """Tests for POST /analyses/batch."""

    @patch("src.api.routes.batch.generate_batch_workbook")
    @patch("src.api.routes.batch.execute_batch")
    @patch("src.api.routes.batch.validate_batch_input")
    def test_valid_csv_returns_success(
        self, mock_validate, mock_execute, mock_workbook, client, tmp_path
    ) -> None:
        """Valid CSV upload returns 200 with run summary and download URL."""
        # Arrange — validation passes with one valid row
        mock_validate.return_value = ValidationResult(
            valid_rows=[
                {
                    "name": "Site A",
                    "latitude": 33.0,
                    "longitude": -112.0,
                    "analysis_type": "production",
                    "racking": "tracker",
                    "gcr": 0.40,
                    "dc_capacity_mw": 5.0,
                    "ac_capacity_mw": 4.0,
                }
            ],
            errors=[],
            warnings=[],
            row_count=1,
            is_valid=True,
        )

        # Arrange — execution succeeds
        mock_execute.return_value = BatchResult(
            run_id="abc123def456",
            total_rows=1,
            succeeded=1,
            failed=0,
            total_runtime_seconds=5.12,
            row_results=[
                RowResult(
                    row_index=1,
                    name="Site A",
                    analysis_type="production",
                    status="success",
                    error=None,
                    runtime_seconds=5.12,
                    results={"annual_energy_mwh": 10000},
                )
            ],
            output_dir=tmp_path,
        )
        mock_workbook.return_value = tmp_path / "batch_results.xlsx"

        # Act
        csv_content = (
            "name,latitude,longitude,analysis_type,racking,gcr,"
            "dc_capacity_mw,ac_capacity_mw\n"
            "Site A,33.0,-112.0,production,tracker,0.40,5.0,4.0\n"
        )
        response = client.post(
            "/analyses/batch",
            files={
                "file": (
                    "batch_input.csv",
                    io.BytesIO(csv_content.encode()),
                    "text/csv",
                )
            },
        )

        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["run_id"] == "abc123def456"
        assert data["total_rows"] == 1
        assert data["succeeded"] == 1
        assert data["failed"] == 0
        assert data["total_runtime_seconds"] == 5.12
        assert data["warnings"] == []
        assert (
            data["files"]["workbook_url"]
            == "/analyses/abc123def456/batch_results"
        )
        assert len(data["summary"]) == 1
        assert data["summary"][0]["name"] == "Site A"
        assert data["summary"][0]["analysis_type"] == "production"
        assert data["summary"][0]["status"] == "success"
        assert data["summary"][0]["error"] is None

    @patch("src.api.routes.batch.validate_batch_input")
    def test_invalid_csv_returns_422_with_errors(
        self, mock_validate, client
    ) -> None:
        """CSV with validation errors returns 422 with structured error list."""
        mock_validate.return_value = ValidationResult(
            valid_rows=[],
            errors=[
                ValidationError(
                    row=2,
                    column="latitude",
                    value=None,
                    message="Required field 'latitude' is missing.",
                ),
                ValidationError(
                    row=2,
                    column="analysis_type",
                    value="bogus",
                    message="Invalid analysis_type 'bogus'.",
                ),
            ],
            warnings=[
                ValidationError(
                    row=3,
                    column="gcr",
                    value=0.7,
                    message="GCR 0.7 is unusually high.",
                ),
            ],
            row_count=2,
            is_valid=False,
        )

        csv_content = (
            "name,longitude,analysis_type\n"
            "Site A,-112.0,bogus\n"
            "Site B,-112.0,production\n"
        )
        response = client.post(
            "/analyses/batch",
            files={
                "file": (
                    "bad_batch.csv",
                    io.BytesIO(csv_content.encode()),
                    "text/csv",
                )
            },
        )

        assert response.status_code == 422
        detail = response.json()["detail"]
        # Errors present
        assert len(detail["errors"]) == 2
        assert detail["errors"][0]["row"] == 2
        assert detail["errors"][0]["column"] == "latitude"
        assert detail["errors"][0]["value"] is None
        assert "latitude" in detail["errors"][0]["message"]
        # Warnings present
        assert len(detail["warnings"]) == 1
        assert detail["warnings"][0]["column"] == "gcr"
        # Row count
        assert detail["row_count"] == 2

    def test_unsupported_extension_returns_400(
        self, client: TestClient
    ) -> None:
        """Non-spreadsheet file extension returns 400."""
        response = client.post(
            "/analyses/batch",
            files={
                "file": (
                    "data.pdf",
                    io.BytesIO(b"fake pdf content"),
                    "application/pdf",
                )
            },
        )

        assert response.status_code == 400
        assert "Unsupported file type" in response.json()["detail"]
        assert ".pdf" in response.json()["detail"]

    @patch("src.api.routes.batch.validate_batch_input")
    def test_too_many_rows_returns_422(
        self, mock_validate, client
    ) -> None:
        """Batch with >25 rows returns 422 with row-limit error."""
        mock_validate.return_value = ValidationResult(
            valid_rows=[],
            errors=[
                ValidationError(
                    row=0,
                    column="",
                    value=30,
                    message="Batch exceeds 25-row limit (30 rows found).",
                )
            ],
            warnings=[],
            row_count=30,
            is_valid=False,
        )

        # Build a CSV with 30 rows (the mock validator returns the error,
        # but we still need a valid-looking CSV for the extension check)
        csv_content = "name,latitude,longitude,analysis_type\n" + (
            "Site,33.0,-112.0,production\n" * 30
        )
        response = client.post(
            "/analyses/batch",
            files={
                "file": (
                    "big_batch.csv",
                    io.BytesIO(csv_content.encode()),
                    "text/csv",
                )
            },
        )

        assert response.status_code == 422
        detail = response.json()["detail"]
        assert any("25-row limit" in e["message"] for e in detail["errors"])
        assert detail["row_count"] == 30

    @patch("src.api.routes.batch.generate_batch_workbook")
    @patch("src.api.routes.batch.execute_batch")
    @patch("src.api.routes.batch.validate_batch_input")
    def test_validation_warnings_included_in_success(
        self, mock_validate, mock_execute, mock_workbook, client, tmp_path
    ) -> None:
        """Validation warnings are passed through in the success response."""
        mock_validate.return_value = ValidationResult(
            valid_rows=[
                {
                    "name": "Site A",
                    "latitude": 33.0,
                    "longitude": -112.0,
                    "analysis_type": "production",
                }
            ],
            errors=[],
            warnings=[
                ValidationError(
                    row=2,
                    column="gcr",
                    value=0.7,
                    message="GCR 0.7 is unusually high for tracker racking.",
                )
            ],
            row_count=1,
            is_valid=True,
        )

        mock_execute.return_value = BatchResult(
            run_id="warn123",
            total_rows=1,
            succeeded=1,
            failed=0,
            total_runtime_seconds=3.0,
            row_results=[
                RowResult(
                    row_index=1,
                    name="Site A",
                    analysis_type="production",
                    status="success",
                    error=None,
                    runtime_seconds=3.0,
                    results={},
                )
            ],
            output_dir=tmp_path,
        )
        mock_workbook.return_value = tmp_path / "batch_results.xlsx"

        csv_content = "name,latitude,longitude,analysis_type\nSite A,33.0,-112.0,production\n"
        response = client.post(
            "/analyses/batch",
            files={
                "file": (
                    "batch.csv",
                    io.BytesIO(csv_content.encode()),
                    "text/csv",
                )
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["warnings"]) == 1
        assert data["warnings"][0]["column"] == "gcr"
        assert data["warnings"][0]["row"] == 2
        assert "unusually high" in data["warnings"][0]["message"]


# ── GET /analyses/{run_id}/batch_results ───────────────────────────────


class TestGetBatchResults:
    """Tests for GET /analyses/{run_id}/batch_results."""

    def test_returns_xlsx_file(
        self, client: TestClient, tmp_path: Path, monkeypatch
    ) -> None:
        """Valid run_id with existing workbook returns xlsx FileResponse."""
        monkeypatch.setattr(
            "src.api.routes.batch.OUTPUT_BASE_DIR", tmp_path
        )

        run_id = "abc123def456"
        run_dir = tmp_path / run_id
        run_dir.mkdir()
        wb_path = run_dir / "batch_results.xlsx"
        # Write minimal xlsx-like content (FileResponse just streams bytes)
        wb_path.write_bytes(b"PK\x03\x04fake-xlsx-content")

        response = client.get(f"/analyses/{run_id}/batch_results")

        assert response.status_code == 200
        assert "spreadsheetml" in response.headers["content-type"]
        assert f"batch_results_{run_id}.xlsx" in response.headers.get(
            "content-disposition", ""
        )

    def test_invalid_run_id_returns_404(
        self, client: TestClient, tmp_path: Path, monkeypatch
    ) -> None:
        """Non-existent run_id returns 404."""
        monkeypatch.setattr(
            "src.api.routes.batch.OUTPUT_BASE_DIR", tmp_path
        )

        response = client.get("/analyses/nonexistent_run/batch_results")

        assert response.status_code == 404
        assert "Batch results not found" in response.json()["detail"]
