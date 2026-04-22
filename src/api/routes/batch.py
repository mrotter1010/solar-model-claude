"""Batch processing API routes.

Provides endpoints for downloading batch input templates, uploading
and processing batch input files, and downloading batch result workbooks.
"""

import tempfile
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

from src.batch.executor import execute_batch
from src.batch.validator import validate_batch_input
from src.batch.workbook import generate_batch_workbook
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

router = APIRouter(prefix="/analyses", tags=["batch"])

TEMPLATE_DIR = Path(__file__).resolve().parent.parent.parent / "batch" / "templates"
OUTPUT_BASE_DIR = Path("outputs/api")

_XLSX_MEDIA_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
_CSV_MEDIA_TYPE = "text/csv"

_ALLOWED_UPLOAD_EXTENSIONS = frozenset({".csv", ".xlsx", ".xls"})


class BatchFilePathRequest(BaseModel):
    """JSON request body for batch processing with a server-side file path."""

    file_path: str


@router.get("/batch/template")
def get_batch_template(format: str = "xlsx") -> FileResponse:
    """Download the batch input template file.

    Args:
        format: Template format — ``"csv"`` or ``"xlsx"`` (default ``"xlsx"``).

    Returns:
        FileResponse with the template file.
    """
    if format not in ("csv", "xlsx"):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported format: {format!r}. Use 'csv' or 'xlsx'.",
        )

    template_path = TEMPLATE_DIR / f"batch_template.{format}"
    if not template_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Template file not found: batch_template.{format}",
        )

    media_type = _XLSX_MEDIA_TYPE if format == "xlsx" else _CSV_MEDIA_TYPE
    return FileResponse(
        template_path,
        media_type=media_type,
        filename=f"batch_template.{format}",
    )


@router.post("/batch/run")
async def run_batch_from_path(body: BatchFilePathRequest) -> dict:
    """Process a batch input file already on the server filesystem.

    The orchestrator uploads files via ``POST /uploads``, which stores
    them on the API container's disk.  This endpoint reads the file from
    ``file_path`` — the same pattern used by buildability's
    ``kmz_file_path``.

    Args:
        body: JSON body with ``file_path`` pointing to an uploaded file.

    Returns:
        JSON with run_id, stats, warnings, file URLs, and per-row summary.
    """
    file_path = Path(body.file_path)

    if not file_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Batch file not found: {body.file_path}",
        )

    ext = file_path.suffix.lower()
    if ext not in _ALLOWED_UPLOAD_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported file type: {ext!r}. "
                "Provide a .csv, .xlsx, or .xls file."
            ),
        )

    return _process_batch_file(file_path)


@router.post("/batch")
async def run_batch(file: UploadFile) -> dict:
    """Upload and process a batch input file.

    Validates all rows, then executes each analysis sequentially.
    Returns JSON with run summary, per-row status, and a download URL
    for the full results workbook.

    Args:
        file: Uploaded CSV or Excel batch input file.

    Returns:
        JSON with run_id, stats, warnings, file URLs, and per-row summary.
    """
    filename = file.filename or "unnamed"
    ext = Path(filename).suffix.lower()

    if ext not in _ALLOWED_UPLOAD_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported file type: {ext!r}. "
                "Upload a .csv, .xlsx, or .xls file."
            ),
        )

    content = await file.read()
    if not content:
        raise HTTPException(status_code=422, detail="Empty file upload.")

    # Save to temp file for the validator
    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
        tmp.write(content)
        tmp_path = Path(tmp.name)

    try:
        return _process_batch_file(tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)


def _process_batch_file(file_path: Path) -> dict:
    """Validate and execute a batch input file.

    Shared logic for both file-upload and file-path entry points.

    Args:
        file_path: Path to the batch input file on disk.

    Returns:
        JSON-serializable dict with run summary.
    """
    try:
        validation = validate_batch_input(file_path)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    if not validation.is_valid or validation.errors:
        raise HTTPException(
            status_code=422,
            detail={
                "errors": [
                    {
                        "row": e.row,
                        "column": e.column,
                        "value": e.value,
                        "message": e.message,
                    }
                    for e in validation.errors
                ],
                "warnings": [
                    {
                        "row": w.row,
                        "column": w.column,
                        "value": w.value,
                        "message": w.message,
                    }
                    for w in validation.warnings
                ],
                "row_count": validation.row_count,
            },
        )

    # Execute all rows
    batch_result = execute_batch(validation.valid_rows)

    # Generate summary workbook
    generate_batch_workbook(batch_result, validation.valid_rows)

    return {
        "run_id": batch_result.run_id,
        "total_rows": batch_result.total_rows,
        "succeeded": batch_result.succeeded,
        "failed": batch_result.failed,
        "total_runtime_seconds": batch_result.total_runtime_seconds,
        "warnings": [
            {
                "row": w.row,
                "column": w.column,
                "value": w.value,
                "message": w.message,
            }
            for w in validation.warnings
        ],
        "files": {
            "workbook_url": (
                f"/analyses/{batch_result.run_id}/batch_results"
            ),
        },
        "summary": [
            {
                "name": rr.name,
                "analysis_type": rr.analysis_type,
                "status": rr.status,
                "error": rr.error,
            }
            for rr in batch_result.row_results
        ],
    }


@router.get("/{run_id}/batch_results")
def get_batch_results(run_id: str) -> FileResponse:
    """Download the batch results Excel workbook.

    Args:
        run_id: The batch run identifier.

    Returns:
        FileResponse with the batch_results.xlsx file.
    """
    workbook_path = OUTPUT_BASE_DIR / run_id / "batch_results.xlsx"
    if not workbook_path.exists():
        raise HTTPException(status_code=404, detail="Batch results not found")

    return FileResponse(
        workbook_path,
        media_type=_XLSX_MEDIA_TYPE,
        filename=f"batch_results_{run_id}.xlsx",
    )
