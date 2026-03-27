"""Result retrieval routes for completed analyses."""

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

router = APIRouter(prefix="/analyses", tags=["results"])

OUTPUT_BASE_DIR = Path("outputs/api")


@router.get("/{run_id}/results")
def get_results(run_id: str) -> dict:
    """Return saved results.json for a completed analysis run.

    Args:
        run_id: The run identifier (matches the directory name under outputs/api/).

    Returns:
        The parsed JSON content of the results file.

    Raises:
        HTTPException: 404 if the run directory or results.json is not found.
    """
    results_path = OUTPUT_BASE_DIR / run_id / "results.json"
    if not results_path.exists():
        raise HTTPException(status_code=404, detail="Run not found")

    return json.loads(results_path.read_text(encoding="utf-8"))


@router.get("/{run_id}/report")
def get_report(run_id: str) -> FileResponse:
    """Return the PDF report for a completed analysis run.

    Args:
        run_id: The run identifier.

    Returns:
        FileResponse streaming the PDF file.

    Raises:
        HTTPException: 404 if the reports directory or PDF is not found.
    """
    reports_dir = OUTPUT_BASE_DIR / run_id / "reports"
    if not reports_dir.exists():
        raise HTTPException(status_code=404, detail="Report not found")

    pdfs = list(reports_dir.glob("*.pdf"))
    if not pdfs:
        raise HTTPException(status_code=404, detail="Report not found")

    return FileResponse(pdfs[0], media_type="application/pdf")


@router.get("/{run_id}/timeseries")
def get_timeseries(run_id: str) -> FileResponse:
    """Return the 8760 timeseries CSV for a completed analysis run.

    Args:
        run_id: The run identifier.

    Returns:
        FileResponse streaming the CSV file.

    Raises:
        HTTPException: 404 if the timeseries directory or CSV is not found.
    """
    ts_dir = OUTPUT_BASE_DIR / run_id / "timeseries"
    if not ts_dir.exists():
        raise HTTPException(status_code=404, detail="Timeseries not found")

    csvs = list(ts_dir.glob("*_8760.csv"))
    if not csvs:
        raise HTTPException(status_code=404, detail="Timeseries not found")

    return FileResponse(csvs[0], media_type="text/csv")
