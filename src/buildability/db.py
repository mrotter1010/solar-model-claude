"""Database persistence for buildability analysis results."""

from pathlib import Path

from sqlalchemy.orm import Session

from src.buildability.models import BuildabilityResult
from src.database.models import BuildabilityRun
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def save_buildability_result(
    result: BuildabilityResult,
    run_name: str,
    source: str,
    session: Session,
    json_output_path: str | None = None,
) -> BuildabilityRun:
    """Persist a BuildabilityResult to the database.

    Args:
        result: BuildabilityResult from the analysis pipeline.
        run_name: Identifier for this run (from CSV pipeline or auto-generated).
        source: Origin of the run — "pipeline" or "standalone".
        session: Active SQLAlchemy session (caller manages commit/rollback).
        json_output_path: Path to the JSON output file, if saved.

    Returns:
        The created BuildabilityRun ORM object.
    """
    # Determine figure directory from figure_paths
    figure_dir = None
    if result.figure_paths:
        first_path = next(iter(result.figure_paths.values()), None)
        if first_path:
            figure_dir = str(Path(first_path).parent)

    record = BuildabilityRun(
        run_name=run_name,
        source=source,
        latitude=result.latitude,
        longitude=result.longitude,
        radius_km=result.radius_km,
        kmz_file_path=result.kmz_file_path,
        total_area_acres=result.total_area_acres,
        buildable_acres=result.buildable_acres,
        buildable_pct=result.buildable_pct,
        soft_exclusion_acres=result.soft_exclusion_acres,
        hard_exclusion_acres=result.hard_exclusion_acres,
        json_output_path=json_output_path or "",
        report_output_path=result.report_path,
        figure_dir_path=figure_dir,
    )

    session.add(record)
    session.flush()

    logger.info(
        f"Saved buildability run '{run_name}' (id={record.id}, source={source})"
    )
    return record
