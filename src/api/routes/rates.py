"""Rate builder routes for the Solar Model API."""

import json
import re
from pathlib import Path

from fastapi import APIRouter, HTTPException

from src.api.schemas.requests import RateBuildRequest
from src.api.schemas.responses import RateBuildResponse
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

router = APIRouter(prefix="/rates", tags=["rates"])


def _sanitize_filename(utility_name: str, tariff_name: str) -> str:
    """Build a filesystem-safe filename from utility and tariff names.

    Replaces spaces, slashes, quotes, and other unsafe characters
    with underscores.

    Args:
        utility_name: Electric utility name.
        tariff_name: Tariff schedule identifier.

    Returns:
        Sanitized string safe for use as a filename stem.
    """
    raw = f"{utility_name}_{tariff_name}"
    return re.sub(r"[\s/\\\"']+", "_", raw)


@router.post("/build", response_model=RateBuildResponse)
def build_rate(request: RateBuildRequest) -> RateBuildResponse:
    """Build and validate a rate schedule.

    Accepts a full RateSchedule payload, validates it via Pydantic,
    stamps source='api', and optionally saves to disk.
    """
    try:
        rate = request.rate

        # Override source to distinguish API-built rates
        rate.source = "api"

        file_path: str | None = None
        if request.save_to_disk:
            filename = _sanitize_filename(rate.utility_name, rate.tariff_name)
            output_dir = Path("uploads/rates")
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"{filename}.json"
            data = rate.model_dump()
            output_path.write_text(
                json.dumps(data, indent=2, default=str), encoding="utf-8"
            )
            file_path = str(output_path)
            logger.info(f"Rate schedule saved to {output_path}")

        return RateBuildResponse(
            rate=rate,
            file_path=file_path,
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Rate build error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
