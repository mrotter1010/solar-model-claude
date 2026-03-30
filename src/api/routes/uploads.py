"""File upload routes for the Solar Model API."""

import json
import os
from enum import Enum
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile
from pydantic import ValidationError

from src.api.schemas.responses import FileUploadResponse
from src.rates.models import RateSchedule
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

router = APIRouter(prefix="/uploads", tags=["uploads"])

UPLOAD_DIR = os.environ.get("UPLOAD_DIR", "uploads")

# Max file sizes in bytes
_MAX_KMZ_BYTES = 50 * 1024 * 1024  # 50 MB
_MAX_LOAD_PROFILE_BYTES = 10 * 1024 * 1024  # 10 MB

# Map file_type path parameter to subdirectory
_SUBDIRS = {
    "rate": "rates",
    "kmz": "kmz",
    "load-profile": "load-profiles",
}


class FileType(str, Enum):
    """Allowed file type path parameters."""

    rate = "rate"
    kmz = "kmz"
    load_profile = "load-profile"


@router.post("/{file_type}", response_model=FileUploadResponse)
async def upload_file(file_type: FileType, file: UploadFile) -> FileUploadResponse:
    """Upload a file to the server.

    Args:
        file_type: One of "rate", "kmz", or "load-profile".
        file: The uploaded file.

    Returns:
        FileUploadResponse with file metadata and storage path.
    """
    content = await file.read()

    # Reject empty files
    if len(content) == 0:
        raise HTTPException(status_code=422, detail="Empty file upload is not allowed.")

    filename = file.filename or "unnamed"

    if file_type == FileType.rate:
        _validate_rate(content)
    elif file_type == FileType.kmz:
        _validate_kmz(filename, len(content))
    elif file_type == FileType.load_profile:
        _validate_load_profile(filename, len(content))

    # Determine storage path and write
    subdir = _SUBDIRS[file_type.value]
    dest_dir = Path(UPLOAD_DIR) / subdir
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filename
    dest_path.write_bytes(content)

    logger.info(f"Uploaded {file_type.value} file: {dest_path} ({len(content)} bytes)")

    return FileUploadResponse(
        file_type=file_type.value,
        filename=filename,
        path=str(dest_path),
        size_bytes=len(content),
    )


def _validate_rate(content: bytes) -> None:
    """Validate rate file: must be valid JSON that parses as RateSchedule."""
    try:
        data = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Rate file is not valid JSON: {exc}",
        ) from exc

    try:
        RateSchedule(**data)
    except ValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Rate file failed RateSchedule validation: {exc}",
        ) from exc


def _validate_kmz(filename: str, size: int) -> None:
    """Validate KMZ file: must have .kmz extension and be under 50MB."""
    if not filename.lower().endswith(".kmz"):
        raise HTTPException(
            status_code=422,
            detail=f"KMZ file must have .kmz extension, got '{filename}'.",
        )
    if size > _MAX_KMZ_BYTES:
        raise HTTPException(
            status_code=422,
            detail=f"KMZ file exceeds 50MB limit ({size:,} bytes).",
        )


def _validate_load_profile(filename: str, size: int) -> None:
    """Validate load profile: must have .csv extension and be under 10MB."""
    if not filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=422,
            detail=f"Load profile must have .csv extension, got '{filename}'.",
        )
    if size > _MAX_LOAD_PROFILE_BYTES:
        raise HTTPException(
            status_code=422,
            detail=f"Load profile exceeds 10MB limit ({size:,} bytes).",
        )
