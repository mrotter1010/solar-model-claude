"""File upload routes for the Solar Model API."""

import json
import os
from enum import Enum
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile
from pydantic import ValidationError

from src.api.schemas.responses import FileUploadResponse
from src.rates.models import RateSchedule
from src.uploads.detector import detect_file_type
from src.uploads.text_extractor import extract_text
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

# Map detected_type → (file_type for response, subdirectory)
_DETECTED_TYPE_MAP = {
    "kmz": ("kmz", "kmz"),
    "load_profile": ("load-profile", "load-profiles"),
    "rate_schedule": ("rate", "rates"),
}


class FileType(str, Enum):
    """Allowed file type path parameters."""

    rate = "rate"
    kmz = "kmz"
    load_profile = "load-profile"


@router.post("", response_model=FileUploadResponse)
async def upload_file_auto(file: UploadFile) -> FileUploadResponse:
    """Upload any file with automatic type detection.

    The backend inspects the filename extension and file content to classify
    the upload as KMZ, load profile, rate schedule, or unknown.  Known types
    are validated and stored in their standard subdirectory.  Unknown files
    are stored in ``uploads/general/`` and have their text extracted (when
    possible) for orchestrator context injection.

    Args:
        file: The uploaded file.

    Returns:
        FileUploadResponse with detection metadata and optional extracted text.
    """
    content = await file.read()

    if len(content) == 0:
        raise HTTPException(status_code=422, detail="Empty file upload is not allowed.")

    filename = file.filename or "unnamed"

    result = detect_file_type(filename, content)

    # --- Known type with high confidence → validate and save ---
    if result.detected_type in _DETECTED_TYPE_MAP and result.confidence == "high":
        file_type_str, subdir = _DETECTED_TYPE_MAP[result.detected_type]

        # Run existing validation for the detected type
        if result.detected_type == "rate_schedule":
            _validate_rate(content)
        elif result.detected_type == "kmz":
            _validate_kmz(filename, len(content))
        elif result.detected_type == "load_profile":
            _validate_load_profile(filename, len(content))

        dest_dir = Path(UPLOAD_DIR) / subdir
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / filename
        dest_path.write_bytes(content)

        logger.info(
            f"Auto-detected {result.detected_type} file: "
            f"{dest_path} ({len(content)} bytes)"
        )

        return FileUploadResponse(
            file_type=file_type_str,
            filename=filename,
            path=str(dest_path),
            size_bytes=len(content),
            detected=True,
            confidence=result.confidence,
            message=result.message,
        )

    # --- Unknown type → save to general/, extract text ---
    dest_dir = Path(UPLOAD_DIR) / "general"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filename
    dest_path.write_bytes(content)

    extracted = extract_text(filename, content)

    logger.info(
        f"Uploaded unknown file: {dest_path} ({len(content)} bytes), "
        f"text extracted: {extracted is not None}"
    )

    return FileUploadResponse(
        file_type="unknown",
        filename=filename,
        path=str(dest_path),
        size_bytes=len(content),
        detected=True,
        confidence=result.confidence,
        message=result.message,
        extracted_text=extracted,
    )


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
    """Validate KMZ/KML file: must have .kmz or .kml extension and be under 50MB."""
    if not (filename.lower().endswith(".kmz") or filename.lower().endswith(".kml")):
        raise HTTPException(
            status_code=422,
            detail=f"Boundary file must have .kmz or .kml extension, got '{filename}'.",
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
