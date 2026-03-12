"""Pydantic validation model for solar site configuration."""

import os
from pathlib import Path

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.utils.exceptions import ConfigValidationError
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class SiteConfig(BaseModel):
    """Validated configuration for a single solar site from CSV input."""

    model_config = ConfigDict(populate_by_name=True)

    # Project Info
    run_name: str
    site_name: str
    customer: str

    # Location
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)

    # Climate data
    weather_file_path: Path | None = None
    resource_file_path: Path | None = None
    data_source: str = "nsrdb"
    solcast_metadata: dict | None = None

    # BESS (store but don't validate - for future use)
    bess_dispatch_required: float | None = None
    bess_optimization_required: float | None = None

    # System Capacity
    dc_size_mw: float = Field(gt=0)
    ac_installed_mw: float = Field(gt=0)
    ac_poi_mw: float = Field(gt=0)

    # System Design
    racking: str
    tilt: float = Field(
        ge=0,
        le=90,
        description="For fixed racking: static tilt angle in degrees. "
        "For tracker racking: rotation limit (±degrees from horizontal).",
    )
    azimuth: float = Field(ge=0, le=360)
    module_orientation: str
    number_of_modules: int = Field(ge=1, le=2)
    ground_clearance_height_m: float = Field(gt=0)

    # Equipment
    panel_model: str
    bifacial: bool
    inverter_model: str

    # Layout
    gcr: float = Field(gt=0, lt=1)

    # Reporting
    report: bool = False

    # Ground truth irradiance data (optional, for site-specific bias correction)
    ground_truth_data_file: Path | None = None

    # Losses (all percentages 0-100)
    shading_percent: float = Field(ge=0, le=100)
    dc_wiring_loss_percent: float = Field(ge=0, le=100)
    ac_wiring_loss_percent: float = Field(ge=0, le=100)
    transformer_losses_percent: float = Field(ge=0, le=100)
    degradation_percent: float = Field(ge=0, le=100)
    availability_percent: float = Field(ge=0, le=100)
    module_mismatch_percent: float = Field(ge=0, le=100)
    lid_percent: float = Field(ge=0, le=100)

    @field_validator("resource_file_path", mode="before")
    @classmethod
    def validate_resource_file_path(cls, v: object) -> Path | None:
        """Validate resource file path from CSV input.

        Handles None (empty cell), empty string, and validates that the
        specified file exists and is readable on disk.
        """
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        path = Path(v) if not isinstance(v, Path) else v
        if not path.exists():
            raise ValueError(f"Resource file not found: {path}")
        if not path.is_file():
            raise ValueError(f"Resource file path is not a file: {path}")
        if not os.access(path, os.R_OK):
            raise ValueError(f"Resource file is not readable: {path}")
        return path

    @field_validator("report", mode="before")
    @classmethod
    def validate_report(cls, v: object) -> bool:
        """Coerce report field from CSV to bool.

        Handles None (empty cell), empty string, and string TRUE/FALSE
        values that pandas may produce when reading CSV columns.
        """
        if v is None or v == "":
            return False
        if isinstance(v, str):
            return v.strip().upper() in ("TRUE", "YES", "1")
        return bool(v)

    @field_validator("ground_truth_data_file", mode="before")
    @classmethod
    def validate_ground_truth_data_file(cls, v: object) -> Path | None:
        """Validate ground truth data file path from CSV input.

        Handles None (empty cell), empty string. If a path is provided,
        validates that the file exists and has a .csv extension.
        """
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        path = Path(v) if not isinstance(v, Path) else v
        if not path.exists():
            raise ValueError(f"Ground truth data file not found: {path}")
        if not path.is_file():
            raise ValueError(f"Ground truth data file path is not a file: {path}")
        if path.suffix.lower() != ".csv":
            raise ValueError(
                f"Ground truth data file must be a .csv file, got: {path.suffix}"
            )
        return path

    @field_validator("racking")
    @classmethod
    def validate_racking(cls, v: str) -> str:
        """Validate racking is 'fixed' or 'tracker' (case-insensitive)."""
        v_lower = v.lower()
        if v_lower not in ["fixed", "tracker"]:
            raise ValueError(f"Racking must be 'fixed' or 'tracker', got '{v}'")
        return v_lower

    @field_validator("module_orientation")
    @classmethod
    def validate_module_orientation(cls, v: str) -> str:
        """Validate module orientation is 'portrait' or 'landscape' (case-insensitive)."""
        v_lower = v.lower()
        if v_lower not in ["portrait", "landscape"]:
            raise ValueError(
                f"Module Orientation must be 'portrait' or 'landscape', got '{v}'"
            )
        return v_lower

    @property
    def system_capacity_kw(self) -> float:
        """Convert DC size from MW to kW for PySAM."""
        return self.dc_size_mw * 1000

    @property
    def tracking_mode(self) -> int:
        """Convert racking string to PySAM tracking mode integer."""
        return 0 if self.racking == "fixed" else 1

    @property
    def rotation_limit(self) -> float | None:
        """Return the tracker rotation limit in degrees, or None for fixed racking.

        For tracker systems, the tilt field represents the maximum rotation
        angle (±degrees from horizontal). For fixed systems, rotation limit
        is not applicable.
        """
        return self.tilt if self.racking == "tracker" else None

    @property
    def availability_for_pysam(self) -> float:
        """Convert unavailability % from CSV to availability % for PySAM.

        The CSV "Availability (%)" column represents downtime/unavailability,
        but PySAM expects availability (uptime). This inverts the value.
        """
        return 100 - self.availability_percent

    @property
    def has_climate_data(self) -> bool:
        """Check if a weather file has been assigned and exists on disk."""
        return self.weather_file_path is not None and self.weather_file_path.exists()

    @property
    def location(self) -> tuple[float, float]:
        """Return (latitude, longitude) tuple."""
        return (self.latitude, self.longitude)


# Required columns in ground truth data files
_GROUND_TRUTH_REQUIRED_COLUMNS = {"year", "month", "ghi_kwh_m2_day", "dni_kwh_m2_day"}


def validate_ground_truth_file_contents(path: Path) -> pd.DataFrame:
    """Validate that a ground truth data file has the required columns.

    Args:
        path: Path to the ground truth CSV file.

    Returns:
        The validated DataFrame.

    Raises:
        ConfigValidationError: If the file cannot be read or is missing
            required columns.
    """
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        raise ConfigValidationError(
            f"Failed to read ground truth data file: {path}",
            context={"path": str(path), "error": str(exc)},
        )

    actual_columns = {c.strip().lower() for c in df.columns}
    missing = _GROUND_TRUTH_REQUIRED_COLUMNS - actual_columns
    if missing:
        raise ConfigValidationError(
            f"Ground truth data file is missing required columns: {sorted(missing)}. "
            f"Expected columns: {sorted(_GROUND_TRUTH_REQUIRED_COLUMNS)}. "
            f"Found columns: {sorted(df.columns.tolist())}",
            context={
                "path": str(path),
                "missing_columns": sorted(missing),
                "found_columns": sorted(df.columns.tolist()),
            },
        )

    # Normalize column names to lowercase
    df.columns = df.columns.str.strip().str.lower()
    return df
