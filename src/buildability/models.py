"""Pydantic models for buildable land assessment configuration and results."""

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class BuildabilityConfig(BaseModel):
    """Standalone configuration for buildable land assessment.

    Used for CLI/standalone analysis outside the CSV pipeline.
    Defines site location, boundary source, and slope thresholds
    for terrain-based buildability classification.
    """

    # Location
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)

    # Boundary source (mutually exclusive)
    kmz_file_path: str | None = None
    radius_km: float | None = None

    # Slope classification thresholds (degrees)
    slope_thresholds: list[float] = Field(default=[5.0, 10.0, 15.0])
    tracker_slope_limit: float = 10.0
    fixed_tilt_slope_limit: float = 20.0

    @field_validator("radius_km", mode="before")
    @classmethod
    def validate_radius_km(cls, v: object) -> float | None:
        """Validate radius is positive when provided.

        Handles None and empty string inputs.
        """
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        v_float = float(v)
        if v_float <= 0:
            raise ValueError(f"radius_km must be > 0, got {v_float}")
        return v_float

    @field_validator("kmz_file_path", mode="before")
    @classmethod
    def validate_kmz_file_path(cls, v: object) -> str | None:
        """Normalize KMZ file path.

        Handles None and empty string inputs.
        """
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        return str(v).strip()

    @model_validator(mode="after")
    def validate_boundary_source(self) -> "BuildabilityConfig":
        """Ensure kmz_file_path and radius_km are not both specified.

        These are mutually exclusive boundary sources. If neither is
        provided, the pipeline will default to a 1.0 km radius at runtime.
        """
        if self.kmz_file_path is not None and self.radius_km is not None:
            raise ValueError(
                "Cannot specify both KMZ File Path and Analysis Radius. "
                "Provide one or neither."
            )
        return self


class BuildabilityResult(BaseModel):
    """Results from a buildable land assessment analysis.

    Contains land cover classification, slope analysis, and metadata
    for a single analysis polygon.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Location
    latitude: float
    longitude: float
    radius_km: float | None = None
    kmz_file_path: str | None = None

    # Polygon
    polygon_wkt: str

    # Land cover summary (acres)
    total_area_acres: float
    buildable_acres: float
    soft_exclusion_acres: float
    hard_exclusion_acres: float
    unclassified_acres: float

    # Land cover summary (percent)
    buildable_pct: float
    soft_exclusion_pct: float
    hard_exclusion_pct: float

    # Detailed breakdowns
    class_breakdown: list[dict]
    slope_stats: dict
    nlcd_metadata: dict
    dem_metadata: dict

    # Visualization paths
    figure_paths: dict[str, str] = {}

    # Report path (set when --report generates a PDF)
    report_path: str | None = None
