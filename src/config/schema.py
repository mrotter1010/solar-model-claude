"""Pydantic validation model for solar site configuration."""

from pydantic import BaseModel, ConfigDict, Field, field_validator


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

    # Losses (all percentages 0-100)
    shading_percent: float = Field(ge=0, le=100)
    dc_wiring_loss_percent: float = Field(ge=0, le=100)
    ac_wiring_loss_percent: float = Field(ge=0, le=100)
    transformer_losses_percent: float = Field(ge=0, le=100)
    degradation_percent: float = Field(ge=0, le=100)
    availability_percent: float = Field(ge=0, le=100)
    module_mismatch_percent: float = Field(ge=0, le=100)
    lid_percent: float = Field(ge=0, le=100)

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
    def location(self) -> tuple[float, float]:
        """Return (latitude, longitude) tuple."""
        return (self.latitude, self.longitude)
