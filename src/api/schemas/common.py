"""Shared Pydantic models for API request and response schemas.

Each model maps cleanly to a subset of SiteConfig fields. The adapter
layer (not here) is responsible for translating these into SiteConfig
objects for the pipeline.
"""

from pydantic import BaseModel, Field, model_validator

from src.rates.models import RateSchedule


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class Location(BaseModel):
    """Site location and identity."""

    name: str
    customer: str = "API"
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    run_name: str | None = None


class SystemDesign(BaseModel):
    """PV system design parameters."""

    dc_capacity_mw: float = Field(gt=0)
    ac_capacity_mw: float = Field(gt=0)
    ac_poi_mw: float | None = Field(default=None, gt=0)
    module: str
    inverter: str
    racking: str
    tilt: float | None = Field(default=None, ge=0, le=90)
    azimuth: float = Field(default=180.0, ge=0, le=360)
    module_orientation: str = "portrait"
    num_modules: int = Field(default=1, ge=1, le=2)
    ground_clearance_m: float = Field(default=1.5, gt=0)
    bifacial: bool = False
    gcr: float = Field(gt=0, lt=1)

    @model_validator(mode="after")
    def validate_racking(self) -> "SystemDesign":
        """Validate racking is 'fixed' or 'tracker' (case-insensitive)."""
        val = self.racking.strip().lower()
        if val not in ("fixed", "tracker"):
            raise ValueError(
                f"racking must be 'fixed' or 'tracker', got '{self.racking}'"
            )
        self.racking = val
        return self

    @model_validator(mode="after")
    def validate_module_orientation(self) -> "SystemDesign":
        """Validate module_orientation is 'portrait' or 'landscape'."""
        val = self.module_orientation.strip().lower()
        if val not in ("portrait", "landscape"):
            raise ValueError(
                f"module_orientation must be 'portrait' or 'landscape', "
                f"got '{self.module_orientation}'"
            )
        self.module_orientation = val
        return self


class Losses(BaseModel):
    """System loss parameters (all percentages 0-100)."""

    shading_pct: float = Field(default=0.0, ge=0, le=100)
    dc_wiring_pct: float = Field(default=2.0, ge=0, le=100)
    ac_wiring_pct: float = Field(default=0.5, ge=0, le=100)
    transformer_pct: float = Field(default=1.0, ge=0, le=100)
    degradation_pct: float = Field(default=0.5, ge=0, le=100)
    availability_pct: float = Field(default=2.5, ge=0, le=100)
    mismatch_pct: float = Field(default=2.0, ge=0, le=100)
    lid_pct: float = Field(default=1.5, ge=0, le=100)


class ResourceOverrides(BaseModel):
    """Optional resource file overrides."""

    resource_file_path: str | None = None
    ground_truth_file: str | None = None


class BuildabilityLocation(BaseModel):
    """Site location for buildability analysis.

    Unlike Location, latitude and longitude are optional here because
    the buildability analyzer can compute the centroid from a KML/KMZ
    polygon boundary. When a KML/KMZ is provided without coordinates,
    the analyzer auto-populates lat/lon from the polygon centroid.
    """

    name: str
    customer: str = "API"
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)
    run_name: str | None = None


class BuildabilityConfig(BaseModel):
    """Buildable land assessment configuration."""

    kmz_file_path: str | None = None
    analysis_radius_km: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_mutual_exclusivity(self) -> "BuildabilityConfig":
        """Ensure kmz_file_path and analysis_radius_km are mutually exclusive."""
        if self.kmz_file_path is not None and self.analysis_radius_km is not None:
            raise ValueError(
                "Cannot specify both kmz_file_path and analysis_radius_km. "
                "Provide one or neither."
            )
        return self


class BillConfig(BaseModel):
    """Bill calculation configuration."""

    rate_file_path: str | None = None
    utility_name: str | None = None
    tariff_name: str | None = None
    rate: RateSchedule | None = None
    load_profile_path: str | None = None
    load_type: str | None = None
    annual_consumption_kwh: float | None = Field(default=None, gt=0)
    peak_demand_kw: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_rate_source(self) -> "BillConfig":
        """Validate exactly one rate source is provided.

        Three mutually exclusive rate sources:
        - rate: inline RateSchedule object
        - rate_file_path: path to a rate JSON file on disk
        - utility_name + tariff_name: OpenEI URDB lookup
        """
        has_rate_file = self.rate_file_path is not None
        has_openei = self.utility_name is not None or self.tariff_name is not None
        has_inline = self.rate is not None

        if has_rate_file and has_openei:
            raise ValueError(
                "Cannot specify both rate_file_path and "
                "utility_name/tariff_name. Use one rate source."
            )
        if has_inline and has_rate_file:
            raise ValueError(
                "Cannot specify both rate (inline) and "
                "rate_file_path. Use one rate source."
            )
        if has_inline and has_openei:
            raise ValueError(
                "Cannot specify both rate (inline) and "
                "utility_name/tariff_name. Use one rate source."
            )
        if not has_rate_file and not has_openei and not has_inline:
            raise ValueError(
                "No rate source specified. Provide either "
                "rate (inline), rate_file_path, or "
                "utility_name + tariff_name."
            )
        if has_openei:
            if self.utility_name is None or self.tariff_name is None:
                raise ValueError(
                    "Both utility_name and tariff_name are required "
                    "when using OpenEI rate lookup."
                )
        return self

    @model_validator(mode="after")
    def validate_load_source(self) -> "BillConfig":
        """Validate exactly one load source is provided."""
        has_load_file = self.load_profile_path is not None
        has_typical = self.load_type is not None

        if has_load_file and has_typical:
            raise ValueError(
                "Cannot specify both load_profile_path and load_type. "
                "Use one load source."
            )
        if not has_load_file and not has_typical:
            raise ValueError(
                "No load source specified. Provide either load_profile_path "
                "or load_type + annual_consumption_kwh."
            )
        if has_load_file:
            if (
                self.annual_consumption_kwh is not None
                or self.peak_demand_kw is not None
                or self.load_type is not None
            ):
                raise ValueError(
                    "Cannot specify load_type, annual_consumption_kwh, or "
                    "peak_demand_kw when load_profile_path is provided."
                )
        if has_typical and self.annual_consumption_kwh is None:
            raise ValueError(
                "annual_consumption_kwh is required when using load_type."
            )
        return self


class BESSConfig(BaseModel):
    """Battery energy storage system configuration."""

    power_mw: float | None = Field(default=None, gt=0)
    duration_hr: float | None = Field(default=None, gt=0)
    rte_pct: float = Field(default=88.0, ge=50, le=100)
    min_soc_pct: float = Field(default=10.0, ge=0, le=100)
    max_soc_pct: float = Field(default=90.0, ge=0, le=100)
    strategy: str = "global"
    installed_cost_per_kwh: float = Field(default=275.0, gt=0)
    cycles_warranty: int = Field(default=5000, gt=0)
    solar_only_charging: bool = False
    grid_only_charging: bool = False

    @model_validator(mode="after")
    def validate_soc_range(self) -> "BESSConfig":
        """Validate min_soc_pct < max_soc_pct."""
        if self.min_soc_pct >= self.max_soc_pct:
            raise ValueError(
                f"min_soc_pct ({self.min_soc_pct}) must be less than "
                f"max_soc_pct ({self.max_soc_pct})"
            )
        return self

    @model_validator(mode="after")
    def validate_charging_exclusivity(self) -> "BESSConfig":
        """Validate solar_only and grid_only charging are mutually exclusive."""
        if self.solar_only_charging and self.grid_only_charging:
            raise ValueError(
                "solar_only_charging and grid_only_charging "
                "are mutually exclusive"
            )
        return self

    @model_validator(mode="after")
    def validate_strategy(self) -> "BESSConfig":
        """Validate strategy is one of the allowed values."""
        allowed = {"global", "peak_shaving", "tou_arbitrage"}
        val = self.strategy.strip().lower()
        if val not in allowed:
            raise ValueError(
                f"strategy must be one of {sorted(allowed)}, "
                f"got '{self.strategy}'"
            )
        self.strategy = val
        return self


class BESSEconomics(BaseModel):
    """BESS sizing optimization economics."""

    discount_rate_pct: float = Field(default=7.0, gt=0)
    project_lifetime_years: int = Field(default=25, gt=0)
    rate_escalation_pct: float = 2.0
    solar_cost_per_kw_dc: float | None = None
    solar_cost_per_kw_ac: float | None = None
    solar_opex_per_kw_dc_year: float = 0.0
    bess_opex_per_kw_year: float = 0.0
    power_min_mw: float | None = None
    power_max_mw: float | None = None
    duration_min_hr: float = 2.0
    duration_max_hr: float = 5.0
    optimize: bool = False

    @model_validator(mode="after")
    def validate_duration_range(self) -> "BESSEconomics":
        """Validate duration_min_hr < duration_max_hr."""
        if self.duration_min_hr >= self.duration_max_hr:
            raise ValueError(
                f"duration_min_hr ({self.duration_min_hr}) must be less than "
                f"duration_max_hr ({self.duration_max_hr})"
            )
        return self


class FTMConfig(BaseModel):
    """Front-of-meter / wholesale dispatch configuration."""

    dispatch_mode: str = "btm"
    iso: str | None = None
    lmp_zone: str | None = None
    lmp_node_ids: str | None = None
    lmp_market: str = "DAY_AHEAD_HOURLY"
    lmp_year: int | None = None
    ancillary_revenue_per_kw_year: float = Field(default=0.0, ge=0)

    @model_validator(mode="after")
    def validate_dispatch_mode(self) -> "FTMConfig":
        """Validate dispatch_mode is 'btm' or 'ftm'."""
        val = self.dispatch_mode.strip().lower()
        if val not in ("btm", "ftm"):
            raise ValueError(
                f"dispatch_mode must be 'btm' or 'ftm', "
                f"got '{self.dispatch_mode}'"
            )
        self.dispatch_mode = val
        return self

    @model_validator(mode="after")
    def validate_iso(self) -> "FTMConfig":
        """Validate ISO is one of the allowed values when provided."""
        if self.iso is None:
            return self
        val = self.iso.strip().lower()
        allowed = {"pjm", "ercot", "caiso"}
        if val not in allowed:
            raise ValueError(
                f"iso must be one of {sorted(allowed)}, got '{self.iso}'"
            )
        self.iso = val
        return self

    @model_validator(mode="after")
    def validate_lmp_market(self) -> "FTMConfig":
        """Validate lmp_market is one of the allowed values."""
        allowed = {"DAY_AHEAD_HOURLY", "REAL_TIME_HOURLY"}
        if self.lmp_market not in allowed:
            raise ValueError(
                f"lmp_market must be one of {sorted(allowed)}, "
                f"got '{self.lmp_market}'"
            )
        return self


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class ProductionSummary(BaseModel):
    """Core production metrics returned by a simulation run."""

    annual_energy_mwh: float
    capacity_factor_dc: float
    capacity_factor_ac: float
    specific_yield_kwh_per_kwp: float
    performance_ratio: float


class LossSummary(BaseModel):
    """Loss breakdown returned by a simulation run."""

    shading_pct: float
    dc_wiring_pct: float
    ac_wiring_pct: float
    transformer_pct: float
    mismatch_pct: float
    lid_pct: float
    availability_pct: float
    subhourly_clipping_pct: float
    inverter_efficiency_pct: float
