"""Request and response schemas for the layout optimization endpoint."""

from pydantic import BaseModel, Field, model_validator


class OptimizationRequest(BaseModel):
    """Request body for a solar layout optimization run.

    Requires either buildable_acres (direct) or buildability_run_id
    (looked up from a previous buildability analysis). Supports four
    modes based on which inputs are provided:

    - production: no economic inputs
    - lcoe: economic inputs without revenue
    - npv: economic + revenue inputs
    - solar_bess: bess_enabled=True (requires NPV-level economics)
    """

    # Location
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)

    # Land area (exactly one required)
    buildable_acres: float | None = Field(default=None, gt=0)
    buildability_run_id: str | None = None

    # System design
    racking: str = "tracker"
    module_name: str | None = None
    inverter_name: str | None = None
    gcr_range: tuple[float, float] | None = None
    gcr_step: float = Field(default=0.05, gt=0)
    dcac_range: tuple[float, float] = (1.15, 1.60)
    dcac_step: float = Field(default=0.05, gt=0)
    utilization_factor: float = Field(default=0.75, gt=0, le=1)

    # BESS configuration
    bess_enabled: bool = False
    dispatch_mode: str | None = None
    charging_mode: str = "solar_and_grid"
    bess_power_fractions: list[float] | None = None
    bess_durations_hr: list[float] | None = None

    # Economic inputs (all-or-nothing for lcoe/npv modes)
    solar_cost_per_kw_dc: float | None = Field(default=None, gt=0)
    solar_opex_per_kw_dc_year: float | None = Field(default=None, ge=0)
    discount_rate_pct: float | None = Field(default=None, gt=0)
    project_lifetime_years: int | None = Field(default=None, gt=0)
    degradation_pct: float | None = Field(default=None, ge=0, le=100)
    itc_pct: float | None = Field(default=None, ge=0, le=100)

    # Revenue inputs (all-or-nothing, requires economics for npv mode)
    energy_price_per_kwh: float | None = Field(default=None, gt=0)
    energy_cost_escalator_pct: float | None = Field(default=None, ge=0)

    # BESS economics
    bess_cost_per_kwh: float | None = Field(default=None, gt=0)
    bess_opex_per_kw_year: float | None = Field(default=None, ge=0)

    # BTM inputs
    rate_schedule: dict | None = None
    load_profile_kwh: list[float] | None = None

    # FTM inputs
    lmp_prices: list[float] | None = None
    lmp_iso: str | None = None
    lmp_zone: str | None = None

    @model_validator(mode="after")
    def validate_buildable_acres_source(self) -> "OptimizationRequest":
        """Exactly one of buildable_acres or buildability_run_id required."""
        has_acres = self.buildable_acres is not None
        has_run_id = self.buildability_run_id is not None
        if has_acres and has_run_id:
            raise ValueError(
                "Provide exactly one of buildable_acres or "
                "buildability_run_id, not both."
            )
        if not has_acres and not has_run_id:
            raise ValueError(
                "Either buildable_acres or buildability_run_id is required."
            )
        return self

    @model_validator(mode="after")
    def validate_racking(self) -> "OptimizationRequest":
        """Validate racking is 'fixed' or 'tracker' (case-insensitive)."""
        val = self.racking.strip().lower()
        if val not in ("fixed", "tracker"):
            raise ValueError(
                f"racking must be 'fixed' or 'tracker', got '{self.racking}'"
            )
        self.racking = val
        return self

    @model_validator(mode="after")
    def validate_bess_requires_dispatch_mode(self) -> "OptimizationRequest":
        """If bess_enabled, dispatch_mode is required."""
        if self.bess_enabled and self.dispatch_mode is None:
            raise ValueError(
                "dispatch_mode is required when bess_enabled is True. "
                "Use 'btm' or 'ftm'."
            )
        return self

    @model_validator(mode="after")
    def validate_dispatch_mode_value(self) -> "OptimizationRequest":
        """Validate dispatch_mode is 'btm' or 'ftm' when provided."""
        if self.dispatch_mode is not None:
            val = self.dispatch_mode.strip().lower()
            if val not in ("btm", "ftm"):
                raise ValueError(
                    f"dispatch_mode must be 'btm' or 'ftm', "
                    f"got '{self.dispatch_mode}'"
                )
            self.dispatch_mode = val
        return self

    @model_validator(mode="after")
    def validate_charging_mode_value(self) -> "OptimizationRequest":
        """Validate charging_mode is 'solar_and_grid' or 'solar_only'."""
        val = self.charging_mode.strip().lower()
        if val not in ("solar_and_grid", "solar_only"):
            raise ValueError(
                "charging_mode must be 'solar_and_grid' or 'solar_only', "
                f"got '{self.charging_mode}'"
            )
        self.charging_mode = val
        return self

    @model_validator(mode="after")
    def validate_gcr_range(self) -> "OptimizationRequest":
        """Validate gcr_range has min < max when provided."""
        if self.gcr_range is not None:
            if self.gcr_range[0] > self.gcr_range[1]:
                raise ValueError(
                    f"gcr_range min ({self.gcr_range[0]}) must be <= "
                    f"max ({self.gcr_range[1]})"
                )
        return self

    @model_validator(mode="after")
    def validate_dcac_range(self) -> "OptimizationRequest":
        """Validate dcac_range has min < max."""
        if self.dcac_range[0] > self.dcac_range[1]:
            raise ValueError(
                f"dcac_range min ({self.dcac_range[0]}) must be <= "
                f"max ({self.dcac_range[1]})"
            )
        return self

    @model_validator(mode="after")
    def validate_load_profile_length(self) -> "OptimizationRequest":
        """Validate load_profile_kwh has 8760 values when provided."""
        if self.load_profile_kwh is not None:
            if len(self.load_profile_kwh) != 8760:
                raise ValueError(
                    f"load_profile_kwh must have exactly 8760 values, "
                    f"got {len(self.load_profile_kwh)}"
                )
        return self

    @model_validator(mode="after")
    def validate_lmp_prices_length(self) -> "OptimizationRequest":
        """Validate lmp_prices has 8760 values when provided."""
        if self.lmp_prices is not None:
            if len(self.lmp_prices) != 8760:
                raise ValueError(
                    f"lmp_prices must have exactly 8760 values, "
                    f"got {len(self.lmp_prices)}"
                )
        return self


class OptimizationResponse(BaseModel):
    """Response body for a successful layout optimization run."""

    run_id: str
    mode: str
    winners: dict
    bess_adds_value: bool | None = None
    bess_winner_detail: dict | None = None
    sweep_metadata: dict
