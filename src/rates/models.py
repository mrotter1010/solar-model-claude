"""Pydantic models for electricity rate structures, load profiles, and bill calculations."""

from pydantic import BaseModel, Field, field_validator, model_validator


class RateTier(BaseModel):
    """A single pricing tier within a rate period.

    Args:
        rate: Price per unit ($/kWh for energy, $/kW for demand).
        max: Tier ceiling in kWh or kW. None means unlimited.
        adj: Rate adjustment adder, reserved for future use.
    """

    rate: float = Field(ge=0)
    max: float | None = None
    adj: float = 0.0


class FixedCharges(BaseModel):
    """Monthly fixed charges applied regardless of usage.

    Args:
        fixed_charge_first_meter: Fixed charge amount per billing period.
        fixed_charge_units: Unit string for the charge period.
    """

    fixed_charge_first_meter: float = Field(default=0.0, ge=0)
    fixed_charge_units: str = "$/month"

    @field_validator("fixed_charge_units")
    @classmethod
    def validate_units(cls, v: str) -> str:
        """Validate fixed charge units are a recognized billing period."""
        allowed = {"$/month", "$/day", "$/year"}
        if v not in allowed:
            raise ValueError(
                f"fixed_charge_units must be one of {sorted(allowed)}, got '{v}'"
            )
        return v


class NetMeteringConfig(BaseModel):
    """Configuration for net energy metering (NEM) export credits.

    Args:
        mode: NEM mode — "none", "flat_rate", "match_import", or "detailed".
        export_rate: Export credit rate in $/kWh (flat_rate mode only).
        export_schedule: 12x24 weekday matrix mapping month-hour to export
            period index (detailed mode).
        export_weekend_schedule: 12x24 weekend matrix mapping month-hour to
            export period index (detailed mode).
        export_rate_structure: Export rate tiers per period (detailed mode).
        true_up_rate: Year-end bank cashout rate in $/kWh.
    """

    mode: str = "none"
    export_rate: float | None = None
    export_schedule: list[list[int]] | None = None
    export_weekend_schedule: list[list[int]] | None = None
    export_rate_structure: list[list[RateTier]] | None = None
    true_up_rate: float = 0.0

    @field_validator("mode", mode="before")
    @classmethod
    def coerce_mode_lowercase(cls, v: object) -> str:
        """Coerce mode to lowercase string."""
        if v is None:
            return "none"
        return str(v).strip().lower()

    @model_validator(mode="after")
    def validate_mode_fields(self) -> "NetMeteringConfig":
        """Validate fields required by the selected NEM mode.

        Rules:
        - mode must be one of: none, flat_rate, match_import, detailed.
        - flat_rate requires export_rate > 0.
        - detailed requires export_schedule (12x24), export_weekend_schedule
          (12x24), and export_rate_structure (≥1 tier per period).
        - match_import and none have no additional requirements.
        """
        allowed_modes = {"none", "flat_rate", "match_import", "detailed"}
        if self.mode not in allowed_modes:
            raise ValueError(
                f"mode must be one of {sorted(allowed_modes)}, got '{self.mode}'"
            )

        if self.mode == "flat_rate":
            if self.export_rate is None:
                raise ValueError(
                    "export_rate is required when mode is 'flat_rate'"
                )
            if self.export_rate <= 0:
                raise ValueError(
                    f"export_rate must be > 0 when mode is 'flat_rate', "
                    f"got {self.export_rate}"
                )

        if self.mode == "detailed":
            if self.export_schedule is None:
                raise ValueError(
                    "export_schedule is required when mode is 'detailed'"
                )
            if self.export_weekend_schedule is None:
                raise ValueError(
                    "export_weekend_schedule is required when mode is 'detailed'"
                )
            if self.export_rate_structure is None:
                raise ValueError(
                    "export_rate_structure is required when mode is 'detailed'"
                )
            for field_name, schedule in [
                ("export_schedule", self.export_schedule),
                ("export_weekend_schedule", self.export_weekend_schedule),
            ]:
                if len(schedule) != 12:
                    raise ValueError(
                        f"{field_name} must have 12 rows (months), "
                        f"got {len(schedule)}"
                    )
                for i, row in enumerate(schedule):
                    if len(row) != 24:
                        raise ValueError(
                            f"{field_name}[{i}] must have 24 columns (hours), "
                            f"got {len(row)}"
                        )
            for i, tiers in enumerate(self.export_rate_structure):
                if len(tiers) < 1:
                    raise ValueError(
                        f"export_rate_structure[{i}] must have at least 1 tier"
                    )

        return self


class RateSchedule(BaseModel):
    """Canonical electricity rate structure modeled after URDB format.

    Contains energy charges, optional TOU demand charges, optional flat
    (non-coincident) demand charges, fixed charges, and optional net
    metering configuration. Schedule matrices map each month-hour
    combination to a period index in the corresponding rate structure array.

    Args:
        utility_name: Name of the electric utility.
        tariff_name: Tariff schedule identifier.
        effective_date: Date the tariff became effective.
        sector: Customer sector (commercial, residential, industrial).
        description: Human-readable tariff description.
        source: Origin of the rate data ("manual" or "openei").
        openei_label: OpenEI URDB label identifier.
        fixed_charges: Fixed monthly charges.
        energyratestructure: Energy rate periods, each a list of tiers.
        energyweekdayschedule: 12x24 matrix mapping month-hour to energy period.
        energyweekendschedule: 12x24 matrix mapping month-hour to energy period.
        demandratestructure: TOU demand rate periods (optional).
        demandweekdayschedule: 12x24 matrix for TOU demand (optional).
        demandweekendschedule: 12x24 matrix for TOU demand (optional).
        flatdemandstructure: Non-coincident demand rate periods (optional).
        flatdemandmonths: 12-element array mapping months to flat demand periods.
        net_metering: Net energy metering configuration.
    """

    utility_name: str
    tariff_name: str
    effective_date: str | None = None
    sector: str = "commercial"
    description: str = ""
    source: str = "manual"
    openei_label: str | None = None
    fixed_charges: FixedCharges = Field(default_factory=FixedCharges)

    # Energy charges (required)
    energyratestructure: list[list[RateTier]]
    energyweekdayschedule: list[list[int]]
    energyweekendschedule: list[list[int]]

    # TOU demand charges (optional as a group)
    demandratestructure: list[list[RateTier]] | None = None
    demandweekdayschedule: list[list[int]] | None = None
    demandweekendschedule: list[list[int]] | None = None

    # Flat (non-coincident) demand charges (optional as a group)
    flatdemandstructure: list[list[RateTier]] | None = None
    flatdemandmonths: list[int] | None = None

    # Net energy metering
    net_metering: NetMeteringConfig = Field(default_factory=NetMeteringConfig)

    @field_validator("source")
    @classmethod
    def validate_source(cls, v: str) -> str:
        """Validate source is manual or openei."""
        allowed = {"manual", "openei"}
        if v not in allowed:
            raise ValueError(f"source must be one of {sorted(allowed)}, got '{v}'")
        return v

    @field_validator("energyweekdayschedule", "energyweekendschedule")
    @classmethod
    def validate_energy_schedule_dimensions(cls, v: list[list[int]]) -> list[list[int]]:
        """Validate energy schedule matrices are exactly 12 rows x 24 columns."""
        if len(v) != 12:
            raise ValueError(
                f"Schedule must have 12 rows (months), got {len(v)}"
            )
        for i, row in enumerate(v):
            if len(row) != 24:
                raise ValueError(
                    f"Schedule row {i} must have 24 columns (hours), got {len(row)}"
                )
        return v

    @field_validator("demandweekdayschedule", "demandweekendschedule")
    @classmethod
    def validate_demand_schedule_dimensions(
        cls, v: list[list[int]] | None
    ) -> list[list[int]] | None:
        """Validate demand schedule matrices are 12x24 when provided."""
        if v is None:
            return v
        if len(v) != 12:
            raise ValueError(
                f"Schedule must have 12 rows (months), got {len(v)}"
            )
        for i, row in enumerate(v):
            if len(row) != 24:
                raise ValueError(
                    f"Schedule row {i} must have 24 columns (hours), got {len(row)}"
                )
        return v

    @model_validator(mode="after")
    def validate_schedule_indices_and_groups(self) -> "RateSchedule":
        """Cross-validate period indices and field group consistency.

        Checks:
        - Energy schedule indices are valid for energyratestructure.
        - Demand schedules and demandratestructure are provided together.
        - Demand schedule indices are valid for demandratestructure.
        - flatdemandstructure and flatdemandmonths are provided together.
        - flatdemandmonths is length 12 with valid indices.
        """
        # Energy schedule index validation
        n_energy_periods = len(self.energyratestructure)
        _validate_schedule_indices(
            self.energyweekdayschedule, n_energy_periods, "energyweekdayschedule"
        )
        _validate_schedule_indices(
            self.energyweekendschedule, n_energy_periods, "energyweekendschedule"
        )

        # TOU demand group consistency
        demand_fields = [
            self.demandratestructure,
            self.demandweekdayschedule,
            self.demandweekendschedule,
        ]
        demand_set = [f is not None for f in demand_fields]
        if any(demand_set) and not all(demand_set):
            raise ValueError(
                "demandratestructure, demandweekdayschedule, and "
                "demandweekendschedule must all be provided together or all omitted"
            )

        if self.demandratestructure is not None:
            n_demand_periods = len(self.demandratestructure)
            _validate_schedule_indices(
                self.demandweekdayschedule, n_demand_periods, "demandweekdayschedule"
            )
            _validate_schedule_indices(
                self.demandweekendschedule, n_demand_periods, "demandweekendschedule"
            )

        # Flat demand group consistency
        flat_fields = [self.flatdemandstructure, self.flatdemandmonths]
        flat_set = [f is not None for f in flat_fields]
        if any(flat_set) and not all(flat_set):
            raise ValueError(
                "flatdemandstructure and flatdemandmonths must both be "
                "provided together or both omitted"
            )

        if self.flatdemandstructure is not None and self.flatdemandmonths is not None:
            if len(self.flatdemandmonths) != 12:
                raise ValueError(
                    f"flatdemandmonths must have 12 elements, "
                    f"got {len(self.flatdemandmonths)}"
                )
            n_flat_periods = len(self.flatdemandstructure)
            for i, idx in enumerate(self.flatdemandmonths):
                if idx < 0 or idx >= n_flat_periods:
                    raise ValueError(
                        f"flatdemandmonths[{i}] = {idx} is out of range "
                        f"for flatdemandstructure with {n_flat_periods} periods"
                    )

        return self


def _validate_schedule_indices(
    schedule: list[list[int]] | None,
    n_periods: int,
    field_name: str,
) -> None:
    """Validate that all indices in a schedule matrix are valid period references.

    Args:
        schedule: 12x24 matrix of period indices.
        n_periods: Number of periods in the corresponding rate structure.
        field_name: Field name for error messages.

    Raises:
        ValueError: If any index is out of range.
    """
    if schedule is None:
        return
    for month_idx, row in enumerate(schedule):
        for hour_idx, period_idx in enumerate(row):
            if period_idx < 0 or period_idx >= n_periods:
                raise ValueError(
                    f"{field_name}[{month_idx}][{hour_idx}] = {period_idx} "
                    f"is out of range for rate structure with {n_periods} periods"
                )


class LoadProfile(BaseModel):
    """Hourly electricity consumption profile for bill calculation.

    Args:
        hourly_kwh: 8760 hourly consumption values in kWh.
        source: Origin of the profile ("customer" or "typical").
        building_type: DOE reference building type (typical profiles only).
        scale_factor: Scaling ratio applied (typical profiles only).
    """

    hourly_kwh: list[float]
    source: str
    building_type: str | None = None
    scale_factor: float | None = None

    # Computed fields populated by model_validator
    annual_consumption_kwh: float = 0.0
    peak_demand_kw: float = 0.0

    @field_validator("hourly_kwh")
    @classmethod
    def validate_length(cls, v: list[float]) -> list[float]:
        """Validate hourly profile is exactly 8760 hours."""
        if len(v) != 8760:
            raise ValueError(
                f"hourly_kwh must have 8760 elements, got {len(v)}"
            )
        return v

    @field_validator("source")
    @classmethod
    def validate_source(cls, v: str) -> str:
        """Validate source is customer or typical."""
        allowed = {"customer", "typical"}
        if v not in allowed:
            raise ValueError(f"source must be one of {sorted(allowed)}, got '{v}'")
        return v

    @model_validator(mode="after")
    def compute_summary_fields(self) -> "LoadProfile":
        """Compute annual consumption and peak demand from hourly data."""
        self.annual_consumption_kwh = sum(self.hourly_kwh)
        self.peak_demand_kw = max(self.hourly_kwh) if self.hourly_kwh else 0.0
        return self


class MonthlyBillDetail(BaseModel):
    """Itemized electricity bill for a single month.

    Args:
        month: Calendar month (1-12).
        energy_charges: Total energy charges for the month in $.
        demand_charges: TOU demand charges for the month in $.
        flat_demand_charges: Non-coincident demand charges for the month in $.
        fixed_charges: Fixed charges for the month in $.
        production_kwh: Solar production during the month in kWh.
        consumption_kwh: Electricity consumption during the month in kWh.
        peak_demand_kw: Peak demand during the month in kW.
    """

    month: int = Field(ge=1, le=12)
    energy_charges: float
    demand_charges: float
    flat_demand_charges: float
    fixed_charges: float
    total: float = 0.0
    production_kwh: float
    consumption_kwh: float
    peak_demand_kw: float
    export_kwh: float = 0.0
    export_credits: float = 0.0
    nem_credits_applied: float = 0.0
    nem_credits_banked_kwh: float = 0.0

    @model_validator(mode="after")
    def compute_total(self) -> "MonthlyBillDetail":
        """Compute total as sum of all charge components minus NEM credits."""
        self.total = (
            self.energy_charges
            + self.demand_charges
            + self.flat_demand_charges
            + self.fixed_charges
            - self.nem_credits_applied
        )
        return self


class BillResult(BaseModel):
    """Complete annual electricity bill with monthly breakdown.

    Args:
        monthly: 12 monthly bill detail records.
    """

    monthly: list[MonthlyBillDetail]

    # Computed annual totals populated by model_validator
    annual_energy_charges: float = 0.0
    annual_demand_charges: float = 0.0
    annual_flat_demand_charges: float = 0.0
    annual_fixed_charges: float = 0.0
    annual_total: float = 0.0
    annual_consumption_kwh: float = 0.0
    annual_production_kwh: float = 0.0
    annual_export_kwh: float = 0.0
    annual_export_credits: float = 0.0
    nem_true_up_credit: float = 0.0

    @field_validator("monthly")
    @classmethod
    def validate_twelve_months(cls, v: list[MonthlyBillDetail]) -> list[MonthlyBillDetail]:
        """Validate that exactly 12 monthly records are provided."""
        if len(v) != 12:
            raise ValueError(f"monthly must have 12 elements, got {len(v)}")
        return v

    @model_validator(mode="after")
    def compute_annual_totals(self) -> "BillResult":
        """Compute annual totals from monthly records."""
        self.annual_energy_charges = sum(m.energy_charges for m in self.monthly)
        self.annual_demand_charges = sum(m.demand_charges for m in self.monthly)
        self.annual_flat_demand_charges = sum(
            m.flat_demand_charges for m in self.monthly
        )
        self.annual_fixed_charges = sum(m.fixed_charges for m in self.monthly)
        self.annual_total = sum(m.total for m in self.monthly) - self.nem_true_up_credit
        self.annual_consumption_kwh = sum(m.consumption_kwh for m in self.monthly)
        self.annual_production_kwh = sum(m.production_kwh for m in self.monthly)
        self.annual_export_kwh = sum(m.export_kwh for m in self.monthly)
        self.annual_export_credits = sum(m.export_credits for m in self.monthly)
        return self


class BillSavings(BaseModel):
    """Comparison of electricity bills with and without solar.

    Args:
        bill_without_solar: Full bill assuming no solar production.
        bill_with_solar: Net bill after solar offsets consumption.
    """

    bill_without_solar: BillResult
    bill_with_solar: BillResult

    # Computed fields populated by model_validator
    monthly_savings: list[float] = Field(default_factory=list)
    annual_savings: float = 0.0
    savings_percent: float = 0.0
    avoided_cost_per_kwh: float = 0.0
    annual_demand_savings: float = 0.0

    @model_validator(mode="after")
    def compute_savings(self) -> "BillSavings":
        """Compute savings metrics from the two bill results."""
        self.monthly_savings = [
            wo.total - ws.total
            for wo, ws in zip(
                self.bill_without_solar.monthly, self.bill_with_solar.monthly
            )
        ]
        self.annual_savings = (
            self.bill_without_solar.annual_total - self.bill_with_solar.annual_total
        )
        if self.bill_without_solar.annual_total > 0:
            self.savings_percent = (
                self.annual_savings / self.bill_without_solar.annual_total * 100
            )
        else:
            self.savings_percent = 0.0

        annual_production = self.bill_with_solar.annual_production_kwh
        if annual_production > 0:
            self.avoided_cost_per_kwh = self.annual_savings / annual_production
        else:
            self.avoided_cost_per_kwh = 0.0

        self.annual_demand_savings = (
            self.bill_without_solar.annual_demand_charges
            + self.bill_without_solar.annual_flat_demand_charges
            - self.bill_with_solar.annual_demand_charges
            - self.bill_with_solar.annual_flat_demand_charges
        )
        return self
