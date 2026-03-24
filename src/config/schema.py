"""Pydantic validation model for solar site configuration."""

import os
from pathlib import Path

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.utils.exceptions import ConfigValidationError
from src.utils.logger import setup_logger
from src.rates.load_profile import get_available_building_types

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

    # BESS
    bess_dispatch_required: bool = False
    bess_optimization_required: float | None = None
    bess_power_mw: float | None = None
    bess_duration_hr: float | None = None
    bess_rte_percent: float = 88.0
    bess_min_soc_percent: float = 10.0
    bess_max_soc_percent: float = 90.0
    bess_strategy: str = "global"
    bess_installed_cost_per_kwh: float = 275.0
    bess_cycles_warranty: int = 5000
    bess_solar_only_charging: bool = False
    bess_grid_only_charging: bool = False

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

    # Buildable Land Assessment
    buildable_land_assessment: bool = False
    kmz_file_path: str | None = None
    analysis_radius_km: float | None = None

    # Bill Calculation
    bill_calculation: bool = False
    rate_file_path: str | None = None
    utility_name: str | None = None
    tariff_name: str | None = None
    load_profile_path: str | None = None
    load_type: str | None = None
    annual_consumption_kwh: float | None = None
    peak_demand_kw: float | None = None

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

    @field_validator("bess_dispatch_required", mode="before")
    @classmethod
    def validate_bess_dispatch_required(cls, v: object) -> bool:
        """Coerce BESS dispatch required field from CSV to bool.

        Handles None (empty cell), empty string, and string TRUE/FALSE
        values that pandas may produce when reading CSV columns.
        """
        if v is None or v == "":
            return False
        if isinstance(v, str):
            return v.strip().upper() in ("TRUE", "YES", "1")
        return bool(v)

    @field_validator("bess_power_mw", "bess_duration_hr", mode="before")
    @classmethod
    def validate_optional_bess_float(cls, v: object) -> float | None:
        """Coerce empty/NaN BESS float fields to None."""
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        return float(v)

    @field_validator(
        "bess_rte_percent",
        "bess_min_soc_percent",
        "bess_max_soc_percent",
        "bess_installed_cost_per_kwh",
        mode="before",
    )
    @classmethod
    def validate_bess_float_defaults(cls, v: object, info: object) -> float:
        """Coerce None/empty to field default for BESS numeric fields."""
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return cls.model_fields[info.field_name].default
        return float(v)

    @field_validator("bess_strategy", mode="before")
    @classmethod
    def validate_bess_strategy(cls, v: object) -> str:
        """Coerce None/empty BESS strategy to default."""
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return "global"
        return str(v).strip().lower()

    @field_validator("bess_cycles_warranty", mode="before")
    @classmethod
    def validate_bess_cycles_warranty(cls, v: object) -> int:
        """Coerce None/empty BESS cycles warranty to default."""
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return 5000
        return int(v)

    @field_validator("bess_solar_only_charging", mode="before")
    @classmethod
    def validate_bess_solar_only_charging(cls, v: object) -> bool:
        """Coerce BESS solar only charging field from CSV to bool.

        Handles None (empty cell), empty string, and string TRUE/FALSE
        values that pandas may produce when reading CSV columns.
        """
        if v is None or v == "":
            return False
        if isinstance(v, str):
            return v.strip().upper() in ("TRUE", "YES", "1")
        return bool(v)

    @field_validator("bess_grid_only_charging", mode="before")
    @classmethod
    def validate_bess_grid_only_charging(cls, v: object) -> bool:
        """Coerce BESS grid only charging field from CSV to bool.

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

    @field_validator("buildable_land_assessment", mode="before")
    @classmethod
    def validate_buildable_land_assessment(cls, v: object) -> bool:
        """Coerce buildable land assessment field from CSV to bool.

        Handles None (empty cell), empty string, and string TRUE/FALSE
        values that pandas may produce when reading CSV columns.
        """
        if v is None or v == "":
            return False
        if isinstance(v, str):
            return v.strip().upper() in ("TRUE", "YES", "1")
        return bool(v)

    @field_validator("analysis_radius_km", mode="before")
    @classmethod
    def validate_analysis_radius_km(cls, v: object) -> float | None:
        """Validate analysis radius is positive when provided.

        Handles None (empty cell) and empty string from CSV input.
        """
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        v_float = float(v)
        if v_float <= 0:
            raise ValueError(
                f"Analysis Radius (km) must be > 0, got {v_float}"
            )
        return v_float

    @field_validator("kmz_file_path", mode="before")
    @classmethod
    def validate_kmz_file_path(cls, v: object) -> str | None:
        """Normalize KMZ file path from CSV input.

        Handles None (empty cell) and empty string from CSV input.
        """
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        return str(v).strip()

    @field_validator("bill_calculation", mode="before")
    @classmethod
    def validate_bill_calculation(cls, v: object) -> bool:
        """Coerce bill calculation field from CSV to bool.

        Handles None (empty cell), empty string, and string TRUE/FALSE
        values that pandas may produce when reading CSV columns.
        """
        if v is None or v == "":
            return False
        if isinstance(v, str):
            return v.strip().upper() in ("TRUE", "YES", "1")
        return bool(v)

    @field_validator("rate_file_path", mode="before")
    @classmethod
    def validate_rate_file_path(cls, v: object) -> str | None:
        """Validate rate file path exists when provided."""
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        path = Path(str(v).strip())
        if not path.exists():
            raise ValueError(f"Rate file not found: {path}")
        return str(path)

    @field_validator("load_profile_path", mode="before")
    @classmethod
    def validate_load_profile_path(cls, v: object) -> str | None:
        """Validate load profile path exists when provided."""
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        path = Path(str(v).strip())
        if not path.exists():
            raise ValueError(f"Load profile file not found: {path}")
        return str(path)

    @field_validator("annual_consumption_kwh", mode="before")
    @classmethod
    def validate_annual_consumption_kwh(cls, v: object) -> float | None:
        """Validate annual consumption is positive when provided."""
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        v_float = float(v)
        if v_float <= 0:
            raise ValueError(
                f"Annual Consumption (kWh) must be > 0, got {v_float}"
            )
        return v_float

    @field_validator("peak_demand_kw", mode="before")
    @classmethod
    def validate_peak_demand_kw(cls, v: object) -> float | None:
        """Validate peak demand is positive when provided."""
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        v_float = float(v)
        if v_float <= 0:
            raise ValueError(
                f"Peak Demand (kW) must be > 0, got {v_float}"
            )
        return v_float

    @field_validator("utility_name", "tariff_name", "load_type", mode="before")
    @classmethod
    def validate_optional_string_fields(cls, v: object) -> str | None:
        """Normalize optional string fields from CSV (empty/NaN → None)."""
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        return str(v).strip()

    @model_validator(mode="after")
    def validate_buildability_fields(self) -> "SiteConfig":
        """Cross-validate buildable land assessment fields.

        When buildable_land_assessment is True, ensures kmz_file_path and
        analysis_radius_km are not both specified (mutually exclusive).
        When buildable_land_assessment is False, skips validation entirely.
        """
        if not self.buildable_land_assessment:
            return self

        if self.kmz_file_path is not None and self.analysis_radius_km is not None:
            raise ValueError(
                "Cannot specify both KMZ File Path and Analysis Radius. "
                "Provide one or neither."
            )

        return self

    @model_validator(mode="after")
    def validate_bill_fields(self) -> "SiteConfig":
        """Cross-validate bill calculation fields.

        When bill_calculation is True, validates rate source and load source
        mutual exclusivity. When False, skips all checks.
        """
        if not self.bill_calculation:
            return self

        # --- Rate source validation ---
        has_rate_file = self.rate_file_path is not None
        has_openei = self.utility_name is not None or self.tariff_name is not None

        if has_rate_file and has_openei:
            raise ValueError(
                "Cannot specify both Rate File Path and Utility Name/Tariff Name. "
                "Use one rate source."
            )

        if not has_rate_file and not has_openei:
            raise ValueError(
                "No rate source specified. Provide either Rate File Path "
                "or Utility Name + Tariff Name."
            )

        if has_openei:
            if self.utility_name is None or self.tariff_name is None:
                raise ValueError(
                    "Both Utility Name and Tariff Name are required "
                    "when using OpenEI rate lookup."
                )

        # --- Load source validation ---
        has_load_file = self.load_profile_path is not None
        has_typical = self.load_type is not None

        if has_load_file and has_typical:
            raise ValueError(
                "Cannot specify both Load Profile Path and Load Type. "
                "Use one load source."
            )

        if not has_load_file and not has_typical:
            raise ValueError(
                "No load source specified. Provide either Load Profile Path "
                "or Load Type + Annual Consumption (kWh)."
            )

        if has_typical:
            available = get_available_building_types()
            if self.load_type not in available:
                raise ValueError(
                    f"Invalid Load Type '{self.load_type}'. "
                    f"Available types: {available}"
                )
            if self.annual_consumption_kwh is None:
                raise ValueError(
                    "Annual Consumption (kWh) is required when using Load Type."
                )

        if has_load_file:
            if (
                self.annual_consumption_kwh is not None
                or self.peak_demand_kw is not None
                or self.load_type is not None
            ):
                raise ValueError(
                    "Cannot specify Load Type, Annual Consumption, or Peak Demand "
                    "when Load Profile Path is provided."
                )

        return self

    @model_validator(mode="after")
    def validate_bess_fields(self) -> "SiteConfig":
        """Cross-validate BESS dispatch fields.

        When bess_dispatch_required is True, validates that all required
        BESS parameters are provided and consistent. When False, skips
        all BESS validation.
        """
        # Charging mode mutual exclusivity (independent of dispatch_required)
        if self.bess_solar_only_charging and self.bess_grid_only_charging:
            raise ValueError(
                "bess_solar_only_charging and bess_grid_only_charging "
                "are mutually exclusive"
            )

        # Charging modes require dispatch to be enabled
        if self.bess_solar_only_charging and not self.bess_dispatch_required:
            raise ValueError(
                "BESS Dispatch Required must be True when "
                "BESS Solar Only Charging is True."
            )

        if self.bess_grid_only_charging and not self.bess_dispatch_required:
            raise ValueError(
                "BESS Dispatch Required must be True when "
                "BESS Grid Only Charging is True."
            )

        if not self.bess_dispatch_required:
            return self

        if self.bess_power_mw is None or self.bess_power_mw <= 0:
            raise ValueError(
                "BESS Power (MW) must be > 0 when BESS Dispatch Required is True."
            )

        if self.bess_duration_hr is None or self.bess_duration_hr <= 0:
            raise ValueError(
                "BESS Duration (hr) must be > 0 when BESS Dispatch Required is True."
            )

        allowed_strategies = {"peak_shaving", "tou_arbitrage", "global"}
        if self.bess_strategy not in allowed_strategies:
            raise ValueError(
                f"BESS Strategy must be one of {sorted(allowed_strategies)}, "
                f"got '{self.bess_strategy}'."
            )

        if self.bess_min_soc_percent >= self.bess_max_soc_percent:
            raise ValueError(
                f"BESS Min SOC ({self.bess_min_soc_percent}%) must be less than "
                f"BESS Max SOC ({self.bess_max_soc_percent}%)."
            )

        if not (50 <= self.bess_rte_percent <= 100):
            raise ValueError(
                f"BESS RTE must be between 50% and 100%, "
                f"got {self.bess_rte_percent}%."
            )

        if self.bess_installed_cost_per_kwh <= 0:
            raise ValueError(
                "BESS Installed Cost ($/kWh) must be > 0 when "
                "BESS Dispatch Required is True."
            )

        if self.bess_cycles_warranty <= 0:
            raise ValueError(
                "BESS Cycles Warranty must be > 0 when "
                "BESS Dispatch Required is True."
            )

        if not self.bill_calculation:
            raise ValueError(
                "Bill Calculation must be True when BESS Dispatch Required is True. "
                "BESS dispatch depends on rate structure and load profile."
            )

        return self

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
