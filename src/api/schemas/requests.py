"""Request schemas for the solar modeling REST API."""

from pydantic import BaseModel, model_validator

from src.api.schemas.common import (
    BESSConfig,
    BESSEconomics,
    BillConfig,
    BuildabilityConfig,
    BuildabilityLocation,
    FTMConfig,
    Location,
    Losses,
    ResourceOverrides,
    SystemDesign,
)
from src.rates.models import RateSchedule


class ProductionRequest(BaseModel):
    """Request body for a production simulation run."""

    site: Location
    system: SystemDesign
    losses: Losses | None = None
    resource: ResourceOverrides | None = None


class BillSavingsRequest(BaseModel):
    """Request body for a production + bill savings simulation run."""

    site: Location
    system: SystemDesign
    bill: BillConfig
    losses: Losses | None = None
    resource: ResourceOverrides | None = None


class BESSRequest(BaseModel):
    """Request body for a BESS dispatch or sizing optimization run."""

    site: Location
    system: SystemDesign
    bess: BESSConfig
    bill: BillConfig | None = None
    ftm: FTMConfig | None = None
    bess_economics: BESSEconomics | None = None
    losses: Losses | None = None
    resource: ResourceOverrides | None = None

    @model_validator(mode="after")
    def validate_btm_requires_bill(self) -> "BESSRequest":
        """Validate that BTM mode requires bill configuration.

        When dispatch_mode is 'btm' (the default) or ftm is not provided,
        the bill field is required because BTM dispatch depends on rate
        structure and load profile for bill calculation.
        """
        is_ftm = self.ftm is not None and self.ftm.dispatch_mode == "ftm"
        if not is_ftm and self.bill is None:
            raise ValueError(
                "bill is required for BTM BESS dispatch. "
                "Provide bill configuration or set ftm.dispatch_mode='ftm'."
            )
        return self

    @model_validator(mode="after")
    def validate_optimization_solar_costs(self) -> "BESSRequest":
        """Validate that optimization requires solar costs (unless grid-only).

        When bess_economics.optimize is True and the BESS is not configured
        for grid-only charging, solar cost fields are required for the NPV
        calculation.
        """
        if self.bess_economics is None or not self.bess_economics.optimize:
            return self
        if self.bess.grid_only_charging:
            return self
        if self.bess_economics.solar_cost_per_kw_dc is None:
            raise ValueError(
                "bess_economics.solar_cost_per_kw_dc is required "
                "when optimize is True."
            )
        if self.bess_economics.solar_cost_per_kw_ac is None:
            raise ValueError(
                "bess_economics.solar_cost_per_kw_ac is required "
                "when optimize is True."
            )
        return self


class BuildabilityRequest(BaseModel):
    """Request body for a buildable land assessment.

    Site name is required. Latitude and longitude are optional when a
    KMZ/KML file is provided — the analyzer computes the polygon
    centroid automatically. When no KMZ/KML is provided, lat/lon are
    required (for radius-based analysis).
    """

    site: BuildabilityLocation
    buildability: BuildabilityConfig | None = None
    include_maps: bool = False

    @model_validator(mode="after")
    def validate_location_or_kmz(self) -> "BuildabilityRequest":
        """Require lat/lon when no KMZ/KML file is provided."""
        has_coords = (
            self.site.latitude is not None and self.site.longitude is not None
        )
        has_kmz = (
            self.buildability is not None
            and self.buildability.kmz_file_path is not None
        )
        if not has_coords and not has_kmz:
            raise ValueError(
                "latitude and longitude are required when no KMZ/KML "
                "file is provided. Either provide site coordinates or "
                "upload a KMZ/KML boundary file."
            )
        return self


class RateBuildRequest(BaseModel):
    """Request body for building a rate schedule via the API.

    Wraps a full RateSchedule (reusing all Pydantic validation from
    src/rates/models.py) plus an option to persist the result to disk.
    """

    rate: RateSchedule
    save_to_disk: bool = False
