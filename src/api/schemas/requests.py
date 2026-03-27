"""Request schemas for the solar modeling REST API."""

from pydantic import BaseModel, model_validator

from src.api.schemas.common import (
    BESSConfig,
    BESSEconomics,
    BillConfig,
    BuildabilityConfig,
    FTMConfig,
    Location,
    Losses,
    ResourceOverrides,
    SystemDesign,
)


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

    Only requires site location (lat/lon). Buildability configuration
    (KMZ file path or analysis radius) is optional — if omitted, the
    analyzer defaults to a 1.0 km radius buffer around the site.
    """

    site: Location
    buildability: BuildabilityConfig | None = None
