"""Response schemas for the solar modeling REST API."""

from pydantic import BaseModel

from src.api.schemas.common import LossSummary, ProductionSummary


class FileLinks(BaseModel):
    """Links to downloadable artifacts for a simulation run."""

    report_url: str | None = None
    timeseries_url: str | None = None


class ProductionResponse(BaseModel):
    """Response body for a successful production simulation run."""

    run_id: str
    status: str
    site_name: str
    production: ProductionSummary
    losses: LossSummary
    monthly_production_mwh: list[float]
    files: FileLinks


class BillSavingsSummary(BaseModel):
    """Bill savings metrics from the rate engine analysis."""

    annual_bill_without_solar: float
    annual_bill_with_solar: float
    annual_savings: float
    savings_percent: float
    avoided_cost_per_kwh: float
    annual_demand_savings: float
    annual_export_kwh: float
    annual_export_credits: float
    nem_true_up_credit: float


class MonthlyBillDetail(BaseModel):
    """Monthly bill comparison for a single month."""

    month: int
    bill_without: float
    bill_with: float
    savings: float


class BillSavingsResponse(BaseModel):
    """Response body for a successful bill savings simulation run."""

    run_id: str
    status: str
    site_name: str
    production: ProductionSummary
    losses: LossSummary
    monthly_production_mwh: list[float]
    bill_savings: BillSavingsSummary
    monthly_bill_detail: list[MonthlyBillDetail]
    files: FileLinks


class BESSDispatchSummary(BaseModel):
    """BESS dispatch metrics from the optimizer."""

    bess_power_mw: float
    bess_duration_hr: float
    bess_capacity_kwh: float
    bess_strategy: str
    annual_cycles: float
    annual_throughput_kwh: float
    capacity_utilization_pct: float
    total_curtailed_kwh: float
    estimated_annual_degradation_pct: float
    charging_source: str


class BESSBillComparison(BaseModel):
    """Bill comparison between solar-only and solar+BESS scenarios."""

    solar_only_annual_bill: float
    solar_plus_bess_annual_bill: float
    bess_incremental_savings: float
    bess_demand_savings: float
    bess_energy_savings: float


class LMPSummary(BaseModel):
    """Locational marginal pricing summary for FTM dispatch."""

    iso: str
    zone: str
    market: str
    year: int
    mean_lmp: float
    min_lmp: float
    max_lmp: float


class FTMEconomics(BaseModel):
    """Front-of-meter economic analysis results."""

    annual_solar_revenue: float
    annual_bess_arbitrage_revenue: float
    annual_ancillary_revenue: float
    annual_gross_revenue: float
    total_project_npv: float
    bess_npv: float
    solar_npv: float
    system_lcoe_per_kwh: float
    total_installed_cost: float


class BESSSizingSummary(BaseModel):
    """BESS sizing optimization results."""

    optimal_power_mw: float
    optimal_duration_hr: float
    optimal_capacity_kwh: float
    bess_npv: float
    total_project_npv: float
    system_lcoe_per_kwh: float | None
    total_installed_cost: float
    combos_evaluated: int


class BESSResponse(BaseModel):
    """Response body for a BESS dispatch or sizing optimization run."""

    run_id: str
    status: str
    site_name: str
    production: ProductionSummary
    losses: LossSummary
    monthly_production_mwh: list[float]
    dispatch: BESSDispatchSummary
    bill_comparison: BESSBillComparison | None = None
    lmp: LMPSummary | None = None
    ftm_economics: FTMEconomics | None = None
    sizing: BESSSizingSummary | None = None
    bill_savings: BillSavingsSummary | None = None
    files: FileLinks


class SlopeDistributionBucket(BaseModel):
    """A single bucket in the slope distribution histogram."""

    range: str
    pixels: int
    percent: float


class SlopeSummary(BaseModel):
    """Terrain slope statistics from buildability analysis."""

    min_degrees: float
    max_degrees: float
    mean_degrees: float
    median_degrees: float
    distribution: list[SlopeDistributionBucket]
    pct_below_tracker_limit: float
    pct_below_fixed_tilt_limit: float


class LandCoverClass(BaseModel):
    """A single land cover class in the breakdown."""

    code: int
    name: str
    category: str
    pixels: int
    acres: float
    percent: float


class BuildabilityResponse(BaseModel):
    """Response body for a successful buildability analysis.

    Contains land cover classification results, slope analysis, and
    key acreage metrics. Omits raw raster metadata (nlcd_metadata,
    dem_metadata) and polygon WKT for brevity — these are available
    in the full JSON output if needed.
    """

    run_id: str
    status: str
    latitude: float
    longitude: float
    radius_km: float | None
    kmz_file_path: str | None
    total_area_acres: float
    buildable_acres: float
    soft_exclusion_acres: float
    hard_exclusion_acres: float
    unclassified_acres: float
    buildable_pct: float
    soft_exclusion_pct: float
    hard_exclusion_pct: float
    class_breakdown: list[LandCoverClass]
    slope: SlopeSummary
    figure_paths: dict[str, str]


class ErrorResponse(BaseModel):
    """Response body for a failed request."""

    error: str
    details: str | None = None
