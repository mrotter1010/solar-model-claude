"""Pipeline runner: executes the solar modeling pipeline for the API layer."""

from pathlib import Path

from src.api.schemas.common import LossSummary, ProductionSummary
from src.api.schemas.responses import (
    BESSBillComparison,
    BESSDispatchSummary,
    BESSResponse,
    BESSSizingSummary,
    BillSavingsResponse,
    BillSavingsSummary,
    BuildabilityResponse,
    FileLinks,
    FTMEconomics,
    LandCoverClass,
    LMPSummary,
    MonthlyBillDetail,
    ProductionResponse,
    SlopeDistributionBucket,
    SlopeSummary,
)
from src.config.schema import SiteConfig
from src.pipeline import SolarModelingPipeline
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def run_production(site_config: SiteConfig, output_dir: Path) -> dict:
    """Run the production pipeline for a single site config.

    Args:
        site_config: Validated site configuration from the adapter.
        output_dir: Directory for pipeline output artifacts.

    Returns:
        Raw results dict from SolarModelingPipeline.run_from_configs().
    """
    pipeline = SolarModelingPipeline(output_dir=output_dir)
    return pipeline.run_from_configs([site_config])


# ---------------------------------------------------------------------------
# Loss-data key mapping (PySAM → LossSummary)
#
# PySAM loss_data key                        → LossSummary field
# -------------------------------------------  --------------------------
# annual_poa_shading_loss_percent             → shading_pct
# annual_dc_wiring_loss_percent               → dc_wiring_pct
# annual_ac_wiring_loss_percent               → ac_wiring_pct
# annual_xfmr_loss_percent                    → transformer_pct
# annual_dc_mismatch_loss_percent             → mismatch_pct
# annual_dc_nameplate_loss_percent            → lid_pct
# annual_ac_perf_adj_loss_percent             → availability_pct
# annual_ac_inv_clip_loss_percent             → subhourly_clipping_pct
# annual_ac_inv_eff_loss_percent              → inverter_efficiency_pct
# ---------------------------------------------------------------------------

_LOSS_KEY_MAP: dict[str, str] = {
    "annual_poa_shading_loss_percent": "shading_pct",
    "annual_dc_wiring_loss_percent": "dc_wiring_pct",
    "annual_ac_wiring_loss_percent": "ac_wiring_pct",
    "annual_xfmr_loss_percent": "transformer_pct",
    "annual_dc_mismatch_loss_percent": "mismatch_pct",
    "annual_dc_nameplate_loss_percent": "lid_pct",
    "annual_ac_perf_adj_loss_percent": "availability_pct",
    "annual_ac_inv_clip_loss_percent": "subhourly_clipping_pct",
    "annual_ac_inv_eff_loss_percent": "inverter_efficiency_pct",
}


def _find_summary(results: dict, run_name: str) -> dict:
    """Find the matching summary in pipeline results by run_name.

    Args:
        results: Raw dict returned by SolarModelingPipeline.run().
        run_name: The run_name to locate in the summaries list.

    Returns:
        The matching summary dict.

    Raises:
        ValueError: If the run_name is not found in the results summaries.
    """
    for s in results["summaries"]:
        if s["run_name"] == run_name:
            return s
    raise ValueError(
        f"Run name '{run_name}' not found in pipeline results. "
        f"Available: {[s['run_name'] for s in results['summaries']]}"
    )


def _extract_common_fields(
    summary: dict, run_name: str
) -> tuple[ProductionSummary, LossSummary, list[float], FileLinks]:
    """Extract production, losses, monthly, and file links from a summary.

    Args:
        summary: A single site summary dict from pipeline results.
        run_name: The run identifier for file link construction.

    Returns:
        Tuple of (ProductionSummary, LossSummary, monthly_mwh, FileLinks).
    """
    # --- Production metrics ---
    annual_energy_mwh = summary["annual_energy_mwh"]
    ac_installed_mw = summary["ac_installed_mw"]

    loss_data = summary.get("loss_data") or {}
    capacity_factor_dc = float(loss_data.get("capacity_factor", 0.0))

    # Always use declared AC capacity, not PySAM's internal AC nameplate
    capacity_factor_ac = (annual_energy_mwh / (ac_installed_mw * 8760)) * 100

    production = ProductionSummary(
        annual_energy_mwh=annual_energy_mwh,
        capacity_factor_dc=capacity_factor_dc,
        capacity_factor_ac=capacity_factor_ac,
        specific_yield_kwh_per_kwp=summary["specific_yield"],
        performance_ratio=summary["performance_ratio"],
    )

    # --- Loss breakdown ---
    loss_fields: dict[str, float] = {}
    for pysam_key, summary_field in _LOSS_KEY_MAP.items():
        loss_fields[summary_field] = float(loss_data.get(pysam_key, 0.0))
    losses = LossSummary(**loss_fields)

    # --- Monthly production ---
    monthly_energy_kwh = loss_data.get("monthly_energy", [0.0] * 12)
    monthly_production_mwh = [v / 1000.0 for v in monthly_energy_kwh]

    # --- File links ---
    files = FileLinks(
        report_url=f"/analyses/{run_name}/report",
        timeseries_url=f"/analyses/{run_name}/timeseries",
    )

    return production, losses, monthly_production_mwh, files


def extract_production_response(
    results: dict, run_name: str, output_dir: Path
) -> ProductionResponse:
    """Extract a ProductionResponse from the raw pipeline results dict.

    Args:
        results: Raw dict returned by SolarModelingPipeline.run().
        run_name: The run_name to locate in the summaries list.
        output_dir: Base output directory (for file link construction).

    Returns:
        Fully populated ProductionResponse.

    Raises:
        ValueError: If the run_name is not found in the results summaries.
    """
    summary = _find_summary(results, run_name)
    production, losses, monthly_production_mwh, files = _extract_common_fields(
        summary, run_name
    )

    return ProductionResponse(
        run_id=run_name,
        status="success",
        site_name=summary["site_name"],
        production=production,
        losses=losses,
        monthly_production_mwh=monthly_production_mwh,
        files=files,
    )


def extract_bill_savings_response(
    results: dict, run_name: str, output_dir: Path
) -> BillSavingsResponse:
    """Extract a BillSavingsResponse from the raw pipeline results dict.

    Args:
        results: Raw dict returned by SolarModelingPipeline.run().
        run_name: The run_name to locate in the summaries list.
        output_dir: Base output directory (for file link construction).

    Returns:
        Fully populated BillSavingsResponse.

    Raises:
        ValueError: If the run_name is not found in the results summaries.
    """
    summary = _find_summary(results, run_name)
    production, losses, monthly_production_mwh, files = _extract_common_fields(
        summary, run_name
    )

    # --- Bill savings ---
    bs = summary.get("bill_savings", {})
    bill_savings = BillSavingsSummary(
        annual_bill_without_solar=bs.get("annual_bill_without_solar", 0.0),
        annual_bill_with_solar=bs.get("annual_bill_with_solar", 0.0),
        annual_savings=bs.get("annual_savings", 0.0),
        savings_percent=bs.get("savings_percent", 0.0),
        avoided_cost_per_kwh=bs.get("avoided_cost_per_kwh", 0.0),
        annual_demand_savings=bs.get("annual_demand_savings", 0.0),
        annual_export_kwh=bs.get("annual_export_kwh", 0.0),
        annual_export_credits=bs.get("annual_export_credits", 0.0),
        nem_true_up_credit=bs.get("nem_true_up_credit", 0.0),
    )

    # --- Monthly bill detail ---
    monthly_detail = [
        MonthlyBillDetail(
            month=m["month"],
            bill_without=m["bill_without"],
            bill_with=m["bill_with"],
            savings=m["savings"],
        )
        for m in bs.get("monthly_detail", [])
    ]

    return BillSavingsResponse(
        run_id=run_name,
        status="success",
        site_name=summary["site_name"],
        production=production,
        losses=losses,
        monthly_production_mwh=monthly_production_mwh,
        bill_savings=bill_savings,
        monthly_bill_detail=monthly_detail,
        files=files,
    )


def extract_bess_response(
    results: dict,
    run_name: str,
    output_dir: Path,
    is_ftm: bool,
    is_optimization: bool,
) -> BESSResponse:
    """Extract a BESSResponse from the raw pipeline results dict.

    Args:
        results: Raw dict returned by SolarModelingPipeline.run().
        run_name: The run_name to locate in the summaries list.
        output_dir: Base output directory (for file link construction).
        is_ftm: Whether this is an FTM dispatch run.
        is_optimization: Whether this is a sizing optimization run.

    Returns:
        Fully populated BESSResponse.

    Raises:
        ValueError: If the run_name is not found in the results summaries.
    """
    summary = _find_summary(results, run_name)
    production, losses, monthly_production_mwh, files = _extract_common_fields(
        summary, run_name
    )

    # --- Dispatch metrics ---
    dispatch_ran = "bess_dispatch" in summary
    bd = summary.get("bess_dispatch", {})

    if dispatch_ran:
        dispatch = BESSDispatchSummary(
            bess_status="success",
            bess_power_mw=bd.get("bess_power_mw", 0.0),
            bess_duration_hr=bd.get("bess_duration_hr", 0.0),
            bess_capacity_kwh=bd.get("bess_capacity_kwh", 0.0),
            bess_strategy=bd.get("bess_strategy", "global"),
            annual_cycles=bd.get("annual_cycles", 0.0),
            annual_throughput_kwh=bd.get("annual_throughput_kwh", 0.0),
            capacity_utilization_pct=bd.get("capacity_utilization_pct", 0.0),
            total_curtailed_kwh=bd.get("total_curtailed_kwh", 0.0),
            estimated_annual_degradation_pct=bd.get(
                "estimated_annual_degradation_pct", 0.0
            ),
            charging_source=bd.get("charging_source", "unknown"),
        )
    else:
        dispatch_error = summary.get("bess_dispatch_error")
        if dispatch_error:
            logger.warning(
                "BESS dispatch failed for %s: %s",
                run_name,
                dispatch_error,
            )
            bess_status = "error"
            bess_error = f"BESS dispatch failed: {dispatch_error}"
        else:
            logger.warning(
                "BESS dispatch data missing from summary for %s — "
                "returning skipped status",
                run_name,
            )
            bess_status = "skipped"
            bess_error = (
                "BESS dispatch was requested but could not run. "
                "Most likely cause: bill calculation was not configured "
                "(required for BTM mode)."
            )
        dispatch = BESSDispatchSummary(
            bess_status=bess_status,
            bess_error=bess_error,
            bess_power_mw=0.0,
            bess_duration_hr=0.0,
            bess_capacity_kwh=0.0,
            bess_strategy="global",
            annual_cycles=0.0,
            annual_throughput_kwh=0.0,
            capacity_utilization_pct=0.0,
            total_curtailed_kwh=0.0,
            estimated_annual_degradation_pct=0.0,
            charging_source="unknown",
        )

    # --- Bill comparison (BTM only) ---
    bill_comparison: BESSBillComparison | None = None
    if not is_ftm:
        bill_comparison = BESSBillComparison(
            solar_only_annual_bill=bd.get("solar_only_annual_bill", 0.0),
            solar_plus_bess_annual_bill=bd.get(
                "solar_plus_bess_annual_bill", 0.0
            ),
            bess_incremental_savings=bd.get("bess_incremental_savings", 0.0),
            bess_demand_savings=bd.get("bess_demand_savings", 0.0),
            bess_energy_savings=bd.get("bess_energy_savings", 0.0),
        )

    # --- Bill savings (BTM with bill calculation) ---
    bill_savings: BillSavingsSummary | None = None
    if not is_ftm:
        bs = summary.get("bill_savings")
        if bs is not None:
            bill_savings = BillSavingsSummary(
                annual_bill_without_solar=bs.get(
                    "annual_bill_without_solar", 0.0
                ),
                annual_bill_with_solar=bs.get("annual_bill_with_solar", 0.0),
                annual_savings=bs.get("annual_savings", 0.0),
                savings_percent=bs.get("savings_percent", 0.0),
                avoided_cost_per_kwh=bs.get("avoided_cost_per_kwh", 0.0),
                annual_demand_savings=bs.get("annual_demand_savings", 0.0),
                annual_export_kwh=bs.get("annual_export_kwh", 0.0),
                annual_export_credits=bs.get("annual_export_credits", 0.0),
                nem_true_up_credit=bs.get("nem_true_up_credit", 0.0),
            )

    # --- LMP summary (FTM only) ---
    lmp: LMPSummary | None = None
    if is_ftm:
        lmp_data = summary.get("lmp")
        if lmp_data is not None:
            lmp = LMPSummary(
                iso=lmp_data["iso"],
                zone=lmp_data["zone"],
                market=lmp_data["market"],
                year=lmp_data["year"],
                mean_lmp=lmp_data["mean_lmp"],
                min_lmp=lmp_data["min_lmp"],
                max_lmp=lmp_data["max_lmp"],
            )

    # --- FTM economics (FTM only) ---
    ftm_economics: FTMEconomics | None = None
    if is_ftm:
        fe = summary.get("ftm_economics")
        if fe is not None:
            ftm_economics = FTMEconomics(
                annual_solar_revenue=fe["annual_solar_revenue"],
                annual_bess_arbitrage_revenue=fe["annual_bess_arbitrage_revenue"],
                annual_ancillary_revenue=fe["annual_ancillary_revenue"],
                annual_gross_revenue=fe["annual_gross_revenue"],
                total_project_npv=fe["total_project_npv"],
                bess_npv=fe["bess_npv"],
                solar_npv=fe["solar_npv"],
                system_lcoe_per_kwh=fe["system_lcoe_per_kwh"],
                total_installed_cost=fe["total_installed_cost"],
            )

    # --- Sizing summary (optimization only) ---
    sizing: BESSSizingSummary | None = None
    if is_optimization:
        sz = summary.get("bess_sizing")
        if sz is not None:
            sizing = BESSSizingSummary(
                optimal_power_mw=sz["optimal_power_mw"],
                optimal_duration_hr=sz["optimal_duration_hr"],
                optimal_capacity_kwh=sz["optimal_capacity_kwh"],
                bess_npv=sz["bess_npv"],
                total_project_npv=sz["total_project_npv"],
                system_lcoe_per_kwh=sz.get("system_lcoe_per_kwh"),
                total_installed_cost=sz["total_installed_cost"],
                combos_evaluated=sz["combos_evaluated"],
            )

    return BESSResponse(
        run_id=run_name,
        status="success",
        site_name=summary["site_name"],
        production=production,
        losses=losses,
        monthly_production_mwh=monthly_production_mwh,
        dispatch=dispatch,
        bill_comparison=bill_comparison,
        lmp=lmp,
        ftm_economics=ftm_economics,
        sizing=sizing,
        bill_savings=bill_savings,
        files=files,
    )


# ---------------------------------------------------------------------------
# Buildability runner (standalone — no CSV shim needed)
# ---------------------------------------------------------------------------


def run_buildability(
    site_config: SiteConfig,
    include_maps: bool = False,
    output_dir: Path | None = None,
) -> "BuildabilityResult":
    """Run buildability analysis directly from a SiteConfig.

    Unlike production/bill/BESS runs, buildability is a standalone module
    that doesn't go through the CSV pipeline. This constructs a
    BuildabilityConfig and calls the analyzer directly.

    Always generates PNG visualizations when output_dir is provided,
    since the buildability PDF report embeds them.

    Args:
        site_config: SiteConfig with buildable_land_assessment=True.
        include_maps: Retained for API schema compatibility.
        output_dir: Directory for output artifacts. When provided,
            PNG map figures are always generated for PDF embedding.

    Returns:
        BuildabilityResult from the analyzer.
    """
    from src.buildability.analyzer import run_buildability_analysis
    from src.buildability.models import BuildabilityConfig

    config = BuildabilityConfig(
        latitude=site_config.latitude,
        longitude=site_config.longitude,
        kmz_file_path=site_config.kmz_file_path,
        radius_km=site_config.analysis_radius_km,
    )

    if output_dir is not None:
        return run_buildability_analysis(config, output_dir=output_dir)
    return run_buildability_analysis(config)


def extract_buildability_response(
    result: object, run_name: str
) -> BuildabilityResponse:
    """Extract a BuildabilityResponse from a BuildabilityResult.

    Maps the BuildabilityResult fields to the API response schema.
    Omits raw raster metadata (nlcd_metadata, dem_metadata) and
    polygon WKT for API brevity.

    Args:
        result: BuildabilityResult from the analyzer.
        run_name: The run identifier.

    Returns:
        Fully populated BuildabilityResponse.
    """
    # Map class_breakdown list[dict] → list[LandCoverClass]
    class_breakdown = [
        LandCoverClass(
            code=cls["code"],
            name=cls["name"],
            category=cls["category"],
            pixels=cls["pixels"],
            acres=cls["acres"],
            percent=cls["percent"],
        )
        for cls in result.class_breakdown
    ]

    # Map slope_stats dict → SlopeSummary
    ss = result.slope_stats
    slope = SlopeSummary(
        min_degrees=ss["min_degrees"],
        max_degrees=ss["max_degrees"],
        mean_degrees=ss["mean_degrees"],
        median_degrees=ss["median_degrees"],
        distribution=[
            SlopeDistributionBucket(
                range=b["range"],
                pixels=b["pixels"],
                percent=b["percent"],
            )
            for b in ss["distribution"]
        ],
        pct_below_tracker_limit=ss["pct_below_tracker_limit"],
        pct_below_fixed_tilt_limit=ss["pct_below_fixed_tilt_limit"],
    )

    return BuildabilityResponse(
        run_id=run_name,
        status="success",
        latitude=result.latitude,
        longitude=result.longitude,
        radius_km=result.radius_km,
        kmz_file_path=result.kmz_file_path,
        total_area_acres=result.total_area_acres,
        buildable_acres=result.buildable_acres,
        soft_exclusion_acres=result.soft_exclusion_acres,
        hard_exclusion_acres=result.hard_exclusion_acres,
        unclassified_acres=result.unclassified_acres,
        buildable_pct=result.buildable_pct,
        soft_exclusion_pct=result.soft_exclusion_pct,
        hard_exclusion_pct=result.hard_exclusion_pct,
        class_breakdown=class_breakdown,
        slope=slope,
        figure_paths=result.figure_paths,
    )
