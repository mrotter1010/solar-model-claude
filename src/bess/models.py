"""Pydantic models for BESS dispatch engine."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from src.config.schema import SiteConfig


class BESSConfig(BaseModel):
    """Derived BESS configuration with computed physical parameters.

    All values are derived from the SiteConfig BESS fields. Use the
    ``from_site_config`` class method to construct.

    Args:
        bess_power_kw: Rated power in kW (bess_power_mw * 1000).
        bess_duration_hr: Storage duration in hours.
        capacity_kwh: Nameplate energy capacity (power_kw * duration_hr).
        usable_capacity_kwh: Usable energy between min and max SOC.
        min_soc_kwh: Minimum state of charge in kWh.
        max_soc_kwh: Maximum state of charge in kWh.
        charge_efficiency: One-way charge efficiency (sqrt of RTE).
        discharge_efficiency: One-way discharge efficiency (sqrt of RTE).
        degradation_cost_per_kwh: Amortized cost per kWh throughput.
        strategy: Dispatch strategy name.
    """

    bess_power_kw: float = Field(gt=0)
    bess_duration_hr: float = Field(gt=0)
    capacity_kwh: float = Field(gt=0)
    usable_capacity_kwh: float = Field(gt=0)
    min_soc_kwh: float = Field(ge=0)
    max_soc_kwh: float = Field(gt=0)
    charge_efficiency: float = Field(gt=0, le=1)
    discharge_efficiency: float = Field(gt=0, le=1)
    degradation_cost_per_kwh: float = Field(gt=0)
    bess_cycles_warranty: int = Field(default=5000, gt=0)
    strategy: str

    @classmethod
    def from_site_config(cls, site_config: SiteConfig) -> BESSConfig:
        """Construct BESSConfig from a validated SiteConfig.

        Args:
            site_config: Site configuration with BESS fields populated.

        Returns:
            BESSConfig with all derived values computed.

        Raises:
            ValueError: If required BESS fields are missing.
        """
        from src.config.schema import SiteConfig as _SC  # noqa: F811

        if site_config.bess_power_mw is None:
            raise ValueError("bess_power_mw is required")
        if site_config.bess_duration_hr is None:
            raise ValueError("bess_duration_hr is required")

        bess_power_kw = site_config.bess_power_mw * 1000
        capacity_kwh = bess_power_kw * site_config.bess_duration_hr
        rte_fraction = site_config.bess_rte_percent / 100
        efficiency = math.sqrt(rte_fraction)
        total_installed_cost = site_config.bess_installed_cost_per_kwh * capacity_kwh

        return cls(
            bess_power_kw=bess_power_kw,
            bess_duration_hr=site_config.bess_duration_hr,
            capacity_kwh=capacity_kwh,
            usable_capacity_kwh=(
                capacity_kwh
                * (site_config.bess_max_soc_percent - site_config.bess_min_soc_percent)
                / 100
            ),
            min_soc_kwh=capacity_kwh * site_config.bess_min_soc_percent / 100,
            max_soc_kwh=capacity_kwh * site_config.bess_max_soc_percent / 100,
            charge_efficiency=efficiency,
            discharge_efficiency=efficiency,
            degradation_cost_per_kwh=(
                total_installed_cost
                / (site_config.bess_cycles_warranty * capacity_kwh * 2)
            ),
            bess_cycles_warranty=site_config.bess_cycles_warranty,
            strategy=site_config.bess_strategy,
        )


class HourlyDispatch(BaseModel):
    """Single-hour BESS dispatch state.

    Args:
        hour: Hour index (0-8759).
        charge_kw: Charging power in kW (positive = charging).
        discharge_kw: Discharging power in kW (positive = discharging).
        soc_kwh: State of charge at end of hour in kWh.
        net_load_kw: Net load after BESS dispatch in kW.
        curtailed_kw: Curtailed excess solar in kW.
        export_kw: Grid export in kW (positive = exporting). Zero when NEM inactive.
    """

    hour: int = Field(ge=0, lt=8760)
    charge_kw: float = Field(ge=0)
    discharge_kw: float = Field(ge=0)
    soc_kwh: float = Field(ge=0)
    net_load_kw: float
    curtailed_kw: float = Field(ge=0)
    export_kw: float = Field(default=0.0, ge=0)
    solar_export_kw: float = Field(default=0.0, ge=0)
    grid_charge_kw: float = Field(default=0.0, ge=0)


class BatteryMetrics(BaseModel):
    """Computed annual BESS performance metrics.

    Args:
        annual_throughput_kwh: Total discharge energy over the year.
        annual_cycles: Equivalent full cycles (throughput / usable capacity).
        average_daily_cycles: annual_cycles / 365.
        capacity_utilization_pct: Fraction of max daily cycling used (%).
        average_discharge_depth_pct: Average per-event depth as % of usable capacity.
        max_discharge_depth_pct: Max single-event depth as % of usable capacity.
        total_curtailed_kwh: Total curtailed solar energy.
        hours_charging: Hours where charge_kw > 0.1.
        hours_discharging: Hours where discharge_kw > 0.1.
        hours_idle: 8760 - charging - discharging.
        estimated_annual_degradation_pct: Linear degradation estimate (%).
        total_export_kwh: Total grid export energy over the year.
        total_export_hours: Number of hours with grid export > 0.
        charging_source: Battery charging source ("any", "solar_only", "grid_only").
    """

    annual_throughput_kwh: float = Field(ge=0)
    annual_cycles: float = Field(ge=0)
    average_daily_cycles: float = Field(ge=0)
    capacity_utilization_pct: float = Field(ge=0)
    average_discharge_depth_pct: float = Field(ge=0)
    max_discharge_depth_pct: float = Field(ge=0)
    total_curtailed_kwh: float = Field(ge=0)
    hours_charging: int = Field(ge=0)
    hours_discharging: int = Field(ge=0)
    hours_idle: int = Field(ge=0)
    estimated_annual_degradation_pct: float = Field(ge=0)
    total_export_kwh: float = Field(default=0.0, ge=0)
    total_export_hours: int = Field(default=0, ge=0)
    charging_source: str = "any"


class DispatchResult(BaseModel):
    """Complete BESS dispatch result for a full year.

    Args:
        config: BESS configuration used for the dispatch.
        hourly_dispatch: 8760 hourly dispatch states.
        monthly_solve_status: Solver status string per month (12 entries).
        metrics: Computed battery performance metrics (populated by metrics module).
        heatmap_data: 12x24 average battery power matrix (populated by metrics module).
    """

    config: BESSConfig
    hourly_dispatch: list[HourlyDispatch]
    monthly_solve_status: list[str]
    metrics: BatteryMetrics | None = None
    heatmap_data: list[list[float]] | None = None
    ftm_revenue: float | None = None


class BESSBillComparison(BaseModel):
    """Bill comparison showing incremental BESS savings over solar-only.

    Args:
        solar_only_annual_bill: Annual bill with solar but no BESS.
        solar_plus_bess_annual_bill: Annual bill with solar + BESS.
        bess_incremental_savings: Total incremental savings from BESS.
        bess_demand_savings: Demand charge savings from BESS.
        bess_energy_savings: Energy charge savings from BESS.
        solar_only_export_kwh: Annual export kWh in solar-only scenario.
        solar_only_export_credits: Annual export credits ($) in solar-only scenario.
        solar_plus_bess_export_kwh: Annual export kWh in solar+BESS scenario.
        solar_plus_bess_export_credits: Annual export credits ($) in solar+BESS scenario.
    """

    solar_only_annual_bill: float
    solar_plus_bess_annual_bill: float
    bess_incremental_savings: float
    bess_demand_savings: float
    bess_energy_savings: float
    solar_only_export_kwh: float = 0.0
    solar_only_export_credits: float = 0.0
    solar_plus_bess_export_kwh: float = 0.0
    solar_plus_bess_export_credits: float = 0.0


class SweepComboResult(BaseModel):
    """Result for a single BESS power/duration combo in the sizing sweep.

    Args:
        power_mw: BESS rated power in MW.
        duration_hr: BESS storage duration in hours.
        capacity_kwh: Nameplate energy capacity (power_kw * duration_hr).
        installed_cost: Total BESS installed cost ($).
        bess_incremental_savings: Year 1 incremental savings from BESS ($/yr).
        bess_npv: Net present value of the BESS investment ($).
        annual_cycles: Equivalent full cycles per year.
        annual_degradation_pct: Estimated annual capacity degradation (%).
        solver_status: Monthly LP solver status strings (12 entries).
    """

    power_mw: float
    duration_hr: float
    capacity_kwh: float
    installed_cost: float
    bess_incremental_savings: float
    bess_npv: float
    annual_cycles: float
    annual_degradation_pct: float
    solver_status: list[str]


class ProjectEconomics(BaseModel):
    """Full project economics for the optimal solar+BESS configuration.

    Args:
        total_project_npv: Combined solar + BESS NPV ($).
        solar_npv: Solar-only NPV ($). Zero for grid-only BESS.
        bess_npv: BESS-only NPV ($).
        system_lcoe_per_kwh: Levelized cost of energy ($/kWh). None for grid-only.
        total_installed_cost: Combined solar + BESS installed cost ($).
        solar_cost: Solar installed cost ($). Zero for grid-only BESS.
        bess_cost: BESS installed cost ($).
        total_annual_savings: Year 1 combined savings ($/yr).
        bess_incremental_savings: Year 1 BESS-only savings ($/yr).
        annual_production_mwh: Annual solar production (MWh).
        lifetime_generation_mwh: Degradation-adjusted lifetime generation (MWh).
        optimal_power_mw: Selected BESS power (MW).
        optimal_duration_hr: Selected BESS duration (hr).
        optimal_capacity_kwh: Selected BESS capacity (kWh).
        combos_evaluated: Number of power/duration combos evaluated.
    """

    total_project_npv: float
    solar_npv: float | None
    bess_npv: float
    system_lcoe_per_kwh: float | None
    total_installed_cost: float
    solar_cost: float
    bess_cost: float
    total_annual_savings: float
    bess_incremental_savings: float
    annual_production_mwh: float
    lifetime_generation_mwh: float
    optimal_power_mw: float
    optimal_duration_hr: float
    optimal_capacity_kwh: float
    combos_evaluated: int

    # FTM-specific fields (optional, None for BTM)
    solar_revenue: float | None = None
    bess_arbitrage_revenue: float | None = None
    ancillary_revenue: float | None = None
    gross_revenue: float | None = None


class SizingResult(BaseModel):
    """Complete BESS sizing optimization result.

    Args:
        winner: Best combo by NPV from the sweep.
        economics: Full project economics for the winning combo.
        all_combos: All evaluated power/duration combos with results.
        dispatch_result: Full dispatch result for the winning combo.
        bill_comparison: Bill comparison for the winning combo.
    """

    winner: SweepComboResult
    economics: ProjectEconomics
    all_combos: list[SweepComboResult]
    dispatch_result: DispatchResult
    bill_comparison: BESSBillComparison | None = None
