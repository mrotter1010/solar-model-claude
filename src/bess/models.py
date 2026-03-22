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
    """

    hour: int = Field(ge=0, lt=8760)
    charge_kw: float = Field(ge=0)
    discharge_kw: float = Field(ge=0)
    soc_kwh: float = Field(ge=0)
    net_load_kw: float
    curtailed_kw: float = Field(ge=0)


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


class BESSBillComparison(BaseModel):
    """Bill comparison showing incremental BESS savings over solar-only.

    Args:
        solar_only_annual_bill: Annual bill with solar but no BESS.
        solar_plus_bess_annual_bill: Annual bill with solar + BESS.
        bess_incremental_savings: Total incremental savings from BESS.
        bess_demand_savings: Demand charge savings from BESS.
        bess_energy_savings: Energy charge savings from BESS.
    """

    solar_only_annual_bill: float
    solar_plus_bess_annual_bill: float
    bess_incremental_savings: float
    bess_demand_savings: float
    bess_energy_savings: float
