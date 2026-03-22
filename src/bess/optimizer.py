"""Monthly BESS dispatch optimizer using linear programming (PuLP/CBC)."""

import pulp
from pydantic import BaseModel

from src.bess.models import BESSConfig


class MonthSolveResult(BaseModel):
    """Result of a single-month BESS dispatch optimization.

    Args:
        charge_kw: Per-hour charging power in kW.
        discharge_kw: Per-hour discharging power in kW.
        soc_kwh: Per-hour ending state of charge in kWh.
        net_load_kw: Per-hour net load after solar and battery in kW.
        peak_demand_by_period: Peak kW per TOU demand period.
        peak_demand_flat: Peak kW for non-coincident (flat) demand.
        solver_status: PuLP solver status string.
        final_soc_kwh: Ending SOC of last hour (for month-to-month carryover).
    """

    charge_kw: list[float]
    discharge_kw: list[float]
    soc_kwh: list[float]
    net_load_kw: list[float]
    curtailed_kw: list[float]
    peak_demand_by_period: dict[str, float]
    peak_demand_flat: float
    solver_status: str
    final_soc_kwh: float


def solve_month(
    load_kw: list[float],
    production_kw: list[float],
    config: BESSConfig,
    energy_rate_per_hour: list[float],
    demand_periods: dict[str, list[int]],
    demand_rates: dict[str, float],
    flat_demand_rate: float,
    initial_soc_kwh: float,
) -> MonthSolveResult:
    """Solve the optimal BESS dispatch for a single month via LP.

    Minimizes total electricity cost (energy charges + demand charges)
    minus battery degradation cost, subject to physical battery constraints.

    Args:
        load_kw: Hourly load in kW for this month.
        production_kw: Hourly solar production in kW for this month.
        config: BESS configuration with physical parameters.
        energy_rate_per_hour: $/kWh energy rate for each hour.
        demand_periods: Mapping of period name to list of hour indices
            within this month for TOU demand charge tracking.
        demand_rates: $/kW rate for each TOU demand period.
        flat_demand_rate: $/kW rate for non-coincident demand charge.
        initial_soc_kwh: Starting SOC for this month in kWh.

    Returns:
        MonthSolveResult with optimal dispatch schedule and solver status.
    """
    n_hours = len(load_kw)

    prob = pulp.LpProblem("bess_month", pulp.LpMinimize)

    # --- Decision variables ---
    charge = [
        pulp.LpVariable(f"charge_{t}", 0, config.bess_power_kw)
        for t in range(n_hours)
    ]
    discharge = [
        pulp.LpVariable(f"discharge_{t}", 0, config.bess_power_kw)
        for t in range(n_hours)
    ]
    soc = [
        pulp.LpVariable(f"soc_{t}", config.min_soc_kwh, config.max_soc_kwh)
        for t in range(n_hours)
    ]
    net_demand = [
        pulp.LpVariable(f"net_demand_{t}", 0)
        for t in range(n_hours)
    ]
    curtailed = [
        pulp.LpVariable(f"curtailed_{t}", 0)
        for t in range(n_hours)
    ]

    # Peak demand tracking variables
    peak_period: dict[str, pulp.LpVariable] = {}
    for p in demand_periods:
        peak_period[p] = pulp.LpVariable(f"peak_{p}", 0)
    peak_flat = pulp.LpVariable("peak_flat", 0)

    # --- Objective ---
    # Strategy coefficients
    energy_coeff = 0.0 if config.strategy == "peak_shaving" else 1.0
    demand_coeff = 0.0 if config.strategy == "tou_arbitrage" else 1.0

    obj = pulp.lpSum(
        net_demand[t] * energy_rate_per_hour[t] * energy_coeff
        for t in range(n_hours)
    )
    obj += pulp.lpSum(
        peak_period[p] * demand_rates[p] * demand_coeff
        for p in demand_periods
    )
    obj += peak_flat * flat_demand_rate * demand_coeff
    obj += config.degradation_cost_per_kwh * pulp.lpSum(
        charge[t] + discharge[t] for t in range(n_hours)
    )
    # Tiebreaker: minimize curtailment when it doesn't affect cost
    obj += 1e-6 * pulp.lpSum(curtailed[t] for t in range(n_hours))

    prob += obj

    # --- Constraints ---
    for t in range(n_hours):
        # Energy balance: net_demand = load - production + curtailed + charge - discharge
        # curtailed absorbs excess solar (reduces effective production)
        # net_demand >= 0 (variable bound), curtailed >= 0 (variable bound)
        prob += (
            net_demand[t]
            == load_kw[t] - production_kw[t] + curtailed[t] + charge[t] - discharge[t],
            f"energy_balance_{t}",
        )

        # SOC continuity
        prev_soc = initial_soc_kwh if t == 0 else soc[t - 1]
        prob += (
            soc[t]
            == prev_soc
            + charge[t] * config.charge_efficiency
            - discharge[t] / config.discharge_efficiency,
            f"soc_continuity_{t}",
        )

    # Peak demand tracking: TOU periods
    for p, hours in demand_periods.items():
        for t in hours:
            prob += (
                peak_period[p] >= net_demand[t],
                f"peak_{p}_hour_{t}",
            )

    # Peak demand tracking: flat (non-coincident)
    for t in range(n_hours):
        prob += (
            peak_flat >= net_demand[t],
            f"peak_flat_hour_{t}",
        )

    # --- Solve ---
    solver = pulp.PULP_CBC_CMD(msg=0)
    prob.solve(solver)

    status = pulp.LpStatus[prob.status]

    # --- Extract results ---
    charge_out = [charge[t].varValue or 0.0 for t in range(n_hours)]
    discharge_out = [discharge[t].varValue or 0.0 for t in range(n_hours)]
    soc_out = [soc[t].varValue or 0.0 for t in range(n_hours)]
    net_load_out = [net_demand[t].varValue or 0.0 for t in range(n_hours)]
    curtailed_out = [curtailed[t].varValue or 0.0 for t in range(n_hours)]

    peak_by_period = {
        p: peak_period[p].varValue or 0.0 for p in demand_periods
    }
    peak_flat_val = peak_flat.varValue or 0.0
    final_soc = soc_out[-1] if soc_out else initial_soc_kwh

    return MonthSolveResult(
        charge_kw=charge_out,
        discharge_kw=discharge_out,
        soc_kwh=soc_out,
        net_load_kw=net_load_out,
        curtailed_kw=curtailed_out,
        peak_demand_by_period=peak_by_period,
        peak_demand_flat=peak_flat_val,
        solver_status=status,
        final_soc_kwh=final_soc,
    )
