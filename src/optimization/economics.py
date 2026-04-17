"""Economic calculations for solar layout optimization.

Pure functions with no side effects. All rate inputs are decimals (e.g., 0.07
for 7%), not percentages.
"""


def _validate_positive(name: str, value: float) -> None:
    """Raise ValueError if value is not strictly positive."""
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got {value}")


def _validate_rate(name: str, value: float, *, allow_zero: bool = False) -> None:
    """Raise ValueError if rate is not a valid decimal fraction.

    Args:
        name: Parameter name for error messages.
        value: The rate to validate.
        allow_zero: If True, 0.0 is accepted.
    """
    if allow_zero:
        if value < 0:
            raise ValueError(f"{name} must be >= 0, got {value}")
    else:
        if value <= 0:
            raise ValueError(f"{name} must be > 0, got {value}")
    if value >= 1.0:
        raise ValueError(
            f"{name} must be < 1.0 (decimal, not percentage), got {value}"
        )


def compute_lcoe(
    capex_total: float,
    annual_energy_kwh: float,
    opex_per_year: float,
    discount_rate: float,
    project_lifetime_years: int,
    degradation_rate: float,
    itc_rate: float = 0.30,
) -> dict:
    """Compute levelized cost of energy (LCOE).

    Args:
        capex_total: Total capital cost ($).
        annual_energy_kwh: Year-1 annual energy production (kWh).
        opex_per_year: Annual O&M cost ($/yr).
        discount_rate: Discount rate as decimal (e.g., 0.07 for 7%).
        project_lifetime_years: Analysis period in years.
        degradation_rate: Annual degradation as decimal (e.g., 0.005 for 0.5%).
        itc_rate: Investment tax credit as decimal (e.g., 0.30 for 30%).

    Returns:
        Dict with LCOE metrics.

    Raises:
        ValueError: If any input is out of valid range.
    """
    _validate_positive("capex_total", capex_total)
    _validate_positive("annual_energy_kwh", annual_energy_kwh)
    _validate_positive("opex_per_year", opex_per_year)
    _validate_rate("discount_rate", discount_rate)
    _validate_positive("project_lifetime_years", project_lifetime_years)
    _validate_rate("degradation_rate", degradation_rate)
    _validate_rate("itc_rate", itc_rate, allow_zero=True)

    net_capex = capex_total * (1 - itc_rate)

    total_discounted_energy = 0.0
    total_discounted_opex = 0.0
    lifetime_energy = 0.0

    for t in range(1, project_lifetime_years + 1):
        energy_t = annual_energy_kwh * (1 - degradation_rate) ** t
        discount_factor = (1 + discount_rate) ** t

        total_discounted_energy += energy_t / discount_factor
        total_discounted_opex += opex_per_year / discount_factor
        lifetime_energy += energy_t

    lcoe_per_kwh = (net_capex + total_discounted_opex) / total_discounted_energy
    lcoe_per_mwh = lcoe_per_kwh * 1000

    return {
        "lcoe_per_kwh": round(lcoe_per_kwh, 6),
        "lcoe_per_mwh": round(lcoe_per_mwh, 2),
        "net_capex": round(net_capex, 2),
        "total_discounted_energy_kwh": round(total_discounted_energy),
        "total_discounted_opex": round(total_discounted_opex, 2),
        "lifetime_energy_kwh": round(lifetime_energy),
    }


def _compute_npv_at_rate(
    net_capex: float,
    cashflows: list[float],
    rate: float,
) -> float:
    """Compute NPV at a given discount rate for IRR search."""
    npv = -net_capex
    for t, cf in enumerate(cashflows, start=1):
        npv += cf / (1 + rate) ** t
    return npv


def _compute_irr(
    net_capex: float,
    cashflows: list[float],
    max_iterations: int = 1000,
    tolerance: float = 1e-8,
) -> float | None:
    """Compute internal rate of return via bisection search.

    Args:
        net_capex: Net capital expenditure (positive number).
        cashflows: List of annual undiscounted net cashflows.
        max_iterations: Maximum bisection iterations.
        tolerance: Convergence threshold for NPV.

    Returns:
        IRR as decimal, or None if no solution found in [-0.5, 5.0].
    """
    lo, hi = -0.5, 5.0

    npv_lo = _compute_npv_at_rate(net_capex, cashflows, lo)
    npv_hi = _compute_npv_at_rate(net_capex, cashflows, hi)

    # Need a sign change for bisection to work
    if npv_lo * npv_hi > 0:
        return None

    for _ in range(max_iterations):
        mid = (lo + hi) / 2
        npv_mid = _compute_npv_at_rate(net_capex, cashflows, mid)

        if abs(npv_mid) < tolerance:
            return mid

        if npv_lo * npv_mid < 0:
            hi = mid
            npv_hi = npv_mid
        else:
            lo = mid
            npv_lo = npv_mid

    return (lo + hi) / 2


def compute_solar_npv(
    capex_total: float,
    annual_revenue: float,
    opex_per_year: float,
    discount_rate: float,
    project_lifetime_years: int,
    degradation_rate: float,
    energy_cost_escalator: float,
    itc_rate: float = 0.30,
) -> dict:
    """Compute net present value of a solar investment.

    Args:
        capex_total: Total capital cost ($).
        annual_revenue: Year-1 annual revenue or bill savings ($/yr).
        opex_per_year: Annual O&M cost ($/yr).
        discount_rate: Discount rate as decimal (e.g., 0.07 for 7%).
        project_lifetime_years: Analysis period in years.
        degradation_rate: Annual degradation as decimal (e.g., 0.005).
            Revenue degrades with production.
        energy_cost_escalator: Annual energy cost escalation as decimal
            (e.g., 0.02 for 2%/yr). Must be < 0.20.
        itc_rate: Investment tax credit as decimal (e.g., 0.30 for 30%).

    Returns:
        Dict with NPV metrics including cashflows, payback, and IRR.

    Raises:
        ValueError: If any input is out of valid range.
    """
    _validate_positive("capex_total", capex_total)
    _validate_positive("annual_revenue", annual_revenue)
    _validate_positive("opex_per_year", opex_per_year)
    _validate_rate("discount_rate", discount_rate)
    _validate_positive("project_lifetime_years", project_lifetime_years)
    _validate_rate("degradation_rate", degradation_rate)
    _validate_rate("itc_rate", itc_rate, allow_zero=True)

    if energy_cost_escalator < 0:
        raise ValueError(
            f"energy_cost_escalator must be >= 0, got {energy_cost_escalator}"
        )
    if energy_cost_escalator >= 0.20:
        raise ValueError(
            f"energy_cost_escalator must be < 0.20, got {energy_cost_escalator}"
        )

    net_capex = capex_total * (1 - itc_rate)

    annual_cashflows: list[float] = []
    npv = -net_capex

    for t in range(1, project_lifetime_years + 1):
        revenue_t = (
            annual_revenue
            * (1 - degradation_rate) ** t
            * (1 + energy_cost_escalator) ** t
        )
        net_cf_t = revenue_t - opex_per_year
        annual_cashflows.append(round(net_cf_t, 2))
        npv += net_cf_t / (1 + discount_rate) ** t

    # Simple payback: year when cumulative undiscounted net cashflow exceeds net_capex
    simple_payback: float | None = None
    cumulative = 0.0
    for t, cf in enumerate(annual_cashflows, start=1):
        cumulative += cf
        if cumulative >= net_capex:
            # Interpolate within the year
            overshoot = cumulative - net_capex
            fraction = overshoot / cf if cf > 0 else 0
            simple_payback = round(t - fraction, 1)
            break

    irr = _compute_irr(net_capex, annual_cashflows)
    if irr is not None:
        irr = round(irr, 6)

    return {
        "npv": round(npv, 2),
        "net_capex": round(net_capex, 2),
        "annual_cashflows": annual_cashflows,
        "simple_payback_years": simple_payback,
        "irr": irr,
    }
