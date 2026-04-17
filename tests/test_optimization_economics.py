"""Tests for src/optimization/economics.py — LCOE and NPV calculations."""

import pytest

from src.optimization.economics import compute_lcoe, compute_solar_npv


# ---------------------------------------------------------------------------
# Reference case constants: 5 MW DC utility-scale solar
# ---------------------------------------------------------------------------
CAPEX_PER_KW = 1200.0  # $/kW
SYSTEM_KW = 5000.0  # 5 MW
CAPEX_TOTAL = CAPEX_PER_KW * SYSTEM_KW  # $6,000,000
ANNUAL_ENERGY_MWH = 9000.0  # ~CF 20.5%
ANNUAL_ENERGY_KWH = ANNUAL_ENERGY_MWH * 1000  # 9,000,000 kWh
OPEX_PER_KW_YR = 20.0  # $/kW-DC/yr
OPEX_PER_YEAR = OPEX_PER_KW_YR * SYSTEM_KW  # $100,000/yr
DISCOUNT_RATE = 0.07
LIFETIME = 25
DEGRADATION = 0.005
ITC_RATE = 0.30
ANNUAL_REVENUE = 500_000.0  # $/yr
ESCALATOR = 0.02


class TestLcoeBasicCalculation:
    """Verify LCOE against manual computation."""

    def test_reference_case_manual_verification(self) -> None:
        """5 MW system: $6M capex, 9 GWh/yr, $100k O&M, 7% discount, 30% ITC."""
        result = compute_lcoe(
            capex_total=CAPEX_TOTAL,
            annual_energy_kwh=ANNUAL_ENERGY_KWH,
            opex_per_year=OPEX_PER_YEAR,
            discount_rate=DISCOUNT_RATE,
            project_lifetime_years=LIFETIME,
            degradation_rate=DEGRADATION,
            itc_rate=ITC_RATE,
        )

        # Manual: net_capex = 6_000_000 * 0.70 = 4_200_000
        assert result["net_capex"] == 4_200_000.0

        # Compute expected LCOE manually
        net_capex = 4_200_000.0
        disc_energy = 0.0
        disc_opex = 0.0
        lifetime_energy = 0.0
        for t in range(1, 26):
            e_t = ANNUAL_ENERGY_KWH * (1 - DEGRADATION) ** t
            df = (1 + DISCOUNT_RATE) ** t
            disc_energy += e_t / df
            disc_opex += OPEX_PER_YEAR / df
            lifetime_energy += e_t

        expected_lcoe_kwh = (net_capex + disc_opex) / disc_energy
        expected_lcoe_mwh = expected_lcoe_kwh * 1000

        assert result["lcoe_per_kwh"] == pytest.approx(expected_lcoe_kwh, rel=1e-4)
        assert result["lcoe_per_mwh"] == pytest.approx(expected_lcoe_mwh, rel=1e-4)
        assert result["total_discounted_energy_kwh"] == pytest.approx(
            disc_energy, abs=1
        )
        assert result["total_discounted_opex"] == pytest.approx(disc_opex, rel=1e-4)
        assert result["lifetime_energy_kwh"] == pytest.approx(lifetime_energy, abs=1)

    def test_lcoe_in_reasonable_range(self) -> None:
        """Utility-scale solar with 30% ITC: expect roughly $30-60/MWh."""
        result = compute_lcoe(
            capex_total=CAPEX_TOTAL,
            annual_energy_kwh=ANNUAL_ENERGY_KWH,
            opex_per_year=OPEX_PER_YEAR,
            discount_rate=DISCOUNT_RATE,
            project_lifetime_years=LIFETIME,
            degradation_rate=DEGRADATION,
            itc_rate=ITC_RATE,
        )
        assert 30.0 <= result["lcoe_per_mwh"] <= 60.0, (
            f"LCOE={result['lcoe_per_mwh']} $/MWh outside 30-60 range"
        )


class TestLcoeItcSensitivity:
    """Verify ITC impact on LCOE."""

    def test_zero_itc_increases_lcoe(self) -> None:
        """No ITC → higher LCOE than 30% ITC."""
        with_itc = compute_lcoe(
            capex_total=CAPEX_TOTAL,
            annual_energy_kwh=ANNUAL_ENERGY_KWH,
            opex_per_year=OPEX_PER_YEAR,
            discount_rate=DISCOUNT_RATE,
            project_lifetime_years=LIFETIME,
            degradation_rate=DEGRADATION,
            itc_rate=0.30,
        )
        no_itc = compute_lcoe(
            capex_total=CAPEX_TOTAL,
            annual_energy_kwh=ANNUAL_ENERGY_KWH,
            opex_per_year=OPEX_PER_YEAR,
            discount_rate=DISCOUNT_RATE,
            project_lifetime_years=LIFETIME,
            degradation_rate=DEGRADATION,
            itc_rate=0.0,
        )
        assert no_itc["lcoe_per_mwh"] > with_itc["lcoe_per_mwh"]
        assert no_itc["net_capex"] == CAPEX_TOTAL  # Full capex, no credit


class TestLcoeDegradationSensitivity:
    """Higher degradation → higher LCOE (less energy over lifetime)."""

    def test_higher_degradation_increases_lcoe(self) -> None:
        low_deg = compute_lcoe(
            capex_total=CAPEX_TOTAL,
            annual_energy_kwh=ANNUAL_ENERGY_KWH,
            opex_per_year=OPEX_PER_YEAR,
            discount_rate=DISCOUNT_RATE,
            project_lifetime_years=LIFETIME,
            degradation_rate=0.003,
            itc_rate=ITC_RATE,
        )
        high_deg = compute_lcoe(
            capex_total=CAPEX_TOTAL,
            annual_energy_kwh=ANNUAL_ENERGY_KWH,
            opex_per_year=OPEX_PER_YEAR,
            discount_rate=DISCOUNT_RATE,
            project_lifetime_years=LIFETIME,
            degradation_rate=0.01,
            itc_rate=ITC_RATE,
        )
        assert high_deg["lcoe_per_mwh"] > low_deg["lcoe_per_mwh"]


class TestNpvBasicCalculation:
    """Verify NPV against expected behavior."""

    def test_reference_case_positive_npv(self) -> None:
        """5 MW system, $500k/yr revenue, 2% escalator, 30% ITC → positive NPV."""
        result = compute_solar_npv(
            capex_total=CAPEX_TOTAL,
            annual_revenue=ANNUAL_REVENUE,
            opex_per_year=OPEX_PER_YEAR,
            discount_rate=DISCOUNT_RATE,
            project_lifetime_years=LIFETIME,
            degradation_rate=DEGRADATION,
            energy_cost_escalator=ESCALATOR,
            itc_rate=ITC_RATE,
        )
        # Net capex = 6M * 0.70 = $4.2M
        assert result["net_capex"] == 4_200_000.0
        # With $500k/yr revenue growing 2%/yr minus 0.5% degradation,
        # and $100k/yr opex, NPV should be positive
        assert result["npv"] > 0
        assert len(result["annual_cashflows"]) == LIFETIME

    def test_npv_manual_verification(self) -> None:
        """Verify NPV computation matches hand calculation."""
        result = compute_solar_npv(
            capex_total=CAPEX_TOTAL,
            annual_revenue=ANNUAL_REVENUE,
            opex_per_year=OPEX_PER_YEAR,
            discount_rate=DISCOUNT_RATE,
            project_lifetime_years=LIFETIME,
            degradation_rate=DEGRADATION,
            energy_cost_escalator=ESCALATOR,
            itc_rate=ITC_RATE,
        )

        # Manual NPV
        net_capex = CAPEX_TOTAL * (1 - ITC_RATE)
        expected_npv = -net_capex
        for t in range(1, LIFETIME + 1):
            rev_t = ANNUAL_REVENUE * (1 - DEGRADATION) ** t * (1 + ESCALATOR) ** t
            cf_t = rev_t - OPEX_PER_YEAR
            expected_npv += cf_t / (1 + DISCOUNT_RATE) ** t

        assert result["npv"] == pytest.approx(expected_npv, rel=1e-4)


class TestNpvEscalatorSensitivity:
    """Zero escalator → lower NPV than 2% escalator."""

    def test_zero_escalator_lowers_npv(self) -> None:
        with_esc = compute_solar_npv(
            capex_total=CAPEX_TOTAL,
            annual_revenue=ANNUAL_REVENUE,
            opex_per_year=OPEX_PER_YEAR,
            discount_rate=DISCOUNT_RATE,
            project_lifetime_years=LIFETIME,
            degradation_rate=DEGRADATION,
            energy_cost_escalator=0.02,
            itc_rate=ITC_RATE,
        )
        no_esc = compute_solar_npv(
            capex_total=CAPEX_TOTAL,
            annual_revenue=ANNUAL_REVENUE,
            opex_per_year=OPEX_PER_YEAR,
            discount_rate=DISCOUNT_RATE,
            project_lifetime_years=LIFETIME,
            degradation_rate=DEGRADATION,
            energy_cost_escalator=0.0,
            itc_rate=ITC_RATE,
        )
        assert with_esc["npv"] > no_esc["npv"]


class TestPayback:
    """Verify simple payback falls in a reasonable range."""

    def test_payback_in_reasonable_range(self) -> None:
        """Typical solar with ITC: expect payback in 8-15 years."""
        result = compute_solar_npv(
            capex_total=CAPEX_TOTAL,
            annual_revenue=ANNUAL_REVENUE,
            opex_per_year=OPEX_PER_YEAR,
            discount_rate=DISCOUNT_RATE,
            project_lifetime_years=LIFETIME,
            degradation_rate=DEGRADATION,
            energy_cost_escalator=ESCALATOR,
            itc_rate=ITC_RATE,
        )
        assert result["simple_payback_years"] is not None
        assert 5.0 <= result["simple_payback_years"] <= 20.0, (
            f"Payback={result['simple_payback_years']} outside 5-20 range"
        )


class TestIrr:
    """Verify IRR calculation."""

    def test_irr_reasonable_for_reference_case(self) -> None:
        """Typical solar with ITC: expect IRR in 5-20% range."""
        result = compute_solar_npv(
            capex_total=CAPEX_TOTAL,
            annual_revenue=ANNUAL_REVENUE,
            opex_per_year=OPEX_PER_YEAR,
            discount_rate=DISCOUNT_RATE,
            project_lifetime_years=LIFETIME,
            degradation_rate=DEGRADATION,
            energy_cost_escalator=ESCALATOR,
            itc_rate=ITC_RATE,
        )
        assert result["irr"] is not None
        assert 0.05 <= result["irr"] <= 0.20, (
            f"IRR={result['irr']:.4f} outside 5-20% range"
        )

    def test_irr_none_when_never_profitable(self) -> None:
        """When revenue is much less than opex, IRR should be None or negative."""
        result = compute_solar_npv(
            capex_total=CAPEX_TOTAL,
            annual_revenue=50_000.0,  # Way below $100k opex
            opex_per_year=OPEX_PER_YEAR,
            discount_rate=DISCOUNT_RATE,
            project_lifetime_years=LIFETIME,
            degradation_rate=DEGRADATION,
            energy_cost_escalator=ESCALATOR,
            itc_rate=ITC_RATE,
        )
        # Project never pays back — IRR either None or deeply negative
        if result["irr"] is not None:
            assert result["irr"] < 0


class TestInputValidation:
    """Verify ValueError for invalid inputs."""

    # -- LCOE validation --
    def test_lcoe_zero_capex(self) -> None:
        with pytest.raises(ValueError, match="capex_total must be > 0"):
            compute_lcoe(0, ANNUAL_ENERGY_KWH, OPEX_PER_YEAR, 0.07, 25, 0.005)

    def test_lcoe_zero_energy(self) -> None:
        with pytest.raises(ValueError, match="annual_energy_kwh must be > 0"):
            compute_lcoe(CAPEX_TOTAL, 0, OPEX_PER_YEAR, 0.07, 25, 0.005)

    def test_lcoe_percentage_discount_rate_rejected(self) -> None:
        """7.0 (percentage) should be rejected — must pass 0.07 (decimal)."""
        with pytest.raises(ValueError, match="discount_rate must be < 1.0"):
            compute_lcoe(CAPEX_TOTAL, ANNUAL_ENERGY_KWH, OPEX_PER_YEAR, 7.0, 25, 0.005)

    def test_lcoe_percentage_degradation_rejected(self) -> None:
        """Passing 5.0 (meaning 5%) should be rejected — must pass 0.05."""
        with pytest.raises(ValueError, match="degradation_rate must be < 1.0"):
            compute_lcoe(
                CAPEX_TOTAL, ANNUAL_ENERGY_KWH, OPEX_PER_YEAR, 0.07, 25, 5.0
            )

    def test_lcoe_negative_itc(self) -> None:
        with pytest.raises(ValueError, match="itc_rate must be >= 0"):
            compute_lcoe(
                CAPEX_TOTAL,
                ANNUAL_ENERGY_KWH,
                OPEX_PER_YEAR,
                0.07,
                25,
                0.005,
                itc_rate=-0.1,
            )

    # -- NPV validation --
    def test_npv_zero_revenue(self) -> None:
        with pytest.raises(ValueError, match="annual_revenue must be > 0"):
            compute_solar_npv(CAPEX_TOTAL, 0, OPEX_PER_YEAR, 0.07, 25, 0.005, 0.02)

    def test_npv_negative_escalator(self) -> None:
        with pytest.raises(ValueError, match="energy_cost_escalator must be >= 0"):
            compute_solar_npv(
                CAPEX_TOTAL, ANNUAL_REVENUE, OPEX_PER_YEAR, 0.07, 25, 0.005, -0.01
            )

    def test_npv_escalator_too_high(self) -> None:
        with pytest.raises(ValueError, match="energy_cost_escalator must be < 0.20"):
            compute_solar_npv(
                CAPEX_TOTAL, ANNUAL_REVENUE, OPEX_PER_YEAR, 0.07, 25, 0.005, 0.25
            )

    def test_npv_percentage_discount_rate_rejected(self) -> None:
        with pytest.raises(ValueError, match="discount_rate must be < 1.0"):
            compute_solar_npv(
                CAPEX_TOTAL, ANNUAL_REVENUE, OPEX_PER_YEAR, 7.0, 25, 0.005, 0.02
            )
