"""Tests for FTM economics functions in sizing_optimizer."""

import json
from pathlib import Path

import pytest

from src.bess.models import (
    BatteryMetrics,
    BESSConfig,
    DispatchResult,
    HourlyDispatch,
)
from src.bess.sizing_optimizer import (
    compute_bess_npv,
    compute_ftm_bess_npv,
    compute_ftm_project_economics,
    compute_ftm_solar_revenue,
    compute_project_economics,
)
from src.config.schema import SiteConfig
from src.rates.models import BillResult, MonthlyBillDetail

# Output directory for manual inspection
_OUTPUT_DIR = Path("outputs/test_results/m14e_ftm_economics")
_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _base_site_kwargs() -> dict:
    """Return minimal valid SiteConfig kwargs."""
    return {
        "run_name": "FTMEconTest",
        "site_name": "FTMTestSite",
        "customer": "TestCustomer",
        "latitude": 33.45,
        "longitude": -112.07,
        "dc_size_mw": 6.25,
        "ac_installed_mw": 5.0,
        "ac_poi_mw": 5.0,
        "racking": "tracker",
        "tilt": 55.0,
        "azimuth": 180.0,
        "module_orientation": "portrait",
        "number_of_modules": 1,
        "ground_clearance_height_m": 1.5,
        "panel_model": "CSI Solar Co. Ltd. CS3U-355P",
        "bifacial": False,
        "inverter_model": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
        "gcr": 0.35,
        "shading_percent": 0.0,
        "dc_wiring_loss_percent": 2.0,
        "ac_wiring_loss_percent": 1.0,
        "transformer_losses_percent": 1.0,
        "degradation_percent": 0.5,
        "availability_percent": 2.5,
        "module_mismatch_percent": 1.0,
        "lid_percent": 1.5,
    }


def _ftm_site_config(**overrides: object) -> SiteConfig:
    """Build a SiteConfig for FTM economics tests.

    Uses FTM dispatch mode with bill_calculation enabled (required by
    bess_dispatch_required validation).
    """
    kw = _base_site_kwargs()
    kw.update({
        "dispatch_mode": "ftm",
        "bess_dispatch_required": True,
        "bess_power_mw": 2.0,
        "bess_duration_hr": 4.0,
        "bill_calculation": True,
        "utility_name": "Test Utility",
        "tariff_name": "Test Rate",
        "load_type": "MediumOffice",
        "annual_consumption_kwh": 500000.0,
        "solar_cost_per_kw_dc": 1200.0,
        "solar_cost_per_kw_ac": 200.0,
        "solar_opex_per_kw_dc_year": 15.0,
        "bess_opex_per_kw_year": 10.0,
        "discount_rate_pct": 8.0,
        "project_lifetime_years": 25,
        "rate_escalation_pct": 2.0,
        "degradation_percent": 0.5,
        "bess_installed_cost_per_kwh": 275.0,
        "ancillary_revenue_per_kw_year": 20.0,
    })
    kw.update(overrides)
    return SiteConfig(**kw)


def _make_ftm_dispatch_result(
    power_kw: float = 2000.0,
    duration_hr: float = 4.0,
    solar_export_kw_values: list[float] | None = None,
) -> DispatchResult:
    """Build a minimal FTM DispatchResult with solar_export_kw populated."""
    capacity_kwh = power_kw * duration_hr
    config = BESSConfig(
        bess_power_kw=power_kw,
        bess_duration_hr=duration_hr,
        capacity_kwh=capacity_kwh,
        usable_capacity_kwh=capacity_kwh * 0.8,
        min_soc_kwh=capacity_kwh * 0.1,
        max_soc_kwh=capacity_kwh * 0.9,
        charge_efficiency=0.938,
        discharge_efficiency=0.938,
        degradation_cost_per_kwh=0.0275,
        bess_cycles_warranty=5000,
        strategy="global",
    )
    metrics = BatteryMetrics(
        annual_throughput_kwh=capacity_kwh * 300,
        annual_cycles=300.0,
        average_daily_cycles=0.82,
        capacity_utilization_pct=82.0,
        average_discharge_depth_pct=70.0,
        max_discharge_depth_pct=95.0,
        total_curtailed_kwh=0.0,
        hours_charging=2000,
        hours_discharging=2000,
        hours_idle=4760,
        estimated_annual_degradation_pct=3.0,
    )

    if solar_export_kw_values is None:
        solar_export_kw_values = [0.0] * 8760

    hourly = [
        HourlyDispatch(
            hour=h,
            charge_kw=0.0,
            discharge_kw=0.0,
            soc_kwh=capacity_kwh * 0.1,
            net_load_kw=0.0,
            curtailed_kw=0.0,
            solar_export_kw=solar_export_kw_values[h],
        )
        for h in range(8760)
    ]
    return DispatchResult(
        config=config,
        hourly_dispatch=hourly,
        monthly_solve_status=["Optimal"] * 12,
        metrics=metrics,
        ftm_revenue=50000.0,
    )


# ===========================================================================
# Tests: compute_ftm_bess_npv
# ===========================================================================


class TestComputeFtmBessNpv:
    """Tests for compute_ftm_bess_npv()."""

    def test_basic_npv(self) -> None:
        """Known inputs, verify NPV by hand calculation.

        Setup: $100k revenue, $500k cost, 3% degradation, 2% escalation,
        $10/kW O&M, 2000 kW, $20/kW ancillary, 8% discount, 3-year life.

        Year 1: revenue = 100000 * (1.02)^0 * (0.97)^0 = 100000
                 net_opex = 10*2000 - 20*2000 = 20000 - 40000 = -20000
                 cash_flow = 100000 - (-20000) = 120000
                 PV = 120000 / 1.08 = 111111.11

        Year 2: revenue = 100000 * 1.02 * 0.97 = 98940.0
                 net_opex = -20000
                 cash_flow = 98940 + 20000 = 118940
                 PV = 118940 / 1.08^2 = 101956.79

        Year 3: revenue = 100000 * 1.02^2 * 0.97^2 = 97893.61
                 net_opex = -20000
                 cash_flow = 97893.61 + 20000 = 117893.61
                 PV = 117893.61 / 1.08^3 = 93604.05

        NPV = -500000 + 111111.11 + 101956.79 + 93604.05 = -193328.05
        """
        npv = compute_ftm_bess_npv(
            bess_arbitrage_revenue=100000.0,
            bess_installed_cost=500000.0,
            bess_annual_degradation_pct=3.0,
            revenue_escalation_pct=2.0,
            bess_opex_per_kw_year=10.0,
            bess_power_kw=2000.0,
            ancillary_revenue_per_kw_year=20.0,
            discount_rate_pct=8.0,
            project_lifetime_years=3,
        )

        # Hand-computed NPV
        expected = -500000.0 + 111111.11 + 101956.79 + 93604.05
        assert npv == pytest.approx(expected, rel=1e-4)

        # Write output
        output = {
            "test": "basic_npv",
            "npv": round(npv, 2),
            "expected": round(expected, 2),
        }
        (_OUTPUT_DIR / "ftm_npv_basic.json").write_text(json.dumps(output, indent=2))

    def test_with_ancillary_increases_npv(self) -> None:
        """Ancillary revenue reduces effective opex, increasing NPV."""
        npv_no_ancillary = compute_ftm_bess_npv(
            bess_arbitrage_revenue=80000.0,
            bess_installed_cost=400000.0,
            bess_annual_degradation_pct=2.0,
            revenue_escalation_pct=2.0,
            bess_opex_per_kw_year=15.0,
            bess_power_kw=2000.0,
            ancillary_revenue_per_kw_year=0.0,
            discount_rate_pct=8.0,
            project_lifetime_years=25,
        )

        npv_with_ancillary = compute_ftm_bess_npv(
            bess_arbitrage_revenue=80000.0,
            bess_installed_cost=400000.0,
            bess_annual_degradation_pct=2.0,
            revenue_escalation_pct=2.0,
            bess_opex_per_kw_year=15.0,
            bess_power_kw=2000.0,
            ancillary_revenue_per_kw_year=20.0,
            discount_rate_pct=8.0,
            project_lifetime_years=25,
        )

        # Ancillary revenue should increase NPV
        assert npv_with_ancillary > npv_no_ancillary

        # The difference should be the PV of ancillary stream (flat $40k/yr)
        ancillary_annual = 20.0 * 2000.0  # $40,000/yr
        expected_diff = sum(
            ancillary_annual / (1.08 ** n) for n in range(1, 26)
        )
        actual_diff = npv_with_ancillary - npv_no_ancillary
        assert actual_diff == pytest.approx(expected_diff, rel=1e-6)

        output = {
            "test": "ancillary_increases_npv",
            "npv_no_ancillary": round(npv_no_ancillary, 2),
            "npv_with_ancillary": round(npv_with_ancillary, 2),
            "ancillary_pv": round(expected_diff, 2),
        }
        (_OUTPUT_DIR / "ftm_npv_ancillary.json").write_text(json.dumps(output, indent=2))

    def test_zero_revenue_npv(self) -> None:
        """Zero arbitrage revenue: NPV = -installed_cost + PV(-net_opex).

        With no ancillary, net_opex is positive, so cash flows are negative.
        NPV should be less than -installed_cost.
        """
        npv = compute_ftm_bess_npv(
            bess_arbitrage_revenue=0.0,
            bess_installed_cost=500000.0,
            bess_annual_degradation_pct=3.0,
            revenue_escalation_pct=2.0,
            bess_opex_per_kw_year=10.0,
            bess_power_kw=2000.0,
            ancillary_revenue_per_kw_year=0.0,
            discount_rate_pct=8.0,
            project_lifetime_years=25,
        )

        # With zero revenue and zero ancillary, each year is -opex
        bess_opex = 10.0 * 2000.0  # $20,000/yr
        expected = -500000.0 + sum(
            -bess_opex / (1.08 ** n) for n in range(1, 26)
        )
        assert npv == pytest.approx(expected, rel=1e-6)
        assert npv < -500000.0  # Worse than just losing the capital

    def test_matches_btm_structure(self) -> None:
        """Same revenue as BTM savings, ancillary=0 → same NPV as compute_bess_npv.

        Verifies structural equivalence between FTM and BTM NPV formulas
        when ancillary revenue is zero.
        """
        common_kwargs = {
            "bess_installed_cost": 400000.0,
            "bess_annual_degradation_pct": 2.5,
            "bess_opex_per_kw_year": 12.0,
            "bess_power_kw": 1500.0,
            "discount_rate_pct": 7.0,
            "project_lifetime_years": 25,
        }
        revenue_savings = 60000.0

        btm_npv = compute_bess_npv(
            bess_incremental_savings=revenue_savings,
            rate_escalation_pct=2.0,
            **common_kwargs,
        )

        ftm_npv = compute_ftm_bess_npv(
            bess_arbitrage_revenue=revenue_savings,
            revenue_escalation_pct=2.0,
            ancillary_revenue_per_kw_year=0.0,
            **common_kwargs,
        )

        assert ftm_npv == pytest.approx(btm_npv, rel=1e-10)

        output = {
            "test": "matches_btm_structure",
            "btm_npv": round(btm_npv, 2),
            "ftm_npv": round(ftm_npv, 2),
            "difference": round(ftm_npv - btm_npv, 10),
        }
        (_OUTPUT_DIR / "ftm_npv_btm_match.json").write_text(json.dumps(output, indent=2))


# ===========================================================================
# Tests: compute_ftm_project_economics
# ===========================================================================


class TestComputeFtmProjectEconomics:
    """Tests for compute_ftm_project_economics()."""

    def test_basic_economics(self) -> None:
        """Verify all ProjectEconomics fields are populated for FTM."""
        site = _ftm_site_config()

        bess_npv = compute_ftm_bess_npv(
            bess_arbitrage_revenue=80000.0,
            bess_installed_cost=2200000.0,
            bess_annual_degradation_pct=3.0,
            revenue_escalation_pct=2.0,
            bess_opex_per_kw_year=10.0,
            bess_power_kw=2000.0,
            ancillary_revenue_per_kw_year=20.0,
            discount_rate_pct=8.0,
            project_lifetime_years=25,
        )

        econ = compute_ftm_project_economics(
            bess_npv=bess_npv,
            bess_installed_cost=2200000.0,
            bess_arbitrage_revenue=80000.0,
            bess_power_mw=2.0,
            bess_duration_hr=4.0,
            bess_opex_per_kw_year=10.0,
            ancillary_revenue_per_kw_year=20.0,
            site_config=site,
            annual_solar_revenue=200000.0,
            annual_production_kwh=10_000_000.0,
        )

        # All core fields populated
        assert econ.total_project_npv is not None
        assert econ.solar_npv is not None
        assert econ.bess_npv == bess_npv
        assert econ.system_lcoe_per_kwh is not None
        assert econ.total_installed_cost > 0
        assert econ.solar_cost > 0
        assert econ.bess_cost == 2200000.0
        assert econ.annual_production_mwh == 10000.0
        assert econ.lifetime_generation_mwh > 0
        assert econ.optimal_power_mw == 2.0
        assert econ.optimal_duration_hr == 4.0
        assert econ.optimal_capacity_kwh == 8000.0

        # FTM-specific fields populated
        assert econ.solar_revenue == 200000.0
        assert econ.bess_arbitrage_revenue == 80000.0
        ancillary = 20.0 * 2000.0  # $40,000
        assert econ.ancillary_revenue == ancillary
        assert econ.gross_revenue == 200000.0 + 80000.0 + ancillary

        # total_annual_savings stores gross revenue for FTM
        assert econ.total_annual_savings == econ.gross_revenue

        # total_project_npv = solar_npv + bess_npv
        assert econ.total_project_npv == pytest.approx(
            econ.solar_npv + econ.bess_npv, rel=1e-10
        )

        output = {
            "test": "basic_economics",
            "total_project_npv": round(econ.total_project_npv, 2),
            "solar_npv": round(econ.solar_npv, 2),
            "bess_npv": round(econ.bess_npv, 2),
            "gross_revenue": round(econ.gross_revenue, 2),
            "lcoe": round(econ.system_lcoe_per_kwh, 4) if econ.system_lcoe_per_kwh else None,
            "total_installed_cost": round(econ.total_installed_cost, 2),
        }
        (_OUTPUT_DIR / "ftm_project_economics.json").write_text(
            json.dumps(output, indent=2)
        )

    def test_ftm_economics_no_solar_cost(self) -> None:
        """When solar cost inputs are missing, LCOE and solar_npv are None.

        Verifies that compute_ftm_project_economics sets system_lcoe_per_kwh
        and solar_npv to None when solar_cost_per_kw_dc and solar_cost_per_kw_ac
        are both None. total_project_npv should equal bess_npv.
        """
        site = _ftm_site_config(
            solar_cost_per_kw_dc=None,
            solar_cost_per_kw_ac=None,
        )

        bess_npv = compute_ftm_bess_npv(
            bess_arbitrage_revenue=80000.0,
            bess_installed_cost=2200000.0,
            bess_annual_degradation_pct=3.0,
            revenue_escalation_pct=2.0,
            bess_opex_per_kw_year=10.0,
            bess_power_kw=2000.0,
            ancillary_revenue_per_kw_year=20.0,
            discount_rate_pct=8.0,
            project_lifetime_years=25,
        )

        econ = compute_ftm_project_economics(
            bess_npv=bess_npv,
            bess_installed_cost=2200000.0,
            bess_arbitrage_revenue=80000.0,
            bess_power_mw=2.0,
            bess_duration_hr=4.0,
            bess_opex_per_kw_year=10.0,
            ancillary_revenue_per_kw_year=20.0,
            site_config=site,
            annual_solar_revenue=200000.0,
            annual_production_kwh=10_000_000.0,
        )

        # LCOE should be None — cannot compute without solar capex
        assert econ.system_lcoe_per_kwh is None

        # Solar NPV should be None — no solar cost data
        assert econ.solar_npv is None

        # Total project NPV should equal BESS NPV only
        assert econ.total_project_npv == pytest.approx(bess_npv, rel=1e-10)

        # Other fields still populated
        assert econ.bess_npv == bess_npv
        assert econ.bess_cost == 2200000.0
        assert econ.solar_cost == 0.0
        assert econ.annual_production_mwh == 10000.0
        assert econ.lifetime_generation_mwh > 0
        assert econ.solar_revenue == 200000.0
        assert econ.bess_arbitrage_revenue == 80000.0

        output = {
            "test": "ftm_economics_no_solar_cost",
            "system_lcoe_per_kwh": econ.system_lcoe_per_kwh,
            "solar_npv": econ.solar_npv,
            "total_project_npv": round(econ.total_project_npv, 2),
            "bess_npv": round(econ.bess_npv, 2),
        }
        (_OUTPUT_DIR / "ftm_no_solar_cost.json").write_text(
            json.dumps(output, indent=2)
        )

    def test_ftm_economics_zero_solar_cost(self) -> None:
        """Zero solar cost (explicit 0.0) treated same as None for LCOE."""
        site = _ftm_site_config(
            solar_cost_per_kw_dc=0.0,
            solar_cost_per_kw_ac=0.0,
        )

        bess_npv = -500000.0

        econ = compute_ftm_project_economics(
            bess_npv=bess_npv,
            bess_installed_cost=2200000.0,
            bess_arbitrage_revenue=50000.0,
            bess_power_mw=2.0,
            bess_duration_hr=4.0,
            bess_opex_per_kw_year=0.0,
            ancillary_revenue_per_kw_year=0.0,
            site_config=site,
            annual_solar_revenue=100000.0,
            annual_production_kwh=8_000_000.0,
        )

        assert econ.system_lcoe_per_kwh is None
        assert econ.solar_npv is None
        assert econ.total_project_npv == pytest.approx(bess_npv, rel=1e-10)

    def test_ftm_economics_with_solar_cost(self) -> None:
        """When solar cost is provided, LCOE and solar_npv are computed."""
        site = _ftm_site_config(
            solar_cost_per_kw_dc=1200.0,
            solar_cost_per_kw_ac=200.0,
        )

        bess_npv = -500000.0

        econ = compute_ftm_project_economics(
            bess_npv=bess_npv,
            bess_installed_cost=2200000.0,
            bess_arbitrage_revenue=50000.0,
            bess_power_mw=2.0,
            bess_duration_hr=4.0,
            bess_opex_per_kw_year=0.0,
            ancillary_revenue_per_kw_year=0.0,
            site_config=site,
            annual_solar_revenue=100000.0,
            annual_production_kwh=8_000_000.0,
        )

        # LCOE should be a real value, not None
        assert econ.system_lcoe_per_kwh is not None
        assert econ.system_lcoe_per_kwh > 0.01  # Should be in realistic range

        # Solar NPV should be a float
        assert econ.solar_npv is not None
        assert isinstance(econ.solar_npv, float)

        # Total NPV = solar + bess
        assert econ.total_project_npv == pytest.approx(
            econ.solar_npv + bess_npv, rel=1e-10
        )


# ===========================================================================
# Tests: compute_ftm_solar_revenue
# ===========================================================================


class TestComputeFtmSolarRevenue:
    """Tests for compute_ftm_solar_revenue()."""

    def test_known_solar_revenue(self) -> None:
        """Synthetic dispatch with known solar_export and LMP.

        Hours 0-11: solar_export=5000 kW, LMP=$50/MWh → 5000*50/1000 = $250/hr
        Hours 12-23: solar_export=3000 kW, LMP=$30/MWh → 3000*30/1000 = $90/hr
        Remaining hours: zero export.

        Expected: 12 * $250 + 12 * $90 = $3000 + $1080 = $4080
        """
        solar_export = [0.0] * 8760
        lmp_prices = [0.0] * 8760

        for h in range(12):
            solar_export[h] = 5000.0
            lmp_prices[h] = 50.0
        for h in range(12, 24):
            solar_export[h] = 3000.0
            lmp_prices[h] = 30.0

        dispatch = _make_ftm_dispatch_result(solar_export_kw_values=solar_export)
        revenue = compute_ftm_solar_revenue(dispatch, lmp_prices)

        expected = 12 * (5000.0 * 50.0 / 1000) + 12 * (3000.0 * 30.0 / 1000)
        assert revenue == pytest.approx(expected, rel=1e-10)
        assert revenue == pytest.approx(4080.0, rel=1e-10)

    def test_zero_export_zero_revenue(self) -> None:
        """No solar export → zero revenue."""
        dispatch = _make_ftm_dispatch_result(solar_export_kw_values=[0.0] * 8760)
        lmp_prices = [50.0] * 8760

        revenue = compute_ftm_solar_revenue(dispatch, lmp_prices)
        assert revenue == 0.0

    def test_zero_lmp_zero_revenue(self) -> None:
        """Zero LMP prices → zero revenue even with export."""
        solar_export = [1000.0] * 8760
        dispatch = _make_ftm_dispatch_result(solar_export_kw_values=solar_export)
        lmp_prices = [0.0] * 8760

        revenue = compute_ftm_solar_revenue(dispatch, lmp_prices)
        assert revenue == 0.0


# ===========================================================================
# Tests: BTM unchanged
# ===========================================================================


class TestBtmProjectEconomicsUnchanged:
    """Verify existing BTM compute_project_economics still works identically."""

    def test_btm_economics_no_ftm_fields(self) -> None:
        """BTM ProjectEconomics should have FTM fields as None."""
        kw = _base_site_kwargs()
        kw.update({
            "bess_dispatch_required": True,
            "bess_optimization_required": True,
            "bess_power_mw": 1.0,
            "bess_duration_hr": 4.0,
            "bill_calculation": True,
            "utility_name": "Test Utility",
            "tariff_name": "Test Rate",
            "load_type": "MediumOffice",
            "annual_consumption_kwh": 500000.0,
            "solar_cost_per_kw_dc": 1200.0,
            "solar_cost_per_kw_ac": 200.0,
            "solar_opex_per_kw_dc_year": 15.0,
            "bess_opex_per_kw_year": 10.0,
            "discount_rate_pct": 8.0,
            "project_lifetime_years": 25,
            "rate_escalation_pct": 2.0,
            "degradation_percent": 0.5,
            "bess_installed_cost_per_kwh": 275.0,
        })
        site = SiteConfig(**kw)

        # Build minimal BillResults
        def make_bill(total: float) -> BillResult:
            monthly_charge = total / 12
            return BillResult(
                monthly=[
                    MonthlyBillDetail(
                        month=m,
                        energy_charges=monthly_charge,
                        demand_charges=0.0,
                        flat_demand_charges=0.0,
                        fixed_charges=0.0,
                        production_kwh=0.0,
                        consumption_kwh=0.0,
                        peak_demand_kw=0.0,
                    )
                    for m in range(1, 13)
                ]
            )

        solar_only_bill = make_bill(200000.0)
        bill_without_solar = make_bill(300000.0)

        bess_npv = compute_bess_npv(
            bess_incremental_savings=25000.0,
            bess_installed_cost=1100000.0,
            bess_annual_degradation_pct=3.0,
            rate_escalation_pct=2.0,
            bess_opex_per_kw_year=10.0,
            bess_power_kw=1000.0,
            discount_rate_pct=8.0,
            project_lifetime_years=25,
        )

        econ = compute_project_economics(
            bess_npv=bess_npv,
            bess_installed_cost=1100000.0,
            bess_incremental_savings=25000.0,
            bess_power_mw=1.0,
            bess_duration_hr=4.0,
            bess_opex_per_kw_year=10.0,
            site_config=site,
            solar_only_bill=solar_only_bill,
            bill_without_solar=bill_without_solar,
            annual_production_kwh=10_000_000.0,
        )

        # Core BTM fields populated
        assert econ.total_project_npv is not None
        assert econ.solar_npv is not None
        assert econ.bess_npv == bess_npv
        assert econ.total_installed_cost > 0
        assert econ.bess_incremental_savings == 25000.0
        assert econ.total_annual_savings == (300000.0 - 200000.0) + 25000.0

        # FTM-specific fields should be None (not set by BTM path)
        assert econ.solar_revenue is None
        assert econ.bess_arbitrage_revenue is None
        assert econ.ancillary_revenue is None
        assert econ.gross_revenue is None
