"""Tests for BESS NPV calculation and project economics."""

import json
from pathlib import Path

import pytest

from src.bess.models import ProjectEconomics
from src.bess.sizing_optimizer import compute_bess_npv, compute_project_economics
from src.config.schema import SiteConfig
from src.rates.models import BillResult, MonthlyBillDetail

# Output directory for manual inspection
_OUTPUT_DIR = Path("outputs/test_results/m14c_npv")
_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helper: build minimal BillResult with a target annual_total
# ---------------------------------------------------------------------------


def _make_bill_result(annual_total: float) -> BillResult:
    """Build a minimal BillResult with the given annual total.

    Spreads the total evenly across 12 months with zero demand/fixed charges.
    """
    monthly_charge = annual_total / 12
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


# ---------------------------------------------------------------------------
# Helper: build minimal SiteConfig for economics tests
# ---------------------------------------------------------------------------


def _base_site_kwargs() -> dict:
    """Return minimal valid SiteConfig kwargs."""
    return {
        "run_name": "NpvTest",
        "site_name": "NpvTestSite",
        "customer": "TestCustomer",
        "latitude": 33.45,
        "longitude": -112.07,
        "dc_size_mw": 10.0,
        "ac_installed_mw": 8.0,
        "ac_poi_mw": 8.0,
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


def _solar_bess_site(**overrides: object) -> SiteConfig:
    """Build a SiteConfig with solar+BESS optimization fields."""
    kw = _base_site_kwargs()
    kw.update({
        "bess_dispatch_required": True,
        "bess_optimization_required": True,
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
    })
    kw.update(overrides)
    return SiteConfig(**kw)


# ===========================================================================
# compute_bess_npv tests
# ===========================================================================


class TestComputeBessNpv:
    """Tests for the BESS NPV calculation."""

    def test_zero_savings_returns_negative_npv(self) -> None:
        """Zero savings → NPV = -(installed_cost + PV of opex stream).

        With $0 savings and $500/yr opex (50 kW * $10/kW/yr), the NPV
        should be -$50,000 minus the present value of the opex annuity.
        """
        npv = compute_bess_npv(
            bess_incremental_savings=0.0,
            bess_installed_cost=50000.0,
            bess_annual_degradation_pct=2.0,
            rate_escalation_pct=2.0,
            bess_opex_per_kw_year=10.0,
            bess_power_kw=50.0,
            discount_rate_pct=8.0,
            project_lifetime_years=10,
        )
        # PV of $500/yr opex over 10 years at 8%: $3,355.04
        assert npv == pytest.approx(-53355.0407, abs=1.0)

    def test_known_hand_calculated_case(self) -> None:
        """$10K/yr, $50K cost, 2% deg, 2% esc, 0 opex, 8% disc, 10yr.

        Hand calculation:
        - Escalation and degradation nearly cancel: 1.02 * 0.98 = 0.9996
        - Year 1 PV: 10000 / 1.08 = $9,259.26
        - Sum of PV(savings): $66,997.01
        - NPV = -50000 + 66997.01 = $16,997.01
        """
        npv = compute_bess_npv(
            bess_incremental_savings=10000.0,
            bess_installed_cost=50000.0,
            bess_annual_degradation_pct=2.0,
            rate_escalation_pct=2.0,
            bess_opex_per_kw_year=0.0,
            bess_power_kw=0.0,
            discount_rate_pct=8.0,
            project_lifetime_years=10,
        )
        assert npv == pytest.approx(16997.0093, abs=1.0)

        # Write intermediate output for inspection
        cash_flows = []
        for n in range(1, 11):
            savings = 10000 * (1.02) ** (n - 1) * (0.98) ** (n - 1)
            pv = savings / (1.08) ** n
            cash_flows.append({
                "year": n,
                "savings": round(savings, 4),
                "pv_savings": round(pv, 4),
            })
        output = {
            "test": "known_hand_calculated_case",
            "inputs": {
                "savings_yr1": 10000,
                "cost": 50000,
                "degradation_pct": 2.0,
                "escalation_pct": 2.0,
                "opex": 0,
                "discount_pct": 8.0,
                "lifetime": 10,
            },
            "cash_flows": cash_flows,
            "npv": round(npv, 4),
        }
        (_OUTPUT_DIR / "npv_hand_calc.json").write_text(
            json.dumps(output, indent=2)
        )

    def test_with_opex(self) -> None:
        """Same as hand-calculated case but with $500/yr opex.

        NPV should decrease by the PV of the $500/yr opex stream ($3,355.04).
        """
        npv_no_opex = compute_bess_npv(
            bess_incremental_savings=10000.0,
            bess_installed_cost=50000.0,
            bess_annual_degradation_pct=2.0,
            rate_escalation_pct=2.0,
            bess_opex_per_kw_year=0.0,
            bess_power_kw=50.0,
            discount_rate_pct=8.0,
            project_lifetime_years=10,
        )
        npv_with_opex = compute_bess_npv(
            bess_incremental_savings=10000.0,
            bess_installed_cost=50000.0,
            bess_annual_degradation_pct=2.0,
            rate_escalation_pct=2.0,
            bess_opex_per_kw_year=10.0,
            bess_power_kw=50.0,
            discount_rate_pct=8.0,
            project_lifetime_years=10,
        )
        # PV of opex stream: $3,355.04
        assert npv_with_opex == pytest.approx(13641.9686, abs=1.0)
        assert npv_no_opex - npv_with_opex == pytest.approx(3355.0407, abs=1.0)

    def test_zero_degradation_zero_escalation_matches_annuity(self) -> None:
        """With 0% degradation and 0% escalation, savings are constant.

        NPV should equal -cost + savings * annuity_factor(r, n).
        Annuity factor = (1 - (1+r)^-n) / r
        """
        npv = compute_bess_npv(
            bess_incremental_savings=10000.0,
            bess_installed_cost=50000.0,
            bess_annual_degradation_pct=0.0,
            rate_escalation_pct=0.0,
            bess_opex_per_kw_year=0.0,
            bess_power_kw=0.0,
            discount_rate_pct=8.0,
            project_lifetime_years=10,
        )
        annuity_factor = (1 - (1.08) ** (-10)) / 0.08
        expected = -50000 + 10000 * annuity_factor
        assert npv == pytest.approx(expected, abs=0.01)
        assert npv == pytest.approx(17100.8140, abs=1.0)

    def test_high_degradation_reduces_npv(self) -> None:
        """20%/yr degradation → battery loses value fast, NPV much lower."""
        npv = compute_bess_npv(
            bess_incremental_savings=10000.0,
            bess_installed_cost=50000.0,
            bess_annual_degradation_pct=20.0,
            rate_escalation_pct=2.0,
            bess_opex_per_kw_year=0.0,
            bess_power_kw=0.0,
            discount_rate_pct=8.0,
            project_lifetime_years=10,
        )
        assert npv == pytest.approx(-14417.6786, abs=1.0)
        # Confirm it's much lower than the 2% degradation case
        assert npv < 0

    def test_single_year_lifetime(self) -> None:
        """1-year lifetime → NPV = -cost + (savings - opex) / (1 + r)."""
        npv = compute_bess_npv(
            bess_incremental_savings=10000.0,
            bess_installed_cost=50000.0,
            bess_annual_degradation_pct=2.0,
            rate_escalation_pct=2.0,
            bess_opex_per_kw_year=0.0,
            bess_power_kw=0.0,
            discount_rate_pct=8.0,
            project_lifetime_years=1,
        )
        # Year 1: savings = 10000 * 1.02^0 * 0.98^0 = 10000
        # NPV = -50000 + 10000/1.08 = -40740.74
        expected = -50000 + 10000 / 1.08
        assert npv == pytest.approx(expected, abs=0.01)
        assert npv == pytest.approx(-40740.7407, abs=1.0)


# ===========================================================================
# compute_project_economics tests
# ===========================================================================


class TestComputeProjectEconomics:
    """Tests for the full project economics calculation."""

    def test_solar_plus_bess_basic(self) -> None:
        """Basic solar+BESS case: verify NPV additivity and LCOE.

        Site: 10 MW DC, 8 MW AC, solar cost $1200/kW DC + $200/kW AC.
        Solar cost = 10000*1200 + 8000*200 = $13,600,000.
        BESS: 5 MW / 4 hr, $275/kWh → $5,500,000 installed.
        Solar savings: $600K - $300K = $300K/yr.
        BESS savings: $50K/yr.
        """
        site = _solar_bess_site()
        bill_without = _make_bill_result(600000.0)
        solar_only = _make_bill_result(300000.0)

        # Pre-compute BESS NPV
        bess_installed_cost = 5000 * 4 * 275  # 5MW * 4hr * 275 $/kWh = $5.5M
        bess_npv = compute_bess_npv(
            bess_incremental_savings=50000.0,
            bess_installed_cost=bess_installed_cost,
            bess_annual_degradation_pct=3.0,
            rate_escalation_pct=2.0,
            bess_opex_per_kw_year=10.0,
            bess_power_kw=5000.0,
            discount_rate_pct=8.0,
            project_lifetime_years=25,
        )

        econ = compute_project_economics(
            bess_npv=bess_npv,
            bess_installed_cost=bess_installed_cost,
            bess_incremental_savings=50000.0,
            bess_power_mw=5.0,
            bess_duration_hr=4.0,
            bess_opex_per_kw_year=10.0,
            site_config=site,
            solar_only_bill=solar_only,
            bill_without_solar=bill_without,
            annual_production_kwh=20_000_000.0,
        )

        # Verify NPV additivity
        assert econ.total_project_npv == pytest.approx(
            econ.solar_npv + econ.bess_npv, abs=0.01
        )

        # LCOE should be positive
        assert econ.system_lcoe_per_kwh is not None
        assert econ.system_lcoe_per_kwh > 0

        # Verify cost breakdown
        assert econ.solar_cost == pytest.approx(
            10000 * 1200.0 + 8000 * 200.0, abs=0.01
        )
        assert econ.bess_cost == pytest.approx(bess_installed_cost, abs=0.01)
        assert econ.total_installed_cost == pytest.approx(
            econ.solar_cost + econ.bess_cost, abs=0.01
        )

        # Verify savings
        assert econ.total_annual_savings == pytest.approx(
            300000.0 + 50000.0, abs=0.01
        )
        assert econ.bess_incremental_savings == pytest.approx(50000.0, abs=0.01)

        # Verify production
        assert econ.annual_production_mwh == pytest.approx(20000.0, abs=0.01)

        # Verify optimal sizing
        assert econ.optimal_power_mw == 5.0
        assert econ.optimal_duration_hr == 4.0
        assert econ.optimal_capacity_kwh == pytest.approx(20000.0, abs=0.01)

        # Write output for inspection
        output = {
            "test": "solar_plus_bess_basic",
            "solar_cost": econ.solar_cost,
            "bess_cost": econ.bess_cost,
            "total_installed_cost": econ.total_installed_cost,
            "solar_npv": round(econ.solar_npv, 2),
            "bess_npv": round(econ.bess_npv, 2),
            "total_project_npv": round(econ.total_project_npv, 2),
            "system_lcoe_per_kwh": round(econ.system_lcoe_per_kwh, 6),
            "total_annual_savings": econ.total_annual_savings,
            "annual_production_mwh": econ.annual_production_mwh,
            "lifetime_generation_mwh": round(econ.lifetime_generation_mwh, 2),
        }
        (_OUTPUT_DIR / "economics_solar_bess.json").write_text(
            json.dumps(output, indent=2)
        )

    def test_grid_only_bess(self) -> None:
        """Grid-only BESS: solar_npv=0, solar_cost=0, LCOE=None."""
        site = _solar_bess_site(
            bess_grid_only_charging=True,
            bess_solar_only_charging=False,
            solar_cost_per_kw_dc=None,
            solar_cost_per_kw_ac=None,
            bess_power_min_mw=1.0,
            bess_power_max_mw=10.0,
        )
        bill_without = _make_bill_result(200000.0)
        solar_only = _make_bill_result(200000.0)  # No solar production

        bess_installed_cost = 3000 * 4 * 275  # 3MW * 4hr = $3.3M
        bess_npv = compute_bess_npv(
            bess_incremental_savings=30000.0,
            bess_installed_cost=bess_installed_cost,
            bess_annual_degradation_pct=4.0,
            rate_escalation_pct=2.0,
            bess_opex_per_kw_year=10.0,
            bess_power_kw=3000.0,
            discount_rate_pct=8.0,
            project_lifetime_years=25,
        )

        econ = compute_project_economics(
            bess_npv=bess_npv,
            bess_installed_cost=bess_installed_cost,
            bess_incremental_savings=30000.0,
            bess_power_mw=3.0,
            bess_duration_hr=4.0,
            bess_opex_per_kw_year=10.0,
            site_config=site,
            solar_only_bill=solar_only,
            bill_without_solar=bill_without,
            annual_production_kwh=0.0,
        )

        # Grid-only: no solar component
        assert econ.solar_npv == 0.0
        assert econ.solar_cost == 0.0
        assert econ.system_lcoe_per_kwh is None
        assert econ.total_project_npv == pytest.approx(bess_npv, abs=0.01)
        assert econ.total_installed_cost == pytest.approx(
            bess_installed_cost, abs=0.01
        )
        assert econ.lifetime_generation_mwh == 0.0

        # Write output for inspection
        output = {
            "test": "grid_only_bess",
            "solar_npv": econ.solar_npv,
            "bess_npv": round(econ.bess_npv, 2),
            "total_project_npv": round(econ.total_project_npv, 2),
            "system_lcoe_per_kwh": econ.system_lcoe_per_kwh,
            "total_installed_cost": econ.total_installed_cost,
        }
        (_OUTPUT_DIR / "economics_grid_only.json").write_text(
            json.dumps(output, indent=2)
        )

    def test_lifetime_generation_accounts_for_degradation(self) -> None:
        """Lifetime generation should be less than annual * lifetime.

        With 0.5% degradation over 25 years:
        lifetime = annual * sum((0.995)^(n-1) for n=1..25)
        Sum ≈ 23.56 (less than 25.0)
        """
        site = _solar_bess_site(degradation_percent=0.5, project_lifetime_years=25)
        bill_without = _make_bill_result(500000.0)
        solar_only = _make_bill_result(250000.0)

        econ = compute_project_economics(
            bess_npv=0.0,
            bess_installed_cost=0.0,
            bess_incremental_savings=0.0,
            bess_power_mw=0.0,
            bess_duration_hr=0.0,
            bess_opex_per_kw_year=0.0,
            site_config=site,
            solar_only_bill=solar_only,
            bill_without_solar=bill_without,
            annual_production_kwh=20_000_000.0,
        )

        # Undegraded lifetime: 20000 MWh * 25 = 500,000 MWh
        undegraded = 20_000_000.0 / 1000 * 25
        assert econ.lifetime_generation_mwh < undegraded
        assert econ.lifetime_generation_mwh > 0

        # Manual calculation
        deg_sum = sum((0.995) ** (n - 1) for n in range(1, 26))
        expected = 20_000_000.0 * deg_sum / 1000
        assert econ.lifetime_generation_mwh == pytest.approx(expected, rel=1e-6)

    def test_total_installed_cost_is_sum(self) -> None:
        """total_installed_cost = solar_cost + bess_cost."""
        site = _solar_bess_site()
        bill_without = _make_bill_result(500000.0)
        solar_only = _make_bill_result(300000.0)

        bess_cost = 2000 * 3 * 275  # 2MW * 3hr * 275
        econ = compute_project_economics(
            bess_npv=1000.0,
            bess_installed_cost=bess_cost,
            bess_incremental_savings=20000.0,
            bess_power_mw=2.0,
            bess_duration_hr=3.0,
            bess_opex_per_kw_year=0.0,
            site_config=site,
            solar_only_bill=solar_only,
            bill_without_solar=bill_without,
            annual_production_kwh=15_000_000.0,
        )

        assert econ.total_installed_cost == pytest.approx(
            econ.solar_cost + econ.bess_cost, abs=0.01
        )

    def test_all_fields_populated(self) -> None:
        """All ProjectEconomics fields should be populated (non-None except LCOE)."""
        site = _solar_bess_site()
        bill_without = _make_bill_result(500000.0)
        solar_only = _make_bill_result(300000.0)

        econ = compute_project_economics(
            bess_npv=10000.0,
            bess_installed_cost=5_500_000.0,
            bess_incremental_savings=50000.0,
            bess_power_mw=5.0,
            bess_duration_hr=4.0,
            bess_opex_per_kw_year=10.0,
            site_config=site,
            solar_only_bill=solar_only,
            bill_without_solar=bill_without,
            annual_production_kwh=20_000_000.0,
        )

        assert isinstance(econ, ProjectEconomics)
        assert econ.total_project_npv is not None
        assert econ.solar_npv is not None
        assert econ.bess_npv is not None
        assert econ.system_lcoe_per_kwh is not None  # Non-grid-only
        assert econ.total_installed_cost > 0
        assert econ.solar_cost > 0
        assert econ.bess_cost > 0
        assert econ.total_annual_savings > 0
        assert econ.bess_incremental_savings > 0
        assert econ.annual_production_mwh > 0
        assert econ.lifetime_generation_mwh > 0
        assert econ.optimal_power_mw == 5.0
        assert econ.optimal_duration_hr == 4.0
        assert econ.optimal_capacity_kwh == pytest.approx(20000.0, abs=0.01)
        assert econ.combos_evaluated == 0  # Set externally by optimizer


# ===========================================================================
# Model import tests
# ===========================================================================


class TestModelImports:
    """Verify new model classes are importable and constructible."""

    def test_sweep_combo_result(self) -> None:
        """SweepComboResult should be importable and constructible."""
        from src.bess.models import SweepComboResult

        combo = SweepComboResult(
            power_mw=5.0,
            duration_hr=4.0,
            capacity_kwh=20000.0,
            installed_cost=5_500_000.0,
            bess_incremental_savings=50000.0,
            bess_npv=10000.0,
            annual_cycles=300.0,
            annual_degradation_pct=6.0,
            solver_status=["Optimal"] * 12,
        )
        assert combo.power_mw == 5.0
        assert combo.capacity_kwh == 20000.0

    def test_project_economics(self) -> None:
        """ProjectEconomics should be importable and constructible."""
        econ = ProjectEconomics(
            total_project_npv=100000.0,
            solar_npv=80000.0,
            bess_npv=20000.0,
            system_lcoe_per_kwh=0.05,
            total_installed_cost=15_000_000.0,
            solar_cost=12_000_000.0,
            bess_cost=3_000_000.0,
            total_annual_savings=350000.0,
            bess_incremental_savings=50000.0,
            annual_production_mwh=20000.0,
            lifetime_generation_mwh=470000.0,
            optimal_power_mw=5.0,
            optimal_duration_hr=4.0,
            optimal_capacity_kwh=20000.0,
            combos_evaluated=20,
        )
        assert econ.system_lcoe_per_kwh == 0.05

    def test_sizing_result(self) -> None:
        """SizingResult should be importable and constructible."""
        from src.bess.models import (
            BESSBillComparison,
            BESSConfig,
            BatteryMetrics,
            DispatchResult,
            HourlyDispatch,
            SizingResult,
            SweepComboResult,
        )

        combo = SweepComboResult(
            power_mw=5.0,
            duration_hr=4.0,
            capacity_kwh=20000.0,
            installed_cost=5_500_000.0,
            bess_incremental_savings=50000.0,
            bess_npv=10000.0,
            annual_cycles=300.0,
            annual_degradation_pct=6.0,
            solver_status=["Optimal"] * 12,
        )
        econ = ProjectEconomics(
            total_project_npv=100000.0,
            solar_npv=80000.0,
            bess_npv=20000.0,
            system_lcoe_per_kwh=0.05,
            total_installed_cost=15_000_000.0,
            solar_cost=12_000_000.0,
            bess_cost=3_000_000.0,
            total_annual_savings=350000.0,
            bess_incremental_savings=50000.0,
            annual_production_mwh=20000.0,
            lifetime_generation_mwh=470000.0,
            optimal_power_mw=5.0,
            optimal_duration_hr=4.0,
            optimal_capacity_kwh=20000.0,
            combos_evaluated=20,
        )
        config = BESSConfig(
            bess_power_kw=5000.0,
            bess_duration_hr=4.0,
            capacity_kwh=20000.0,
            usable_capacity_kwh=16000.0,
            min_soc_kwh=2000.0,
            max_soc_kwh=18000.0,
            charge_efficiency=0.938,
            discharge_efficiency=0.938,
            degradation_cost_per_kwh=0.0275,
            bess_cycles_warranty=5000,
            strategy="global",
        )
        metrics = BatteryMetrics(
            annual_throughput_kwh=4_800_000.0,
            annual_cycles=300.0,
            average_daily_cycles=0.82,
            capacity_utilization_pct=82.0,
            average_discharge_depth_pct=70.0,
            max_discharge_depth_pct=95.0,
            total_curtailed_kwh=0.0,
            hours_charging=2000,
            hours_discharging=2000,
            hours_idle=4760,
            estimated_annual_degradation_pct=6.0,
        )
        dispatch = DispatchResult(
            config=config,
            hourly_dispatch=[
                HourlyDispatch(
                    hour=h,
                    charge_kw=0.0,
                    discharge_kw=0.0,
                    soc_kwh=2000.0,
                    net_load_kw=100.0,
                    curtailed_kw=0.0,
                )
                for h in range(8760)
            ],
            monthly_solve_status=["Optimal"] * 12,
            metrics=metrics,
        )
        comparison = BESSBillComparison(
            solar_only_annual_bill=300000.0,
            solar_plus_bess_annual_bill=250000.0,
            bess_incremental_savings=50000.0,
            bess_demand_savings=30000.0,
            bess_energy_savings=20000.0,
        )

        result = SizingResult(
            winner=combo,
            economics=econ,
            all_combos=[combo],
            dispatch_result=dispatch,
            bill_comparison=comparison,
        )
        assert result.winner.power_mw == 5.0
        assert result.economics.total_project_npv == 100000.0
