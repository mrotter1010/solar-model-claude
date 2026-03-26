"""Tests for FTM BESS sizing optimization.

Verifies that run_ftm_sizing_optimization correctly sweeps
power/duration combos, selects the best NPV winner, and
produces complete SizingResult with FTM-specific economics.
"""

import math

import pytest

from src.bess.models import SizingResult
from src.bess.sizing_optimizer import (
    build_sweep_grid,
    run_ftm_sizing_optimization,
    run_sizing_optimization,
)
from src.config.schema import SiteConfig
from src.lmp.models import LMPData


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_site_config(**overrides: object) -> SiteConfig:
    """Build a minimal SiteConfig for FTM sizing tests."""
    defaults = {
        "run_name": "ftm_sizing_test",
        "site_name": "ftm_sizing_site",
        "customer": "ftm_customer",
        "latitude": 33.45,
        "longitude": -112.07,
        "dc_size_mw": 10.0,
        "ac_installed_mw": 8.0,
        "ac_poi_mw": 8.0,
        "racking": "tracker",
        "tilt": 60.0,
        "azimuth": 180.0,
        "module_orientation": "portrait",
        "number_of_modules": 1,
        "ground_clearance_height_m": 1.5,
        "panel_model": "test_panel",
        "bifacial": False,
        "inverter_model": "test_inverter",
        "gcr": 0.35,
        "shading_percent": 0.0,
        "dc_wiring_loss_percent": 2.0,
        "ac_wiring_loss_percent": 1.0,
        "transformer_losses_percent": 1.0,
        "degradation_percent": 0.5,
        "availability_percent": 2.0,
        "module_mismatch_percent": 2.0,
        "lid_percent": 1.5,
        # BESS sizing defaults
        "bess_power_mw": 1.0,
        "bess_duration_hr": 2.0,
        "bess_rte_percent": 88.0,
        "bess_min_soc_percent": 10.0,
        "bess_max_soc_percent": 90.0,
        "bess_strategy": "global",
        "bess_installed_cost_per_kwh": 275.0,
        "bess_cycles_warranty": 5000,
        "bess_opex_per_kw_year": 10.0,
        "ancillary_revenue_per_kw_year": 20.0,
        # Solar cost fields (required for optimization)
        "solar_cost_per_kw_dc": 1000.0,
        "solar_cost_per_kw_ac": 200.0,
        "solar_opex_per_kw_dc_year": 10.0,
        # Optimization fields
        "dispatch_mode": "ftm",
        "bess_dispatch_required": True,
        "bess_optimization_required": True,
        "bess_power_min_mw": 0.5,
        "bess_power_max_mw": 1.0,
        "bess_duration_min_hr": 2.0,
        "bess_duration_max_hr": 3.0,
        # Financial
        "rate_escalation_pct": 2.0,
        "discount_rate_pct": 8.0,
        "project_lifetime_years": 25,
    }
    defaults.update(overrides)
    return SiteConfig(**defaults)


def _make_lmp_data(prices: list[float]) -> LMPData:
    """Build LMPData with 8760 hourly prices."""
    return LMPData(
        iso="test_iso",
        zone="test_zone",
        market="DAY_AHEAD_HOURLY",
        year=2024,
        prices=prices,
    )


def _make_production_with_spread() -> tuple[list[float], list[float]]:
    """Create synthetic production and LMP with favorable spread.

    Returns:
        Tuple of (production_kwh, lmp_prices) both 8760 hourly.
        Production: 5000 kW daytime (6-17), 0 nighttime.
        LMP: $20/MWh off-peak (0-7, 21-23), $120/MWh on-peak (8-20).
    """
    production = []
    lmp = []
    for _ in range(365):
        # Production: daytime solar
        production.extend([0.0] * 6)
        production.extend([5000.0] * 12)
        production.extend([0.0] * 6)
        # LMP: low off-peak, high on-peak
        lmp.extend([20.0] * 8)
        lmp.extend([120.0] * 13)
        lmp.extend([20.0] * 3)
    return production, lmp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFTMSizingBasic:
    """Basic FTM sizing optimization functionality."""

    def test_ftm_sizing_basic(self) -> None:
        """Synthetic LMP + production: sweep runs and returns SizingResult."""
        site = _make_site_config()
        production, lmp_prices = _make_production_with_spread()
        lmp_data = _make_lmp_data(lmp_prices)

        result = run_ftm_sizing_optimization(
            site_config=site,
            production_kwh=production,
            lmp_data=lmp_data,
        )

        # Structural checks
        assert isinstance(result, SizingResult)
        assert result.winner is not None
        assert result.economics is not None
        assert result.dispatch_result is not None
        assert result.bill_comparison is None  # FTM has no bill comparison
        assert len(result.all_combos) > 0

        # Winner fields populated
        assert result.winner.power_mw > 0
        assert result.winner.duration_hr > 0
        assert result.winner.capacity_kwh > 0
        assert result.winner.installed_cost > 0

        # Dispatch result is for the winner
        assert len(result.dispatch_result.hourly_dispatch) == 8760
        assert result.dispatch_result.ftm_revenue is not None


class TestFTMSizingPositiveNPV:
    """Verify winner NPV is positive with favorable LMP spread."""

    def test_ftm_sizing_winner_has_positive_npv(self) -> None:
        """With large LMP spread ($20/$120), winner NPV should be positive."""
        site = _make_site_config()
        production, lmp_prices = _make_production_with_spread()
        lmp_data = _make_lmp_data(lmp_prices)

        result = run_ftm_sizing_optimization(
            site_config=site,
            production_kwh=production,
            lmp_data=lmp_data,
        )

        # Winner should have positive NPV (favorable spread)
        assert result.winner.bess_npv > 0, (
            f"Expected positive NPV with favorable spread, got {result.winner.bess_npv:.2f}"
        )

        # Winner should be the max NPV across all combos
        max_npv = max(c.bess_npv for c in result.all_combos)
        assert result.winner.bess_npv == max_npv


class TestFTMSizingPowerBounds:
    """Verify sweep respects power min/max bounds."""

    def test_ftm_sizing_respects_power_bounds(self) -> None:
        """Set explicit power min/max, verify all combos within bounds."""
        site = _make_site_config(
            bess_power_min_mw=0.8,
            bess_power_max_mw=1.2,
            bess_duration_min_hr=2.0,
            bess_duration_max_hr=3.0,
        )
        production, lmp_prices = _make_production_with_spread()
        lmp_data = _make_lmp_data(lmp_prices)

        result = run_ftm_sizing_optimization(
            site_config=site,
            production_kwh=production,
            lmp_data=lmp_data,
        )

        # All combos must be within specified bounds
        for combo in result.all_combos:
            assert combo.power_mw >= 0.8 - 0.01, (
                f"Power {combo.power_mw} below min 0.8"
            )
            assert combo.power_mw <= 1.2 + 0.01, (
                f"Power {combo.power_mw} above max 1.2"
            )
            assert combo.duration_hr >= 2.0 - 0.01, (
                f"Duration {combo.duration_hr} below min 2.0"
            )
            assert combo.duration_hr <= 3.0 + 0.01, (
                f"Duration {combo.duration_hr} above max 3.0"
            )


class TestFTMSizingSweepGridReused:
    """Verify build_sweep_grid produces same combos for BTM and FTM."""

    def test_ftm_sizing_sweep_grid_reused(self) -> None:
        """build_sweep_grid output is identical regardless of dispatch_mode.

        build_sweep_grid only reads power/duration bounds from SiteConfig,
        so we use model_construct to bypass full validation for the BTM
        config (avoids needing real rate_schedule/load_profile paths).
        """
        # FTM config (fully validated)
        ftm_site = _make_site_config(dispatch_mode="ftm")

        # BTM config with same sizing bounds (bypass validation)
        btm_site = SiteConfig.model_construct(
            dispatch_mode="btm",
            ac_installed_mw=8.0,
            bess_power_min_mw=0.5,
            bess_power_max_mw=1.0,
            bess_duration_min_hr=2.0,
            bess_duration_max_hr=3.0,
        )

        ftm_grid = build_sweep_grid(ftm_site)
        btm_grid = build_sweep_grid(btm_site)

        assert ftm_grid == btm_grid, (
            f"Grids differ: FTM has {len(ftm_grid)} combos, BTM has {len(btm_grid)}"
        )


class TestFTMSizingEconomicsPopulated:
    """Verify winner economics have FTM-specific fields."""

    def test_ftm_sizing_economics_populated(self) -> None:
        """Winner ProjectEconomics includes solar_revenue, bess_arbitrage_revenue, etc."""
        site = _make_site_config()
        production, lmp_prices = _make_production_with_spread()
        lmp_data = _make_lmp_data(lmp_prices)

        result = run_ftm_sizing_optimization(
            site_config=site,
            production_kwh=production,
            lmp_data=lmp_data,
        )

        econ = result.economics

        # FTM-specific fields
        assert econ.solar_revenue is not None
        assert econ.solar_revenue > 0, "Solar revenue should be positive with production"
        assert econ.bess_arbitrage_revenue is not None
        assert econ.ancillary_revenue is not None
        assert econ.ancillary_revenue > 0, "Ancillary revenue configured at $20/kW/yr"
        assert econ.gross_revenue is not None
        assert econ.gross_revenue > 0

        # Standard economics fields
        assert econ.total_project_npv != 0
        assert econ.solar_npv != 0
        assert econ.bess_npv != 0
        assert econ.total_installed_cost > 0
        assert econ.solar_cost > 0
        assert econ.bess_cost > 0
        assert econ.annual_production_mwh > 0
        assert econ.lifetime_generation_mwh > 0
        assert econ.system_lcoe_per_kwh is not None
        assert econ.system_lcoe_per_kwh > 0
        assert econ.combos_evaluated > 0

        # Optimal sizing from winner
        assert econ.optimal_power_mw == result.winner.power_mw
        assert econ.optimal_duration_hr == result.winner.duration_hr


class TestBTMSizingUnchanged:
    """Verify BTM sizing is not affected by FTM additions."""

    def test_btm_sizing_unchanged(self) -> None:
        """run_sizing_optimization still works identically for BTM.

        This test runs a minimal BTM sizing sweep with mocked dispatch
        returning real Pydantic model objects to confirm the existing
        BTM code path was not modified.
        """
        from unittest.mock import MagicMock, patch

        from src.bess.models import (
            BatteryMetrics,
            BESSBillComparison,
            BESSConfig,
            DispatchResult,
            HourlyDispatch,
        )

        # Build a BTM site config (bypass file-path validators)
        btm_site = SiteConfig.model_construct(
            run_name="btm_unchanged_test",
            site_name="btm_unchanged_site",
            customer="btm_customer",
            latitude=33.45,
            longitude=-112.07,
            dc_size_mw=10.0,
            ac_installed_mw=8.0,
            ac_poi_mw=8.0,
            racking="tracker",
            dispatch_mode="btm",
            bess_dispatch_required=True,
            bess_optimization_required=True,
            bill_calculation=True,
            bess_power_mw=1.0,
            bess_duration_hr=2.0,
            bess_power_min_mw=0.5,
            bess_power_max_mw=0.6,
            bess_duration_min_hr=2.0,
            bess_duration_max_hr=2.5,
            bess_installed_cost_per_kwh=275.0,
            bess_opex_per_kw_year=10.0,
            bess_rte_percent=88.0,
            bess_min_soc_percent=10.0,
            bess_max_soc_percent=90.0,
            bess_cycles_warranty=5000,
            bess_strategy="global",
            rate_escalation_pct=2.0,
            discount_rate_pct=8.0,
            project_lifetime_years=25,
            degradation_percent=0.5,
            solar_cost_per_kw_dc=1000.0,
            solar_cost_per_kw_ac=200.0,
            solar_opex_per_kw_dc_year=10.0,
            bess_grid_only_charging=False,
            bess_solar_only_charging=False,
        )

        production = [3000.0] * 8760

        def _make_btm_dispatch(power_kw: float, duration_hr: float) -> DispatchResult:
            """Build a real DispatchResult for mock returns."""
            cap = power_kw * duration_hr
            return DispatchResult(
                config=BESSConfig(
                    bess_power_kw=power_kw,
                    bess_duration_hr=duration_hr,
                    capacity_kwh=cap,
                    usable_capacity_kwh=cap * 0.8,
                    min_soc_kwh=cap * 0.1,
                    max_soc_kwh=cap * 0.9,
                    charge_efficiency=0.938,
                    discharge_efficiency=0.938,
                    degradation_cost_per_kwh=0.0275,
                    strategy="global",
                ),
                hourly_dispatch=[
                    HourlyDispatch(
                        hour=h, charge_kw=0.0, discharge_kw=0.0,
                        soc_kwh=cap * 0.1, net_load_kw=100.0, curtailed_kw=0.0,
                    )
                    for h in range(8760)
                ],
                monthly_solve_status=["Optimal"] * 12,
                metrics=BatteryMetrics(
                    annual_throughput_kwh=cap * 300,
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
                ),
                heatmap_data=[[0.0] * 24 for _ in range(12)],
            )

        mock_bill = BESSBillComparison(
            solar_only_annual_bill=100000.0,
            solar_plus_bess_annual_bill=85000.0,
            bess_incremental_savings=15000.0,
            bess_demand_savings=10000.0,
            bess_energy_savings=5000.0,
        )

        def mock_dispatch_fn(site_config, production_kwh, rate_schedule,
                             load_profile, solar_only_bill):
            """Return real DispatchResult + BillComparison for any combo."""
            power_kw = site_config.bess_power_mw * 1000
            duration_hr = site_config.bess_duration_hr
            return _make_btm_dispatch(power_kw, duration_hr), mock_bill

        mock_solar_bill = MagicMock()
        mock_solar_bill.annual_total = 100000.0
        mock_bill_without = MagicMock()
        mock_bill_without.annual_total = 150000.0

        with patch(
            "src.bess.sizing_optimizer.run_bess_dispatch",
            side_effect=mock_dispatch_fn,
        ):
            result = run_sizing_optimization(
                site_config=btm_site,
                production_kwh=production,
                rate_schedule=MagicMock(),
                load_profile=MagicMock(),
                solar_only_bill=mock_solar_bill,
                bill_without_solar=mock_bill_without,
            )

        # BTM sizing still works
        assert isinstance(result, SizingResult)
        assert result.winner is not None
        assert result.bill_comparison is not None  # BTM has bill comparison
        assert len(result.all_combos) > 0
        assert result.economics.combos_evaluated == len(result.all_combos)
