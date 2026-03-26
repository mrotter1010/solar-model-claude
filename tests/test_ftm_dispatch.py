"""Tests for FTM (front-of-the-meter) BESS dispatch optimization."""

import math

import pytest

from src.bess.dispatch_runner import run_ftm_dispatch
from src.bess.models import BESSConfig
from src.bess.optimizer import MonthSolveResult, solve_month
from src.config.schema import SiteConfig
from src.lmp.models import LMPData
from src.rates.tou_mapper import get_month_ranges

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EFF = math.sqrt(0.88)  # ~0.9381 one-way efficiency

_HOURS_PER_MONTH = [744, 672, 744, 720, 744, 720, 744, 744, 720, 744, 720, 744]


def _make_config(
    strategy: str = "global",
    degradation_cost_per_kwh: float = 0.001,
    power_kw: float = 500.0,
    duration_hr: float = 4.0,
) -> BESSConfig:
    """Build a BESSConfig directly (no SiteConfig dependency)."""
    capacity_kwh = power_kw * duration_hr
    min_soc_pct = 10.0
    max_soc_pct = 90.0
    return BESSConfig(
        bess_power_kw=power_kw,
        bess_duration_hr=duration_hr,
        capacity_kwh=capacity_kwh,
        usable_capacity_kwh=capacity_kwh * (max_soc_pct - min_soc_pct) / 100,
        min_soc_kwh=capacity_kwh * min_soc_pct / 100,
        max_soc_kwh=capacity_kwh * max_soc_pct / 100,
        charge_efficiency=_EFF,
        discharge_efficiency=_EFF,
        degradation_cost_per_kwh=degradation_cost_per_kwh,
        strategy=strategy,
    )


def _make_site_config(**bess_overrides: object) -> SiteConfig:
    """Build a minimal SiteConfig with BESS fields for FTM testing."""
    defaults = {
        "run_name": "ftm_test",
        "site_name": "ftm_site",
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
        # BESS defaults: 500 kW / 4hr
        "bess_power_mw": 0.5,
        "bess_duration_hr": 4.0,
        "bess_rte_percent": 88.0,
        "bess_min_soc_percent": 10.0,
        "bess_max_soc_percent": 90.0,
        "bess_strategy": "global",
        "bess_installed_cost_per_kwh": 275.0,
        "bess_cycles_warranty": 5000,
    }
    defaults.update(bess_overrides)
    return SiteConfig(**defaults)


def _make_lmp_data(prices: list[float]) -> LMPData:
    """Build an LMPData object with 8760 prices."""
    return LMPData(
        iso="test_iso",
        zone="test_zone",
        market="DAY_AHEAD_HOURLY",
        year=2024,
        prices=prices,
    )


# ---------------------------------------------------------------------------
# 1. FTM solve_month — flat LMP, basic revenue
# ---------------------------------------------------------------------------


class TestFTMSolveMonthBasic:
    """Flat LMP price: verify solver status and revenue calculation."""

    def test_flat_lmp_revenue(self) -> None:
        """With flat $50/MWh LMP and available solar, revenue > 0."""
        n_hours = 720  # 30-day month
        config = _make_config()

        # 800 kW solar, 0 load → 800 kW available
        result = solve_month(
            load_kw=[0.0] * n_hours,
            production_kw=[800.0] * n_hours,
            config=config,
            energy_rate_per_hour=[0.0] * n_hours,
            demand_periods={},
            demand_rates={},
            flat_demand_rate=0.0,
            initial_soc_kwh=config.min_soc_kwh,
            dispatch_mode="ftm",
            lmp_prices=[50.0] * n_hours,  # $50/MWh = $0.05/kWh
        )

        assert result.solver_status == "Optimal"
        assert result.ftm_revenue is not None
        assert result.ftm_revenue > 0

        # Revenue should be approximately 800 kW * 720 h * $0.05/kWh = $28,800
        # (solar export dominates; battery adds a small amount)
        assert result.ftm_revenue > 25000

        # FTM-specific fields populated
        assert result.solar_export_kw is not None
        assert len(result.solar_export_kw) == n_hours

        # BTM fields are zeroed/empty
        assert result.peak_demand_by_period == {}
        assert result.peak_demand_flat == 0.0
        assert result.net_load_kw == [0.0] * n_hours


# ---------------------------------------------------------------------------
# 2. FTM solve_month — arbitrage (low/high LMP)
# ---------------------------------------------------------------------------


class TestFTMSolveMonthArbitrage:
    """Low/high LMP pattern: battery charges low, discharges high."""

    def test_arbitrage_pattern(self) -> None:
        """With $20/MWh off-peak and $100/MWh on-peak, battery does arbitrage."""
        n_hours = 720
        config = _make_config()

        # LMP pattern: hours 0-7 low, 8-20 high, 21-23 low (daily)
        daily_lmp = (
            [20.0] * 8     # hours 0-7: $20/MWh
            + [100.0] * 13  # hours 8-20: $100/MWh
            + [20.0] * 3    # hours 21-23: $20/MWh
        )
        lmp_prices = daily_lmp * (n_hours // 24)

        # No solar — pure battery arbitrage from grid
        result = solve_month(
            load_kw=[0.0] * n_hours,
            production_kw=[0.0] * n_hours,
            config=config,
            energy_rate_per_hour=[0.0] * n_hours,
            demand_periods={},
            demand_rates={},
            flat_demand_rate=0.0,
            initial_soc_kwh=config.min_soc_kwh,
            dispatch_mode="ftm",
            lmp_prices=lmp_prices,
        )

        assert result.solver_status == "Optimal"
        assert result.ftm_revenue is not None

        # Revenue should be positive from buying low / selling high
        assert result.ftm_revenue > 0

        # Verify charge happens during low-price hours and discharge during high
        total_charge = sum(result.charge_kw)
        total_discharge = sum(result.discharge_kw)
        assert total_charge > 0, "Battery should charge during low-price hours"
        assert total_discharge > 0, "Battery should discharge during high-price hours"

        # Check first day: charge in hours 0-7, discharge in hours 8-20
        charge_low = sum(result.charge_kw[t] for t in range(8))
        discharge_high = sum(result.discharge_kw[t] for t in range(8, 21))
        assert charge_low > 0, "Should charge during off-peak hours"
        assert discharge_high > 0, "Should discharge during on-peak hours"


# ---------------------------------------------------------------------------
# 3. FTM solve_month — negative LMP
# ---------------------------------------------------------------------------


class TestFTMSolveMonthNegativeLMP:
    """Negative LMP prices: solar should be curtailed, not exported."""

    def test_negative_lmp_curtailment(self) -> None:
        """With negative LMP, exporting would cost money. Prefer curtailment.

        Note: The battery may still cycle because grid charging earns money
        at negative prices (you get paid to absorb power), and the RTE losses
        mean less kWh must be discharged than was charged. This creates a
        small arbitrage even at uniform negative prices.
        """
        n_hours = 720
        config = _make_config()

        result = solve_month(
            load_kw=[0.0] * n_hours,
            production_kw=[500.0] * n_hours,
            config=config,
            energy_rate_per_hour=[0.0] * n_hours,
            demand_periods={},
            demand_rates={},
            flat_demand_rate=0.0,
            initial_soc_kwh=config.min_soc_kwh,
            dispatch_mode="ftm",
            lmp_prices=[-20.0] * n_hours,  # Negative $20/MWh
        )

        assert result.solver_status == "Optimal"
        assert result.solar_export_kw is not None

        # Solar export should be near zero (exporting at negative price costs money)
        total_export = sum(result.solar_export_kw)
        assert total_export < 1.0, (
            f"Expected near-zero solar export with negative LMP, got {total_export:.2f}"
        )

        # Curtailment should be high (most solar curtailed)
        total_curtailed = sum(result.curtailed_kw)
        assert total_curtailed > 300000, (
            f"Expected significant curtailment, got {total_curtailed:.2f}"
        )

        # Revenue should still be positive: battery earns from grid charge
        # arbitrage (paid to charge, lose less discharging due to RTE)
        assert result.ftm_revenue is not None
        assert result.ftm_revenue > 0


# ---------------------------------------------------------------------------
# 4. FTM solve_month — solar-only charging
# ---------------------------------------------------------------------------


class TestFTMSolveMonthSolarOnlyCharging:
    """When solar_only_charging=True, grid_charge must be zero."""

    def test_grid_charge_is_zero(self) -> None:
        """Grid charge variable forced to zero under solar-only constraint."""
        n_hours = 720
        config = _make_config()

        # High LMP spread to incentivize grid arbitrage (which should be blocked)
        daily_lmp = [10.0] * 8 + [200.0] * 13 + [10.0] * 3
        lmp_prices = daily_lmp * (n_hours // 24)

        result = solve_month(
            load_kw=[0.0] * n_hours,
            production_kw=[300.0] * n_hours,
            config=config,
            energy_rate_per_hour=[0.0] * n_hours,
            demand_periods={},
            demand_rates={},
            flat_demand_rate=0.0,
            initial_soc_kwh=config.min_soc_kwh,
            dispatch_mode="ftm",
            lmp_prices=lmp_prices,
            solar_only_charging=True,
        )

        assert result.solver_status == "Optimal"
        assert result.grid_charge_kw is not None

        # Grid charge should be zero at all hours
        for t in range(n_hours):
            assert result.grid_charge_kw[t] < 0.01, (
                f"Hour {t}: grid_charge={result.grid_charge_kw[t]:.4f}, "
                f"expected 0 with solar_only_charging"
            )

        # Battery should still charge from solar
        total_charge = sum(result.charge_kw)
        assert total_charge > 0, "Battery should charge from solar"


# ---------------------------------------------------------------------------
# 5. FTM solve_month — no battery (zero-sized)
# ---------------------------------------------------------------------------


class TestFTMSolveMonthNoBattery:
    """Zero-sized battery: only solar export revenue."""

    def test_solar_export_only(self) -> None:
        """With zero power battery, all solar goes to export or curtailment."""
        n_hours = 720
        config = _make_config(power_kw=0.001, duration_hr=0.001)
        # Tiny battery to avoid division-by-zero in BESSConfig validators

        result = solve_month(
            load_kw=[0.0] * n_hours,
            production_kw=[1000.0] * n_hours,
            config=config,
            energy_rate_per_hour=[0.0] * n_hours,
            demand_periods={},
            demand_rates={},
            flat_demand_rate=0.0,
            initial_soc_kwh=config.min_soc_kwh,
            dispatch_mode="ftm",
            lmp_prices=[40.0] * n_hours,  # $40/MWh
        )

        assert result.solver_status == "Optimal"
        assert result.solar_export_kw is not None

        # Nearly all solar exported (battery too small to matter)
        total_export = sum(result.solar_export_kw)
        total_available = 1000.0 * n_hours
        assert total_export > total_available * 0.99, (
            f"Expected ~all solar exported, got {total_export:.0f} / {total_available:.0f}"
        )

        # Revenue ≈ 1000 kW * 720h * $0.04/kWh = $28,800
        assert result.ftm_revenue is not None
        assert abs(result.ftm_revenue - 28800) < 100


# ---------------------------------------------------------------------------
# 6. FTM run_ftm_dispatch — full 8760
# ---------------------------------------------------------------------------


class TestFTMRunDispatchBasic:
    """Full-year FTM dispatch with synthetic data."""

    def test_full_year_dispatch(self) -> None:
        """Run 8760-hour FTM dispatch, verify structure and positive revenue."""
        site_config = _make_site_config()

        # Synthetic solar: 5000 kW daytime, 0 nighttime
        production = []
        for _ in range(365):
            production.extend([0.0] * 6)       # hours 0-5
            production.extend([5000.0] * 12)    # hours 6-17
            production.extend([0.0] * 6)        # hours 18-23

        # Flat $50/MWh LMP
        lmp_data = _make_lmp_data([50.0] * 8760)

        result = run_ftm_dispatch(
            site_config=site_config,
            production_kwh=production,
            lmp_data=lmp_data,
        )

        # Structure checks
        assert len(result.hourly_dispatch) == 8760
        assert len(result.monthly_solve_status) == 12
        assert all(s == "Optimal" for s in result.monthly_solve_status)

        # Revenue checks
        assert result.ftm_revenue is not None
        assert result.ftm_revenue > 0

        # Metrics populated
        assert result.metrics is not None
        assert result.heatmap_data is not None
        assert len(result.heatmap_data) == 12
        assert all(len(row) == 24 for row in result.heatmap_data)

        # FTM fields present on hourly records
        for h in result.hourly_dispatch[:24]:
            assert h.solar_export_kw >= 0
            assert h.grid_charge_kw >= 0

        print(f"\n[FTM Full Year] total_revenue=${result.ftm_revenue:,.2f}")
        print(f"[FTM Full Year] annual_cycles={result.metrics.annual_cycles:.1f}")


# ---------------------------------------------------------------------------
# 7. FTM run_ftm_dispatch — parasitic load reduces available solar
# ---------------------------------------------------------------------------


class TestFTMRunDispatchWithParasiticLoad:
    """Parasitic load reduces available solar for export."""

    def test_load_reduces_revenue(self) -> None:
        """Revenue with load < revenue without load (load eats solar)."""
        site_config = _make_site_config()

        # Flat solar, flat LMP
        production = [3000.0] * 8760
        lmp_data = _make_lmp_data([60.0] * 8760)

        # No load
        result_no_load = run_ftm_dispatch(
            site_config=site_config,
            production_kwh=production,
            lmp_data=lmp_data,
            load_kwh=None,
        )

        # With 500 kW parasitic load
        result_with_load = run_ftm_dispatch(
            site_config=site_config,
            production_kwh=production,
            lmp_data=lmp_data,
            load_kwh=[500.0] * 8760,
        )

        assert result_no_load.ftm_revenue is not None
        assert result_with_load.ftm_revenue is not None

        # Load reduces available solar → less revenue
        assert result_with_load.ftm_revenue < result_no_load.ftm_revenue, (
            f"Expected load to reduce revenue: "
            f"no_load=${result_no_load.ftm_revenue:,.2f}, "
            f"with_load=${result_with_load.ftm_revenue:,.2f}"
        )

        # Revenue reduction should be roughly 500 kW * 8760h * $0.06/kWh = $262,800
        revenue_diff = result_no_load.ftm_revenue - result_with_load.ftm_revenue
        assert revenue_diff > 200000, (
            f"Revenue difference {revenue_diff:,.2f} seems too small"
        )


# ---------------------------------------------------------------------------
# 8. BTM backward compatibility
# ---------------------------------------------------------------------------


class TestBTMUnchanged:
    """Verify BTM dispatch is unaffected by FTM additions."""

    def test_btm_default_mode(self) -> None:
        """Default dispatch_mode='btm' produces identical BTM results."""
        n_hours = 720
        config = _make_config(strategy="tou_arbitrage")
        load = [500.0] * n_hours
        production = [0.0] * n_hours

        daily_rates = [0.10] * 8 + [0.30] * 13 + [0.10] * 3
        rates = daily_rates * (n_hours // 24)

        # Call without dispatch_mode (default = "btm")
        result = solve_month(
            load_kw=load,
            production_kw=production,
            config=config,
            energy_rate_per_hour=rates,
            demand_periods={},
            demand_rates={},
            flat_demand_rate=0.0,
            initial_soc_kwh=config.min_soc_kwh,
        )

        assert result.solver_status == "Optimal"

        # BTM-specific fields populated
        cost_with = sum(result.net_load_kw[t] * rates[t] for t in range(n_hours))
        cost_without = sum(load[t] * rates[t] for t in range(n_hours))
        assert cost_with < cost_without, "BTM TOU arbitrage should reduce cost"

        # FTM fields are None (not populated in BTM mode)
        assert result.solar_export_kw is None
        assert result.grid_charge_kw is None
        assert result.ftm_revenue is None

    def test_btm_explicit_mode(self) -> None:
        """Explicit dispatch_mode='btm' matches default behavior."""
        n_hours = 720
        config = _make_config(strategy="global")

        result_default = solve_month(
            load_kw=[1000.0] * n_hours,
            production_kw=[0.0] * n_hours,
            config=config,
            energy_rate_per_hour=[0.20] * n_hours,
            demand_periods={},
            demand_rates={},
            flat_demand_rate=5.0,
            initial_soc_kwh=config.min_soc_kwh,
        )

        result_explicit = solve_month(
            load_kw=[1000.0] * n_hours,
            production_kw=[0.0] * n_hours,
            config=config,
            energy_rate_per_hour=[0.20] * n_hours,
            demand_periods={},
            demand_rates={},
            flat_demand_rate=5.0,
            initial_soc_kwh=config.min_soc_kwh,
            dispatch_mode="btm",
        )

        # Results should be identical
        assert result_default.solver_status == result_explicit.solver_status
        assert result_default.peak_demand_flat == result_explicit.peak_demand_flat
        assert result_default.final_soc_kwh == result_explicit.final_soc_kwh
        assert len(result_default.charge_kw) == len(result_explicit.charge_kw)
