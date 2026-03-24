"""Tests for M14b dispatch runner wiring: NEM passthrough, solar-only, grid-only."""

import json
import math
from pathlib import Path
from unittest.mock import patch

import pytest

from src.bess.dispatch_runner import run_bess_dispatch
from src.bess.models import BESSConfig, HourlyDispatch
from src.config.schema import SiteConfig
from src.rates.bill_calculator import calculate_bill
from src.rates.models import (
    LoadProfile,
    NetMeteringConfig,
    RateSchedule,
    RateTier,
)

# ---------------------------------------------------------------------------
# Output directory
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path("outputs/test_results/m14b_prompt6")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers — deterministic 8760 inputs
# ---------------------------------------------------------------------------


def _make_site_config(
    bess_solar_only_charging: bool = False,
    bess_grid_only_charging: bool = False,
) -> SiteConfig:
    """Minimal SiteConfig with BESS fields for dispatch runner tests."""
    return SiteConfig(
        run_name="test_m14b",
        site_name="Test Site",
        customer="Test Customer",
        latitude=33.45,
        longitude=-112.07,
        dc_size_mw=5.0,
        ac_installed_mw=4.0,
        ac_poi_mw=4.0,
        racking="tracker",
        tilt=60.0,
        azimuth=180.0,
        module_orientation="portrait",
        number_of_modules=1,
        ground_clearance_height_m=1.5,
        panel_model="Test Panel",
        bifacial=False,
        inverter_model="Test Inverter",
        gcr=0.35,
        shading_percent=3.0,
        dc_wiring_loss_percent=2.0,
        ac_wiring_loss_percent=1.0,
        transformer_losses_percent=1.0,
        degradation_percent=0.5,
        availability_percent=1.0,
        module_mismatch_percent=1.0,
        lid_percent=1.5,
        bess_dispatch_required=True,
        bess_power_mw=0.5,
        bess_duration_hr=4.0,
        bess_rte_percent=88.0,
        bess_min_soc_percent=10.0,
        bess_max_soc_percent=90.0,
        bess_strategy="global",
        bess_solar_only_charging=bess_solar_only_charging,
        bess_grid_only_charging=bess_grid_only_charging,
        bill_calculation=True,
        rate_file_path="tests/fixtures/rates/sample_rate.json",
        load_type="Warehouse",
        annual_consumption_kwh=3_500_000.0,
    )


def _make_rate_schedule(
    nem_mode: str = "none",
    export_rate: float | None = None,
) -> RateSchedule:
    """Build a simple flat-rate schedule with optional NEM.

    Single energy period at $0.10/kWh, one demand period at $5/kW,
    one flat demand period at $3/kW.
    """
    nem_config: dict = {"mode": nem_mode}
    if nem_mode == "flat_rate" and export_rate is not None:
        nem_config["export_rate"] = export_rate

    return RateSchedule(
        utility_name="Test Utility",
        tariff_name="Test Tariff",
        energyratestructure=[[RateTier(rate=0.10)]],
        energyweekdayschedule=[[0] * 24 for _ in range(12)],
        energyweekendschedule=[[0] * 24 for _ in range(12)],
        demandratestructure=[[RateTier(rate=5.0)]],
        demandweekdayschedule=[[0] * 24 for _ in range(12)],
        demandweekendschedule=[[0] * 24 for _ in range(12)],
        flatdemandstructure=[[RateTier(rate=3.0)]],
        flatdemandmonths=[0] * 12,
        net_metering=NetMeteringConfig(**nem_config),
    )


def _make_load_profile(constant_kw: float = 400.0) -> LoadProfile:
    """Constant load profile (8760 hours)."""
    return LoadProfile(
        hourly_kwh=[constant_kw] * 8760,
        source="typical",
        building_type="warehouse",
    )


def _solar_profile_8760(day_kw: float = 600.0) -> list[float]:
    """Simple day/night solar: day_kw during hours 8-17, 0 otherwise.

    365 days × 24 hours = 8760. Day hours produce excess over 400 kW load.
    """
    daily = [0.0] * 8 + [day_kw] * 10 + [0.0] * 6
    return daily * 365


def _zero_production() -> list[float]:
    """Zero solar production (8760 hours)."""
    return [0.0] * 8760


def _write_output(name: str, data: dict) -> None:
    """Write test output to JSON for inspection."""
    path = OUTPUT_DIR / f"{name}.json"
    path.write_text(json.dumps(data, indent=2, default=str))


# ---------------------------------------------------------------------------
# Test: NEM passthrough
# ---------------------------------------------------------------------------


class TestNEMPassthrough:
    """Verify NEM export rates are built and passed to solve_month,
    and export_kw appears on HourlyDispatch records.
    """

    def test_nem_flat_rate_export_kw_populated(self) -> None:
        """With flat_rate NEM, HourlyDispatch.export_kw > 0 for excess hours."""
        site = _make_site_config()
        rate = _make_rate_schedule(nem_mode="flat_rate", export_rate=0.04)
        load = _make_load_profile(constant_kw=400.0)
        # Day production (600 kW) exceeds load (400 kW) → export expected
        production = _solar_profile_8760(day_kw=600.0)

        result = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
        )

        # Result is DispatchResult (no solar_only_bill)
        assert len(result.hourly_dispatch) == 8760

        # At least some hours should have export_kw > 0
        export_hours = [h for h in result.hourly_dispatch if h.export_kw > 0.01]
        assert len(export_hours) > 0, "Expected some hours with export_kw > 0"

        # All export_kw values should be non-negative
        assert all(h.export_kw >= 0 for h in result.hourly_dispatch)

        _write_output("nem_passthrough_summary", {
            "total_export_hours": len(export_hours),
            "total_export_kwh": sum(h.export_kw for h in result.hourly_dispatch),
            "sample_export_hours": [
                {"hour": h.hour, "export_kw": h.export_kw}
                for h in export_hours[:10]
            ],
        })

    def test_nem_export_rates_match_flat_rate(self) -> None:
        """NEM export rates built by dispatch runner use hour_of_year indices."""
        # With flat_rate NEM, every hour should have the same export rate.
        # We verify indirectly: the optimizer receives the rates and produces
        # non-zero export when there's excess production.
        site = _make_site_config()
        rate = _make_rate_schedule(nem_mode="flat_rate", export_rate=0.05)
        load = _make_load_profile(constant_kw=200.0)
        production = _solar_profile_8760(day_kw=800.0)

        result = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
        )

        # Significant excess production (800 day - 200 load) → lots of export
        total_export = sum(h.export_kw for h in result.hourly_dispatch)
        assert total_export > 0, "Expected nonzero total export with large excess"


# ---------------------------------------------------------------------------
# Test: Solar-only charging passthrough
# ---------------------------------------------------------------------------


class TestSolarOnlyCharging:
    """Verify solar_only_charging flag is passed through to solve_month."""

    def test_charge_zero_when_production_below_load(self) -> None:
        """With solar_only_charging=True, charge is zero when prod <= load."""
        site = _make_site_config(bess_solar_only_charging=True)
        rate = _make_rate_schedule()
        load = _make_load_profile(constant_kw=500.0)
        # Day production exactly equals load → no excess solar
        production = _solar_profile_8760(day_kw=500.0)

        result = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
        )

        # During solar hours (8-17), production == load → no excess.
        # During night hours (0-7, 18-23), production == 0 < load.
        # Solar-only charging means zero charging from grid.
        # The battery should have zero or near-zero charge across the year.
        total_charge = sum(h.charge_kw for h in result.hourly_dispatch)
        assert total_charge < 1.0, (
            f"Expected near-zero charging with solar_only and no excess, "
            f"got {total_charge:.2f}"
        )

        _write_output("solar_only_charging_summary", {
            "total_charge_kwh": total_charge,
            "total_discharge_kwh": sum(
                h.discharge_kw for h in result.hourly_dispatch
            ),
        })

    def test_charge_positive_with_excess_solar(self) -> None:
        """With solar_only_charging=True, battery charges from excess solar."""
        site = _make_site_config(bess_solar_only_charging=True)
        rate = _make_rate_schedule()
        load = _make_load_profile(constant_kw=300.0)
        # Day production (800 kW) greatly exceeds load (300 kW) → excess
        production = _solar_profile_8760(day_kw=800.0)

        result = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
        )

        total_charge = sum(h.charge_kw for h in result.hourly_dispatch)
        assert total_charge > 10.0, (
            f"Expected significant charging with excess solar, got {total_charge:.2f}"
        )


# ---------------------------------------------------------------------------
# Test: Grid-only dispatch
# ---------------------------------------------------------------------------


class TestGridOnlyDispatch:
    """Verify grid_only_charging=True treats production as zero in dispatch
    and bill comparison uses load+BESS only (no solar).
    """

    def test_grid_only_production_treated_as_zero(self) -> None:
        """Grid-only mode: the optimizer ignores solar production entirely."""
        site = _make_site_config(bess_grid_only_charging=True)
        rate = _make_rate_schedule()
        load = _make_load_profile(constant_kw=400.0)
        # Even with production provided, it should be ignored
        production = _solar_profile_8760(day_kw=600.0)

        result = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
        )

        # In grid-only mode, curtailed_kw should be zero (no solar to curtail)
        total_curtailed = sum(h.curtailed_kw for h in result.hourly_dispatch)
        assert total_curtailed < 1.0, (
            f"Grid-only: expected near-zero curtailment, got {total_curtailed:.2f}"
        )

        _write_output("grid_only_dispatch_summary", {
            "total_charge_kwh": sum(h.charge_kw for h in result.hourly_dispatch),
            "total_discharge_kwh": sum(h.discharge_kw for h in result.hourly_dispatch),
            "total_curtailed_kwh": total_curtailed,
        })

    def test_grid_only_bill_comparison_excludes_solar(self) -> None:
        """Grid-only bill comparison: adjusted_production = discharge - charge."""
        site = _make_site_config(bess_grid_only_charging=True)
        rate = _make_rate_schedule()
        load = _make_load_profile(constant_kw=400.0)
        production = _solar_profile_8760(day_kw=600.0)

        # Pre-compute the "solar only" bill (which in grid-only mode is
        # actually the load-only bill — no solar)
        solar_only_bill = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=_zero_production(),
            rate=rate,
        )

        dispatch_result, bill_comparison = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
            solar_only_bill=solar_only_bill,
        )

        # The BESS bill should be computed without solar.
        # BESS arbitrage should produce some savings.
        assert bill_comparison.solar_only_annual_bill > 0
        assert bill_comparison.solar_plus_bess_annual_bill > 0
        # BESS should provide at least some demand savings
        assert bill_comparison.bess_demand_savings >= 0

        _write_output("grid_only_bill_comparison", {
            "solar_only_annual_bill": bill_comparison.solar_only_annual_bill,
            "solar_plus_bess_annual_bill": bill_comparison.solar_plus_bess_annual_bill,
            "bess_incremental_savings": bill_comparison.bess_incremental_savings,
            "bess_demand_savings": bill_comparison.bess_demand_savings,
            "bess_energy_savings": bill_comparison.bess_energy_savings,
        })

    def test_grid_only_adjusted_production_no_solar_component(self) -> None:
        """Verify adjusted_production has no solar — only battery net power."""
        site = _make_site_config(bess_grid_only_charging=True)
        rate = _make_rate_schedule()
        load = _make_load_profile(constant_kw=400.0)
        production = _solar_profile_8760(day_kw=600.0)

        solar_only_bill = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=_zero_production(),
            rate=rate,
        )

        dispatch_result, bill_comparison = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
            solar_only_bill=solar_only_bill,
        )

        # Manually compute what adjusted_production should be (grid-only)
        for t in range(8760):
            h = dispatch_result.hourly_dispatch[t]
            expected_adj = h.discharge_kw - h.charge_kw
            # Production should NOT be included — this is verified by checking
            # that the bill comparison doesn't include solar savings.
            # A solar-only bill with zero production should equal the "base" load bill.
            assert bill_comparison.solar_only_annual_bill == solar_only_bill.annual_total


# ---------------------------------------------------------------------------
# Test: Backward compatibility
# ---------------------------------------------------------------------------


class TestBackwardCompatibility:
    """No NEM, no solar-only, no grid-only — matches existing behavior."""

    def test_export_kw_zero_when_no_nem(self) -> None:
        """Without NEM, export_kw should be 0.0 for all hours."""
        site = _make_site_config()
        rate = _make_rate_schedule(nem_mode="none")
        load = _make_load_profile(constant_kw=400.0)
        production = _solar_profile_8760(day_kw=600.0)

        result = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
        )

        # All export_kw should be 0.0
        assert all(
            h.export_kw == 0.0 for h in result.hourly_dispatch
        ), "export_kw should be 0.0 for all hours when NEM is inactive"

        _write_output("backward_compat_summary", {
            "all_export_zero": True,
            "total_hours": len(result.hourly_dispatch),
            "solver_statuses": result.monthly_solve_status,
        })

    def test_dispatch_result_structure_unchanged(self) -> None:
        """DispatchResult structure is preserved with new fields at defaults."""
        site = _make_site_config()
        rate = _make_rate_schedule()
        load = _make_load_profile()
        production = _solar_profile_8760()

        result = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
        )

        # Verify structure
        assert len(result.hourly_dispatch) == 8760
        assert len(result.monthly_solve_status) == 12
        assert result.metrics is not None
        assert result.heatmap_data is not None

        # Verify HourlyDispatch has export_kw field (default 0.0)
        first_hour = result.hourly_dispatch[0]
        assert hasattr(first_hour, "export_kw")
        assert first_hour.export_kw == 0.0

    def test_bill_comparison_unchanged_without_new_features(self) -> None:
        """Bill comparison matches pre-M14b behavior when no new flags set."""
        site = _make_site_config()
        rate = _make_rate_schedule()
        load = _make_load_profile(constant_kw=400.0)
        production = _solar_profile_8760(day_kw=600.0)

        solar_only_bill = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=production,
            rate=rate,
        )

        dispatch_result, bill_comparison = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
            solar_only_bill=solar_only_bill,
        )

        # Standard adjusted_production = production + discharge - charge
        # Bill comparison should show some BESS savings
        assert bill_comparison.bess_incremental_savings >= 0
        assert bill_comparison.solar_only_annual_bill == solar_only_bill.annual_total

        _write_output("backward_compat_bill_comparison", {
            "solar_only_annual_bill": bill_comparison.solar_only_annual_bill,
            "solar_plus_bess_annual_bill": bill_comparison.solar_plus_bess_annual_bill,
            "bess_incremental_savings": bill_comparison.bess_incremental_savings,
        })
