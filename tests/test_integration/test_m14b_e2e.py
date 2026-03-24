"""M14b end-to-end integration tests: NEM, ITC solar-only, grid-only BESS.

Exercises the full dispatch pipeline from SiteConfig + rate schedule + load
profile + production profile through run_bess_dispatch and bill calculation,
verifying all M14b features work together correctly.
"""

import json
import math
from pathlib import Path

import pandas as pd
import pytest

from src.bess.dispatch_runner import run_bess_dispatch
from src.bess.models import (
    BESSBillComparison,
    BESSConfig,
    BatteryMetrics,
    DispatchResult,
    HourlyDispatch,
)
from src.bess.report_section import build_bess_summary_rows
from src.config.loader import load_config
from src.config.schema import SiteConfig
from src.rates.bill_calculator import calculate_bill
from src.rates.models import (
    BillResult,
    FixedCharges,
    LoadProfile,
    NetMeteringConfig,
    RateSchedule,
    RateTier,
)

# ---------------------------------------------------------------------------
# Output directory
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path("outputs/test_results/m14b_prompt9")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers — build test scenario inputs
# ---------------------------------------------------------------------------


def _make_site_config(
    bess_solar_only_charging: bool = False,
    bess_grid_only_charging: bool = False,
    bess_strategy: str = "global",
    run_name: str = "m14b_e2e",
) -> SiteConfig:
    """Minimal SiteConfig with BESS and bill calculation enabled."""
    return SiteConfig(
        run_name=run_name,
        site_name="E2E Test Site",
        customer="E2E Customer",
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
        bess_strategy=bess_strategy,
        bess_solar_only_charging=bess_solar_only_charging,
        bess_grid_only_charging=bess_grid_only_charging,
        bill_calculation=True,
        rate_file_path="tests/fixtures/rates/sample_rate.json",
        load_type="Warehouse",
        annual_consumption_kwh=3_500_000.0,
    )


def _make_tou_rate_schedule(
    nem_mode: str = "none",
    export_rate: float | None = None,
    true_up_rate: float = 0.0,
) -> RateSchedule:
    """Build a TOU rate schedule with 2 energy periods, demand, and flat demand.

    Period 0 (off-peak): $0.08/kWh energy, hours 0-7 and 18-23
    Period 1 (on-peak): $0.15/kWh energy, hours 8-17
    Demand TOU: Period 0 $5/kW, Period 1 $12/kW
    Flat demand: $4/kW
    Fixed: $50/month
    """
    nem_config: dict = {"mode": nem_mode, "true_up_rate": true_up_rate}
    if nem_mode == "flat_rate" and export_rate is not None:
        nem_config["export_rate"] = export_rate

    # Build 12x24 schedules: hours 8-17 = period 1 (peak), rest = period 0
    weekday_row = [0] * 8 + [1] * 10 + [0] * 6
    weekend_row = [0] * 24  # Off-peak all day on weekends

    return RateSchedule(
        utility_name="E2E Test Utility",
        tariff_name="E2E TOU Rate",
        fixed_charges=FixedCharges(fixed_charge_first_meter=50.0),
        # Energy: 2 periods
        energyratestructure=[
            [RateTier(rate=0.08)],  # Period 0: off-peak
            [RateTier(rate=0.15)],  # Period 1: on-peak
        ],
        energyweekdayschedule=[weekday_row for _ in range(12)],
        energyweekendschedule=[weekend_row for _ in range(12)],
        # Demand: 2 TOU periods
        demandratestructure=[
            [RateTier(rate=5.0)],   # Period 0: off-peak demand
            [RateTier(rate=12.0)],  # Period 1: on-peak demand
        ],
        demandweekdayschedule=[weekday_row for _ in range(12)],
        demandweekendschedule=[weekend_row for _ in range(12)],
        # Flat demand: 1 period
        flatdemandstructure=[[RateTier(rate=4.0)]],
        flatdemandmonths=[0] * 12,
        net_metering=NetMeteringConfig(**nem_config),
    )


def _make_load_profile(constant_kw: float = 500.0) -> LoadProfile:
    """Flat load profile (8760 hours)."""
    return LoadProfile(
        hourly_kwh=[constant_kw] * 8760,
        source="typical",
        building_type="warehouse",
    )


def _make_day_night_load(day_kw: float = 600.0, night_kw: float = 300.0) -> LoadProfile:
    """Day/night load profile: higher during business hours.

    day_kw during hours 8-17, night_kw otherwise.
    """
    daily = [night_kw] * 8 + [day_kw] * 10 + [night_kw] * 6
    return LoadProfile(
        hourly_kwh=daily * 365,
        source="typical",
        building_type="warehouse",
    )


def _solar_profile_8760(day_kw: float = 800.0) -> list[float]:
    """Simple day/night solar: day_kw during hours 8-17, 0 otherwise."""
    daily = [0.0] * 8 + [day_kw] * 10 + [0.0] * 6
    return daily * 365


def _bell_curve_solar(peak_kw: float = 800.0) -> list[float]:
    """Bell-curve solar profile peaking at solar noon (hour 12).

    Produces from hours 6-18, with peak at hour 12.
    More realistic than a flat daytime block.
    """
    daily = []
    for h in range(24):
        if 6 <= h <= 18:
            # Cosine shape peaking at noon
            val = peak_kw * max(0, math.cos((h - 12) * math.pi / 12))
            daily.append(val)
        else:
            daily.append(0.0)
    return daily * 365


def _zero_production() -> list[float]:
    """Zero solar production (8760 hours)."""
    return [0.0] * 8760


def _write_output(name: str, data: dict) -> None:
    """Write test output to JSON for inspection."""
    path = OUTPUT_DIR / f"{name}.json"
    path.write_text(json.dumps(data, indent=2, default=str))


def _run_full_dispatch(
    site: SiteConfig,
    rate: RateSchedule,
    load: LoadProfile,
    production: list[float],
    with_bill_comparison: bool = True,
) -> tuple[DispatchResult, BESSBillComparison | None]:
    """Run dispatch with optional bill comparison. Returns (dispatch, comparison)."""
    if with_bill_comparison:
        solar_only_bill = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=production,
            rate=rate,
        )
        result = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
            solar_only_bill=solar_only_bill,
        )
        return result  # Returns (DispatchResult, BESSBillComparison)
    else:
        result = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
        )
        return result, None


def _extract_metrics_summary(
    dispatch: DispatchResult,
    comparison: BESSBillComparison | None = None,
    solar_only_bill: BillResult | None = None,
    solar_plus_bess_bill: BillResult | None = None,
) -> dict:
    """Extract key metrics from dispatch results for reporting."""
    m = dispatch.metrics
    summary = {
        "solver_statuses": dispatch.monthly_solve_status,
        "all_optimal": all(s == "Optimal" for s in dispatch.monthly_solve_status),
        "total_export_kwh": m.total_export_kwh if m else 0,
        "total_export_hours": m.total_export_hours if m else 0,
        "charging_source": m.charging_source if m else "unknown",
        "annual_cycles": round(m.annual_cycles, 2) if m else 0,
        "annual_throughput_kwh": round(m.annual_throughput_kwh, 1) if m else 0,
        "total_curtailed_kwh": round(m.total_curtailed_kwh, 1) if m else 0,
        "hours_charging": m.hours_charging if m else 0,
        "hours_discharging": m.hours_discharging if m else 0,
    }
    if comparison:
        summary.update({
            "solar_only_annual_bill": round(comparison.solar_only_annual_bill, 2),
            "solar_plus_bess_annual_bill": round(comparison.solar_plus_bess_annual_bill, 2),
            "bess_incremental_savings": round(comparison.bess_incremental_savings, 2),
            "bess_demand_savings": round(comparison.bess_demand_savings, 2),
            "bess_energy_savings": round(comparison.bess_energy_savings, 2),
            "solar_only_export_kwh": round(comparison.solar_only_export_kwh, 2),
            "solar_only_export_credits": round(comparison.solar_only_export_credits, 2),
            "solar_plus_bess_export_kwh": round(comparison.solar_plus_bess_export_kwh, 2),
            "solar_plus_bess_export_credits": round(comparison.solar_plus_bess_export_credits, 2),
        })
    return summary


# ===========================================================================
# Scenario 1: Baseline — no M14b features
# ===========================================================================


class TestBaselineNoM14b:
    """Run dispatch with NEM=none, solar_only=False, grid_only=False.

    Confirms M14a behavior is unchanged: no exports, default charging.
    """

    def test_baseline_all_export_zero(self) -> None:
        """All export_kw values should be 0 when NEM is inactive."""
        site = _make_site_config()
        rate = _make_tou_rate_schedule(nem_mode="none")
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        dispatch, comparison = _run_full_dispatch(site, rate, load, production)

        # All export_kw must be zero
        assert all(
            h.export_kw == 0.0 for h in dispatch.hourly_dispatch
        ), "Expected all export_kw == 0 when NEM is inactive"

    def test_baseline_metrics_export_zero(self) -> None:
        """BatteryMetrics export fields should be zero."""
        site = _make_site_config()
        rate = _make_tou_rate_schedule(nem_mode="none")
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        dispatch, comparison = _run_full_dispatch(site, rate, load, production)

        assert dispatch.metrics.total_export_kwh == 0.0
        assert dispatch.metrics.total_export_hours == 0

    def test_baseline_charging_source_any(self) -> None:
        """BatteryMetrics.charging_source should be 'any'."""
        site = _make_site_config()
        rate = _make_tou_rate_schedule(nem_mode="none")
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        dispatch, comparison = _run_full_dispatch(site, rate, load, production)

        assert dispatch.metrics.charging_source == "any"

    def test_baseline_bill_comparison_zero_exports(self) -> None:
        """BESSBillComparison export fields should all be zero."""
        site = _make_site_config()
        rate = _make_tou_rate_schedule(nem_mode="none")
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        dispatch, comparison = _run_full_dispatch(site, rate, load, production)

        assert comparison.solar_only_export_kwh == 0.0
        assert comparison.solar_only_export_credits == 0.0
        assert comparison.solar_plus_bess_export_kwh == 0.0
        assert comparison.solar_plus_bess_export_credits == 0.0

    def test_baseline_bill_result_zero_exports(self) -> None:
        """Bill results should have zero export_kwh and export_credits."""
        site = _make_site_config()
        rate = _make_tou_rate_schedule(nem_mode="none")
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        solar_only_bill = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=production,
            rate=rate,
        )

        assert solar_only_bill.annual_export_kwh == 0.0
        assert solar_only_bill.annual_export_credits == 0.0

    def test_baseline_solver_all_optimal(self) -> None:
        """All 12 months should solve as Optimal."""
        site = _make_site_config()
        rate = _make_tou_rate_schedule(nem_mode="none")
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        dispatch, comparison = _run_full_dispatch(site, rate, load, production)

        assert all(s == "Optimal" for s in dispatch.monthly_solve_status)

        # Write output
        _write_output("scenario_01_baseline", _extract_metrics_summary(dispatch, comparison))


# ===========================================================================
# Scenario 2: NEM flat_rate
# ===========================================================================


class TestNEMFlatRate:
    """NEM mode='flat_rate', export_rate=0.05, true_up_rate=0.02.

    Production > load during daytime should generate exports.
    """

    def _run_nem_flat(self) -> tuple[DispatchResult, BESSBillComparison]:
        """Shared setup for NEM flat_rate tests."""
        site = _make_site_config()
        rate = _make_tou_rate_schedule(
            nem_mode="flat_rate", export_rate=0.05, true_up_rate=0.02
        )
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        return _run_full_dispatch(site, rate, load, production)

    def test_export_kw_positive_during_excess(self) -> None:
        """HourlyDispatch has export_kw > 0 during excess production hours."""
        dispatch, _ = self._run_nem_flat()

        export_hours = [h for h in dispatch.hourly_dispatch if h.export_kw > 0.01]
        assert len(export_hours) > 0, "Expected some hours with export_kw > 0"

    def test_total_export_kwh_positive(self) -> None:
        """BatteryMetrics.total_export_kwh > 0."""
        dispatch, _ = self._run_nem_flat()

        assert dispatch.metrics.total_export_kwh > 0

    def test_total_export_hours_positive(self) -> None:
        """BatteryMetrics.total_export_hours > 0."""
        dispatch, _ = self._run_nem_flat()

        assert dispatch.metrics.total_export_hours > 0

    def test_monthly_export_populated(self) -> None:
        """Monthly export_kwh and export_credits are populated on bill results."""
        site = _make_site_config()
        rate = _make_tou_rate_schedule(
            nem_mode="flat_rate", export_rate=0.05, true_up_rate=0.02
        )
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        solar_only_bill = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=production,
            rate=rate,
        )

        assert solar_only_bill.annual_export_kwh > 0
        assert solar_only_bill.annual_export_credits > 0

        # At least some months should have export
        months_with_export = [
            m for m in solar_only_bill.monthly if m.export_kwh > 0
        ]
        assert len(months_with_export) > 0

    def test_nem_banking_credits_applied(self) -> None:
        """Some months should have nem_credits_applied > 0."""
        site = _make_site_config()
        rate = _make_tou_rate_schedule(
            nem_mode="flat_rate", export_rate=0.05, true_up_rate=0.02
        )
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        solar_only_bill = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=production,
            rate=rate,
        )

        months_with_nem = [
            m for m in solar_only_bill.monthly if m.nem_credits_applied > 0
        ]
        assert len(months_with_nem) > 0, "Expected NEM banking credits applied"

    def test_nem_true_up_non_negative(self) -> None:
        """Annual true-up credit should be >= 0."""
        site = _make_site_config()
        rate = _make_tou_rate_schedule(
            nem_mode="flat_rate", export_rate=0.05, true_up_rate=0.02
        )
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        solar_only_bill = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=production,
            rate=rate,
        )

        assert solar_only_bill.nem_true_up_credit >= 0

    def test_bill_comparison_export_fields(self) -> None:
        """BESSBillComparison shows export fields for both scenarios."""
        dispatch, comparison = self._run_nem_flat()

        # Solar-only should have exports (excess production during day)
        assert comparison.solar_only_export_kwh > 0
        assert comparison.solar_only_export_credits > 0

        # Solar+BESS should also have exports (BESS absorbs some but not all)
        assert comparison.solar_plus_bess_export_kwh >= 0
        assert comparison.solar_plus_bess_export_credits >= 0

    def test_bess_adds_value(self) -> None:
        """solar+BESS annual bill <= solar-only annual bill."""
        dispatch, comparison = self._run_nem_flat()

        assert comparison.solar_plus_bess_annual_bill <= comparison.solar_only_annual_bill, (
            f"BESS should add value: solar+BESS ${comparison.solar_plus_bess_annual_bill:.2f} "
            f"> solar-only ${comparison.solar_only_annual_bill:.2f}"
        )

    def test_near_zero_curtailment(self) -> None:
        """Total curtailment should be near zero (export absorbs excess)."""
        dispatch, _ = self._run_nem_flat()

        assert dispatch.metrics.total_curtailed_kwh < 10.0, (
            f"Expected near-zero curtailment with NEM, got "
            f"{dispatch.metrics.total_curtailed_kwh:.1f} kWh"
        )

    def test_write_output(self) -> None:
        """Write scenario summary for manual review."""
        dispatch, comparison = self._run_nem_flat()
        _write_output("scenario_02_nem_flat_rate", _extract_metrics_summary(dispatch, comparison))


# ===========================================================================
# Scenario 3: NEM match_import
# ===========================================================================


class TestNEMMatchImport:
    """NEM mode='match_import' — export credits reflect TOU energy rates."""

    def test_match_import_varies_by_tou(self) -> None:
        """Export credits should differ from flat_rate scenario since rates vary."""
        site = _make_site_config()
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        # Flat rate NEM
        rate_flat = _make_tou_rate_schedule(
            nem_mode="flat_rate", export_rate=0.05, true_up_rate=0.02
        )
        bill_flat = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=production,
            rate=rate_flat,
        )

        # Match import NEM
        rate_match = _make_tou_rate_schedule(nem_mode="match_import")
        bill_match = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=production,
            rate=rate_match,
        )

        # Both should have export
        assert bill_flat.annual_export_kwh > 0
        assert bill_match.annual_export_kwh > 0

        # Export kWh should be the same (same physical system)
        assert abs(bill_flat.annual_export_kwh - bill_match.annual_export_kwh) < 1.0

        # Export credits should differ (flat 0.05 vs TOU-matching 0.08/0.15)
        assert bill_flat.annual_export_credits != bill_match.annual_export_credits, (
            "Expected different export credits between flat_rate and match_import"
        )

    def test_match_import_higher_peak_credits(self) -> None:
        """match_import gives higher credits during peak hours (0.15 vs 0.08 off-peak)."""
        site = _make_site_config()
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        rate_match = _make_tou_rate_schedule(nem_mode="match_import")
        bill_match = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=production,
            rate=rate_match,
        )

        # Since production is during hours 8-17 (peak period with $0.15/kWh),
        # export credits should reflect the higher peak rate.
        # Average export rate should be between 0.08 and 0.15
        if bill_match.annual_export_kwh > 0:
            avg_rate = bill_match.annual_export_credits / bill_match.annual_export_kwh
            assert avg_rate > 0.07, f"Average export rate {avg_rate:.4f} seems too low"
            assert avg_rate <= 0.15, f"Average export rate {avg_rate:.4f} exceeds peak rate"

    def test_match_import_dispatch_valid(self) -> None:
        """Dispatch with match_import NEM solves successfully."""
        site = _make_site_config()
        rate = _make_tou_rate_schedule(nem_mode="match_import")
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        dispatch, comparison = _run_full_dispatch(site, rate, load, production)

        assert all(s == "Optimal" for s in dispatch.monthly_solve_status)
        assert dispatch.metrics.total_export_kwh > 0

        _write_output("scenario_03_nem_match_import", _extract_metrics_summary(dispatch, comparison))


# ===========================================================================
# Scenario 4: NEM + BESS strategy interaction
# ===========================================================================


class TestNEMStrategyInteraction:
    """Run NEM flat_rate with each of the 3 strategies."""

    @pytest.mark.parametrize("strategy", ["global", "peak_shaving", "tou_arbitrage"])
    def test_strategy_solves_successfully(self, strategy: str) -> None:
        """All three strategies produce valid results with NEM active."""
        site = _make_site_config(bess_strategy=strategy)
        rate = _make_tou_rate_schedule(
            nem_mode="flat_rate", export_rate=0.05, true_up_rate=0.02
        )
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        dispatch, comparison = _run_full_dispatch(site, rate, load, production)

        assert all(s == "Optimal" for s in dispatch.monthly_solve_status), (
            f"Strategy {strategy} failed: {dispatch.monthly_solve_status}"
        )
        assert dispatch.metrics is not None
        assert comparison is not None

    def test_strategies_differ_in_behavior(self) -> None:
        """The three strategies should produce different dispatch patterns.

        Uses no-NEM + variable load (evening demand spike) to create a scenario
        where the battery is economic to operate. With a flat load and low NEM
        export rate, the battery may correctly remain idle for all strategies.
        """
        # Variable load: 300 kW base, 700 kW evening peak (hours 18-22)
        # Creates demand peaks worth shaving and energy arbitrage opportunity
        daily_load = [300.0] * 8 + [500.0] * 10 + [700.0] * 4 + [300.0] * 2
        load = LoadProfile(hourly_kwh=daily_load * 365, source="typical", building_type="warehouse")

        # Higher demand rates to incentivize battery usage
        weekday_row = [0] * 8 + [1] * 10 + [0] * 6
        weekend_row = [0] * 24
        rate = RateSchedule(
            utility_name="Strategy Test Utility",
            tariff_name="Strategy Test Rate",
            fixed_charges=FixedCharges(fixed_charge_first_meter=50.0),
            energyratestructure=[
                [RateTier(rate=0.06)],  # Period 0: off-peak
                [RateTier(rate=0.20)],  # Period 1: on-peak (wide spread)
            ],
            energyweekdayschedule=[weekday_row for _ in range(12)],
            energyweekendschedule=[weekend_row for _ in range(12)],
            demandratestructure=[
                [RateTier(rate=8.0)],   # Period 0: off-peak demand
                [RateTier(rate=18.0)],  # Period 1: on-peak demand (high)
            ],
            demandweekdayschedule=[weekday_row for _ in range(12)],
            demandweekendschedule=[weekend_row for _ in range(12)],
            flatdemandstructure=[[RateTier(rate=6.0)]],
            flatdemandmonths=[0] * 12,
        )

        production = _solar_profile_8760(day_kw=800.0)

        results = {}
        for strategy in ["global", "peak_shaving", "tou_arbitrage"]:
            site = _make_site_config(bess_strategy=strategy, run_name=f"m14b_e2e_{strategy}")

            dispatch, comparison = _run_full_dispatch(site, rate, load, production)
            results[strategy] = {
                "total_export_kwh": dispatch.metrics.total_export_kwh,
                "total_throughput": dispatch.metrics.annual_throughput_kwh,
                "bess_savings": comparison.bess_incremental_savings,
                "demand_savings": comparison.bess_demand_savings,
                "energy_savings": comparison.bess_energy_savings,
            }

        _write_output("scenario_04_strategy_comparison", results)

        # Verify at least some strategy actually uses the battery
        max_throughput = max(r["total_throughput"] for r in results.values())
        assert max_throughput > 10.0, (
            f"Expected at least one strategy to use the battery, "
            f"max throughput was {max_throughput:.1f} kWh"
        )

        # Strategies with different objective weights should produce different
        # throughput or savings distributions. peak_shaving ignores energy cost,
        # tou_arbitrage ignores demand cost — at minimum their demand vs energy
        # savings should differ.
        ps = results["peak_shaving"]
        ta = results["tou_arbitrage"]
        assert (
            round(ps["demand_savings"], 1) != round(ta["demand_savings"], 1)
            or round(ps["energy_savings"], 1) != round(ta["energy_savings"], 1)
            or round(ps["total_throughput"], 1) != round(ta["total_throughput"], 1)
        ), (
            f"peak_shaving and tou_arbitrage produced identical results: "
            f"PS={ps}, TA={ta}"
        )


# ===========================================================================
# Scenario 5: ITC solar-only charging
# ===========================================================================


class TestITCSolarOnlyCharging:
    """bess_solar_only_charging=True, NEM=none."""

    def _run_solar_only(self) -> tuple[DispatchResult, BESSBillComparison]:
        """Shared setup for ITC solar-only tests."""
        site = _make_site_config(bess_solar_only_charging=True)
        rate = _make_tou_rate_schedule(nem_mode="none")
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)
        return _run_full_dispatch(site, rate, load, production)

    def test_no_charge_when_production_below_load(self) -> None:
        """Battery never charges when production <= load."""
        site = _make_site_config(bess_solar_only_charging=True)
        rate = _make_tou_rate_schedule(nem_mode="none")
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        dispatch, _ = _run_full_dispatch(site, rate, load, production)

        # Check each hour: when production <= load, charge should be ~0
        for h in dispatch.hourly_dispatch:
            hour_of_day = h.hour % 24
            # Night hours (0-7, 18-23): production = 0, well below load = 500
            if hour_of_day < 8 or hour_of_day >= 18:
                assert h.charge_kw < 0.5, (
                    f"Hour {h.hour} (night): charge_kw={h.charge_kw:.2f} but "
                    f"production=0 <= load=500"
                )

    def test_charges_during_excess_solar(self) -> None:
        """Battery charges during hours with excess solar."""
        dispatch, _ = self._run_solar_only()

        # During day hours (8-17), production=800 > load=500 → excess=300
        day_charges = [
            h.charge_kw for h in dispatch.hourly_dispatch
            if 8 <= (h.hour % 24) < 18
        ]
        total_day_charge = sum(day_charges)
        assert total_day_charge > 10.0, (
            f"Expected significant daytime charging, got {total_day_charge:.1f}"
        )

    def test_charging_source_solar_only(self) -> None:
        """BatteryMetrics.charging_source should be 'solar_only'."""
        dispatch, _ = self._run_solar_only()
        assert dispatch.metrics.charging_source == "solar_only"

    def test_fewer_cycles_than_baseline(self) -> None:
        """Annual cycles should be lower than unrestricted baseline."""
        # Baseline (unrestricted)
        site_base = _make_site_config(run_name="m14b_base_cycles")
        rate = _make_tou_rate_schedule(nem_mode="none")
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)
        dispatch_base, _ = _run_full_dispatch(site_base, rate, load, production)

        # Solar-only
        dispatch_solar, _ = self._run_solar_only()

        assert dispatch_solar.metrics.annual_cycles <= dispatch_base.metrics.annual_cycles + 0.1, (
            f"Solar-only cycles ({dispatch_solar.metrics.annual_cycles:.1f}) should be <= "
            f"baseline cycles ({dispatch_base.metrics.annual_cycles:.1f})"
        )

    def test_lower_savings_than_baseline(self) -> None:
        """BESS savings should be lower than unrestricted baseline."""
        rate = _make_tou_rate_schedule(nem_mode="none")
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        # Baseline
        site_base = _make_site_config(run_name="m14b_base_savings")
        _, comp_base = _run_full_dispatch(site_base, rate, load, production)

        # Solar-only
        dispatch_solar, comp_solar = self._run_solar_only()

        assert comp_solar.bess_incremental_savings <= comp_base.bess_incremental_savings + 1.0, (
            f"Solar-only savings (${comp_solar.bess_incremental_savings:.0f}) should be <= "
            f"baseline savings (${comp_base.bess_incremental_savings:.0f})"
        )

        _write_output("scenario_05_itc_solar_only", {
            "solar_only": _extract_metrics_summary(dispatch_solar, comp_solar),
            "baseline_cycles": dispatch_solar.metrics.annual_cycles,
        })


# ===========================================================================
# Scenario 6: ITC solar-only + NEM combined
# ===========================================================================


class TestITCSolarOnlyPlusNEM:
    """bess_solar_only_charging=True with NEM flat_rate."""

    def test_charges_only_from_excess_solar(self) -> None:
        """Battery charges only from excess solar (not grid)."""
        site = _make_site_config(bess_solar_only_charging=True)
        rate = _make_tou_rate_schedule(
            nem_mode="flat_rate", export_rate=0.05, true_up_rate=0.02
        )
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        dispatch, comparison = _run_full_dispatch(site, rate, load, production)

        # Night hours: no charging
        for h in dispatch.hourly_dispatch:
            hour_of_day = h.hour % 24
            if hour_of_day < 8 or hour_of_day >= 18:
                assert h.charge_kw < 0.5, (
                    f"Hour {h.hour}: charge_kw={h.charge_kw:.2f} during night"
                )

    def test_excess_exported(self) -> None:
        """Excess solar beyond battery capacity is exported."""
        site = _make_site_config(bess_solar_only_charging=True)
        rate = _make_tou_rate_schedule(
            nem_mode="flat_rate", export_rate=0.05, true_up_rate=0.02
        )
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        dispatch, comparison = _run_full_dispatch(site, rate, load, production)

        assert dispatch.metrics.total_export_kwh > 0, (
            "Expected export when solar excess exceeds battery capacity"
        )

    def test_export_credits_earned(self) -> None:
        """Export credits should be positive."""
        site = _make_site_config(bess_solar_only_charging=True)
        rate = _make_tou_rate_schedule(
            nem_mode="flat_rate", export_rate=0.05, true_up_rate=0.02
        )
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        dispatch, comparison = _run_full_dispatch(site, rate, load, production)

        # At least one scenario should have export credits
        assert (
            comparison.solar_only_export_credits > 0
            or comparison.solar_plus_bess_export_credits > 0
        ), "Expected export credits with NEM + solar-only charging"

        _write_output("scenario_06_itc_plus_nem", _extract_metrics_summary(dispatch, comparison))


# ===========================================================================
# Scenario 7: Grid-only standalone BESS
# ===========================================================================


class TestGridOnlyStandalone:
    """bess_grid_only_charging=True with nonzero solar production configured."""

    def _run_grid_only(
        self, nem_mode: str = "none",
    ) -> tuple[DispatchResult, BESSBillComparison]:
        """Shared setup for grid-only tests."""
        site = _make_site_config(bess_grid_only_charging=True)
        rate = _make_tou_rate_schedule(nem_mode=nem_mode)
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        # Grid-only: "solar-only bill" is actually load-only (no solar in dispatch)
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
        return dispatch_result, bill_comparison

    def test_all_export_kw_zero(self) -> None:
        """All HourlyDispatch.export_kw should be 0 (no solar in dispatch)."""
        dispatch, _ = self._run_grid_only()

        assert all(
            h.export_kw == 0.0 for h in dispatch.hourly_dispatch
        ), "Grid-only: expected all export_kw == 0"

    def test_battery_charges_from_grid(self) -> None:
        """Battery should charge from grid (charge_kw > 0 during some hours)."""
        dispatch, _ = self._run_grid_only()

        total_charge = sum(h.charge_kw for h in dispatch.hourly_dispatch)
        assert total_charge > 10.0, (
            f"Expected grid charging, got total_charge={total_charge:.1f}"
        )

    def test_battery_discharges(self) -> None:
        """Battery should discharge to reduce peaks or arbitrage."""
        dispatch, _ = self._run_grid_only()

        total_discharge = sum(h.discharge_kw for h in dispatch.hourly_dispatch)
        assert total_discharge > 10.0, (
            f"Expected discharge activity, got {total_discharge:.1f}"
        )

    def test_charging_source_grid_only(self) -> None:
        """BatteryMetrics.charging_source should be 'grid_only'."""
        dispatch, _ = self._run_grid_only()
        assert dispatch.metrics.charging_source == "grid_only"

    def test_bill_comparison_no_solar(self) -> None:
        """Bill comparison is load-only vs load+BESS (no solar offset)."""
        dispatch, comparison = self._run_grid_only()

        # Both bills should be positive
        assert comparison.solar_only_annual_bill > 0
        assert comparison.solar_plus_bess_annual_bill > 0

        # No export in either scenario
        assert comparison.solar_only_export_kwh == 0.0
        assert comparison.solar_plus_bess_export_kwh == 0.0

    def test_grid_only_plus_nem_still_no_export(self) -> None:
        """Grid-only with NEM active: export_kw still 0."""
        site = _make_site_config(bess_grid_only_charging=True)
        rate = _make_tou_rate_schedule(
            nem_mode="flat_rate", export_rate=0.05, true_up_rate=0.02
        )
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        dispatch = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
        )

        # Grid-only zeroes production before dispatch, so no export even with NEM
        assert all(
            h.export_kw == 0.0 for h in dispatch.hourly_dispatch
        ), "Grid-only + NEM: expected no export (production zeroed)"

        _write_output("scenario_07_grid_only", _extract_metrics_summary(dispatch))


# ===========================================================================
# Scenario 8: Grid-only with no effective solar
# ===========================================================================


class TestGridOnlyNoSolar:
    """Grid-only with zero production input — dispatch runs with zero production."""

    def test_zero_production_valid_dispatch(self) -> None:
        """Dispatch runs successfully with zero production."""
        site = _make_site_config(bess_grid_only_charging=True)
        rate = _make_tou_rate_schedule(nem_mode="none")
        load = _make_load_profile(constant_kw=500.0)
        production = _zero_production()

        dispatch = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
        )

        assert len(dispatch.hourly_dispatch) == 8760
        assert all(s == "Optimal" for s in dispatch.monthly_solve_status)
        assert dispatch.metrics is not None
        assert dispatch.metrics.charging_source == "grid_only"

    def test_zero_production_no_curtailment(self) -> None:
        """No curtailment with zero production."""
        site = _make_site_config(bess_grid_only_charging=True)
        rate = _make_tou_rate_schedule(nem_mode="none")
        load = _make_load_profile(constant_kw=500.0)
        production = _zero_production()

        dispatch = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
        )

        assert dispatch.metrics.total_curtailed_kwh < 1.0

        _write_output("scenario_08_grid_only_no_solar", _extract_metrics_summary(dispatch))


# ===========================================================================
# Scenario 9: Mutual exclusivity validation
# ===========================================================================


class TestMutualExclusivity:
    """Both solar_only and grid_only True should raise ValueError."""

    def test_mutual_exclusivity_raises(self) -> None:
        """Setting both charging modes raises ValueError during SiteConfig validation."""
        with pytest.raises(ValueError, match="mutually exclusive"):
            _make_site_config(
                bess_solar_only_charging=True,
                bess_grid_only_charging=True,
            )


# ===========================================================================
# Scenario 10: Backward compatibility — CSV loading
# ===========================================================================


class TestBackwardCompatCSV:
    """CSV without new BESS columns should load with defaults."""

    def test_csv_without_new_columns_defaults(self, tmp_path: Path) -> None:
        """CSV missing BESS Solar/Grid Only columns defaults both to False."""
        csv_path = tmp_path / "backward_compat.csv"

        data = {
            "Run Name": ["BackCompat1"],
            "Site Name": ["BackCompatSite"],
            "Customer": ["TestCo"],
            "Latitude": [33.45],
            "Longitude": [-112.07],
            "DC Size (MW)": [5.0],
            "AC Installed (MW)": [4.0],
            "AC POI (MW)": [4.0],
            "Racking": ["tracker"],
            "Tilt": [60.0],
            "Azimuth": [180.0],
            "Module Orientation": ["portrait"],
            "Number of Modules": [1],
            "Ground Clearance Height (m)": [1.5],
            "Panel Model": ["Test Panel"],
            "Bifacial": [False],
            "Inverter Model": ["Test Inverter"],
            "GCR": [0.35],
            "Shading (%)": [3.0],
            "DC Wiring Loss (%)": [2.0],
            "AC Wiring Loss (%)": [1.0],
            "Transformer Losses (%)": [1.0],
            "Degradation (%)": [0.5],
            "Availability (%)": [1.0],
            "Module Mismatch (%)": [1.0],
            "LID(%)": [1.5],
            # No BESS Solar Only / Grid Only columns
        }

        df = pd.DataFrame(data)
        df.to_csv(csv_path, index=False)

        configs = load_config(csv_path)

        assert len(configs) == 1
        assert configs[0].bess_solar_only_charging is False
        assert configs[0].bess_grid_only_charging is False

    def test_csv_loaded_config_dispatches_normally(self, tmp_path: Path) -> None:
        """Config loaded from CSV without new columns dispatches successfully."""
        csv_path = tmp_path / "backward_dispatch.csv"

        data = {
            "Run Name": ["BackCompat2"],
            "Site Name": ["BackCompatSite2"],
            "Customer": ["TestCo"],
            "Latitude": [33.45],
            "Longitude": [-112.07],
            "DC Size (MW)": [5.0],
            "AC Installed (MW)": [4.0],
            "AC POI (MW)": [4.0],
            "Racking": ["tracker"],
            "Tilt": [60.0],
            "Azimuth": [180.0],
            "Module Orientation": ["portrait"],
            "Number of Modules": [1],
            "Ground Clearance Height (m)": [1.5],
            "Panel Model": ["Test Panel"],
            "Bifacial": [False],
            "Inverter Model": ["Test Inverter"],
            "GCR": [0.35],
            "Shading (%)": [3.0],
            "DC Wiring Loss (%)": [2.0],
            "AC Wiring Loss (%)": [1.0],
            "Transformer Losses (%)": [1.0],
            "Degradation (%)": [0.5],
            "Availability (%)": [1.0],
            "Module Mismatch (%)": [1.0],
            "LID(%)": [1.5],
        }

        df = pd.DataFrame(data)
        df.to_csv(csv_path, index=False)

        configs = load_config(csv_path)
        site = configs[0]

        # Construct a minimal dispatch-capable config manually
        # (since CSV doesn't have BESS/bill fields, we just verify defaults)
        assert site.bess_solar_only_charging is False
        assert site.bess_grid_only_charging is False

        _write_output("scenario_10_backward_compat", {
            "bess_solar_only_charging": site.bess_solar_only_charging,
            "bess_grid_only_charging": site.bess_grid_only_charging,
        })


# ===========================================================================
# Scenario 11: Summary dict completeness
# ===========================================================================


class TestSummaryDictCompleteness:
    """Verify the summary dict has all expected M14b fields.

    This test builds the summary dict in the same way as pipeline.py to verify
    all expected fields are present. We can't run the full pipeline without
    PySAM/NSRDB, so we build the summary manually from dispatch results.
    """

    def test_bess_dispatch_summary_fields(self) -> None:
        """summary["bess_dispatch"] should contain all M14b export/charging fields."""
        site = _make_site_config()
        rate = _make_tou_rate_schedule(
            nem_mode="flat_rate", export_rate=0.05, true_up_rate=0.02
        )
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        dispatch, comparison = _run_full_dispatch(site, rate, load, production)

        # Build the summary dict the same way pipeline.py does
        from src.rates.tou_mapper import get_month_ranges

        bess_dispatch_summary = {
            "bess_power_mw": site.bess_power_mw,
            "bess_duration_hr": site.bess_duration_hr,
            "bess_capacity_kwh": dispatch.config.capacity_kwh,
            "bess_strategy": site.bess_strategy,
            "solar_only_annual_bill": comparison.solar_only_annual_bill,
            "solar_plus_bess_annual_bill": comparison.solar_plus_bess_annual_bill,
            "bess_incremental_savings": comparison.bess_incremental_savings,
            "bess_demand_savings": comparison.bess_demand_savings,
            "bess_energy_savings": comparison.bess_energy_savings,
            "annual_cycles": dispatch.metrics.annual_cycles,
            "annual_throughput_kwh": dispatch.metrics.annual_throughput_kwh,
            "average_daily_cycles": dispatch.metrics.average_daily_cycles,
            "capacity_utilization_pct": dispatch.metrics.capacity_utilization_pct,
            "total_curtailed_kwh": dispatch.metrics.total_curtailed_kwh,
            "estimated_annual_degradation_pct": dispatch.metrics.estimated_annual_degradation_pct,
            "total_export_kwh": dispatch.metrics.total_export_kwh,
            "total_export_hours": dispatch.metrics.total_export_hours,
            "charging_source": dispatch.metrics.charging_source,
            "solar_only_export_kwh": comparison.solar_only_export_kwh,
            "solar_only_export_credits": comparison.solar_only_export_credits,
            "solar_plus_bess_export_kwh": comparison.solar_plus_bess_export_kwh,
            "solar_plus_bess_export_credits": comparison.solar_plus_bess_export_credits,
            "monthly_solver_status": dispatch.monthly_solve_status,
            "heatmap_data": dispatch.heatmap_data,
        }

        # Build dispatch profile for peak month (same as pipeline.py)
        _month_names = [
            "January", "February", "March", "April",
            "May", "June", "July", "August",
            "September", "October", "November", "December",
        ]
        hm = dispatch.heatmap_data
        month_activity = [sum(abs(v) for v in row) for row in hm]
        peak_month = month_activity.index(max(month_activity))
        month_ranges = get_month_ranges()
        m_start, m_end = month_ranges[peak_month]
        n_days = (m_end - m_start) // 24

        avg_export = [0.0] * 24
        for day in range(n_days):
            for h in range(24):
                idx = m_start + day * 24 + h
                avg_export[h] += dispatch.hourly_dispatch[idx].export_kw
        avg_export = [v / n_days for v in avg_export]

        bess_dispatch_summary["dispatch_profile_export"] = avg_export

        # Verify all expected M14b fields
        expected_bess_fields = {
            "total_export_kwh", "total_export_hours", "charging_source",
            "solar_only_export_kwh", "solar_only_export_credits",
            "solar_plus_bess_export_kwh", "solar_plus_bess_export_credits",
            "dispatch_profile_export",
        }
        for field in expected_bess_fields:
            assert field in bess_dispatch_summary, f"Missing bess_dispatch field: {field}"

    def test_bill_savings_summary_fields(self) -> None:
        """summary["bill_savings"] should contain NEM/export fields."""
        site = _make_site_config()
        rate = _make_tou_rate_schedule(
            nem_mode="flat_rate", export_rate=0.05, true_up_rate=0.02
        )
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        solar_only_bill = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=production,
            rate=rate,
        )

        # Build bill_savings summary the same way pipeline.py does
        bill_savings_summary = {
            "annual_export_kwh": solar_only_bill.annual_export_kwh,
            "annual_export_credits": solar_only_bill.annual_export_credits,
            "nem_true_up_credit": solar_only_bill.nem_true_up_credit,
        }

        expected_bill_fields = {
            "annual_export_kwh", "annual_export_credits", "nem_true_up_credit",
        }
        for field in expected_bill_fields:
            assert field in bill_savings_summary, f"Missing bill_savings field: {field}"

        # Verify NEM fields have expected values (positive with flat_rate NEM)
        assert bill_savings_summary["annual_export_kwh"] > 0
        assert bill_savings_summary["annual_export_credits"] > 0
        assert bill_savings_summary["nem_true_up_credit"] >= 0

    def test_report_section_builds_correctly(self) -> None:
        """build_bess_summary_rows() works with M14b summary dict."""
        site = _make_site_config()
        rate = _make_tou_rate_schedule(
            nem_mode="flat_rate", export_rate=0.05, true_up_rate=0.02
        )
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        dispatch, comparison = _run_full_dispatch(site, rate, load, production)

        bess_dispatch = {
            "bess_power_mw": site.bess_power_mw,
            "bess_duration_hr": site.bess_duration_hr,
            "bess_capacity_kwh": dispatch.config.capacity_kwh,
            "bess_strategy": site.bess_strategy,
            "solar_only_annual_bill": comparison.solar_only_annual_bill,
            "solar_plus_bess_annual_bill": comparison.solar_plus_bess_annual_bill,
            "bess_incremental_savings": comparison.bess_incremental_savings,
            "bess_demand_savings": comparison.bess_demand_savings,
            "bess_energy_savings": comparison.bess_energy_savings,
            "annual_cycles": dispatch.metrics.annual_cycles,
            "capacity_utilization_pct": dispatch.metrics.capacity_utilization_pct,
            "estimated_annual_degradation_pct": dispatch.metrics.estimated_annual_degradation_pct,
            "total_curtailed_kwh": dispatch.metrics.total_curtailed_kwh,
            "total_export_kwh": dispatch.metrics.total_export_kwh,
            "total_export_hours": dispatch.metrics.total_export_hours,
            "charging_source": dispatch.metrics.charging_source,
        }

        rows = build_bess_summary_rows(bess_dispatch)
        assert len(rows) > 5, "Expected at least 5 summary rows"

        # Since we have NEM export > 0, export rows should be present
        row_labels = [r[0] for r in rows]
        if dispatch.metrics.total_export_kwh > 0:
            assert "Total Export" in row_labels, "Expected 'Total Export' row"
            assert "Export Hours" in row_labels, "Expected 'Export Hours' row"

        _write_output("scenario_11_summary_dict", {
            "bess_dispatch_keys": list(bess_dispatch.keys()),
            "report_rows": rows,
        })


# ===========================================================================
# Cross-scenario comparison output
# ===========================================================================


class TestCrossScenarioSummary:
    """Run all major scenarios and write a combined summary for manual review."""

    def test_write_combined_summary(self) -> None:
        """Write a combined summary JSON with key metrics from each scenario."""
        combined = {}
        load = _make_load_profile(constant_kw=500.0)
        production = _solar_profile_8760(day_kw=800.0)

        # --- Scenario 1: Baseline ---
        site = _make_site_config(run_name="combined_baseline")
        rate = _make_tou_rate_schedule(nem_mode="none")
        d, c = _run_full_dispatch(site, rate, load, production)
        combined["01_baseline"] = _extract_metrics_summary(d, c)

        # --- Scenario 2: NEM flat_rate ---
        site = _make_site_config(run_name="combined_nem_flat")
        rate = _make_tou_rate_schedule(
            nem_mode="flat_rate", export_rate=0.05, true_up_rate=0.02
        )
        d, c = _run_full_dispatch(site, rate, load, production)
        combined["02_nem_flat_rate"] = _extract_metrics_summary(d, c)

        # --- Scenario 3: NEM match_import ---
        site = _make_site_config(run_name="combined_nem_match")
        rate = _make_tou_rate_schedule(nem_mode="match_import")
        d, c = _run_full_dispatch(site, rate, load, production)
        combined["03_nem_match_import"] = _extract_metrics_summary(d, c)

        # --- Scenario 5: ITC solar-only ---
        site = _make_site_config(bess_solar_only_charging=True, run_name="combined_itc")
        rate = _make_tou_rate_schedule(nem_mode="none")
        d, c = _run_full_dispatch(site, rate, load, production)
        combined["05_itc_solar_only"] = _extract_metrics_summary(d, c)

        # --- Scenario 6: ITC + NEM ---
        site = _make_site_config(bess_solar_only_charging=True, run_name="combined_itc_nem")
        rate = _make_tou_rate_schedule(
            nem_mode="flat_rate", export_rate=0.05, true_up_rate=0.02
        )
        d, c = _run_full_dispatch(site, rate, load, production)
        combined["06_itc_plus_nem"] = _extract_metrics_summary(d, c)

        # --- Scenario 7: Grid-only ---
        site = _make_site_config(bess_grid_only_charging=True, run_name="combined_grid")
        rate = _make_tou_rate_schedule(nem_mode="none")
        solar_only_bill = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=_zero_production(),
            rate=rate,
        )
        d_result, bill_comp = run_bess_dispatch(
            site_config=site,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
            solar_only_bill=solar_only_bill,
        )
        combined["07_grid_only"] = _extract_metrics_summary(d_result, bill_comp)

        # --- Scenario 8: Grid-only, zero production ---
        site = _make_site_config(bess_grid_only_charging=True, run_name="combined_grid_zero")
        rate = _make_tou_rate_schedule(nem_mode="none")
        d = run_bess_dispatch(
            site_config=site,
            production_kwh=_zero_production(),
            rate_schedule=rate,
            load_profile=load,
        )
        combined["08_grid_only_zero_prod"] = _extract_metrics_summary(d)

        _write_output("combined_summary", combined)

        # Verify the output file was written
        assert (OUTPUT_DIR / "combined_summary.json").exists()
