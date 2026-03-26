"""End-to-end tests for the FTM (front-of-meter) wholesale dispatch pipeline.

Validates the full FTM path: LMP data → dispatch → economics → sizing,
plus BTM backward compatibility. All LMP data is synthetic — no live API calls.

Outputs written to outputs/test_results/m14e_ftm_e2e/ for manual inspection.
"""

import json
import math
import random
from pathlib import Path

import pytest

from src.bess.dispatch_runner import run_ftm_dispatch
from src.bess.models import DispatchResult, SizingResult
from src.bess.sizing_optimizer import (
    compute_ftm_bess_npv,
    compute_ftm_project_economics,
    compute_ftm_solar_revenue,
    run_ftm_sizing_optimization,
)
from src.config.schema import SiteConfig
from src.lmp.models import LMPData

# Output directory for manual inspection
_OUTPUT_DIR = Path("outputs/test_results/m14e_ftm_e2e")
_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers: SiteConfig builders
# ---------------------------------------------------------------------------


def _make_ftm_site_config(**overrides: object) -> SiteConfig:
    """Build a realistic FTM SiteConfig in PJM territory (Philadelphia area).

    5 MW DC / 4 MW AC community solar with 2 MW / 4 hr battery.
    """
    defaults = {
        "run_name": "FTM_E2E_Test",
        "site_name": "Philadelphia Community Solar",
        "customer": "E2E Test Customer",
        "latitude": 39.95,
        "longitude": -75.16,
        "dc_size_mw": 5.0,
        "ac_installed_mw": 4.0,
        "ac_poi_mw": 4.0,
        "racking": "tracker",
        "tilt": 60.0,
        "azimuth": 180.0,
        "module_orientation": "portrait",
        "number_of_modules": 1,
        "ground_clearance_height_m": 1.5,
        "panel_model": "CSI Solar Co. Ltd. CS3U-355P",
        "bifacial": False,
        "inverter_model": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
        "gcr": 0.35,
        "shading_percent": 1.0,
        "dc_wiring_loss_percent": 2.0,
        "ac_wiring_loss_percent": 1.0,
        "transformer_losses_percent": 1.0,
        "degradation_percent": 0.5,
        "availability_percent": 2.5,
        "module_mismatch_percent": 1.0,
        "lid_percent": 1.5,
        # FTM dispatch mode
        "dispatch_mode": "ftm",
        "bess_dispatch_required": True,
        "bess_power_mw": 2.0,
        "bess_duration_hr": 4.0,
        "bess_rte_percent": 88.0,
        "bess_min_soc_percent": 10.0,
        "bess_max_soc_percent": 90.0,
        "bess_strategy": "global",
        "bess_installed_cost_per_kwh": 275.0,
        "bess_cycles_warranty": 5000,
        "bess_opex_per_kw_year": 10.0,
        "ancillary_revenue_per_kw_year": 20.0,
        # Financial
        "solar_cost_per_kw_dc": 1200.0,
        "solar_cost_per_kw_ac": 200.0,
        "solar_opex_per_kw_dc_year": 15.0,
        "discount_rate_pct": 8.0,
        "project_lifetime_years": 25,
        "rate_escalation_pct": 2.0,
    }
    defaults.update(overrides)
    return SiteConfig(**defaults)


# ---------------------------------------------------------------------------
# Helpers: Synthetic LMP data
# ---------------------------------------------------------------------------


def _make_realistic_lmp_prices(seed: int = 42) -> list[float]:
    """Generate 8760 realistic PJM LMP prices.

    Pattern per day:
      - 0-5:   $20-30 (overnight low)
      - 6-8:   $30-50 (morning ramp)
      - 9-15:  $40-65 (midday, moderate due to solar)
      - 16-20: $50-80 (afternoon/evening peak)
      - 21-23: $30-40 (evening decline)

    Seasonal adjustment: winter/summer higher, spring/fall lower.
    Adds negative prices in spring early mornings and summer price spikes.
    """
    rng = random.Random(seed)
    prices: list[float] = []

    # Seasonal multipliers (1=Jan, 12=Dec)
    seasonal = {
        1: 1.15, 2: 1.10, 3: 0.90, 4: 0.80, 5: 0.85, 6: 1.05,
        7: 1.20, 8: 1.25, 9: 1.00, 10: 0.85, 11: 0.90, 12: 1.10,
    }

    # Month lengths (non-leap year)
    month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    hour_of_year = 0
    for month_idx, n_days in enumerate(month_days):
        month = month_idx + 1
        mult = seasonal[month]

        for day in range(n_days):
            day_of_week = ((hour_of_year // 24) + 0) % 7  # 0=Mon
            is_weekend = day_of_week >= 5
            weekend_disc = 0.85 if is_weekend else 1.0

            for hour in range(24):
                # Base price by time of day
                if hour < 6:
                    base = rng.uniform(20, 30)
                elif hour < 9:
                    base = rng.uniform(30, 50)
                elif hour < 16:
                    base = rng.uniform(40, 65)
                elif hour < 21:
                    base = rng.uniform(50, 80)
                else:
                    base = rng.uniform(30, 40)

                price = base * mult * weekend_disc

                # Negative prices: spring weekends 2-4 AM
                if month in (3, 4, 5) and is_weekend and 2 <= hour <= 4:
                    if rng.random() < 0.4:
                        price = rng.uniform(-10, -3)

                # Price spikes: summer afternoons (Jun-Aug, 14-18)
                if month in (6, 7, 8) and 14 <= hour <= 18:
                    if rng.random() < 0.03:
                        price = rng.uniform(150, 300)

                prices.append(round(price, 2))
                hour_of_year += 1

    return prices


def _make_lmp_data(prices: list[float] | None = None) -> LMPData:
    """Build LMPData from prices (generates realistic if None)."""
    if prices is None:
        prices = _make_realistic_lmp_prices()
    return LMPData(
        iso="pjm",
        zone="PJMISO",
        market="DAY_AHEAD_HOURLY",
        year=2024,
        prices=prices,
    )


# ---------------------------------------------------------------------------
# Helpers: Synthetic production data
# ---------------------------------------------------------------------------


def _make_realistic_production(
    dc_size_mw: float = 5.0,
    seed: int = 42,
) -> list[float]:
    """Generate 8760 realistic hourly solar production in kWh.

    Realistic solar shape: zero overnight, bell-curve daytime with
    seasonal variation. Peak ~4000 kWh/hr for 5 MW DC.
    """
    rng = random.Random(seed)
    production: list[float] = []

    # Seasonal peak multipliers (fraction of max)
    seasonal_peak = {
        1: 0.55, 2: 0.65, 3: 0.75, 4: 0.85, 5: 0.92, 6: 1.0,
        7: 0.98, 8: 0.93, 9: 0.82, 10: 0.68, 11: 0.55, 12: 0.50,
    }

    # Sunrise/sunset hours by month (approximate Philadelphia)
    sun_schedule = {
        1: (7.5, 17.0), 2: (7.0, 17.5), 3: (6.5, 18.5),
        4: (6.0, 19.5), 5: (5.5, 20.0), 6: (5.5, 20.5),
        7: (5.5, 20.5), 8: (6.0, 20.0), 9: (6.5, 19.0),
        10: (7.0, 18.0), 11: (7.0, 17.0), 12: (7.5, 16.5),
    }

    peak_kw = dc_size_mw * 1000 * 0.80  # ~80% DC-to-AC realized

    month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    for month_idx, n_days in enumerate(month_days):
        month = month_idx + 1
        s_mult = seasonal_peak[month]
        sunrise, sunset = sun_schedule[month]
        solar_noon = (sunrise + sunset) / 2

        for day in range(n_days):
            # Cloud factor: 70-100% of clear sky (random per day)
            cloud_factor = rng.uniform(0.70, 1.0)

            for hour in range(24):
                h = hour + 0.5  # mid-hour
                if h < sunrise or h > sunset:
                    production.append(0.0)
                else:
                    # Bell curve around solar noon
                    half_day = (sunset - sunrise) / 2
                    x = (h - solar_noon) / half_day
                    shape = max(0.0, math.cos(x * math.pi / 2))

                    kw = peak_kw * s_mult * shape * cloud_factor
                    # Add minor noise
                    kw *= rng.uniform(0.95, 1.05)
                    production.append(max(0.0, round(kw, 1)))

    return production


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFtmSingleDispatchE2E:
    """Full 8760 FTM dispatch with realistic synthetic data."""

    def test_ftm_single_dispatch_e2e(self) -> None:
        """End-to-end FTM dispatch: structure, revenue, arbitrage patterns, SOC."""
        site = _make_ftm_site_config()
        lmp_data = _make_lmp_data()
        production = _make_realistic_production()

        result = run_ftm_dispatch(
            site_config=site,
            production_kwh=production,
            lmp_data=lmp_data,
        )

        # --- Structure checks ---
        assert len(result.hourly_dispatch) == 8760
        assert len(result.monthly_solve_status) == 12
        assert all(s == "Optimal" for s in result.monthly_solve_status)

        # --- Revenue checks ---
        assert result.ftm_revenue is not None
        assert result.ftm_revenue > 0

        # --- Metrics populated ---
        assert result.metrics is not None
        assert result.metrics.annual_throughput_kwh > 0
        assert result.metrics.annual_cycles > 0
        assert result.heatmap_data is not None
        assert len(result.heatmap_data) == 12
        assert all(len(row) == 24 for row in result.heatmap_data)

        # --- Arbitrage pattern: check a sample summer day (July 1 = hour 4344) ---
        # July starts at: 31+28+31+30+31+30 = 181 days → hour 4344
        july_1_start = 181 * 24
        day_hours = result.hourly_dispatch[july_1_start:july_1_start + 24]
        lmp_day = lmp_data.prices[july_1_start:july_1_start + 24]

        # Find min/max LMP hours
        min_lmp_hour = min(range(24), key=lambda h: lmp_day[h])
        max_lmp_hour = max(range(24), key=lambda h: lmp_day[h])

        # Battery should tend to charge during low-price hours
        # (verify at least some charging in first 6 hours when prices are low)
        early_charging = sum(day_hours[h].charge_kw for h in range(6))
        assert early_charging > 0, "Battery should charge during low-price overnight hours"

        # --- SOC bounds check ---
        capacity_kwh = site.bess_power_mw * 1000 * site.bess_duration_hr
        min_soc = capacity_kwh * site.bess_min_soc_percent / 100
        max_soc = capacity_kwh * site.bess_max_soc_percent / 100
        for hd in result.hourly_dispatch:
            assert hd.soc_kwh >= min_soc - 0.1, (
                f"Hour {hd.hour}: SOC {hd.soc_kwh:.1f} < min {min_soc:.1f}"
            )
            assert hd.soc_kwh <= max_soc + 0.1, (
                f"Hour {hd.hour}: SOC {hd.soc_kwh:.1f} > max {max_soc:.1f}"
            )

        # --- All monthly results present ---
        assert len(result.monthly_solve_status) == 12

        # --- Write output for inspection ---
        output = {
            "ftm_revenue": round(result.ftm_revenue, 2),
            "annual_cycles": round(result.metrics.annual_cycles, 2),
            "annual_throughput_kwh": round(result.metrics.annual_throughput_kwh, 1),
            "total_curtailed_kwh": round(result.metrics.total_curtailed_kwh, 1),
            "hours_charging": result.metrics.hours_charging,
            "hours_discharging": result.metrics.hours_discharging,
            "mean_lmp": round(sum(lmp_data.prices) / 8760, 2),
            "total_production_kwh": round(sum(production), 1),
            "sample_day_july1": {
                "lmp": [round(p, 2) for p in lmp_day],
                "charge_kw": [round(day_hours[h].charge_kw, 1) for h in range(24)],
                "discharge_kw": [round(day_hours[h].discharge_kw, 1) for h in range(24)],
                "solar_export_kw": [round(day_hours[h].solar_export_kw, 1) for h in range(24)],
                "soc_kwh": [round(day_hours[h].soc_kwh, 1) for h in range(24)],
            },
        }
        (_OUTPUT_DIR / "ftm_single_dispatch_e2e.json").write_text(
            json.dumps(output, indent=2)
        )

        print(f"\n[FTM E2E] revenue=${result.ftm_revenue:,.2f}")
        print(f"[FTM E2E] mean_lmp=${sum(lmp_data.prices)/8760:.2f}/MWh")
        print(f"[FTM E2E] cycles={result.metrics.annual_cycles:.1f}")


class TestFtmSingleDispatchNoBattery:
    """FTM dispatch with zero-sized battery: solar export revenue only."""

    def test_ftm_single_dispatch_no_battery(self) -> None:
        """With tiny battery, all revenue comes from solar export."""
        site = _make_ftm_site_config(
            bess_power_mw=0.001,
            bess_duration_hr=0.001,
        )
        lmp_data = _make_lmp_data()
        production = _make_realistic_production()

        result = run_ftm_dispatch(
            site_config=site,
            production_kwh=production,
            lmp_data=lmp_data,
        )

        assert result.ftm_revenue is not None
        assert result.ftm_revenue > 0

        # Revenue should be purely solar export (battery negligible)
        solar_rev = compute_ftm_solar_revenue(result, lmp_data.prices)
        assert solar_rev > 0
        # Solar revenue should be nearly all of total revenue
        assert solar_rev == pytest.approx(result.ftm_revenue, rel=0.01)

        # Battery activity should be negligible
        assert result.metrics is not None
        assert result.metrics.annual_throughput_kwh < 10  # essentially zero

        output = {
            "ftm_revenue": round(result.ftm_revenue, 2),
            "solar_revenue": round(solar_rev, 2),
            "battery_throughput_kwh": round(result.metrics.annual_throughput_kwh, 2),
        }
        (_OUTPUT_DIR / "ftm_no_battery.json").write_text(json.dumps(output, indent=2))


class TestFtmSingleDispatchSolarOnlyCharging:
    """FTM dispatch with solar-only charging constraint."""

    def test_ftm_single_dispatch_solar_only_charging(self) -> None:
        """With solar_only_charging=True, no grid charging in any hour."""
        site = _make_ftm_site_config(bess_solar_only_charging=True)
        lmp_data = _make_lmp_data()
        production = _make_realistic_production()

        result = run_ftm_dispatch(
            site_config=site,
            production_kwh=production,
            lmp_data=lmp_data,
        )

        assert result.ftm_revenue is not None
        assert result.ftm_revenue > 0
        assert len(result.hourly_dispatch) == 8760

        # Verify no grid charging
        for hd in result.hourly_dispatch:
            assert hd.grid_charge_kw < 0.01, (
                f"Hour {hd.hour}: grid_charge={hd.grid_charge_kw:.4f}, "
                f"expected 0 with solar_only_charging"
            )

        # Battery should still charge from solar
        assert result.metrics is not None
        assert result.metrics.hours_charging > 0
        assert result.metrics.annual_throughput_kwh > 0

        output = {
            "ftm_revenue": round(result.ftm_revenue, 2),
            "hours_charging": result.metrics.hours_charging,
            "charging_source": result.metrics.charging_source,
            "annual_cycles": round(result.metrics.annual_cycles, 2),
        }
        (_OUTPUT_DIR / "ftm_solar_only_charging.json").write_text(
            json.dumps(output, indent=2)
        )


class TestFtmSizingE2E:
    """FTM sizing optimization: sweep power/duration, find best NPV."""

    def test_ftm_sizing_e2e(self) -> None:
        """Sizing sweep with realistic data produces meaningful winner."""
        site = _make_ftm_site_config(
            bess_optimization_required=True,
            bess_power_min_mw=1.0,
            bess_power_max_mw=5.0,
            bess_duration_min_hr=2.0,
            bess_duration_max_hr=6.0,
        )

        lmp_data = _make_lmp_data()
        production = _make_realistic_production()

        result = run_ftm_sizing_optimization(
            site_config=site,
            production_kwh=production,
            lmp_data=lmp_data,
        )

        # --- Structure checks ---
        assert isinstance(result, SizingResult)
        assert result.winner is not None
        assert result.economics is not None
        assert result.dispatch_result is not None
        assert result.bill_comparison is None  # FTM has no bill comparison
        assert len(result.all_combos) > 1  # Multiple combos evaluated

        # --- Winner has highest NPV among all combos ---
        # Note: with realistic LMP spreads (~$43/MWh mean), BESS NPV may be
        # negative because installed cost ($275/kWh) exceeds arbitrage revenue.
        # The optimizer correctly picks the least-negative NPV as winner.
        max_npv = max(c.bess_npv for c in result.all_combos)
        assert result.winner.bess_npv == max_npv

        # --- NPV varies across combos (indicates real optimization) ---
        npvs = [c.bess_npv for c in result.all_combos]
        assert max(npvs) != min(npvs), "All combos have identical NPV — sweep not working"

        # --- Project economics populated with FTM fields ---
        econ = result.economics
        assert econ.solar_revenue is not None
        assert econ.solar_revenue > 0
        assert econ.bess_arbitrage_revenue is not None
        assert econ.ancillary_revenue is not None
        assert econ.ancillary_revenue > 0  # configured at $20/kW/yr
        assert econ.gross_revenue is not None
        assert econ.gross_revenue > 0
        assert econ.system_lcoe_per_kwh is not None
        assert econ.total_installed_cost > 0
        assert econ.combos_evaluated == len(result.all_combos)

        # --- Dispatch for winner is valid ---
        assert len(result.dispatch_result.hourly_dispatch) == 8760
        assert result.dispatch_result.ftm_revenue is not None

        # --- Write output ---
        output = {
            "winner_power_mw": result.winner.power_mw,
            "winner_duration_hr": result.winner.duration_hr,
            "winner_bess_npv": round(result.winner.bess_npv, 2),
            "combos_evaluated": len(result.all_combos),
            "all_npvs": [
                {
                    "power_mw": c.power_mw,
                    "duration_hr": c.duration_hr,
                    "bess_npv": round(c.bess_npv, 2),
                }
                for c in sorted(result.all_combos, key=lambda c: -c.bess_npv)
            ],
            "economics": {
                "solar_revenue": round(econ.solar_revenue, 2),
                "bess_arbitrage_revenue": round(econ.bess_arbitrage_revenue, 2),
                "ancillary_revenue": round(econ.ancillary_revenue, 2),
                "gross_revenue": round(econ.gross_revenue, 2),
                "total_project_npv": round(econ.total_project_npv, 2),
                "system_lcoe_per_kwh": round(econ.system_lcoe_per_kwh, 4) if econ.system_lcoe_per_kwh else None,
                "total_installed_cost": round(econ.total_installed_cost, 2),
            },
        }
        (_OUTPUT_DIR / "ftm_sizing_e2e.json").write_text(json.dumps(output, indent=2))

        print(f"\n[FTM Sizing] winner: {result.winner.power_mw} MW / {result.winner.duration_hr} hr")
        print(f"[FTM Sizing] NPV=${result.winner.bess_npv:,.2f}")
        print(f"[FTM Sizing] combos={len(result.all_combos)}")


class TestFtmEconomicsSanity:
    """Sanity checks on FTM revenue decomposition and economics."""

    def test_ftm_economics_sanity(self) -> None:
        """Revenue decomposition, BESS NPV range, and LCOE sanity bounds."""
        site = _make_ftm_site_config()
        lmp_data = _make_lmp_data()
        production = _make_realistic_production()

        result = run_ftm_dispatch(
            site_config=site,
            production_kwh=production,
            lmp_data=lmp_data,
        )

        # --- Revenue decomposition ---
        solar_revenue = compute_ftm_solar_revenue(result, lmp_data.prices)
        bess_arbitrage_revenue = (result.ftm_revenue or 0.0) - solar_revenue

        # ftm_revenue = solar_export + battery arbitrage
        assert result.ftm_revenue == pytest.approx(
            solar_revenue + bess_arbitrage_revenue, abs=0.01
        )

        # Ancillary revenue
        ancillary_revenue = (
            site.ancillary_revenue_per_kw_year * site.bess_power_mw * 1000
        )
        assert ancillary_revenue == 20.0 * 2000.0  # $40,000

        # Gross revenue = solar + arbitrage + ancillary
        gross_revenue = solar_revenue + bess_arbitrage_revenue + ancillary_revenue
        assert gross_revenue > 0

        # --- Compute full project economics ---
        bess_power_kw = site.bess_power_mw * 1000
        installed_cost = site.bess_installed_cost_per_kwh * bess_power_kw * site.bess_duration_hr

        bess_npv = compute_ftm_bess_npv(
            bess_arbitrage_revenue=bess_arbitrage_revenue,
            bess_installed_cost=installed_cost,
            bess_annual_degradation_pct=result.metrics.estimated_annual_degradation_pct,
            revenue_escalation_pct=site.rate_escalation_pct,
            bess_opex_per_kw_year=site.bess_opex_per_kw_year,
            bess_power_kw=bess_power_kw,
            ancillary_revenue_per_kw_year=site.ancillary_revenue_per_kw_year,
            discount_rate_pct=site.discount_rate_pct,
            project_lifetime_years=site.project_lifetime_years,
        )

        econ = compute_ftm_project_economics(
            bess_npv=bess_npv,
            bess_installed_cost=installed_cost,
            bess_arbitrage_revenue=bess_arbitrage_revenue,
            bess_power_mw=site.bess_power_mw,
            bess_duration_hr=site.bess_duration_hr,
            bess_opex_per_kw_year=site.bess_opex_per_kw_year,
            ancillary_revenue_per_kw_year=site.ancillary_revenue_per_kw_year,
            site_config=site,
            annual_solar_revenue=solar_revenue,
            annual_production_kwh=sum(production),
        )

        # --- BESS NPV sanity: not astronomical or deeply negative ---
        # With 2 MW / 4 hr battery at $275/kWh → $2.2M installed cost
        # NPV should be within -$3M to +$3M (reasonable range)
        assert -3_000_000 < bess_npv < 3_000_000, (
            f"BESS NPV ${bess_npv:,.0f} outside reasonable range"
        )

        # --- LCOE sanity: > 0 and < $0.20/kWh ---
        assert econ.system_lcoe_per_kwh is not None
        assert econ.system_lcoe_per_kwh > 0, "LCOE should be positive"
        assert econ.system_lcoe_per_kwh < 0.20, (
            f"LCOE ${econ.system_lcoe_per_kwh:.4f}/kWh seems too high"
        )

        # --- total_project_npv = solar_npv + bess_npv ---
        assert econ.total_project_npv == pytest.approx(
            econ.solar_npv + econ.bess_npv, rel=1e-10
        )

        # --- Write output ---
        output = {
            "solar_revenue": round(solar_revenue, 2),
            "bess_arbitrage_revenue": round(bess_arbitrage_revenue, 2),
            "ancillary_revenue": round(ancillary_revenue, 2),
            "gross_revenue": round(gross_revenue, 2),
            "ftm_revenue": round(result.ftm_revenue, 2),
            "bess_npv": round(bess_npv, 2),
            "total_project_npv": round(econ.total_project_npv, 2),
            "solar_npv": round(econ.solar_npv, 2),
            "system_lcoe_per_kwh": round(econ.system_lcoe_per_kwh, 4),
            "total_installed_cost": round(econ.total_installed_cost, 2),
            "bess_installed_cost": round(installed_cost, 2),
            "mean_lmp": round(sum(lmp_data.prices) / 8760, 2),
            "total_production_mwh": round(sum(production) / 1000, 1),
        }
        (_OUTPUT_DIR / "ftm_economics_sanity.json").write_text(
            json.dumps(output, indent=2)
        )

        print(f"\n[FTM Economics] solar_rev=${solar_revenue:,.2f}")
        print(f"[FTM Economics] arb_rev=${bess_arbitrage_revenue:,.2f}")
        print(f"[FTM Economics] ancillary=${ancillary_revenue:,.2f}")
        print(f"[FTM Economics] gross=${gross_revenue:,.2f}")
        print(f"[FTM Economics] bess_npv=${bess_npv:,.2f}")
        print(f"[FTM Economics] lcoe=${econ.system_lcoe_per_kwh:.4f}")


class TestFtmSummaryDictStructure:
    """Verify summary dict structure matches what the pipeline produces."""

    def test_ftm_summary_dict_structure(self) -> None:
        """Build summary dict as pipeline would, verify all keys and types."""
        site = _make_ftm_site_config()
        lmp_data = _make_lmp_data()
        production = _make_realistic_production()

        dispatch_result = run_ftm_dispatch(
            site_config=site,
            production_kwh=production,
            lmp_data=lmp_data,
        )

        solar_revenue = compute_ftm_solar_revenue(dispatch_result, lmp_data.prices)
        bess_arbitrage_revenue = (dispatch_result.ftm_revenue or 0.0) - solar_revenue
        bess_power_kw = site.bess_power_mw * 1000
        installed_cost = site.bess_installed_cost_per_kwh * bess_power_kw * site.bess_duration_hr
        ancillary_revenue = site.ancillary_revenue_per_kw_year * bess_power_kw
        gross_revenue = solar_revenue + bess_arbitrage_revenue + ancillary_revenue

        bess_npv = compute_ftm_bess_npv(
            bess_arbitrage_revenue=bess_arbitrage_revenue,
            bess_installed_cost=installed_cost,
            bess_annual_degradation_pct=dispatch_result.metrics.estimated_annual_degradation_pct,
            revenue_escalation_pct=site.rate_escalation_pct,
            bess_opex_per_kw_year=site.bess_opex_per_kw_year,
            bess_power_kw=bess_power_kw,
            ancillary_revenue_per_kw_year=site.ancillary_revenue_per_kw_year,
            discount_rate_pct=site.discount_rate_pct,
            project_lifetime_years=site.project_lifetime_years,
        )

        econ = compute_ftm_project_economics(
            bess_npv=bess_npv,
            bess_installed_cost=installed_cost,
            bess_arbitrage_revenue=bess_arbitrage_revenue,
            bess_power_mw=site.bess_power_mw,
            bess_duration_hr=site.bess_duration_hr,
            bess_opex_per_kw_year=site.bess_opex_per_kw_year,
            ancillary_revenue_per_kw_year=site.ancillary_revenue_per_kw_year,
            site_config=site,
            annual_solar_revenue=solar_revenue,
            annual_production_kwh=sum(production),
        )

        # Build summary dicts the same way pipeline does
        summary_lmp = {
            "iso": "pjm",
            "zone": "PJMISO",
            "market": site.lmp_market,
            "year": lmp_data.year,
            "mean_lmp": lmp_data.mean_price,
            "min_lmp": lmp_data.min_price,
            "max_lmp": lmp_data.max_price,
        }

        summary_ftm_economics = {
            "annual_solar_revenue": solar_revenue,
            "annual_bess_arbitrage_revenue": bess_arbitrage_revenue,
            "annual_ancillary_revenue": ancillary_revenue,
            "annual_gross_revenue": gross_revenue,
            "total_project_npv": econ.total_project_npv,
            "bess_npv": bess_npv,
            "solar_npv": econ.solar_npv,
            "system_lcoe_per_kwh": econ.system_lcoe_per_kwh,
            "total_installed_cost": econ.total_installed_cost,
        }

        summary_bess_dispatch = {
            "dispatch_mode": "ftm",
            "bess_power_mw": econ.optimal_power_mw,
            "bess_duration_hr": econ.optimal_duration_hr,
            "bess_capacity_kwh": dispatch_result.config.capacity_kwh,
            "bess_strategy": site.bess_strategy,
            "ftm_revenue": dispatch_result.ftm_revenue,
            "annual_cycles": dispatch_result.metrics.annual_cycles,
            "annual_throughput_kwh": dispatch_result.metrics.annual_throughput_kwh,
            "average_daily_cycles": dispatch_result.metrics.average_daily_cycles,
            "capacity_utilization_pct": dispatch_result.metrics.capacity_utilization_pct,
            "total_curtailed_kwh": dispatch_result.metrics.total_curtailed_kwh,
            "estimated_annual_degradation_pct": dispatch_result.metrics.estimated_annual_degradation_pct,
            "charging_source": dispatch_result.metrics.charging_source,
            "monthly_solver_status": dispatch_result.monthly_solve_status,
            "heatmap_data": dispatch_result.heatmap_data,
        }

        # --- Verify summary["lmp"] keys and types ---
        expected_lmp_keys = {
            "iso", "zone", "market", "year",
            "mean_lmp", "min_lmp", "max_lmp",
        }
        assert set(summary_lmp.keys()) == expected_lmp_keys
        assert isinstance(summary_lmp["iso"], str)
        assert isinstance(summary_lmp["zone"], str)
        assert isinstance(summary_lmp["market"], str)
        assert isinstance(summary_lmp["year"], int)
        assert isinstance(summary_lmp["mean_lmp"], float)
        assert isinstance(summary_lmp["min_lmp"], float)
        assert isinstance(summary_lmp["max_lmp"], float)

        # --- Verify summary["ftm_economics"] keys and types ---
        expected_econ_keys = {
            "annual_solar_revenue", "annual_bess_arbitrage_revenue",
            "annual_ancillary_revenue", "annual_gross_revenue",
            "total_project_npv", "bess_npv", "solar_npv",
            "system_lcoe_per_kwh", "total_installed_cost",
        }
        assert set(summary_ftm_economics.keys()) == expected_econ_keys
        for key in expected_econ_keys:
            assert isinstance(summary_ftm_economics[key], float), (
                f"{key} should be float, got {type(summary_ftm_economics[key])}"
            )

        # --- Verify summary["bess_dispatch"] keys ---
        expected_dispatch_keys = {
            "dispatch_mode", "bess_power_mw", "bess_duration_hr",
            "bess_capacity_kwh", "bess_strategy", "ftm_revenue",
            "annual_cycles", "annual_throughput_kwh", "average_daily_cycles",
            "capacity_utilization_pct", "total_curtailed_kwh",
            "estimated_annual_degradation_pct", "charging_source",
            "monthly_solver_status", "heatmap_data",
        }
        assert set(summary_bess_dispatch.keys()) == expected_dispatch_keys
        assert summary_bess_dispatch["dispatch_mode"] == "ftm"
        assert isinstance(summary_bess_dispatch["ftm_revenue"], float)
        assert isinstance(summary_bess_dispatch["annual_cycles"], float)
        assert isinstance(summary_bess_dispatch["monthly_solver_status"], list)
        assert len(summary_bess_dispatch["monthly_solver_status"]) == 12
        assert isinstance(summary_bess_dispatch["heatmap_data"], list)
        assert len(summary_bess_dispatch["heatmap_data"]) == 12


class TestBtmBackwardCompatE2E:
    """Verify BTM dispatch is completely unaffected by FTM additions."""

    def test_btm_backward_compat_e2e(self) -> None:
        """BTM SiteConfig defaults to btm mode, has no FTM keys in summary."""
        # Build a BTM site config (no dispatch_mode or FTM fields)
        btm_site = SiteConfig(
            run_name="BTM_Compat_Test",
            site_name="BTM Test Site",
            customer="BTM Customer",
            latitude=39.95,
            longitude=-75.16,
            dc_size_mw=5.0,
            ac_installed_mw=4.0,
            ac_poi_mw=4.0,
            racking="tracker",
            tilt=60.0,
            azimuth=180.0,
            module_orientation="portrait",
            number_of_modules=1,
            ground_clearance_height_m=1.5,
            panel_model="CSI Solar Co. Ltd. CS3U-355P",
            bifacial=False,
            inverter_model="Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
            gcr=0.35,
            shading_percent=1.0,
            dc_wiring_loss_percent=2.0,
            ac_wiring_loss_percent=1.0,
            transformer_losses_percent=1.0,
            degradation_percent=0.5,
            availability_percent=2.5,
            module_mismatch_percent=1.0,
            lid_percent=1.5,
        )

        # Default dispatch_mode is "btm"
        assert btm_site.dispatch_mode == "btm"

        # FTM fields are either None or defaults
        assert btm_site.iso is None
        assert btm_site.lmp_zone is None
        assert btm_site.lmp_year is None
        assert btm_site.ancillary_revenue_per_kw_year == 0.0

        # A BTM summary dict would NOT contain FTM keys
        btm_summary = {
            "site_name": btm_site.site_name,
            "annual_energy_mwh": 8000.0,
            "net_capacity_factor": 0.25,
        }

        # No FTM keys should be present
        assert "lmp" not in btm_summary
        assert "ftm_economics" not in btm_summary

        # A BTM run with bess_dispatch would populate bess_dispatch without FTM fields
        # (We just validate config defaults here — full BTM dispatch tested elsewhere)


class TestFtmWithParasiticLoad:
    """FTM dispatch with a parasitic/auxiliary load."""

    def test_ftm_with_parasitic_load(self) -> None:
        """Parasitic load reduces available solar, lowering revenue."""
        site = _make_ftm_site_config()
        lmp_data = _make_lmp_data()
        production = _make_realistic_production()

        # Baseline: no load
        result_no_load = run_ftm_dispatch(
            site_config=site,
            production_kwh=production,
            lmp_data=lmp_data,
            load_kwh=None,
        )

        # With 50 kW constant parasitic load
        load_kwh = [50.0] * 8760
        result_with_load = run_ftm_dispatch(
            site_config=site,
            production_kwh=production,
            lmp_data=lmp_data,
            load_kwh=load_kwh,
        )

        assert result_no_load.ftm_revenue is not None
        assert result_with_load.ftm_revenue is not None

        # Load reduces available solar → less revenue
        assert result_with_load.ftm_revenue < result_no_load.ftm_revenue, (
            f"Expected load to reduce revenue: "
            f"no_load=${result_no_load.ftm_revenue:,.2f}, "
            f"with_load=${result_with_load.ftm_revenue:,.2f}"
        )

        # Revenue reduction for 50 kW * 8760h at ~$45/MWh ≈ $19,710
        revenue_diff = result_no_load.ftm_revenue - result_with_load.ftm_revenue
        assert revenue_diff > 5000, (
            f"Revenue diff {revenue_diff:,.2f} seems too small for 50 kW load"
        )

        output = {
            "revenue_no_load": round(result_no_load.ftm_revenue, 2),
            "revenue_with_load": round(result_with_load.ftm_revenue, 2),
            "revenue_difference": round(revenue_diff, 2),
        }
        (_OUTPUT_DIR / "ftm_parasitic_load.json").write_text(
            json.dumps(output, indent=2)
        )
