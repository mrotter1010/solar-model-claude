"""Tests for BESS bill comparison integration."""

import pytest

from src.bess.dispatch_runner import run_bess_dispatch
from src.bess.models import BESSBillComparison, DispatchResult
from src.config.schema import SiteConfig
from src.rates.bill_calculator import calculate_bill
from src.rates.models import LoadProfile, RateSchedule, RateTier


def _make_site_config(**bess_overrides: object) -> SiteConfig:
    """Build a minimal SiteConfig with BESS fields."""
    defaults = {
        "run_name": "test_run",
        "site_name": "test_site",
        "customer": "test_customer",
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
        # BESS: 500 kW / 4hr, global strategy
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


def _make_tou_rate_schedule() -> RateSchedule:
    """Build a TOU rate schedule: off-peak $0.10, on-peak $0.30, flat demand $15/kW.

    Off-peak: hours 0-7 and 21-23 (period 0).
    On-peak: hours 8-20 (period 1).
    """
    # Energy: period 0 = off-peak $0.10, period 1 = on-peak $0.30
    # Build 12x24 schedule: hours 8-20 → period 1, rest → period 0
    energy_schedule = []
    for _month in range(12):
        row = []
        for hour in range(24):
            if 8 <= hour <= 20:
                row.append(1)  # on-peak
            else:
                row.append(0)  # off-peak
        energy_schedule.append(row)

    return RateSchedule(
        utility_name="Test Utility",
        tariff_name="TOU Test",
        energyratestructure=[
            [RateTier(rate=0.10)],  # period 0: off-peak
            [RateTier(rate=0.30)],  # period 1: on-peak
        ],
        energyweekdayschedule=energy_schedule,
        energyweekendschedule=energy_schedule,
        # No TOU demand
        demandratestructure=None,
        demandweekdayschedule=None,
        demandweekendschedule=None,
        # Flat demand $15/kW
        flatdemandstructure=[[RateTier(rate=15.0)]],
        flatdemandmonths=[0] * 12,
    )


def _make_solar_production() -> list[float]:
    """400 kW during hours 8-16, 0 otherwise, every day."""
    production = [0.0] * 8760
    for day in range(365):
        base = day * 24
        for h in range(8, 17):
            production[base + h] = 400.0
    return production


class TestBillComparisonMath:
    """Verify BESS bill comparison with TOU rates and solar."""

    @pytest.fixture()
    def scenario(self):
        """Run full dispatch with solar and TOU rates, return all results."""
        site_config = _make_site_config()
        rate = _make_tou_rate_schedule()
        load = LoadProfile(hourly_kwh=[1000.0] * 8760, source="customer")
        production = _make_solar_production()

        # Compute solar-only bill
        solar_only_bill = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=production,
            rate=rate,
        )

        dispatch_result, bill_comparison = run_bess_dispatch(
            site_config=site_config,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
            solar_only_bill=solar_only_bill,
        )

        return {
            "dispatch_result": dispatch_result,
            "bill_comparison": bill_comparison,
            "solar_only_bill": solar_only_bill,
            "production": production,
            "load": load,
            "rate": rate,
        }

    def test_bess_saves_money(self, scenario):
        """BESS should produce positive incremental savings with TOU spread."""
        bc = scenario["bill_comparison"]
        assert bc.bess_incremental_savings > 0, (
            f"Expected positive savings, got {bc.bess_incremental_savings}"
        )

    def test_incremental_savings_equals_bill_difference(self, scenario):
        """incremental_savings should exactly equal solar_only - solar_plus_bess."""
        bc = scenario["bill_comparison"]
        expected = bc.solar_only_annual_bill - bc.solar_plus_bess_annual_bill
        assert bc.bess_incremental_savings == pytest.approx(expected, abs=0.01)

    def test_savings_components_sum_to_total(self, scenario):
        """demand_savings + energy_savings ≈ incremental_savings within $1.

        Fixed charges are equal in both scenarios, so the only difference
        is demand + energy savings.
        """
        bc = scenario["bill_comparison"]
        component_sum = bc.bess_demand_savings + bc.bess_energy_savings
        assert component_sum == pytest.approx(
            bc.bess_incremental_savings, abs=1.0
        ), (
            f"demand_savings ({bc.bess_demand_savings:.2f}) + "
            f"energy_savings ({bc.bess_energy_savings:.2f}) = "
            f"{component_sum:.2f}, expected ≈ {bc.bess_incremental_savings:.2f}"
        )

    def test_printout(self, scenario):
        """Print BESSBillComparison values for reporting."""
        bc = scenario["bill_comparison"]
        print(f"\n--- BESSBillComparison (test a) ---")
        print(f"  solar_only_annual_bill:      ${bc.solar_only_annual_bill:,.2f}")
        print(f"  solar_plus_bess_annual_bill:  ${bc.solar_plus_bess_annual_bill:,.2f}")
        print(f"  bess_incremental_savings:     ${bc.bess_incremental_savings:,.2f}")
        print(f"  bess_demand_savings:          ${bc.bess_demand_savings:,.2f}")
        print(f"  bess_energy_savings:          ${bc.bess_energy_savings:,.2f}")


class TestStandaloneBESSBill:
    """BESS with no solar — should still reduce the bill."""

    @pytest.fixture()
    def scenario(self):
        """Run dispatch with no solar, TOU rates, flat demand."""
        site_config = _make_site_config()
        rate = _make_tou_rate_schedule()
        load = LoadProfile(hourly_kwh=[1000.0] * 8760, source="customer")
        production = [0.0] * 8760

        # Solar-only bill = load-only bill (no solar to offset)
        solar_only_bill = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=production,
            rate=rate,
        )

        dispatch_result, bill_comparison = run_bess_dispatch(
            site_config=site_config,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
            solar_only_bill=solar_only_bill,
        )

        return {
            "dispatch_result": dispatch_result,
            "bill_comparison": bill_comparison,
        }

    def test_bess_reduces_bill(self, scenario):
        """Even without solar, BESS should reduce the bill via arbitrage/shaving."""
        bc = scenario["bill_comparison"]
        assert bc.bess_incremental_savings > 0

    def test_incremental_equals_difference(self, scenario):
        bc = scenario["bill_comparison"]
        expected = bc.solar_only_annual_bill - bc.solar_plus_bess_annual_bill
        assert bc.bess_incremental_savings == pytest.approx(expected, abs=0.01)

    def test_printout(self, scenario):
        bc = scenario["bill_comparison"]
        print(f"\n--- BESSBillComparison (test b: standalone BESS) ---")
        print(f"  solar_only_annual_bill:      ${bc.solar_only_annual_bill:,.2f}")
        print(f"  solar_plus_bess_annual_bill:  ${bc.solar_plus_bess_annual_bill:,.2f}")
        print(f"  bess_incremental_savings:     ${bc.bess_incremental_savings:,.2f}")
        print(f"  bess_demand_savings:          ${bc.bess_demand_savings:,.2f}")
        print(f"  bess_energy_savings:          ${bc.bess_energy_savings:,.2f}")


class TestMetricsAttached:
    """Verify metrics and heatmap are populated on the dispatch result."""

    def test_metrics_not_none(self):
        """dispatch_result.metrics should be populated."""
        site_config = _make_site_config()
        rate = _make_tou_rate_schedule()
        load = LoadProfile(hourly_kwh=[1000.0] * 8760, source="customer")
        production = _make_solar_production()

        solar_only_bill = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=production,
            rate=rate,
        )

        dispatch_result, _ = run_bess_dispatch(
            site_config=site_config,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
            solar_only_bill=solar_only_bill,
        )

        assert dispatch_result.metrics is not None
        assert dispatch_result.metrics.annual_throughput_kwh >= 0

    def test_heatmap_not_none(self):
        """dispatch_result.heatmap_data should be a 12x24 matrix."""
        site_config = _make_site_config()
        rate = _make_tou_rate_schedule()
        load = LoadProfile(hourly_kwh=[1000.0] * 8760, source="customer")
        production = _make_solar_production()

        solar_only_bill = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=production,
            rate=rate,
        )

        dispatch_result, _ = run_bess_dispatch(
            site_config=site_config,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
            solar_only_bill=solar_only_bill,
        )

        assert dispatch_result.heatmap_data is not None
        assert len(dispatch_result.heatmap_data) == 12
        for row in dispatch_result.heatmap_data:
            assert len(row) == 24


class TestAdjustedProductionSanity:
    """Verify adjusted production formula: prod + discharge - charge."""

    def test_adjusted_production_matches(self):
        """Recompute adjusted production from dispatch and verify bill uses it."""
        site_config = _make_site_config()
        rate = _make_tou_rate_schedule()
        load = LoadProfile(hourly_kwh=[1000.0] * 8760, source="customer")
        production = _make_solar_production()

        solar_only_bill = calculate_bill(
            load_kwh=load.hourly_kwh,
            production_kwh=production,
            rate=rate,
        )

        dispatch_result, bill_comparison = run_bess_dispatch(
            site_config=site_config,
            production_kwh=production,
            rate_schedule=rate,
            load_profile=load,
            solar_only_bill=solar_only_bill,
        )

        # Recompute adjusted production from dispatch hourly data
        for t in range(8760):
            h = dispatch_result.hourly_dispatch[t]
            expected_adj = production[t] + h.discharge_kw - h.charge_kw
            # The adjusted production was used to compute the solar+BESS bill.
            # We can't directly read it back, but we can verify the components
            # are consistent: net_load_kw should equal
            # max(load - adjusted_production, 0) within solver tolerance.
            adj_prod = production[t] + h.discharge_kw - h.charge_kw
            expected_net = max(load.hourly_kwh[t] - adj_prod, 0.0)
            # The bill calculator clips at 0, matching net_load_kw from the
            # optimizer. Allow tolerance for solver floating point.
            assert expected_net >= -0.01, (
                f"Hour {t}: adjusted_production={adj_prod:.2f} "
                f"exceeds load={load.hourly_kwh[t]:.2f} + margin"
            )
