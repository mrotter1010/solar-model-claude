"""Tests for TOU hour mapper and bill calculator."""

import csv
from pathlib import Path

import pytest

from src.rates.bill_calculator import calculate_bill, calculate_bill_without_solar
from src.rates.models import RateSchedule
from src.rates.rate_parser import load_rate_file
from src.rates.tou_mapper import get_month_ranges, map_hours_to_periods

# ---------------------------------------------------------------------------
# Paths & fixtures
# ---------------------------------------------------------------------------

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "rates"
SAMPLE_RATE_PATH = FIXTURE_DIR / "sample_rate.json"
OUTPUT_DIR = Path(__file__).parent / "test_results" / "bill_calculator"


@pytest.fixture
def sample_rate() -> RateSchedule:
    """Load the PG&E B-20 TOU sample rate fixture."""
    return load_rate_file(SAMPLE_RATE_PATH)


@pytest.fixture
def energy_only_rate() -> RateSchedule:
    """Build a minimal energy-only rate (no demand charges)."""
    return RateSchedule(
        utility_name="Test Utility",
        tariff_name="Flat Rate",
        energyratestructure=[[{"rate": 0.10}]],
        energyweekdayschedule=[[0] * 24 for _ in range(12)],
        energyweekendschedule=[[0] * 24 for _ in range(12)],
    )


def _flat_load(kw: float) -> list[float]:
    """Return an 8760-hour flat load profile at the given kW."""
    return [kw] * 8760


# Days per month (non-leap)
_DAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
_HOURS = [d * 24 for d in _DAYS]


# ===================================================================
# TOU Mapper: get_month_ranges
# ===================================================================


class TestGetMonthRanges:
    """Tests for get_month_ranges utility."""

    def test_returns_12_tuples(self) -> None:
        ranges = get_month_ranges()
        assert len(ranges) == 12

    def test_sum_to_8760(self) -> None:
        ranges = get_month_ranges()
        total = sum(end - start for start, end in ranges)
        assert total == 8760

    def test_ranges_are_contiguous(self) -> None:
        """Each month starts where the previous ended."""
        ranges = get_month_ranges()
        assert ranges[0][0] == 0
        for i in range(1, 12):
            assert ranges[i][0] == ranges[i - 1][1]

    def test_month_hours_match_days(self) -> None:
        """Each month range spans the correct number of hours."""
        ranges = get_month_ranges()
        for i, (start, end) in enumerate(ranges):
            assert end - start == _HOURS[i]


# ===================================================================
# TOU Mapper: map_hours_to_periods
# ===================================================================


class TestMapHoursToPeriods:
    """Tests for map_hours_to_periods."""

    def test_returns_8760_entries(self, sample_rate: RateSchedule) -> None:
        result = map_hours_to_periods(sample_rate)
        assert len(result) == 8760

    def test_january_hour_0(self, sample_rate: RateSchedule) -> None:
        """Hour 0 should be January, hour 0, Monday (day_of_week=0)."""
        result = map_hours_to_periods(sample_rate)
        h0 = result[0]
        assert h0["month"] == 1
        assert h0["hour"] == 0
        assert h0["day_of_week"] == 0  # Monday
        assert h0["is_weekend"] is False

    def test_first_saturday_is_weekend(self, sample_rate: RateSchedule) -> None:
        """First Saturday is Jan 6 (day index 5), hour 0 = hour 5*24=120."""
        result = map_hours_to_periods(sample_rate)
        # Jan 1 = Mon(0), Jan 2 = Tue(1), ..., Jan 6 = Sat(5)
        sat_start = 5 * 24  # hour 120
        h_sat = result[sat_start]
        assert h_sat["month"] == 1
        assert h_sat["day_of_week"] == 5
        assert h_sat["is_weekend"] is True

    def test_first_sunday_is_weekend(self, sample_rate: RateSchedule) -> None:
        """First Sunday is Jan 7 (day index 6), hour 0 = hour 6*24=144."""
        result = map_hours_to_periods(sample_rate)
        sun_start = 6 * 24
        h_sun = result[sun_start]
        assert h_sun["month"] == 1
        assert h_sun["day_of_week"] == 6
        assert h_sun["is_weekend"] is True

    def test_summer_weekday_peak_energy_period(
        self, sample_rate: RateSchedule
    ) -> None:
        """A summer weekday peak hour should map to energy period 0."""
        result = map_hours_to_periods(sample_rate)
        # Find first weekday in June (month_idx=5, starts on Friday)
        # June 1 = Friday (weekday), hour 16 = first peak hour
        june_start = sum(_HOURS[:5])  # hours before June
        june_day1_hour16 = june_start + 16
        h = result[june_day1_hour16]
        assert h["month"] == 6
        assert h["hour"] == 16
        assert h["is_weekend"] is False
        assert h["energy_period"] == 0  # summer peak

    def test_summer_weekday_offpeak_energy_period(
        self, sample_rate: RateSchedule
    ) -> None:
        """A summer weekday non-peak hour should map to energy period 1."""
        result = map_hours_to_periods(sample_rate)
        june_start = sum(_HOURS[:5])
        june_day1_hour10 = june_start + 10
        h = result[june_day1_hour10]
        assert h["month"] == 6
        assert h["hour"] == 10
        assert h["energy_period"] == 1  # summer off-peak

    def test_winter_weekend_energy_period(self, sample_rate: RateSchedule) -> None:
        """A winter weekend hour should map to energy period 2."""
        result = map_hours_to_periods(sample_rate)
        # First Saturday = Jan 6 (day 5), hour 10
        sat_hour10 = 5 * 24 + 10
        h = result[sat_hour10]
        assert h["month"] == 1
        assert h["is_weekend"] is True
        assert h["energy_period"] == 2  # winter

    def test_demand_period_none_for_energy_only(
        self, energy_only_rate: RateSchedule
    ) -> None:
        """Demand period should be None when rate has no demand schedules."""
        result = map_hours_to_periods(energy_only_rate)
        assert all(h["demand_period"] is None for h in result)

    def test_demand_period_present_with_demand_rate(
        self, sample_rate: RateSchedule
    ) -> None:
        """Demand period should be an int when rate has demand schedules."""
        result = map_hours_to_periods(sample_rate)
        assert all(isinstance(h["demand_period"], int) for h in result)

    def test_summer_weekday_demand_peak_period(
        self, sample_rate: RateSchedule
    ) -> None:
        """Summer weekday peak demand hour should map to demand period 0."""
        result = map_hours_to_periods(sample_rate)
        june_start = sum(_HOURS[:5])
        h = result[june_start + 16]  # June 1, hour 16
        assert h["demand_period"] == 0  # peak demand

    def test_winter_demand_period(self, sample_rate: RateSchedule) -> None:
        """Winter hours should map to demand period 1 ($0 rate)."""
        result = map_hours_to_periods(sample_rate)
        h = result[0]  # January 1, hour 0
        assert h["demand_period"] == 1


# ===================================================================
# Bill Calculator: flat load, zero production
# ===================================================================

# Expected values hand-calculated from the sample rate fixture.
# Month start days: Jan=Mon, Feb=Thu, Mar=Thu, Apr=Sun, May=Tue,
#   Jun=Fri, Jul=Sun, Aug=Wed, Sep=Sat, Oct=Mon, Nov=Thu, Dec=Sat
#
# Winter months (Jan-May, Oct-Dec): all hours at $0.07/kWh.
#   Energy = days * 24 * 100 * 0.07.
#   TOU demand = $0 (period 1 at $0/kW). Flat demand = 100 * $18 = $1800.
#
# Summer months (Jun-Sep): weekday peak (4h at $0.15) + offpeak (20h at $0.08),
#   weekend all at $0.08. TOU demand = 100 * $5 = $500.
#   Flat demand = 100 * $20 = $2000.
#
# Fixed = $25/month every month.

# Weekday/weekend counts per month (non-leap, Mon Jan 1):
_WEEKDAYS = [23, 20, 22, 21, 23, 21, 22, 23, 20, 23, 22, 21]
_WEEKENDS = [8, 8, 9, 9, 8, 9, 9, 8, 10, 8, 8, 10]

# Expected energy charges at 100 kW flat
_WINTER_MONTHS = {0, 1, 2, 3, 4, 9, 10, 11}  # 0-indexed
_EXPECTED_ENERGY: list[float] = []
for _m in range(12):
    if _m in _WINTER_MONTHS:
        _EXPECTED_ENERGY.append(_DAYS[_m] * 24 * 100 * 0.07)
    else:
        # Summer: weekday = 4h*$0.15 + 20h*$0.08 = $220/day at 100kW
        #         weekend = 24h*$0.08 = $192/day at 100kW
        _wd = _WEEKDAYS[_m] * (4 * 100 * 0.15 + 20 * 100 * 0.08)
        _we = _WEEKENDS[_m] * (24 * 100 * 0.08)
        _EXPECTED_ENERGY.append(_wd + _we)


class TestBillCalculatorFlatLoad:
    """Tests for calculate_bill with 100 kW flat load, zero production."""

    def test_monthly_energy_charges(self, sample_rate: RateSchedule) -> None:
        """Verify energy charges match hand-calculated values per month."""
        bill = calculate_bill(_flat_load(100), [0.0] * 8760, sample_rate)
        for i, detail in enumerate(bill.monthly):
            assert detail.energy_charges == pytest.approx(
                _EXPECTED_ENERGY[i], abs=0.01
            ), f"Month {i + 1} energy mismatch"

    def test_winter_demand_charges_zero(self, sample_rate: RateSchedule) -> None:
        """Winter TOU demand charges should be $0 (period 1 rate = $0/kW)."""
        bill = calculate_bill(_flat_load(100), [0.0] * 8760, sample_rate)
        for m in [0, 1, 2, 3, 4, 9, 10, 11]:  # winter months
            # Period 1 has $0 rate, period 0 not active in winter
            # So TOU demand = 100 * $0 = $0. But peak also in period 1 at $0.
            assert bill.monthly[m].demand_charges == pytest.approx(0.0, abs=0.01), (
                f"Month {m + 1} winter demand should be $0"
            )

    def test_summer_tou_demand_charges(self, sample_rate: RateSchedule) -> None:
        """Summer TOU demand should be 100 kW * $5/kW = $500."""
        bill = calculate_bill(_flat_load(100), [0.0] * 8760, sample_rate)
        for m in [5, 6, 7, 8]:  # summer months
            assert bill.monthly[m].demand_charges == pytest.approx(500.0, abs=0.01), (
                f"Month {m + 1} summer demand should be $500"
            )

    def test_flat_demand_charges(self, sample_rate: RateSchedule) -> None:
        """Flat demand = 100 kW * $20 (summer) or $18 (winter)."""
        bill = calculate_bill(_flat_load(100), [0.0] * 8760, sample_rate)
        for m in range(12):
            if m in _WINTER_MONTHS:
                expected = 100 * 18.0
            else:
                expected = 100 * 20.0
            assert bill.monthly[m].flat_demand_charges == pytest.approx(
                expected, abs=0.01
            ), f"Month {m + 1} flat demand mismatch"

    def test_fixed_charges(self, sample_rate: RateSchedule) -> None:
        """Fixed charges should be $25/month every month."""
        bill = calculate_bill(_flat_load(100), [0.0] * 8760, sample_rate)
        for detail in bill.monthly:
            assert detail.fixed_charges == pytest.approx(25.0, abs=0.01)

    def test_annual_fixed_charges(self, sample_rate: RateSchedule) -> None:
        """Annual fixed charges = $25 * 12 = $300."""
        bill = calculate_bill(_flat_load(100), [0.0] * 8760, sample_rate)
        assert bill.annual_fixed_charges == pytest.approx(300.0, abs=0.01)

    def test_peak_demand_kw(self, sample_rate: RateSchedule) -> None:
        """Peak demand should be 100 kW for every month (flat load)."""
        bill = calculate_bill(_flat_load(100), [0.0] * 8760, sample_rate)
        for detail in bill.monthly:
            assert detail.peak_demand_kw == pytest.approx(100.0, abs=0.01)

    def test_monthly_totals_match_expected(
        self, sample_rate: RateSchedule
    ) -> None:
        """Verify each monthly total matches the hand-calculated value."""
        bill = calculate_bill(_flat_load(100), [0.0] * 8760, sample_rate)
        expected_totals = [
            7033.00, 6529.00, 7033.00, 6865.00, 7033.00,
            8873.00, 9093.00, 9121.00, 8845.00,
            7033.00, 6865.00, 7033.00,
        ]
        for i, detail in enumerate(bill.monthly):
            assert detail.total == pytest.approx(expected_totals[i], abs=0.01), (
                f"Month {i + 1}: expected ${expected_totals[i]}, got ${detail.total}"
            )

    def test_annual_total(self, sample_rate: RateSchedule) -> None:
        """Annual total should be $91,356."""
        bill = calculate_bill(_flat_load(100), [0.0] * 8760, sample_rate)
        assert bill.annual_total == pytest.approx(91356.0, abs=1.0)

    def test_write_output_csv(self, sample_rate: RateSchedule) -> None:
        """Write monthly bill details to CSV for manual inspection."""
        bill = calculate_bill(_flat_load(100), [0.0] * 8760, sample_rate)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / "flat_100kw_zero_production.csv"
        with output_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "month", "energy_charges", "demand_charges",
                "flat_demand_charges", "fixed_charges", "total",
            ])
            for detail in bill.monthly:
                writer.writerow([
                    detail.month,
                    f"{detail.energy_charges:.2f}",
                    f"{detail.demand_charges:.2f}",
                    f"{detail.flat_demand_charges:.2f}",
                    f"{detail.fixed_charges:.2f}",
                    f"{detail.total:.2f}",
                ])
        assert output_path.exists()


# ===================================================================
# Bill Calculator: flat load with flat production
# ===================================================================


class TestBillCalculatorWithProduction:
    """Tests for calculate_bill with production offsetting load."""

    def test_half_production_halves_energy_charges(
        self, sample_rate: RateSchedule
    ) -> None:
        """50 kW production against 100 kW load → 50 kW net → half energy."""
        bill_zero = calculate_bill(_flat_load(100), [0.0] * 8760, sample_rate)
        bill_half = calculate_bill(_flat_load(100), [50.0] * 8760, sample_rate)
        for i in range(12):
            expected = bill_zero.monthly[i].energy_charges / 2
            assert bill_half.monthly[i].energy_charges == pytest.approx(
                expected, abs=0.01
            ), f"Month {i + 1} energy not halved"

    def test_half_production_halves_demand(
        self, sample_rate: RateSchedule
    ) -> None:
        """50 kW net load → 50 kW peak demand → half demand charges."""
        bill_half = calculate_bill(_flat_load(100), [50.0] * 8760, sample_rate)
        for m in [5, 6, 7, 8]:  # summer months
            assert bill_half.monthly[m].demand_charges == pytest.approx(
                250.0, abs=0.01
            ), f"Month {m + 1} demand should be 50*5=250"

    def test_half_production_peak_demand_kw(
        self, sample_rate: RateSchedule
    ) -> None:
        """Peak demand should be 50 kW when net load is 50 kW flat."""
        bill_half = calculate_bill(_flat_load(100), [50.0] * 8760, sample_rate)
        for detail in bill_half.monthly:
            assert detail.peak_demand_kw == pytest.approx(50.0, abs=0.01)

    def test_production_kwh_tracked(self, sample_rate: RateSchedule) -> None:
        """Monthly production_kwh should reflect actual production."""
        bill = calculate_bill(_flat_load(100), [50.0] * 8760, sample_rate)
        for i, detail in enumerate(bill.monthly):
            expected = _HOURS[i] * 50.0
            assert detail.production_kwh == pytest.approx(expected, abs=0.01)


# ===================================================================
# Bill Calculator: zero load
# ===================================================================


class TestBillCalculatorZeroLoad:
    """Tests for calculate_bill with zero load."""

    def test_zero_load_zero_production(self, sample_rate: RateSchedule) -> None:
        """All charges except fixed should be zero."""
        bill = calculate_bill([0.0] * 8760, [0.0] * 8760, sample_rate)
        for detail in bill.monthly:
            assert detail.energy_charges == pytest.approx(0.0, abs=0.01)
            assert detail.demand_charges == pytest.approx(0.0, abs=0.01)
            assert detail.flat_demand_charges == pytest.approx(0.0, abs=0.01)
            assert detail.fixed_charges == pytest.approx(25.0, abs=0.01)
            assert detail.total == pytest.approx(25.0, abs=0.01)
        assert bill.annual_total == pytest.approx(300.0, abs=0.01)


# ===================================================================
# Bill Calculator: net load clamping
# ===================================================================


class TestBillCalculatorNetLoadClamping:
    """Tests for net load floor clamping."""

    def test_production_exceeds_load_clamped(
        self, sample_rate: RateSchedule
    ) -> None:
        """When production > load, net load clamps to 0 (no NEM credit)."""
        # 50 kW load, 100 kW production → net should be 0, not -50
        bill = calculate_bill(_flat_load(50), [100.0] * 8760, sample_rate)
        for detail in bill.monthly:
            assert detail.energy_charges == pytest.approx(0.0, abs=0.01)
            assert detail.peak_demand_kw == pytest.approx(0.0, abs=0.01)

    def test_demand_based_on_net_load_not_gross(
        self, sample_rate: RateSchedule
    ) -> None:
        """Demand should reflect net load, not gross load."""
        # Variable load/production: load alternates 100/0 kW each hour,
        # production is constant 50 kW. Net load = max(load-50, 0) = 50/0.
        load = [100.0 if h % 2 == 0 else 0.0 for h in range(8760)]
        prod = [50.0] * 8760
        bill = calculate_bill(load, prod, sample_rate)
        # Peak net load should be 50 kW (not 100 kW)
        for detail in bill.monthly:
            assert detail.peak_demand_kw == pytest.approx(50.0, abs=0.01)


# ===================================================================
# Bill Calculator: consistency checks
# ===================================================================


class TestBillCalculatorConsistency:
    """Cross-validation tests for the bill calculator."""

    def test_without_solar_matches_zero_production(
        self, sample_rate: RateSchedule
    ) -> None:
        """calculate_bill_without_solar should match calculate_bill with zeros."""
        load = _flat_load(100)
        bill_a = calculate_bill_without_solar(load, sample_rate)
        bill_b = calculate_bill(load, [0.0] * 8760, sample_rate)
        assert bill_a.annual_total == pytest.approx(bill_b.annual_total, abs=0.01)
        for i in range(12):
            assert bill_a.monthly[i].total == pytest.approx(
                bill_b.monthly[i].total, abs=0.01
            )

    def test_energy_only_rate_no_demand(
        self, energy_only_rate: RateSchedule
    ) -> None:
        """Energy-only rate should produce zero demand charges."""
        bill = calculate_bill(_flat_load(100), [0.0] * 8760, energy_only_rate)
        assert bill.annual_demand_charges == pytest.approx(0.0)
        assert bill.annual_flat_demand_charges == pytest.approx(0.0)
        # Energy = 8760 * 100 * $0.10 = $87,600
        assert bill.annual_energy_charges == pytest.approx(87600.0, abs=0.01)

    def test_annual_consumption_kwh(self, sample_rate: RateSchedule) -> None:
        """Annual consumption should equal sum of hourly load."""
        bill = calculate_bill(_flat_load(100), [0.0] * 8760, sample_rate)
        assert bill.annual_consumption_kwh == pytest.approx(876000.0, abs=0.01)

    def test_annual_production_kwh_with_solar(
        self, sample_rate: RateSchedule
    ) -> None:
        """Annual production should equal sum of hourly production."""
        bill = calculate_bill(_flat_load(100), [50.0] * 8760, sample_rate)
        assert bill.annual_production_kwh == pytest.approx(438000.0, abs=0.01)
