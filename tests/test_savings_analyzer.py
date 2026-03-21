"""Tests for savings analyzer: bill comparison with and without solar."""

import csv
from pathlib import Path

import pytest

from src.rates.models import RateSchedule
from src.rates.rate_parser import load_rate_file
from src.rates.savings_analyzer import analyze_savings
from src.rates.tou_mapper import get_month_ranges

# ---------------------------------------------------------------------------
# Paths & fixtures
# ---------------------------------------------------------------------------

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "rates"
SAMPLE_RATE_PATH = FIXTURE_DIR / "sample_rate.json"
OUTPUT_DIR = Path(__file__).parent / "test_results" / "savings_analyzer"

# Hours per month (non-leap year)
_DAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
_HOURS = [d * 24 for d in _DAYS]


@pytest.fixture
def sample_rate() -> RateSchedule:
    """Load the PG&E B-20 TOU sample rate fixture."""
    return load_rate_file(SAMPLE_RATE_PATH)


def _flat_load(kw: float) -> list[float]:
    """Return an 8760-hour flat load profile at the given kW."""
    return [kw] * 8760


def _seasonal_production(summer_kw: float, winter_kw: float) -> list[float]:
    """Build an 8760 production profile with seasonal variation.

    Summer months (Jun-Sep, month indices 5-8) use summer_kw.
    All other months use winter_kw.
    """
    production: list[float] = []
    for month_idx, hours in enumerate(_HOURS):
        kw = summer_kw if 5 <= month_idx <= 8 else winter_kw
        production.extend([kw] * hours)
    return production


# ===================================================================
# Basic savings: 100 kW load, 50 kW production
# ===================================================================


class TestBasicSavings:
    """Tests for analyze_savings with 100 kW load and 50 kW flat production."""

    def test_annual_savings_positive(self, sample_rate: RateSchedule) -> None:
        """Annual savings should be positive when production offsets load."""
        result = analyze_savings(_flat_load(100), [50.0] * 8760, sample_rate)
        assert result.annual_savings > 0

    def test_savings_percent_reasonable(self, sample_rate: RateSchedule) -> None:
        """Savings percent should be roughly 40-50%.

        50 kW offsets half the energy, but demand charges also drop
        proportionally for a flat load profile.
        """
        result = analyze_savings(_flat_load(100), [50.0] * 8760, sample_rate)
        assert 40.0 <= result.savings_percent <= 55.0

    def test_monthly_savings_length_and_positive(
        self, sample_rate: RateSchedule
    ) -> None:
        """monthly_savings should have 12 non-negative entries."""
        result = analyze_savings(_flat_load(100), [50.0] * 8760, sample_rate)
        assert len(result.monthly_savings) == 12
        assert all(s >= 0 for s in result.monthly_savings)

    def test_avoided_cost_positive(self, sample_rate: RateSchedule) -> None:
        """avoided_cost_per_kwh should be positive with production."""
        result = analyze_savings(_flat_load(100), [50.0] * 8760, sample_rate)
        assert result.avoided_cost_per_kwh > 0

    def test_demand_savings_positive(self, sample_rate: RateSchedule) -> None:
        """Demand savings should be positive (flat load halves peak)."""
        result = analyze_savings(_flat_load(100), [50.0] * 8760, sample_rate)
        assert result.annual_demand_savings > 0

    def test_bills_are_consistent(self, sample_rate: RateSchedule) -> None:
        """bill_without total should exceed bill_with total."""
        result = analyze_savings(_flat_load(100), [50.0] * 8760, sample_rate)
        assert result.bill_without_solar.annual_total > result.bill_with_solar.annual_total

    def test_write_output(self, sample_rate: RateSchedule) -> None:
        """Write savings details to CSV for manual inspection."""
        result = analyze_savings(_flat_load(100), [50.0] * 8760, sample_rate)

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # Monthly detail CSV
        detail_path = OUTPUT_DIR / "basic_savings_monthly.csv"
        with detail_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "month", "bill_without", "bill_with", "savings",
                "demand_without", "demand_with",
            ])
            for i in range(12):
                wo = result.bill_without_solar.monthly[i]
                ws = result.bill_with_solar.monthly[i]
                writer.writerow([
                    i + 1,
                    f"{wo.total:.2f}",
                    f"{ws.total:.2f}",
                    f"{result.monthly_savings[i]:.2f}",
                    f"{wo.demand_charges + wo.flat_demand_charges:.2f}",
                    f"{ws.demand_charges + ws.flat_demand_charges:.2f}",
                ])

        # Summary line
        summary_path = OUTPUT_DIR / "basic_savings_summary.csv"
        with summary_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "annual_without", "annual_with", "annual_savings",
                "savings_percent", "avoided_cost_per_kwh",
                "annual_demand_savings",
            ])
            writer.writerow([
                f"{result.bill_without_solar.annual_total:.2f}",
                f"{result.bill_with_solar.annual_total:.2f}",
                f"{result.annual_savings:.2f}",
                f"{result.savings_percent:.2f}",
                f"{result.avoided_cost_per_kwh:.4f}",
                f"{result.annual_demand_savings:.2f}",
            ])

        assert detail_path.exists()
        assert summary_path.exists()


# ===================================================================
# Zero production
# ===================================================================


class TestZeroProduction:
    """Tests for analyze_savings with zero production."""

    def test_zero_savings(self, sample_rate: RateSchedule) -> None:
        """Annual savings should be exactly 0 with no production."""
        result = analyze_savings(_flat_load(100), [0.0] * 8760, sample_rate)
        assert result.annual_savings == pytest.approx(0.0, abs=0.01)

    def test_zero_savings_percent(self, sample_rate: RateSchedule) -> None:
        """Savings percent should be 0 with no production."""
        result = analyze_savings(_flat_load(100), [0.0] * 8760, sample_rate)
        assert result.savings_percent == pytest.approx(0.0, abs=0.01)

    def test_zero_avoided_cost(self, sample_rate: RateSchedule) -> None:
        """avoided_cost_per_kwh should be 0 with no production."""
        result = analyze_savings(_flat_load(100), [0.0] * 8760, sample_rate)
        assert result.avoided_cost_per_kwh == pytest.approx(0.0, abs=0.001)

    def test_bills_equal(self, sample_rate: RateSchedule) -> None:
        """bill_with should equal bill_without when production is zero."""
        result = analyze_savings(_flat_load(100), [0.0] * 8760, sample_rate)
        for i in range(12):
            assert result.bill_with_solar.monthly[i].total == pytest.approx(
                result.bill_without_solar.monthly[i].total, abs=0.01
            )

    def test_zero_demand_savings(self, sample_rate: RateSchedule) -> None:
        """Demand savings should be 0 with no production."""
        result = analyze_savings(_flat_load(100), [0.0] * 8760, sample_rate)
        assert result.annual_demand_savings == pytest.approx(0.0, abs=0.01)


# ===================================================================
# Production equals load (100% offset)
# ===================================================================


class TestFullOffset:
    """Tests for analyze_savings when production equals load every hour."""

    def test_energy_charges_zero_with_solar(
        self, sample_rate: RateSchedule
    ) -> None:
        """Energy charges should be $0 when net load is 0 everywhere."""
        result = analyze_savings(_flat_load(100), [100.0] * 8760, sample_rate)
        assert result.bill_with_solar.annual_energy_charges == pytest.approx(
            0.0, abs=0.01
        )

    def test_demand_charges_zero_with_solar(
        self, sample_rate: RateSchedule
    ) -> None:
        """TOU and flat demand charges should be $0 when peak net load is 0."""
        result = analyze_savings(_flat_load(100), [100.0] * 8760, sample_rate)
        assert result.bill_with_solar.annual_demand_charges == pytest.approx(
            0.0, abs=0.01
        )
        assert result.bill_with_solar.annual_flat_demand_charges == pytest.approx(
            0.0, abs=0.01
        )

    def test_only_fixed_charges_remain(self, sample_rate: RateSchedule) -> None:
        """Bill with solar should equal 12 * fixed charge ($25) = $300."""
        result = analyze_savings(_flat_load(100), [100.0] * 8760, sample_rate)
        assert result.bill_with_solar.annual_total == pytest.approx(300.0, abs=0.01)

    def test_savings_percent_near_maximum(
        self, sample_rate: RateSchedule
    ) -> None:
        """Savings percent = (1 - 300/total_without) * 100.

        total_without = $91,356 (from Prompt 2 validation).
        savings_pct = (1 - 300/91356) * 100 ≈ 99.67%.
        """
        result = analyze_savings(_flat_load(100), [100.0] * 8760, sample_rate)
        expected_pct = (1 - 300.0 / result.bill_without_solar.annual_total) * 100
        assert result.savings_percent == pytest.approx(expected_pct, abs=0.01)

    def test_savings_percent_above_99(self, sample_rate: RateSchedule) -> None:
        """Savings should be above 99% when only fixed charges remain."""
        result = analyze_savings(_flat_load(100), [100.0] * 8760, sample_rate)
        assert result.savings_percent > 99.0


# ===================================================================
# Production exceeds load (NEM clamping)
# ===================================================================


class TestExcessProduction:
    """Tests for analyze_savings when production exceeds load."""

    def test_excess_same_as_full_offset(self, sample_rate: RateSchedule) -> None:
        """50 kW load + 100 kW production should match 100 kW + 100 kW case.

        Both result in net load = 0 everywhere (excess is curtailed).
        """
        result_excess = analyze_savings(
            _flat_load(50), [100.0] * 8760, sample_rate
        )
        result_full = analyze_savings(
            _flat_load(100), [100.0] * 8760, sample_rate
        )
        # bill_with should be $300 (fixed only) in both cases
        assert result_excess.bill_with_solar.annual_total == pytest.approx(
            300.0, abs=0.01
        )
        assert result_full.bill_with_solar.annual_total == pytest.approx(
            300.0, abs=0.01
        )

    def test_bill_without_differs(self, sample_rate: RateSchedule) -> None:
        """bill_without should differ since the loads differ (50 vs 100 kW)."""
        result_50 = analyze_savings(
            _flat_load(50), [100.0] * 8760, sample_rate
        )
        result_100 = analyze_savings(
            _flat_load(100), [100.0] * 8760, sample_rate
        )
        assert result_50.bill_without_solar.annual_total < (
            result_100.bill_without_solar.annual_total
        )


# ===================================================================
# Seasonal production pattern
# ===================================================================


class TestSeasonalProduction:
    """Tests for analyze_savings with seasonal production variation."""

    def test_summer_savings_exceed_winter(
        self, sample_rate: RateSchedule
    ) -> None:
        """Monthly savings in summer should exceed winter savings.

        Summer has 80 kW production (more offset) plus higher energy rates.
        Winter has 40 kW production.
        """
        production = _seasonal_production(summer_kw=80.0, winter_kw=40.0)
        result = analyze_savings(_flat_load(100), production, sample_rate)

        summer_savings = [result.monthly_savings[m] for m in [5, 6, 7, 8]]
        winter_savings = [result.monthly_savings[m] for m in [0, 1, 2, 3, 4, 9, 10, 11]]

        min_summer = min(summer_savings)
        max_winter = max(winter_savings)
        assert min_summer > max_winter, (
            f"Min summer savings ${min_summer:.2f} should exceed "
            f"max winter savings ${max_winter:.2f}"
        )

    def test_demand_savings_in_summer(self, sample_rate: RateSchedule) -> None:
        """Summer TOU demand savings should be positive (80 kW offset)."""
        production = _seasonal_production(summer_kw=80.0, winter_kw=40.0)
        result = analyze_savings(_flat_load(100), production, sample_rate)

        # Summer months should have lower demand charges with solar
        for m in [5, 6, 7, 8]:
            wo = result.bill_without_solar.monthly[m]
            ws = result.bill_with_solar.monthly[m]
            demand_without = wo.demand_charges + wo.flat_demand_charges
            demand_with = ws.demand_charges + ws.flat_demand_charges
            assert demand_with < demand_without, (
                f"Month {m + 1}: demand with solar (${demand_with:.2f}) "
                f"should be less than without (${demand_without:.2f})"
            )

    def test_annual_savings_positive(self, sample_rate: RateSchedule) -> None:
        """Annual savings should be positive with seasonal production."""
        production = _seasonal_production(summer_kw=80.0, winter_kw=40.0)
        result = analyze_savings(_flat_load(100), production, sample_rate)
        assert result.annual_savings > 0
        assert result.avoided_cost_per_kwh > 0

    def test_production_kwh_tracked(self, sample_rate: RateSchedule) -> None:
        """bill_with_solar should track correct annual production."""
        production = _seasonal_production(summer_kw=80.0, winter_kw=40.0)
        result = analyze_savings(_flat_load(100), production, sample_rate)
        expected = sum(production)
        assert result.bill_with_solar.annual_production_kwh == pytest.approx(
            expected, abs=1.0
        )
