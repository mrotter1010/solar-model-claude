"""Tests for rate engine Pydantic models and rate file parser."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.rates.models import (
    BillResult,
    BillSavings,
    FixedCharges,
    LoadProfile,
    MonthlyBillDetail,
    RateSchedule,
    RateTier,
)
from src.rates.rate_parser import RateFileError, load_rate_file, validate_rate_schedule

# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "rates"
SAMPLE_RATE_PATH = FIXTURE_DIR / "sample_rate.json"


def _make_schedule_12x24(value: int) -> list[list[int]]:
    """Build a 12x24 matrix filled with a single value."""
    return [[value] * 24 for _ in range(12)]


def _make_energy_only_rate() -> dict:
    """Build a minimal energy-only rate schedule dict."""
    return {
        "utility_name": "Test Utility",
        "tariff_name": "Flat Rate",
        "energyratestructure": [[{"rate": 0.10}]],
        "energyweekdayschedule": _make_schedule_12x24(0),
        "energyweekendschedule": _make_schedule_12x24(0),
    }


def _make_monthly_bill(month: int, **overrides: float) -> dict:
    """Build a MonthlyBillDetail dict with sensible defaults."""
    defaults = {
        "month": month,
        "energy_charges": 100.0,
        "demand_charges": 50.0,
        "flat_demand_charges": 20.0,
        "fixed_charges": 25.0,
        "production_kwh": 1000.0,
        "consumption_kwh": 2000.0,
        "peak_demand_kw": 50.0,
    }
    defaults.update(overrides)
    return defaults


def _make_bill_result(**overrides: float) -> BillResult:
    """Build a BillResult with 12 identical months."""
    monthly = [MonthlyBillDetail(**_make_monthly_bill(m + 1, **overrides)) for m in range(12)]
    return BillResult(monthly=monthly)


# ===================================================================
# RateTier tests
# ===================================================================


class TestRateTier:
    """Tests for RateTier model."""

    def test_valid_tier(self) -> None:
        tier = RateTier(rate=0.15)
        assert tier.rate == 0.15
        assert tier.max is None
        assert tier.adj == 0.0

    def test_tier_with_all_fields(self) -> None:
        tier = RateTier(rate=0.10, max=500.0, adj=0.02)
        assert tier.rate == 0.10
        assert tier.max == 500.0
        assert tier.adj == 0.02

    def test_negative_rate_rejected(self) -> None:
        with pytest.raises(ValidationError, match="rate"):
            RateTier(rate=-0.01)


# ===================================================================
# FixedCharges tests
# ===================================================================


class TestFixedCharges:
    """Tests for FixedCharges model."""

    def test_defaults(self) -> None:
        fc = FixedCharges()
        assert fc.fixed_charge_first_meter == 0.0
        assert fc.fixed_charge_units == "$/month"

    def test_valid_units(self) -> None:
        for unit in ("$/month", "$/day", "$/year"):
            fc = FixedCharges(fixed_charge_units=unit)
            assert fc.fixed_charge_units == unit

    def test_invalid_units_rejected(self) -> None:
        with pytest.raises(ValidationError, match="fixed_charge_units"):
            FixedCharges(fixed_charge_units="$/week")

    def test_negative_charge_rejected(self) -> None:
        with pytest.raises(ValidationError, match="fixed_charge_first_meter"):
            FixedCharges(fixed_charge_first_meter=-1.0)


# ===================================================================
# RateSchedule tests
# ===================================================================


class TestRateScheduleValid:
    """Tests for valid RateSchedule construction."""

    def test_energy_only_rate(self) -> None:
        """Minimal energy-only rate with no demand charges."""
        data = _make_energy_only_rate()
        rs = RateSchedule(**data)
        assert rs.utility_name == "Test Utility"
        assert rs.tariff_name == "Flat Rate"
        assert rs.source == "manual"
        assert rs.demandratestructure is None
        assert rs.flatdemandstructure is None
        assert len(rs.energyratestructure) == 1
        assert rs.energyratestructure[0][0].rate == 0.10

    def test_full_rate_with_demand_and_flat(self) -> None:
        """Full rate with energy, TOU demand, flat demand, and fixed charges."""
        data = _make_energy_only_rate()
        data.update({
            "demandratestructure": [[{"rate": 5.0}], [{"rate": 0.0}]],
            "demandweekdayschedule": _make_schedule_12x24(0),
            "demandweekendschedule": _make_schedule_12x24(1),
            "flatdemandstructure": [[{"rate": 20.0}]],
            "flatdemandmonths": [0] * 12,
            "fixed_charges": {
                "fixed_charge_first_meter": 25.0,
                "fixed_charge_units": "$/month",
            },
        })
        rs = RateSchedule(**data)
        assert rs.demandratestructure is not None
        assert len(rs.demandratestructure) == 2
        assert rs.flatdemandstructure is not None
        assert rs.flatdemandmonths == [0] * 12
        assert rs.fixed_charges.fixed_charge_first_meter == 25.0

    def test_multiple_energy_periods(self) -> None:
        """Rate schedule with 3 energy periods (peak, off-peak, winter)."""
        schedule_summer_peak = [
            [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1]
            if 5 <= m <= 8 else
            [2] * 24
            for m in range(12)
        ]
        data = {
            "utility_name": "Test Utility",
            "tariff_name": "TOU-3",
            "energyratestructure": [
                [{"rate": 0.15}],
                [{"rate": 0.08}],
                [{"rate": 0.07}],
            ],
            "energyweekdayschedule": schedule_summer_peak,
            "energyweekendschedule": _make_schedule_12x24(2),
        }
        rs = RateSchedule(**data)
        assert len(rs.energyratestructure) == 3

    def test_source_openei(self) -> None:
        """Rate with source='openei' and openei_label."""
        data = _make_energy_only_rate()
        data["source"] = "openei"
        data["openei_label"] = "5a3ab3215457a3f8408b456e"
        rs = RateSchedule(**data)
        assert rs.source == "openei"
        assert rs.openei_label == "5a3ab3215457a3f8408b456e"


class TestRateScheduleValidation:
    """Tests for RateSchedule validation failures."""

    def test_schedule_wrong_row_count(self) -> None:
        """Schedule with != 12 rows should fail."""
        data = _make_energy_only_rate()
        data["energyweekdayschedule"] = [[0] * 24 for _ in range(11)]
        with pytest.raises(ValidationError, match="12 rows"):
            RateSchedule(**data)

    def test_schedule_wrong_column_count(self) -> None:
        """Schedule row with != 24 columns should fail."""
        data = _make_energy_only_rate()
        data["energyweekdayschedule"] = [[0] * 23 for _ in range(12)]
        with pytest.raises(ValidationError, match="24 columns"):
            RateSchedule(**data)

    def test_schedule_invalid_period_index(self) -> None:
        """Period index >= len(ratestructure) should fail."""
        data = _make_energy_only_rate()
        # energyratestructure has 1 period (index 0 only), but schedule uses index 1
        bad_schedule = _make_schedule_12x24(0)
        bad_schedule[5][16] = 1
        data["energyweekdayschedule"] = bad_schedule
        with pytest.raises(ValidationError, match="out of range"):
            RateSchedule(**data)

    def test_demand_structure_without_schedules(self) -> None:
        """demandratestructure without schedules should fail."""
        data = _make_energy_only_rate()
        data["demandratestructure"] = [[{"rate": 5.0}]]
        with pytest.raises(ValidationError, match="provided together"):
            RateSchedule(**data)

    def test_demand_schedules_without_structure(self) -> None:
        """Demand schedules without demandratestructure should fail."""
        data = _make_energy_only_rate()
        data["demandweekdayschedule"] = _make_schedule_12x24(0)
        data["demandweekendschedule"] = _make_schedule_12x24(0)
        with pytest.raises(ValidationError, match="provided together"):
            RateSchedule(**data)

    def test_demand_partial_schedules(self) -> None:
        """demandratestructure + weekday only (no weekend) should fail."""
        data = _make_energy_only_rate()
        data["demandratestructure"] = [[{"rate": 5.0}]]
        data["demandweekdayschedule"] = _make_schedule_12x24(0)
        with pytest.raises(ValidationError, match="provided together"):
            RateSchedule(**data)

    def test_flat_demand_structure_without_months(self) -> None:
        """flatdemandstructure without flatdemandmonths should fail."""
        data = _make_energy_only_rate()
        data["flatdemandstructure"] = [[{"rate": 20.0}]]
        with pytest.raises(ValidationError, match="provided together"):
            RateSchedule(**data)

    def test_flat_demand_months_without_structure(self) -> None:
        """flatdemandmonths without flatdemandstructure should fail."""
        data = _make_energy_only_rate()
        data["flatdemandmonths"] = [0] * 12
        with pytest.raises(ValidationError, match="provided together"):
            RateSchedule(**data)

    def test_flat_demand_months_wrong_length(self) -> None:
        """flatdemandmonths with != 12 elements should fail."""
        data = _make_energy_only_rate()
        data["flatdemandstructure"] = [[{"rate": 20.0}]]
        data["flatdemandmonths"] = [0] * 10
        with pytest.raises(ValidationError, match="12 elements"):
            RateSchedule(**data)

    def test_flat_demand_months_invalid_index(self) -> None:
        """flatdemandmonths index >= len(flatdemandstructure) should fail."""
        data = _make_energy_only_rate()
        data["flatdemandstructure"] = [[{"rate": 20.0}]]
        # flatdemandstructure has 1 period, but month 5 points to index 1
        months = [0] * 12
        months[5] = 1
        data["flatdemandmonths"] = months
        with pytest.raises(ValidationError, match="out of range"):
            RateSchedule(**data)

    def test_invalid_source_rejected(self) -> None:
        """source other than 'manual' or 'openei' should fail."""
        data = _make_energy_only_rate()
        data["source"] = "utility_website"
        with pytest.raises(ValidationError, match="source"):
            RateSchedule(**data)

    def test_demand_schedule_invalid_period_index(self) -> None:
        """Demand schedule with invalid period index should fail."""
        data = _make_energy_only_rate()
        data["demandratestructure"] = [[{"rate": 5.0}]]
        bad_schedule = _make_schedule_12x24(0)
        bad_schedule[0][0] = 1  # Only 1 period, index 1 is invalid
        data["demandweekdayschedule"] = bad_schedule
        data["demandweekendschedule"] = _make_schedule_12x24(0)
        with pytest.raises(ValidationError, match="out of range"):
            RateSchedule(**data)


# ===================================================================
# LoadProfile tests
# ===================================================================


class TestLoadProfile:
    """Tests for LoadProfile model."""

    def test_valid_profile(self) -> None:
        """Valid 8760-hour profile with computed fields."""
        hourly = [10.0] * 8760
        lp = LoadProfile(hourly_kwh=hourly, source="customer")
        assert lp.annual_consumption_kwh == pytest.approx(87600.0)
        assert lp.peak_demand_kw == pytest.approx(10.0)
        assert lp.source == "customer"

    def test_computed_fields_vary(self) -> None:
        """Peak and annual should reflect actual hourly variation."""
        hourly = [5.0] * 8759 + [100.0]
        lp = LoadProfile(hourly_kwh=hourly, source="customer")
        assert lp.peak_demand_kw == pytest.approx(100.0)
        assert lp.annual_consumption_kwh == pytest.approx(5.0 * 8759 + 100.0)

    def test_wrong_length_rejected(self) -> None:
        """Profile with != 8760 hours should fail."""
        with pytest.raises(ValidationError, match="8760"):
            LoadProfile(hourly_kwh=[10.0] * 8000, source="customer")

    def test_typical_source_with_building_type(self) -> None:
        """Typical profile with building_type and scale_factor."""
        lp = LoadProfile(
            hourly_kwh=[5.0] * 8760,
            source="typical",
            building_type="MediumOffice",
            scale_factor=1.5,
        )
        assert lp.source == "typical"
        assert lp.building_type == "MediumOffice"
        assert lp.scale_factor == 1.5

    def test_invalid_source_rejected(self) -> None:
        """Source other than 'customer' or 'typical' should fail."""
        with pytest.raises(ValidationError, match="source"):
            LoadProfile(hourly_kwh=[5.0] * 8760, source="estimated")


# ===================================================================
# MonthlyBillDetail tests
# ===================================================================


class TestMonthlyBillDetail:
    """Tests for MonthlyBillDetail computed total."""

    def test_computed_total(self) -> None:
        """Total should be sum of all charge components."""
        bill = MonthlyBillDetail(**_make_monthly_bill(1))
        assert bill.total == pytest.approx(195.0)  # 100 + 50 + 20 + 25

    def test_zero_charges(self) -> None:
        """All-zero charges should yield zero total."""
        bill = MonthlyBillDetail(
            month=6,
            energy_charges=0.0,
            demand_charges=0.0,
            flat_demand_charges=0.0,
            fixed_charges=0.0,
            production_kwh=0.0,
            consumption_kwh=0.0,
            peak_demand_kw=0.0,
        )
        assert bill.total == pytest.approx(0.0)

    def test_month_bounds(self) -> None:
        """Month must be 1-12."""
        with pytest.raises(ValidationError, match="month"):
            MonthlyBillDetail(**_make_monthly_bill(0))
        with pytest.raises(ValidationError, match="month"):
            MonthlyBillDetail(**_make_monthly_bill(13))


# ===================================================================
# BillResult tests
# ===================================================================


class TestBillResult:
    """Tests for BillResult computed annual totals."""

    def test_computed_annual_totals(self) -> None:
        """Annual totals should be sums of 12 monthly values."""
        br = _make_bill_result()
        # Each month: energy=100, demand=50, flat_demand=20, fixed=25, total=195
        assert br.annual_energy_charges == pytest.approx(1200.0)
        assert br.annual_demand_charges == pytest.approx(600.0)
        assert br.annual_flat_demand_charges == pytest.approx(240.0)
        assert br.annual_fixed_charges == pytest.approx(300.0)
        assert br.annual_total == pytest.approx(2340.0)
        assert br.annual_consumption_kwh == pytest.approx(24000.0)
        assert br.annual_production_kwh == pytest.approx(12000.0)

    def test_wrong_month_count_rejected(self) -> None:
        """BillResult with != 12 monthly records should fail."""
        monthly = [MonthlyBillDetail(**_make_monthly_bill(m + 1)) for m in range(11)]
        with pytest.raises(ValidationError, match="12"):
            BillResult(monthly=monthly)


# ===================================================================
# BillSavings tests
# ===================================================================


class TestBillSavings:
    """Tests for BillSavings computed savings metrics."""

    def test_computed_savings(self) -> None:
        """Basic savings computation."""
        bill_without = _make_bill_result()
        # With solar: half the energy charges, same demand/fixed
        bill_with = _make_bill_result(energy_charges=50.0, production_kwh=1000.0)

        savings = BillSavings(
            bill_without_solar=bill_without,
            bill_with_solar=bill_with,
        )
        # Monthly savings: 195 - 145 = 50 per month
        assert len(savings.monthly_savings) == 12
        assert all(s == pytest.approx(50.0) for s in savings.monthly_savings)
        assert savings.annual_savings == pytest.approx(600.0)

        # savings_percent: 600 / 2340 * 100
        expected_pct = 600.0 / 2340.0 * 100
        assert savings.savings_percent == pytest.approx(expected_pct)

        # avoided_cost_per_kwh: 600 / (1000 * 12)
        assert savings.avoided_cost_per_kwh == pytest.approx(600.0 / 12000.0)

    def test_zero_production_avoided_cost(self) -> None:
        """avoided_cost_per_kwh should be 0 when production is 0."""
        bill_without = _make_bill_result()
        bill_with = _make_bill_result(production_kwh=0.0)

        savings = BillSavings(
            bill_without_solar=bill_without,
            bill_with_solar=bill_with,
        )
        assert savings.avoided_cost_per_kwh == pytest.approx(0.0)

    def test_demand_savings(self) -> None:
        """annual_demand_savings should reflect demand + flat demand reductions."""
        bill_without = _make_bill_result(demand_charges=50.0, flat_demand_charges=20.0)
        bill_with = _make_bill_result(
            demand_charges=30.0, flat_demand_charges=15.0, production_kwh=500.0
        )

        savings = BillSavings(
            bill_without_solar=bill_without,
            bill_with_solar=bill_with,
        )
        # (50+20)*12 - (30+15)*12 = 840 - 540 = 300
        assert savings.annual_demand_savings == pytest.approx(300.0)

    def test_zero_bill_without_solar(self) -> None:
        """savings_percent should be 0 when bill_without_solar total is 0."""
        bill_without = _make_bill_result(
            energy_charges=0.0,
            demand_charges=0.0,
            flat_demand_charges=0.0,
            fixed_charges=0.0,
        )
        bill_with = _make_bill_result(
            energy_charges=0.0,
            demand_charges=0.0,
            flat_demand_charges=0.0,
            fixed_charges=0.0,
            production_kwh=0.0,
        )

        savings = BillSavings(
            bill_without_solar=bill_without,
            bill_with_solar=bill_with,
        )
        assert savings.savings_percent == pytest.approx(0.0)


# ===================================================================
# rate_parser tests
# ===================================================================


class TestRateParser:
    """Tests for load_rate_file and validate_rate_schedule."""

    def test_load_sample_rate_fixture(self) -> None:
        """Load the sample PG&E B-20 fixture and verify key fields."""
        rs = load_rate_file(SAMPLE_RATE_PATH)
        assert rs.utility_name == "Pacific Gas & Electric Co"
        assert rs.tariff_name == "B-20 TOU"
        assert rs.source == "manual"
        assert rs.fixed_charges.fixed_charge_first_meter == 25.0
        # 3 energy periods: peak(0.15), off-peak(0.08), winter(0.07)
        assert len(rs.energyratestructure) == 3
        assert rs.energyratestructure[0][0].rate == pytest.approx(0.15)
        assert rs.energyratestructure[1][0].rate == pytest.approx(0.08)
        assert rs.energyratestructure[2][0].rate == pytest.approx(0.07)
        # TOU demand: 2 periods
        assert rs.demandratestructure is not None
        assert len(rs.demandratestructure) == 2
        assert rs.demandratestructure[0][0].rate == pytest.approx(5.0)
        # Flat demand: 2 periods (summer/winter)
        assert rs.flatdemandstructure is not None
        assert len(rs.flatdemandstructure) == 2
        assert rs.flatdemandmonths is not None
        # June-Sep (indices 5-8) should map to period 0 (summer)
        assert rs.flatdemandmonths[5] == 0
        assert rs.flatdemandmonths[8] == 0
        # Jan (index 0) should map to period 1 (winter)
        assert rs.flatdemandmonths[0] == 1

    def test_load_sample_rate_schedule_verification(self) -> None:
        """Verify the energy schedule has correct summer peak hours."""
        rs = load_rate_file(SAMPLE_RATE_PATH)
        # June (index 5) weekday: hours 16-19 should be period 0 (peak)
        for hour in [16, 17, 18, 19]:
            assert rs.energyweekdayschedule[5][hour] == 0
        # June weekday: hour 10 should be period 1 (off-peak summer)
        assert rs.energyweekdayschedule[5][10] == 1
        # January (index 0) weekday: all hours should be period 2 (winter)
        assert all(h == 2 for h in rs.energyweekdayschedule[0])
        # June weekend: no peak hours (all off-peak summer)
        assert all(h == 1 for h in rs.energyweekendschedule[5])

    def test_load_missing_file(self) -> None:
        """Loading a nonexistent file should raise RateFileError."""
        with pytest.raises(RateFileError, match="not found"):
            load_rate_file(Path("/nonexistent/rate.json"))

    def test_load_invalid_json(self, tmp_path: Path) -> None:
        """Loading invalid JSON should raise RateFileError."""
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("{not valid json")
        with pytest.raises(RateFileError, match="Invalid JSON"):
            load_rate_file(bad_file)

    def test_load_missing_required_fields(self, tmp_path: Path) -> None:
        """Loading JSON missing required fields should raise RateFileError."""
        incomplete = tmp_path / "incomplete.json"
        incomplete.write_text(json.dumps({"utility_name": "Test"}))
        with pytest.raises(RateFileError, match="validation failed"):
            load_rate_file(incomplete)

    def test_validate_rate_schedule_from_dict(self) -> None:
        """validate_rate_schedule should accept a valid dict."""
        data = _make_energy_only_rate()
        rs = validate_rate_schedule(data)
        assert rs.utility_name == "Test Utility"

    def test_validate_rate_schedule_invalid_dict(self) -> None:
        """validate_rate_schedule should raise on invalid dict."""
        with pytest.raises(RateFileError, match="validation failed"):
            validate_rate_schedule({"utility_name": "Test"})
