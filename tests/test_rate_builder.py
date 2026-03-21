"""Tests for rate schedule builder."""

import json
from pathlib import Path

import pytest

from src.rates.rate_builder import (
    RateBuilderError,
    build_and_save,
    build_rate_schedule,
    save_rate_file,
)
from src.rates.rate_parser import load_rate_file

RESULTS_DIR = Path("tests/test_results/rate_builder")

# -- Shared fixtures ----------------------------------------------------------


SUMMER_WINTER_SEASONS = {
    "summer": [6, 7, 8, 9],
    "winter": [1, 2, 3, 4, 5, 10, 11, 12],
}

PEAK_OFFPEAK_PERIODS = {
    "peak": {
        "weekday_hours": [16, 17, 18, 19],
        "weekend_hours": [],
    },
    "off_peak": {
        "weekday_hours": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 20, 21, 22, 23],
        "weekend_hours": list(range(24)),
    },
}

TWO_SEASON_ENERGY_RATES = {
    "summer": {"peak": 0.15, "off_peak": 0.08},
    "winter": {"peak": 0.12, "off_peak": 0.07},
}

TWO_SEASON_DEMAND_RATES = {
    "summer": {"peak": 5.00, "off_peak": 0.00},
    "winter": {"peak": 3.00, "off_peak": 0.00},
}

FLAT_DEMAND_RATES = {"summer": 20.00, "winter": 18.00}


# -- Tests ---------------------------------------------------------------------


class TestBuildSimpleTwoSeasonTOU:
    """Test build_rate_schedule with summer/winter, peak/off-peak."""

    @pytest.fixture()
    def rate(self) -> "from src.rates.models import RateSchedule":
        """Build a two-season TOU rate schedule."""
        return build_rate_schedule(
            utility_name="Test Utility",
            tariff_name="TOU-Test",
            seasons=SUMMER_WINTER_SEASONS,
            tou_periods=PEAK_OFFPEAK_PERIODS,
            energy_rates=TWO_SEASON_ENERGY_RATES,
            fixed_charge_monthly=25.00,
        )

    def test_returns_valid_rate_schedule(self, rate) -> None:
        """Builder should return a valid RateSchedule model."""
        from src.rates.models import RateSchedule

        assert isinstance(rate, RateSchedule)
        assert rate.utility_name == "Test Utility"
        assert rate.tariff_name == "TOU-Test"

    def test_energy_period_count(self, rate) -> None:
        """2 seasons x 2 TOU periods = 4 energy periods."""
        assert len(rate.energyratestructure) == 4

    def test_summer_peak_rate(self, rate) -> None:
        """Summer peak energy rate should be $0.15/kWh."""
        # Period indices: summer×peak=0, summer×off_peak=1,
        # winter×peak=2, winter×off_peak=3
        assert rate.energyratestructure[0][0].rate == 0.15

    def test_winter_offpeak_rate(self, rate) -> None:
        """Winter off-peak energy rate should be $0.07/kWh."""
        assert rate.energyratestructure[3][0].rate == 0.07

    def test_schedule_dimensions(self, rate) -> None:
        """Schedule matrices should be 12 rows x 24 columns."""
        assert len(rate.energyweekdayschedule) == 12
        assert len(rate.energyweekendschedule) == 12
        for row in rate.energyweekdayschedule:
            assert len(row) == 24
        for row in rate.energyweekendschedule:
            assert len(row) == 24

    def test_june_weekday_hour17_is_summer_peak(self, rate) -> None:
        """June (month 6, index 5) weekday hour 17 should map to summer peak (period 0)."""
        assert rate.energyweekdayschedule[5][17] == 0

    def test_december_weekend_hour10_is_winter_offpeak(self, rate) -> None:
        """December (month 12, index 11) weekend hour 10 should map to winter off-peak (period 3)."""
        assert rate.energyweekendschedule[11][10] == 3

    def test_fixed_charge(self, rate) -> None:
        """Fixed charge should be $25/month."""
        assert rate.fixed_charges.fixed_charge_first_meter == 25.00

    def test_no_demand_charges(self, rate) -> None:
        """Without demand_rates, demand structures should be None."""
        assert rate.demandratestructure is None
        assert rate.demandweekdayschedule is None
        assert rate.demandweekendschedule is None

    def test_write_output_for_inspection(self, rate) -> None:
        """Write the built rate to test_results for manual inspection."""
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        output_path = RESULTS_DIR / "built_rate.json"
        save_rate_file(rate, output_path)
        assert output_path.exists()
        # Verify it's valid JSON
        data = json.loads(output_path.read_text())
        assert data["utility_name"] == "Test Utility"


class TestBuildFlatRate:
    """Test a flat rate with no TOU differentiation."""

    def test_flat_rate_single_period(self) -> None:
        """Single season + single period should produce 1 energy period."""
        rate = build_rate_schedule(
            utility_name="Flat Utility",
            tariff_name="Flat-Rate",
            seasons={"annual": list(range(1, 13))},
            tou_periods={
                "all": {
                    "weekday_hours": list(range(24)),
                    "weekend_hours": list(range(24)),
                }
            },
            energy_rates={"annual": {"all": 0.10}},
        )
        # 1 season × 1 period = 1 energy period
        assert len(rate.energyratestructure) == 1
        assert rate.energyratestructure[0][0].rate == 0.10

        # All schedule entries should be period 0
        for row in rate.energyweekdayschedule:
            assert all(v == 0 for v in row)
        for row in rate.energyweekendschedule:
            assert all(v == 0 for v in row)


class TestBuildWithDemandCharges:
    """Test building with TOU demand and flat demand charges."""

    @pytest.fixture()
    def rate(self):
        """Build a rate with energy, demand, and flat demand charges."""
        return build_rate_schedule(
            utility_name="Demand Utility",
            tariff_name="Demand-TOU",
            seasons=SUMMER_WINTER_SEASONS,
            tou_periods=PEAK_OFFPEAK_PERIODS,
            energy_rates=TWO_SEASON_ENERGY_RATES,
            demand_rates=TWO_SEASON_DEMAND_RATES,
            flat_demand_rates=FLAT_DEMAND_RATES,
            fixed_charge_monthly=30.00,
        )

    def test_demand_rate_structure_populated(self, rate) -> None:
        """demandratestructure should have 4 periods (2 seasons x 2 TOU)."""
        assert rate.demandratestructure is not None
        assert len(rate.demandratestructure) == 4

    def test_summer_peak_demand_rate(self, rate) -> None:
        """Summer peak demand rate should be $5.00/kW."""
        assert rate.demandratestructure[0][0].rate == 5.00

    def test_demand_schedule_dimensions(self, rate) -> None:
        """Demand schedule matrices should be 12x24."""
        assert len(rate.demandweekdayschedule) == 12
        assert len(rate.demandweekendschedule) == 12

    def test_flat_demand_structure_populated(self, rate) -> None:
        """flatdemandstructure should have 2 periods (one per season)."""
        assert rate.flatdemandstructure is not None
        assert len(rate.flatdemandstructure) == 2
        assert rate.flatdemandstructure[0][0].rate == 20.00  # summer
        assert rate.flatdemandstructure[1][0].rate == 18.00  # winter

    def test_flat_demand_months(self, rate) -> None:
        """flatdemandmonths should map summer months to period 0, winter to period 1."""
        assert rate.flatdemandmonths is not None
        assert len(rate.flatdemandmonths) == 12
        # June (index 5) is summer → period 0
        assert rate.flatdemandmonths[5] == 0
        # January (index 0) is winter → period 1
        assert rate.flatdemandmonths[0] == 1


class TestValidationErrors:
    """Test that invalid inputs raise RateBuilderError."""

    def test_incomplete_seasons_raises(self) -> None:
        """Seasons missing month 6 should raise an error."""
        bad_seasons = {
            "summer": [7, 8, 9],  # missing month 6
            "winter": [1, 2, 3, 4, 5, 10, 11, 12],
        }
        with pytest.raises(RateBuilderError, match="missing months"):
            build_rate_schedule(
                utility_name="X",
                tariff_name="X",
                seasons=bad_seasons,
                tou_periods=PEAK_OFFPEAK_PERIODS,
                energy_rates=TWO_SEASON_ENERGY_RATES,
            )

    def test_incomplete_weekday_hours_raises(self) -> None:
        """TOU periods missing weekday hour 12 should raise an error."""
        bad_periods = {
            "peak": {
                "weekday_hours": [16, 17, 18, 19],
                "weekend_hours": [],
            },
            "off_peak": {
                # Missing hour 12
                "weekday_hours": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 20, 21, 22, 23],
                "weekend_hours": list(range(24)),
            },
        }
        with pytest.raises(RateBuilderError, match="missing hours"):
            build_rate_schedule(
                utility_name="X",
                tariff_name="X",
                seasons=SUMMER_WINTER_SEASONS,
                tou_periods=bad_periods,
                energy_rates=TWO_SEASON_ENERGY_RATES,
            )

    def test_missing_energy_rate_raises(self) -> None:
        """energy_rates missing summer/peak combo should raise an error."""
        bad_rates = {
            "summer": {"off_peak": 0.08},  # missing "peak"
            "winter": {"peak": 0.12, "off_peak": 0.07},
        }
        with pytest.raises(RateBuilderError, match="missing rate.*summer.*peak"):
            build_rate_schedule(
                utility_name="X",
                tariff_name="X",
                seasons=SUMMER_WINTER_SEASONS,
                tou_periods=PEAK_OFFPEAK_PERIODS,
                energy_rates=bad_rates,
            )


class TestBuildAndSave:
    """Test the build_and_save convenience function."""

    def test_round_trip(self, tmp_path: Path) -> None:
        """Build, save, then load with load_rate_file and verify round-trip."""
        out_path = tmp_path / "test_rate.json"
        result_path = build_and_save(
            path=out_path,
            utility_name="RoundTrip Utility",
            tariff_name="RT-1",
            seasons=SUMMER_WINTER_SEASONS,
            tou_periods=PEAK_OFFPEAK_PERIODS,
            energy_rates=TWO_SEASON_ENERGY_RATES,
            fixed_charge_monthly=15.00,
        )

        assert result_path == out_path
        assert out_path.exists()

        # Load back with rate_parser
        loaded = load_rate_file(out_path)
        assert loaded.utility_name == "RoundTrip Utility"
        assert loaded.tariff_name == "RT-1"
        assert len(loaded.energyratestructure) == 4
        assert loaded.energyratestructure[0][0].rate == 0.15
        assert loaded.fixed_charges.fixed_charge_first_meter == 15.00

        # Verify schedule matrices match
        assert loaded.energyweekdayschedule[5][17] == 0  # June weekday h17 = summer peak
