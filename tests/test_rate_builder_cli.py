"""Tests for rate builder CLI interactive flow."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.rates.rate_builder_cli import parse_int_list, run_cli
from src.rates.rate_parser import load_rate_file

RESULTS_DIR = Path("tests/test_results/rate_builder_cli")


class TestParseIntList:
    """Test the parse_int_list helper."""

    def test_comma_separated(self) -> None:
        """Simple comma-separated integers."""
        assert parse_int_list("1,2,3") == [1, 2, 3]

    def test_range(self) -> None:
        """Dash range expands to all integers inclusive."""
        assert parse_int_list("1-5") == [1, 2, 3, 4, 5]

    def test_mixed(self) -> None:
        """Mix of individual values and ranges."""
        assert parse_int_list("1,2,5-8,12") == [1, 2, 5, 6, 7, 8, 12]

    def test_hour_list(self) -> None:
        """Typical TOU peak hour list."""
        assert parse_int_list("16,17,18,19,20") == [16, 17, 18, 19, 20]

    def test_deduplication(self) -> None:
        """Duplicate values are removed."""
        assert parse_int_list("1,1,2,2-3") == [1, 2, 3]


class TestFlatRateFlow:
    """Test CLI flow for a simple flat rate."""

    def test_flat_rate_flow(self, tmp_path: Path) -> None:
        """Flat rate: no TOU, no demand, fixed charge."""
        output_file = tmp_path / "flat_rate.json"
        inputs = [
            "Test Utility",        # utility name
            "Flat Rate",           # tariff name
            "",                    # sector (default: commercial)
            "n",                   # not TOU
            "0.10",                # energy rate
            "n",                   # no demand
            "15.00",               # fixed charge
            str(output_file),      # output path
        ]

        with patch("builtins.input", side_effect=inputs):
            result_path = run_cli()

        assert result_path == output_file
        assert output_file.exists()

        # Verify valid JSON
        data = json.loads(output_file.read_text())
        assert data["utility_name"] == "Test Utility"

        # Load with rate_parser and verify structure
        rate = load_rate_file(output_file)
        assert len(rate.energyratestructure) == 1
        assert rate.energyratestructure[0][0].rate == 0.10
        assert rate.fixed_charges.fixed_charge_first_meter == 15.00

        # All schedule entries should be period 0
        for row in rate.energyweekdayschedule:
            assert all(v == 0 for v in row)
        for row in rate.energyweekendschedule:
            assert all(v == 0 for v in row)

        # No demand charges
        assert rate.demandratestructure is None
        assert rate.flatdemandstructure is None

        # Write for inspection
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        (RESULTS_DIR / "flat_rate.json").write_text(
            json.dumps(data, indent=2)
        )


class TestTOUTwoSeasonFlow:
    """Test CLI flow for a two-season TOU rate with demand charges."""

    def test_tou_two_season_flow(self, tmp_path: Path) -> None:
        """Full TOU: 2 seasons, 2 periods, TOU demand, flat demand, fixed."""
        output_file = tmp_path / "tou_rate.json"
        inputs = [
            "PG&E",               # utility name
            "B-20 TOU",           # tariff name
            "",                   # sector (default: commercial)
            "y",                  # is TOU
            # Seasons
            "2",                  # number of seasons
            "summer",             # season 1 name
            "6,7,8,9",           # season 1 months
            "winter",             # season 2 name
            "1-5,10-12",         # season 2 months
            # TOU periods
            "2",                  # number of periods
            "peak",               # period 1 name
            "16,17,18,19,20",    # period 1 weekday hours
            "none",               # period 1 weekend hours
            "off_peak",           # period 2 name
            "0-15,21-23",        # period 2 weekday hours
            "0-23",              # period 2 weekend hours
            # Energy rates
            "0.15",               # summer/peak
            "0.08",               # summer/off_peak
            "0.12",               # winter/peak
            "0.07",               # winter/off_peak
            # TOU demand
            "y",                  # include TOU demand
            "5.00",               # summer/peak demand
            "0",                  # summer/off_peak demand
            "3.00",               # winter/peak demand
            "0",                  # winter/off_peak demand
            # Flat demand
            "y",                  # include flat demand
            "20",                 # summer flat demand
            "18",                 # winter flat demand
            # Fixed
            "25",                 # fixed charge
            # Output
            str(output_file),     # output path
        ]

        with patch("builtins.input", side_effect=inputs):
            result_path = run_cli()

        assert result_path == output_file
        assert output_file.exists()

        rate = load_rate_file(output_file)

        # 4 energy periods (2 seasons x 2 TOU periods)
        assert len(rate.energyratestructure) == 4
        assert rate.energyratestructure[0][0].rate == 0.15  # summer peak
        assert rate.energyratestructure[1][0].rate == 0.08  # summer off_peak
        assert rate.energyratestructure[2][0].rate == 0.12  # winter peak
        assert rate.energyratestructure[3][0].rate == 0.07  # winter off_peak

        # Demand charges present
        assert rate.demandratestructure is not None
        assert len(rate.demandratestructure) == 4
        assert rate.demandratestructure[0][0].rate == 5.00  # summer peak
        assert rate.demandweekdayschedule is not None
        assert rate.demandweekendschedule is not None

        # Flat demand present
        assert rate.flatdemandstructure is not None
        assert len(rate.flatdemandstructure) == 2
        assert rate.flatdemandstructure[0][0].rate == 20.00  # summer
        assert rate.flatdemandstructure[1][0].rate == 18.00  # winter
        assert rate.flatdemandmonths is not None

        # Fixed charge
        assert rate.fixed_charges.fixed_charge_first_meter == 25.00

        # Verify schedule: June weekday hour 17 = summer peak (period 0)
        assert rate.energyweekdayschedule[5][17] == 0

        # December weekend hour 10 = winter off_peak (period 3)
        assert rate.energyweekendschedule[11][10] == 3

        # Write for inspection
        data = json.loads(output_file.read_text())
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        (RESULTS_DIR / "tou_rate.json").write_text(
            json.dumps(data, indent=2)
        )


class TestFlatRateWithDemand:
    """Test CLI flow for flat rate with demand charges."""

    def test_flat_rate_with_demand(self, tmp_path: Path) -> None:
        """Flat rate with demand charge and fixed charge."""
        output_file = tmp_path / "flat_demand_rate.json"
        inputs = [
            "SCE",                # utility name
            "GS-1",              # tariff name
            "",                   # sector (default)
            "n",                  # not TOU
            "0.12",               # energy rate
            "y",                  # include demand
            "15.00",              # demand charge
            "10",                 # fixed charge
            str(output_file),     # output path
        ]

        with patch("builtins.input", side_effect=inputs):
            run_cli()

        rate = load_rate_file(output_file)

        # Single energy period
        assert len(rate.energyratestructure) == 1
        assert rate.energyratestructure[0][0].rate == 0.12

        # Flat demand structure present
        assert rate.flatdemandstructure is not None
        assert len(rate.flatdemandstructure) == 1
        assert rate.flatdemandstructure[0][0].rate == 15.00

        # Flat demand months: all map to period 0 (single season)
        assert rate.flatdemandmonths == [0] * 12

        # No TOU demand
        assert rate.demandratestructure is None

        # Fixed charge
        assert rate.fixed_charges.fixed_charge_first_meter == 10.00
