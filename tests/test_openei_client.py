"""Tests for OpenEI URDB API client (all HTTP calls mocked)."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.rates.openei_client import (
    OpenEIError,
    fetch_and_translate,
    search_utilities,
    translate_urdb_to_rate_schedule,
)

OUTPUT_DIR = Path("outputs/test_results/openei")


def _make_flat_schedule(period: int = 0) -> list[list[int]]:
    """Create a 12x24 schedule matrix filled with a single period index."""
    return [[period] * 24 for _ in range(12)]


def _make_mock_urdb_record(
    *,
    include_demand: bool = True,
    include_flat_demand: bool = False,
    missing_rate_field: bool = False,
) -> dict:
    """Build a realistic mock URDB rate record.

    Args:
        include_demand: Whether to include TOU demand charges.
        include_flat_demand: Whether to include flat demand charges.
        missing_rate_field: If True, omit "rate" key from a tier.
    """
    # Energy: 2 periods (off-peak, on-peak), each with 1 tier
    energy_tier_0 = [{"rate": 0.08, "unit": "kWh"}]
    if missing_rate_field:
        energy_tier_1 = [{"unit": "kWh", "max": 1000}]  # no "rate" key
    else:
        energy_tier_1 = [{"rate": 0.15, "unit": "kWh"}]

    # TOU schedule: period 0 at night (0-7, 19-23), period 1 during day (8-18)
    weekday_schedule = []
    for _ in range(12):
        row = [0] * 8 + [1] * 11 + [0] * 5
        weekday_schedule.append(row)
    weekend_schedule = _make_flat_schedule(0)

    record = {
        "utility": "Arizona Public Service Co",
        "name": "E-TOU-GS Medium General Service TOU",
        "label": "aps-etou-gs-2023",
        "sector": "Commercial",
        "description": "Time-of-use general service rate for medium commercial customers",
        "startdate": 1672531200,  # 2023-01-01
        "energyratestructure": [energy_tier_0, energy_tier_1],
        "energyweekdayschedule": weekday_schedule,
        "energyweekendschedule": weekend_schedule,
        "fixedchargefirstmeter": 32.44,
        "fixedchargeunits": "$/month",
    }

    if include_demand:
        record["demandratestructure"] = [
            [{"rate": 0.0}],   # off-peak demand
            [{"rate": 12.50}],  # on-peak demand
        ]
        record["demandweekdayschedule"] = weekday_schedule
        record["demandweekendschedule"] = weekend_schedule

    if include_flat_demand:
        record["flatdemandstructure"] = [
            [{"rate": 5.75}],
        ]
        record["flatdemandmonths"] = [0] * 12

    return record


class TestTranslateUrdbToRateSchedule:
    """Tests for URDB → RateSchedule translation."""

    def test_full_translation(self) -> None:
        """Translate a complete URDB record with energy + TOU demand."""
        urdb = _make_mock_urdb_record()
        schedule = translate_urdb_to_rate_schedule(urdb)

        # Identity fields
        assert schedule.utility_name == "Arizona Public Service Co"
        assert schedule.tariff_name == "E-TOU-GS Medium General Service TOU"
        assert schedule.source == "openei"
        assert schedule.openei_label == "aps-etou-gs-2023"
        assert schedule.effective_date == "2023-01-01"

        # Energy rate structure: 2 periods
        assert len(schedule.energyratestructure) == 2
        assert schedule.energyratestructure[0][0].rate == 0.08
        assert schedule.energyratestructure[1][0].rate == 0.15

        # Schedules are 12x24
        assert len(schedule.energyweekdayschedule) == 12
        assert len(schedule.energyweekdayschedule[0]) == 24

        # TOU demand present
        assert schedule.demandratestructure is not None
        assert len(schedule.demandratestructure) == 2
        assert schedule.demandratestructure[1][0].rate == 12.50

        # Fixed charges
        assert schedule.fixed_charges.fixed_charge_first_meter == 32.44

        # Write mock record for inspection
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out_path = OUTPUT_DIR / "mock_urdb_record.json"
        with open(out_path, "w") as f:
            json.dump(urdb, f, indent=2)

    def test_energy_only_rate(self) -> None:
        """URDB record with no demand charges → demand fields are None."""
        urdb = _make_mock_urdb_record(include_demand=False)
        schedule = translate_urdb_to_rate_schedule(urdb)

        assert schedule.demandratestructure is None
        assert schedule.demandweekdayschedule is None
        assert schedule.demandweekendschedule is None
        assert schedule.flatdemandstructure is None
        assert schedule.flatdemandmonths is None

        # Energy still works
        assert len(schedule.energyratestructure) == 2

    def test_with_flat_demand(self) -> None:
        """URDB record with flat demand structure maps correctly."""
        urdb = _make_mock_urdb_record(include_demand=False, include_flat_demand=True)
        schedule = translate_urdb_to_rate_schedule(urdb)

        assert schedule.flatdemandstructure is not None
        assert len(schedule.flatdemandstructure) == 1
        assert schedule.flatdemandstructure[0][0].rate == 5.75
        assert schedule.flatdemandmonths == [0] * 12

    def test_missing_rate_field_defaults_to_zero(self) -> None:
        """URDB tier with no 'rate' key defaults to 0.0."""
        urdb = _make_mock_urdb_record(include_demand=False, missing_rate_field=True)
        schedule = translate_urdb_to_rate_schedule(urdb)

        # Period 1 had no "rate" key — should default to 0.0
        assert schedule.energyratestructure[1][0].rate == 0.0


class TestSearchUtilitiesNoApiKey:
    """Tests for API key validation."""

    def test_no_api_key_raises_error(self) -> None:
        """No env var and no parameter → raises OpenEIError."""
        with patch.dict("os.environ", {}, clear=True):
            # Ensure OPENEI_API_KEY is not set
            with pytest.raises(OpenEIError, match="No OpenEI API key"):
                search_utilities("Arizona Public Service")


class TestFetchAndTranslateMocked:
    """Tests for the convenience fetch_and_translate function with mocked HTTP."""

    @patch("src.rates.openei_client._make_request")
    def test_picks_correct_rate_and_translates(self, mock_request: MagicMock) -> None:
        """Mock HTTP: search returns 2 rates, fetch returns full record."""
        # First call: search_rates returns 2 items
        search_response = {
            "items": [
                {
                    "label": "old-rate-2020",
                    "name": "E-TOU-GS Medium General Service TOU",
                    "utility": "Arizona Public Service Co",
                    "sector": "Commercial",
                    "startdate": 1577836800,  # 2020-01-01
                    "description": "Old version",
                },
                {
                    "label": "new-rate-2023",
                    "name": "E-TOU-GS Medium General Service TOU",
                    "utility": "Arizona Public Service Co",
                    "sector": "Commercial",
                    "startdate": 1672531200,  # 2023-01-01
                    "description": "Current version",
                },
            ]
        }

        # Second call: fetch_rate returns the full record
        full_record = _make_mock_urdb_record()
        full_record["label"] = "new-rate-2023"
        fetch_response = {"items": [full_record]}

        mock_request.side_effect = [search_response, fetch_response]

        schedule = fetch_and_translate(
            utility_name="Arizona Public Service Co",
            tariff_name="E-TOU-GS",
            api_key="test-key-123",
        )

        # Verify it picked the newer rate (2023)
        assert schedule.openei_label == "new-rate-2023"
        assert schedule.utility_name == "Arizona Public Service Co"
        assert schedule.source == "openei"

        # Verify _make_request was called twice (search + fetch)
        assert mock_request.call_count == 2

        # Second call should have fetched the newer label
        fetch_call_params = mock_request.call_args_list[1]
        assert fetch_call_params[0][0] == {"getpage": "new-rate-2023"}

    @patch("src.rates.openei_client._make_request")
    def test_no_match_raises_error(self, mock_request: MagicMock) -> None:
        """No matching tariff name raises OpenEIError listing available rates."""
        # First call: filtered search returns empty
        mock_request.side_effect = [
            {"items": []},  # search with tariff filter → no match
            {  # fallback unfiltered search
                "items": [
                    {
                        "label": "x",
                        "name": "Residential Basic",
                        "utility": "Test Utility",
                        "sector": "Residential",
                        "startdate": 0,
                        "description": "",
                    }
                ]
            },
        ]

        with pytest.raises(OpenEIError, match="No rate matching"):
            fetch_and_translate(
                utility_name="Test Utility",
                tariff_name="NonExistent",
                api_key="test-key-123",
            )
