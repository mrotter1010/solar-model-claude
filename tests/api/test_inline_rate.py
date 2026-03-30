"""Tests for inline rate acceptance: BillConfig validation, RateBuildResponse
type, and run_bill_calculation rate_schedule parameter."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError
from starlette.testclient import TestClient

from src.api.app import create_app
from src.api.schemas.common import BillConfig
from src.rates.models import RateSchedule


@pytest.fixture()
def client() -> TestClient:
    """Create a TestClient for the FastAPI app."""
    app = create_app()
    return TestClient(app)


def _schedule_12x24(value: int = 0) -> list[list[int]]:
    """Build a uniform 12x24 schedule matrix."""
    return [[value] * 24 for _ in range(12)]


def _minimal_rate_schedule() -> RateSchedule:
    """Build a minimal valid RateSchedule for testing."""
    return RateSchedule(
        utility_name="Test Utility",
        tariff_name="Test Tariff",
        source="api",
        energyratestructure=[[{"rate": 0.10}]],
        energyweekdayschedule=_schedule_12x24(0),
        energyweekendschedule=_schedule_12x24(0),
    )


def _minimal_rate_dict() -> dict:
    """Build a minimal valid rate schedule dict for JSON payloads."""
    return {
        "utility_name": "Test Utility",
        "tariff_name": "Test Tariff",
        "energyratestructure": [[{"rate": 0.10}]],
        "energyweekdayschedule": _schedule_12x24(0),
        "energyweekendschedule": _schedule_12x24(0),
    }


# ---------------------------------------------------------------------------
# BillConfig validation: inline rate is a valid rate source
# ---------------------------------------------------------------------------


class TestBillConfigInlineRate:
    """BillConfig accepts inline rate as a rate source."""

    def test_rate_alone_is_valid(self) -> None:
        """Providing rate alone (with a load source) is valid."""
        config = BillConfig(
            rate=_minimal_rate_schedule(),
            load_type="SmallOffice",
            annual_consumption_kwh=100000.0,
        )
        assert config.rate is not None
        assert config.rate_file_path is None
        assert config.utility_name is None

    def test_rate_file_path_alone_is_valid(self) -> None:
        """Providing rate_file_path alone is valid."""
        config = BillConfig(
            rate_file_path="/tmp/rate.json",
            load_type="SmallOffice",
            annual_consumption_kwh=100000.0,
        )
        assert config.rate_file_path == "/tmp/rate.json"
        assert config.rate is None

    def test_utility_tariff_alone_is_valid(self) -> None:
        """Providing utility_name + tariff_name alone is valid."""
        config = BillConfig(
            utility_name="PG&E",
            tariff_name="B-19",
            load_type="SmallOffice",
            annual_consumption_kwh=100000.0,
        )
        assert config.utility_name == "PG&E"
        assert config.tariff_name == "B-19"
        assert config.rate is None

    def test_no_rate_source_raises(self) -> None:
        """Providing no rate source raises validation error."""
        with pytest.raises(ValidationError, match="No rate source specified"):
            BillConfig(
                load_type="SmallOffice",
                annual_consumption_kwh=100000.0,
            )

    def test_rate_and_rate_file_path_raises(self) -> None:
        """Providing both rate and rate_file_path raises validation error."""
        with pytest.raises(ValidationError, match="Cannot specify both"):
            BillConfig(
                rate=_minimal_rate_schedule(),
                rate_file_path="/tmp/rate.json",
                load_type="SmallOffice",
                annual_consumption_kwh=100000.0,
            )

    def test_rate_and_openei_raises(self) -> None:
        """Providing both rate and utility_name raises validation error."""
        with pytest.raises(ValidationError, match="Cannot specify both"):
            BillConfig(
                rate=_minimal_rate_schedule(),
                utility_name="PG&E",
                tariff_name="B-19",
                load_type="SmallOffice",
                annual_consumption_kwh=100000.0,
            )


# ---------------------------------------------------------------------------
# RateBuildResponse now returns a proper RateSchedule
# ---------------------------------------------------------------------------


class TestRateBuildResponseType:
    """POST /rates/build returns a RateSchedule (not dict) with source='api'."""

    def test_response_rate_is_rate_schedule(self, client: TestClient) -> None:
        """Response rate field is a RateSchedule with source='api'."""
        body = {
            "rate": _minimal_rate_dict(),
            "save_to_disk": False,
        }
        resp = client.post("/rates/build", json=body)

        assert resp.status_code == 200
        data = resp.json()
        rate = data["rate"]

        # Verify it's a full RateSchedule structure (not a flat dict)
        assert rate["source"] == "api"
        assert rate["utility_name"] == "Test Utility"
        assert rate["tariff_name"] == "Test Tariff"
        assert len(rate["energyratestructure"]) == 1
        assert len(rate["energyweekdayschedule"]) == 12

        # Verify we can deserialize back to RateSchedule
        deserialized = RateSchedule(**rate)
        assert deserialized.source == "api"
        assert deserialized.utility_name == "Test Utility"


# ---------------------------------------------------------------------------
# run_bill_calculation accepts inline rate_schedule
# ---------------------------------------------------------------------------


class TestRunBillCalculationInlineRate:
    """run_bill_calculation uses provided rate_schedule directly."""

    @patch("src.rates.bill_runner.analyze_savings")
    @patch("src.rates.bill_runner.load_typical_profile")
    def test_inline_rate_skips_file_loading(
        self,
        mock_load_profile: MagicMock,
        mock_analyze: MagicMock,
    ) -> None:
        """When rate_schedule is provided, file/URDB loading is skipped."""
        from src.rates.bill_runner import run_bill_calculation

        # Create a SiteConfig with bill_calculation=True but NO rate source
        # (no rate_file_path, no utility_name/tariff_name)
        site_config = MagicMock()
        site_config.bill_calculation = True
        site_config.rate_file_path = None
        site_config.utility_name = None
        site_config.tariff_name = None
        site_config.load_profile_path = None
        site_config.load_type = "SmallOffice"
        site_config.annual_consumption_kwh = 100000.0
        site_config.latitude = 33.45
        site_config.longitude = -112.07
        site_config.site_name = "TestSite"

        # Mock load profile
        mock_profile = MagicMock()
        mock_profile.hourly_kwh = [10.0] * 8760
        mock_profile.annual_consumption_kwh = 87600.0
        mock_profile.peak_demand_kw = 10.0
        mock_profile.scale_factor = 1.0
        mock_load_profile.return_value = mock_profile

        # Mock savings analysis
        mock_savings = MagicMock()
        mock_savings.annual_savings = 5000.0
        mock_savings.savings_percent = 25.0
        mock_analyze.return_value = mock_savings

        inline_rate = _minimal_rate_schedule()
        hourly_prod = [5.0] * 8760

        result = run_bill_calculation(
            site_config, hourly_prod, rate_schedule=inline_rate
        )

        # Verify result was produced (not None — which would happen
        # without rate_schedule since no file path or URDB config)
        assert result is not None
        assert result.bill_savings is mock_savings
        assert result.rate_schedule is inline_rate

        # Verify analyze_savings was called with the inline rate
        mock_analyze.assert_called_once()
        call_kwargs = mock_analyze.call_args
        assert call_kwargs.kwargs["rate"] is inline_rate

    @patch("src.rates.bill_runner.load_rate_file")
    @patch("src.rates.bill_runner.analyze_savings")
    @patch("src.rates.bill_runner.load_typical_profile")
    def test_no_inline_rate_falls_through_to_file(
        self,
        mock_load_profile: MagicMock,
        mock_analyze: MagicMock,
        mock_load_rate: MagicMock,
    ) -> None:
        """Without rate_schedule, falls back to rate_file_path loading."""
        from src.rates.bill_runner import run_bill_calculation

        site_config = MagicMock()
        site_config.bill_calculation = True
        site_config.rate_file_path = "/tmp/test_rate.json"
        site_config.utility_name = None
        site_config.tariff_name = None
        site_config.load_profile_path = None
        site_config.load_type = "SmallOffice"
        site_config.annual_consumption_kwh = 100000.0
        site_config.latitude = 33.45
        site_config.longitude = -112.07
        site_config.site_name = "TestSite"

        mock_load_rate.return_value = _minimal_rate_schedule()

        mock_profile = MagicMock()
        mock_profile.hourly_kwh = [10.0] * 8760
        mock_profile.annual_consumption_kwh = 87600.0
        mock_profile.peak_demand_kw = 10.0
        mock_profile.scale_factor = 1.0
        mock_load_profile.return_value = mock_profile

        mock_savings = MagicMock()
        mock_analyze.return_value = mock_savings

        result = run_bill_calculation(site_config, [5.0] * 8760)

        assert result is not None
        # Verify it loaded from file, not inline
        mock_load_rate.assert_called_once()
