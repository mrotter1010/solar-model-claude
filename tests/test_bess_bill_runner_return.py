"""Tests for BillCalculationResult return type from run_bill_calculation."""

from pathlib import Path

from src.config.schema import SiteConfig
from src.rates.bill_runner import BillCalculationResult, run_bill_calculation
from src.rates.models import BillSavings, LoadProfile, RateSchedule

SAMPLE_RATE_PATH = Path("tests/fixtures/rates/sample_rate.json")


def _make_bill_site_config() -> SiteConfig:
    """Build a minimal SiteConfig with bill_calculation=True."""
    return SiteConfig(
        run_name="test_run",
        site_name="test_site",
        customer="test_customer",
        latitude=33.45,
        longitude=-112.07,
        dc_size_mw=10.0,
        ac_installed_mw=8.0,
        ac_poi_mw=8.0,
        racking="tracker",
        tilt=60.0,
        azimuth=180.0,
        module_orientation="portrait",
        number_of_modules=1,
        ground_clearance_height_m=1.5,
        panel_model="test_panel",
        bifacial=False,
        inverter_model="test_inverter",
        gcr=0.35,
        shading_percent=0.0,
        dc_wiring_loss_percent=2.0,
        ac_wiring_loss_percent=1.0,
        transformer_losses_percent=1.0,
        degradation_percent=0.5,
        availability_percent=2.0,
        module_mismatch_percent=2.0,
        lid_percent=1.5,
        # Bill calculation fields
        bill_calculation=True,
        rate_file_path=str(SAMPLE_RATE_PATH),
        load_type="MediumOffice",
        annual_consumption_kwh=2_000_000.0,
    )


class TestBillCalculationResultReturn:
    """Verify run_bill_calculation returns BillCalculationResult with all fields."""

    def test_returns_bill_calculation_result(self):
        """Return type should be BillCalculationResult, not bare BillSavings."""
        site = _make_bill_site_config()
        production = [300.0] * 8760

        result = run_bill_calculation(site, production)

        assert result is not None
        assert isinstance(result, BillCalculationResult)

    def test_bill_savings_populated(self):
        """bill_savings field should be a BillSavings instance."""
        site = _make_bill_site_config()
        production = [300.0] * 8760

        result = run_bill_calculation(site, production)

        assert isinstance(result.bill_savings, BillSavings)
        assert result.bill_savings.annual_savings > 0

    def test_rate_schedule_populated(self):
        """rate_schedule field should be a RateSchedule instance."""
        site = _make_bill_site_config()
        production = [300.0] * 8760

        result = run_bill_calculation(site, production)

        assert isinstance(result.rate_schedule, RateSchedule)
        assert result.rate_schedule.utility_name == "Pacific Gas & Electric Co"

    def test_load_profile_populated(self):
        """load_profile field should be a LoadProfile instance."""
        site = _make_bill_site_config()
        production = [300.0] * 8760

        result = run_bill_calculation(site, production)

        assert isinstance(result.load_profile, LoadProfile)
        assert len(result.load_profile.hourly_kwh) == 8760
        assert result.load_profile.annual_consumption_kwh > 0

    def test_disabled_returns_none(self):
        """When bill_calculation=False, should return None."""
        site = _make_bill_site_config()
        site_dict = site.model_dump()
        site_dict["bill_calculation"] = False
        # Remove bill-specific fields that trigger validation
        site_dict.pop("rate_file_path", None)
        site_dict.pop("load_type", None)
        site_dict.pop("annual_consumption_kwh", None)
        disabled_site = SiteConfig(**site_dict)

        result = run_bill_calculation(disabled_site, [300.0] * 8760)

        assert result is None
