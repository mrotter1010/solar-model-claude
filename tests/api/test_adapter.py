"""Tests for the API adapter: request → SiteConfig mapping."""

from pathlib import Path

import pytest

from src.api.adapter import (
    bess_request_to_site_config,
    bill_savings_request_to_site_config,
    production_request_to_site_config,
)
from src.api.schemas.common import (
    BESSConfig,
    BESSEconomics,
    BillConfig,
    FTMConfig,
    Location,
    Losses,
    ResourceOverrides,
    SystemDesign,
)
from src.api.schemas.requests import BESSRequest, BillSavingsRequest, ProductionRequest


def _minimal_request(**overrides: object) -> ProductionRequest:
    """Build a ProductionRequest with minimal required fields."""
    site_kwargs = {
        "name": "TestSite",
        "latitude": 33.45,
        "longitude": -112.07,
    }
    system_kwargs = {
        "dc_capacity_mw": 13.0,
        "ac_capacity_mw": 10.0,
        "module": "CSI Solar Co. Ltd. CS3U-355P",
        "inverter": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
        "racking": "tracker",
        "gcr": 0.35,
    }
    # Allow caller to override nested fields
    site_kwargs.update(overrides.pop("site", {}))
    system_kwargs.update(overrides.pop("system", {}))

    return ProductionRequest(
        site=Location(**site_kwargs),
        system=SystemDesign(**system_kwargs),
        **overrides,
    )


class TestMinimalRequest:
    """ProductionRequest with only required fields produces a valid SiteConfig."""

    def test_minimal_fields_produce_valid_site_config(self) -> None:
        request = _minimal_request()
        config = production_request_to_site_config(request)

        assert config.site_name == "TestSite"
        assert config.latitude == 33.45
        assert config.longitude == -112.07
        assert config.dc_size_mw == 13.0
        assert config.ac_installed_mw == 10.0
        assert config.panel_model == "CSI Solar Co. Ltd. CS3U-355P"
        assert config.inverter_model == (
            "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]"
        )
        assert config.racking == "tracker"
        assert config.gcr == 0.35

    def test_customer_defaults_to_api(self) -> None:
        request = _minimal_request()
        config = production_request_to_site_config(request)
        assert config.customer == "API"


class TestFullRequest:
    """ProductionRequest with all fields populates SiteConfig correctly."""

    def test_all_fields_mapped(self, tmp_path: Path) -> None:
        # Create real files so SiteConfig path validators pass
        weather_file = tmp_path / "weather.csv"
        weather_file.write_text("Year,Month,Day\n2023,1,1\n")
        gt_file = tmp_path / "ground_truth.csv"
        gt_file.write_text("year,month,ghi_kwh_m2_day,dni_kwh_m2_day\n2023,1,5.0,6.0\n")

        request = ProductionRequest(
            site=Location(
                name="FullSite",
                customer="Acme Corp",
                latitude=40.0,
                longitude=-75.0,
                run_name="full_run_001",
            ),
            system=SystemDesign(
                dc_capacity_mw=5.0,
                ac_capacity_mw=4.0,
                ac_poi_mw=3.5,
                module="TestModule",
                inverter="TestInverter",
                racking="fixed",
                tilt=30.0,
                azimuth=190.0,
                module_orientation="landscape",
                num_modules=2,
                ground_clearance_m=2.0,
                bifacial=True,
                gcr=0.4,
            ),
            losses=Losses(
                shading_pct=3.0,
                dc_wiring_pct=1.5,
                ac_wiring_pct=0.8,
                transformer_pct=0.5,
                degradation_pct=0.3,
                availability_pct=1.0,
                mismatch_pct=1.2,
                lid_pct=0.9,
            ),
            resource=ResourceOverrides(
                resource_file_path=str(weather_file),
                ground_truth_file=str(gt_file),
            ),
        )
        config = production_request_to_site_config(request)

        # Project info
        assert config.run_name == "full_run_001"
        assert config.site_name == "FullSite"
        assert config.customer == "Acme Corp"

        # Location
        assert config.latitude == 40.0
        assert config.longitude == -75.0

        # System capacity
        assert config.dc_size_mw == 5.0
        assert config.ac_installed_mw == 4.0
        assert config.ac_poi_mw == 3.5

        # System design
        assert config.racking == "fixed"
        assert config.tilt == 30.0
        assert config.azimuth == 190.0
        assert config.module_orientation == "landscape"
        assert config.number_of_modules == 2
        assert config.ground_clearance_height_m == 2.0

        # Equipment
        assert config.panel_model == "TestModule"
        assert config.bifacial is True
        assert config.inverter_model == "TestInverter"

        # Layout
        assert config.gcr == 0.4

        # Losses
        assert config.shading_percent == 3.0
        assert config.dc_wiring_loss_percent == 1.5
        assert config.ac_wiring_loss_percent == 0.8
        assert config.transformer_losses_percent == 0.5
        assert config.degradation_percent == 0.3
        assert config.availability_percent == 1.0
        assert config.module_mismatch_percent == 1.2
        assert config.lid_percent == 0.9

        # Resource overrides
        assert config.resource_file_path == weather_file
        assert config.ground_truth_data_file == gt_file


class TestRunNameAutoGeneration:
    """run_name is auto-generated when not provided."""

    def test_run_name_generated_when_none(self) -> None:
        request = _minimal_request()
        config = production_request_to_site_config(request)

        # Should start with site name and contain a timestamp
        assert config.run_name.startswith("TestSite_")
        # Format: TestSite_YYYYMMDD_HHMMSS → at least 22 chars
        assert len(config.run_name) >= 22

    def test_run_name_preserved_when_provided(self) -> None:
        request = _minimal_request(site={"run_name": "my_custom_run"})
        config = production_request_to_site_config(request)
        assert config.run_name == "my_custom_run"

    def test_auto_run_name_spaces_replaced(self) -> None:
        """Site name with spaces → auto-generated run_name has underscores."""
        request = _minimal_request(site={"name": "Phoenix Test Site"})
        config = production_request_to_site_config(request)
        assert " " not in config.run_name
        assert config.run_name.startswith("Phoenix_Test_Site_")

    def test_explicit_run_name_spaces_sanitized(self) -> None:
        """User-provided run_name with spaces is sanitized."""
        request = _minimal_request(
            site={"run_name": "my run/with spaces"}
        )
        config = production_request_to_site_config(request)
        assert config.run_name == "my_run_with_spaces"

    def test_run_name_slashes_and_quotes_sanitized(self) -> None:
        """Slashes and quotes in run_name are replaced with underscores."""
        request = _minimal_request(
            site={"run_name": 'site/"special\'name'}
        )
        config = production_request_to_site_config(request)
        assert "/" not in config.run_name
        assert '"' not in config.run_name
        assert "'" not in config.run_name


class TestTiltInference:
    """Tilt defaults based on racking type when not specified."""

    def test_fixed_racking_default_tilt_25(self) -> None:
        request = _minimal_request(system={"racking": "fixed"})
        config = production_request_to_site_config(request)
        assert config.tilt == 25.0

    def test_tracker_racking_default_tilt_60(self) -> None:
        request = _minimal_request(system={"racking": "tracker"})
        config = production_request_to_site_config(request)
        assert config.tilt == 60.0

    def test_explicit_tilt_overrides_default(self) -> None:
        request = _minimal_request(system={"racking": "fixed", "tilt": 15.0})
        config = production_request_to_site_config(request)
        assert config.tilt == 15.0


class TestAcPoiDefault:
    """ac_poi_mw defaults to ac_capacity_mw when not provided."""

    def test_ac_poi_defaults_to_ac_capacity(self) -> None:
        request = _minimal_request()
        config = production_request_to_site_config(request)
        assert config.ac_poi_mw == 10.0  # matches ac_capacity_mw

    def test_ac_poi_explicit_value_preserved(self) -> None:
        request = _minimal_request(system={"ac_poi_mw": 8.0})
        config = production_request_to_site_config(request)
        assert config.ac_poi_mw == 8.0


class TestLossesDefaults:
    """Default Losses() applied when request.losses is None."""

    def test_default_losses_applied(self) -> None:
        request = _minimal_request(losses=None)
        config = production_request_to_site_config(request)

        defaults = Losses()
        assert config.shading_percent == defaults.shading_pct
        assert config.dc_wiring_loss_percent == defaults.dc_wiring_pct
        assert config.ac_wiring_loss_percent == defaults.ac_wiring_pct
        assert config.transformer_losses_percent == defaults.transformer_pct
        assert config.degradation_percent == defaults.degradation_pct
        assert config.availability_percent == defaults.availability_pct
        assert config.module_mismatch_percent == defaults.mismatch_pct
        assert config.lid_percent == defaults.lid_pct


class TestReportAlwaysTrue:
    """API requests always have report=True."""

    def test_report_is_true(self) -> None:
        request = _minimal_request()
        config = production_request_to_site_config(request)
        assert config.report is True


class TestDisabledFeatures:
    """Bill, BESS, and buildability flags are all False."""

    def test_bill_calculation_false(self) -> None:
        request = _minimal_request()
        config = production_request_to_site_config(request)
        assert config.bill_calculation is False

    def test_bess_dispatch_false(self) -> None:
        request = _minimal_request()
        config = production_request_to_site_config(request)
        assert config.bess_dispatch_required is False

    def test_bess_optimization_false(self) -> None:
        request = _minimal_request()
        config = production_request_to_site_config(request)
        assert config.bess_optimization_required is False

    def test_buildable_land_assessment_false(self) -> None:
        request = _minimal_request()
        config = production_request_to_site_config(request)
        assert config.buildable_land_assessment is False


# ---------------------------------------------------------------------------
# BillSavingsRequest adapter tests
# ---------------------------------------------------------------------------


def _bill_savings_request(
    tmp_path: Path, **overrides: object
) -> BillSavingsRequest:
    """Build a BillSavingsRequest with minimal required fields.

    Creates real temp files for rate and load paths since SiteConfig
    validates file existence.
    """
    # Create real files so SiteConfig path validators pass
    rate_file = tmp_path / "rates.json"
    if not rate_file.exists():
        rate_file.write_text("{}")
    load_file = tmp_path / "load.csv"
    if not load_file.exists():
        load_file.write_text("timestamp,kwh\n2024-01-01 00:00,100\n")

    site_kwargs: dict = {
        "name": "TestSite",
        "latitude": 33.45,
        "longitude": -112.07,
    }
    system_kwargs: dict = {
        "dc_capacity_mw": 13.0,
        "ac_capacity_mw": 10.0,
        "module": "CSI Solar Co. Ltd. CS3U-355P",
        "inverter": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
        "racking": "tracker",
        "gcr": 0.35,
    }
    bill_kwargs: dict = {
        "rate_file_path": str(rate_file),
        "load_profile_path": str(load_file),
    }
    site_kwargs.update(overrides.pop("site", {}))
    system_kwargs.update(overrides.pop("system", {}))
    bill_kwargs.update(overrides.pop("bill", {}))

    return BillSavingsRequest(
        site=Location(**site_kwargs),
        system=SystemDesign(**system_kwargs),
        bill=BillConfig(**bill_kwargs),
        **overrides,
    )


class TestBillSavingsAdapter:
    """BillSavingsRequest → SiteConfig mapping."""

    def test_bill_calculation_enabled(self, tmp_path: Path) -> None:
        """bill_calculation is True for bill savings requests."""
        request = _bill_savings_request(tmp_path)
        config = bill_savings_request_to_site_config(request)
        assert config.bill_calculation is True

    def test_rate_file_path_mapped(self, tmp_path: Path) -> None:
        """rate_file_path from BillConfig flows to SiteConfig."""
        request = _bill_savings_request(tmp_path)
        config = bill_savings_request_to_site_config(request)
        assert str(config.rate_file_path) == str(tmp_path / "rates.json")

    def test_openei_fields_mapped(self, tmp_path: Path) -> None:
        """utility_name and tariff_name from BillConfig flow to SiteConfig."""
        load_file = tmp_path / "load.csv"
        load_file.write_text("timestamp,kwh\n2024-01-01 00:00,100\n")
        request = _bill_savings_request(
            tmp_path,
            bill={
                "rate_file_path": None,
                "load_profile_path": str(load_file),
                "utility_name": "Arizona Public Service",
                "tariff_name": "E-12 TOU",
            },
        )
        config = bill_savings_request_to_site_config(request)
        assert config.utility_name == "Arizona Public Service"
        assert config.tariff_name == "E-12 TOU"
        assert config.rate_file_path is None

    def test_load_profile_path_mapped(self, tmp_path: Path) -> None:
        """load_profile_path from BillConfig flows to SiteConfig."""
        request = _bill_savings_request(tmp_path)
        config = bill_savings_request_to_site_config(request)
        assert str(config.load_profile_path) == str(tmp_path / "load.csv")

    def test_typical_load_fields_mapped(self, tmp_path: Path) -> None:
        """load_type, annual_consumption_kwh, peak_demand_kw flow to SiteConfig."""
        rate_file = tmp_path / "rates.json"
        rate_file.write_text("{}")
        request = _bill_savings_request(
            tmp_path,
            bill={
                "rate_file_path": str(rate_file),
                "load_profile_path": None,
                "load_type": "MediumOffice",
                "annual_consumption_kwh": 500_000.0,
                "peak_demand_kw": 150.0,
            },
        )
        config = bill_savings_request_to_site_config(request)
        assert config.load_type == "MediumOffice"
        assert config.annual_consumption_kwh == 500_000.0
        assert config.peak_demand_kw == 150.0

    def test_production_fields_still_correct(self, tmp_path: Path) -> None:
        """Common production fields are correctly mapped via shared helper."""
        request = _bill_savings_request(
            tmp_path, site={"run_name": "bill_run_001"}
        )
        config = bill_savings_request_to_site_config(request)

        # Verify production fields work the same as ProductionRequest adapter
        assert config.run_name == "bill_run_001"
        assert config.site_name == "TestSite"
        assert config.dc_size_mw == 13.0
        assert config.racking == "tracker"
        assert config.report is True

    def test_bess_and_buildability_disabled(self, tmp_path: Path) -> None:
        """BESS and buildability remain disabled for bill savings runs."""
        request = _bill_savings_request(tmp_path)
        config = bill_savings_request_to_site_config(request)
        assert config.bess_dispatch_required is False
        assert config.bess_optimization_required is False
        assert config.buildable_land_assessment is False


# ---------------------------------------------------------------------------
# BESSRequest adapter tests
# ---------------------------------------------------------------------------

_SYSTEM_KWARGS: dict = {
    "dc_capacity_mw": 13.0,
    "ac_capacity_mw": 10.0,
    "module": "CSI Solar Co. Ltd. CS3U-355P",
    "inverter": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
    "racking": "tracker",
    "gcr": 0.35,
}

_SITE_KWARGS: dict = {
    "name": "TestSite",
    "latitude": 33.45,
    "longitude": -112.07,
}


def _bess_btm_request(tmp_path: Path, **overrides: object) -> BESSRequest:
    """Build a BTM BESSRequest with minimal required fields."""
    rate_file = tmp_path / "rates.json"
    if not rate_file.exists():
        rate_file.write_text("{}")
    load_file = tmp_path / "load.csv"
    if not load_file.exists():
        load_file.write_text("timestamp,kwh\n2024-01-01 00:00,100\n")

    bess_kwargs: dict = overrides.pop("bess", {"power_mw": 5.0, "duration_hr": 4.0})
    bill_kwargs: dict = overrides.pop(
        "bill",
        {"rate_file_path": str(rate_file), "load_profile_path": str(load_file)},
    )

    return BESSRequest(
        site=Location(**_SITE_KWARGS),
        system=SystemDesign(**_SYSTEM_KWARGS),
        bess=BESSConfig(**bess_kwargs),
        bill=BillConfig(**bill_kwargs) if bill_kwargs is not None else None,
        **overrides,
    )


class TestBESSAdapterBTM:
    """BESSRequest BTM → SiteConfig mapping."""

    def test_bess_dispatch_required_true(self, tmp_path: Path) -> None:
        """BTM BESS request sets bess_dispatch_required=True."""
        request = _bess_btm_request(tmp_path)
        config = bess_request_to_site_config(request)
        assert config.bess_dispatch_required is True

    def test_bill_calculation_true(self, tmp_path: Path) -> None:
        """BTM BESS request sets bill_calculation=True."""
        request = _bess_btm_request(tmp_path)
        config = bess_request_to_site_config(request)
        assert config.bill_calculation is True

    def test_dispatch_mode_btm(self, tmp_path: Path) -> None:
        """BTM BESS request defaults dispatch_mode to 'btm'."""
        request = _bess_btm_request(tmp_path)
        config = bess_request_to_site_config(request)
        assert config.dispatch_mode == "btm"

    def test_bess_config_fields_mapped(self, tmp_path: Path) -> None:
        """BESSConfig fields map to SiteConfig bess_* fields."""
        request = _bess_btm_request(
            tmp_path,
            bess={
                "power_mw": 5.0,
                "duration_hr": 4.0,
                "rte_pct": 85.0,
                "min_soc_pct": 15.0,
                "max_soc_pct": 95.0,
                "strategy": "peak_shaving",
                "installed_cost_per_kwh": 300.0,
                "cycles_warranty": 6000,
                "solar_only_charging": True,
            },
        )
        config = bess_request_to_site_config(request)

        assert config.bess_power_mw == 5.0
        assert config.bess_duration_hr == 4.0
        assert config.bess_rte_percent == 85.0
        assert config.bess_min_soc_percent == 15.0
        assert config.bess_max_soc_percent == 95.0
        assert config.bess_strategy == "peak_shaving"
        assert config.bess_installed_cost_per_kwh == 300.0
        assert config.bess_cycles_warranty == 6000
        assert config.bess_solar_only_charging is True
        assert config.bess_grid_only_charging is False

    def test_buildability_disabled(self, tmp_path: Path) -> None:
        """BESS requests always disable buildable land assessment."""
        request = _bess_btm_request(tmp_path)
        config = bess_request_to_site_config(request)
        assert config.buildable_land_assessment is False

    def test_optimization_disabled_by_default(self, tmp_path: Path) -> None:
        """Without bess_economics, optimization is disabled."""
        request = _bess_btm_request(tmp_path)
        config = bess_request_to_site_config(request)
        assert config.bess_optimization_required is False


class TestBESSAdapterFTM:
    """BESSRequest FTM → SiteConfig mapping."""

    def test_dispatch_mode_ftm(self, tmp_path: Path) -> None:
        """FTM request sets dispatch_mode='ftm'."""
        request = BESSRequest(
            site=Location(**_SITE_KWARGS),
            system=SystemDesign(**_SYSTEM_KWARGS),
            bess=BESSConfig(power_mw=5.0, duration_hr=4.0),
            ftm=FTMConfig(dispatch_mode="ftm", iso="pjm", lmp_zone="AEP"),
        )
        config = bess_request_to_site_config(request)
        assert config.dispatch_mode == "ftm"

    def test_bill_calculation_false_for_ftm(self, tmp_path: Path) -> None:
        """FTM without bill sets bill_calculation=False."""
        request = BESSRequest(
            site=Location(**_SITE_KWARGS),
            system=SystemDesign(**_SYSTEM_KWARGS),
            bess=BESSConfig(power_mw=5.0, duration_hr=4.0),
            ftm=FTMConfig(dispatch_mode="ftm", iso="pjm", lmp_zone="AEP"),
        )
        config = bess_request_to_site_config(request)
        assert config.bill_calculation is False

    def test_ftm_config_fields_mapped(self, tmp_path: Path) -> None:
        """FTMConfig fields map to SiteConfig fields."""
        request = BESSRequest(
            site=Location(**_SITE_KWARGS),
            system=SystemDesign(**_SYSTEM_KWARGS),
            bess=BESSConfig(power_mw=5.0, duration_hr=4.0),
            ftm=FTMConfig(
                dispatch_mode="ftm",
                iso="pjm",
                lmp_zone="AEP",
                lmp_node_ids="12345",
                lmp_market="DAY_AHEAD_HOURLY",
                lmp_year=2023,
                ancillary_revenue_per_kw_year=10.0,
            ),
        )
        config = bess_request_to_site_config(request)

        assert config.iso == "pjm"
        assert config.lmp_zone == "AEP"
        assert config.lmp_node_ids == "12345"
        assert config.lmp_market == "DAY_AHEAD_HOURLY"
        assert config.lmp_year == 2023
        assert config.ancillary_revenue_per_kw_year == 10.0


class TestBESSAdapterOptimization:
    """BESSRequest optimization → SiteConfig mapping."""

    def test_optimization_required_true(self, tmp_path: Path) -> None:
        """Optimization request sets bess_optimization_required=True."""
        request = _bess_btm_request(
            tmp_path,
            bess_economics=BESSEconomics(
                optimize=True,
                solar_cost_per_kw_dc=1200.0,
                solar_cost_per_kw_ac=200.0,
            ),
        )
        config = bess_request_to_site_config(request)
        assert config.bess_optimization_required is True

    def test_economics_fields_mapped(self, tmp_path: Path) -> None:
        """BESSEconomics fields map to SiteConfig fields."""
        request = _bess_btm_request(
            tmp_path,
            bess_economics=BESSEconomics(
                optimize=True,
                discount_rate_pct=8.0,
                project_lifetime_years=30,
                rate_escalation_pct=3.0,
                solar_cost_per_kw_dc=1200.0,
                solar_cost_per_kw_ac=200.0,
                solar_opex_per_kw_dc_year=15.0,
                bess_opex_per_kw_year=5.0,
                power_min_mw=2.0,
                power_max_mw=10.0,
                duration_min_hr=2.0,
                duration_max_hr=4.0,
            ),
        )
        config = bess_request_to_site_config(request)

        assert config.discount_rate_pct == 8.0
        assert config.project_lifetime_years == 30
        assert config.rate_escalation_pct == 3.0
        assert config.solar_cost_per_kw_dc == 1200.0
        assert config.solar_cost_per_kw_ac == 200.0
        assert config.solar_opex_per_kw_dc_year == 15.0
        assert config.bess_opex_per_kw_year == 5.0
        assert config.bess_power_min_mw == 2.0
        assert config.bess_power_max_mw == 10.0
        assert config.bess_duration_min_hr == 2.0
        assert config.bess_duration_max_hr == 4.0


class TestBESSRequestValidation:
    """BESSRequest validator tests."""

    def test_btm_without_bill_raises(self) -> None:
        """BTM mode without bill config raises ValidationError."""
        with pytest.raises(Exception, match="bill is required"):
            BESSRequest(
                site=Location(**_SITE_KWARGS),
                system=SystemDesign(**_SYSTEM_KWARGS),
                bess=BESSConfig(power_mw=5.0, duration_hr=4.0),
                bill=None,
            )

    def test_ftm_without_bill_is_valid(self) -> None:
        """FTM mode without bill config is valid."""
        request = BESSRequest(
            site=Location(**_SITE_KWARGS),
            system=SystemDesign(**_SYSTEM_KWARGS),
            bess=BESSConfig(power_mw=5.0, duration_hr=4.0),
            ftm=FTMConfig(dispatch_mode="ftm", iso="pjm", lmp_zone="AEP"),
            bill=None,
        )
        assert request.bill is None

    def test_optimization_missing_solar_cost_dc_raises(self, tmp_path: Path) -> None:
        """Optimization without solar_cost_per_kw_dc raises ValidationError."""
        rate_file = tmp_path / "rates.json"
        rate_file.write_text("{}")
        load_file = tmp_path / "load.csv"
        load_file.write_text("timestamp,kwh\n2024-01-01 00:00,100\n")

        with pytest.raises(Exception, match="solar_cost_per_kw_dc"):
            BESSRequest(
                site=Location(**_SITE_KWARGS),
                system=SystemDesign(**_SYSTEM_KWARGS),
                bess=BESSConfig(power_mw=5.0, duration_hr=4.0),
                bill=BillConfig(
                    rate_file_path=str(rate_file),
                    load_profile_path=str(load_file),
                ),
                bess_economics=BESSEconomics(
                    optimize=True,
                    solar_cost_per_kw_dc=None,
                    solar_cost_per_kw_ac=200.0,
                ),
            )

    def test_optimization_missing_solar_cost_ac_raises(self, tmp_path: Path) -> None:
        """Optimization without solar_cost_per_kw_ac raises ValidationError."""
        rate_file = tmp_path / "rates.json"
        rate_file.write_text("{}")
        load_file = tmp_path / "load.csv"
        load_file.write_text("timestamp,kwh\n2024-01-01 00:00,100\n")

        with pytest.raises(Exception, match="solar_cost_per_kw_ac"):
            BESSRequest(
                site=Location(**_SITE_KWARGS),
                system=SystemDesign(**_SYSTEM_KWARGS),
                bess=BESSConfig(power_mw=5.0, duration_hr=4.0),
                bill=BillConfig(
                    rate_file_path=str(rate_file),
                    load_profile_path=str(load_file),
                ),
                bess_economics=BESSEconomics(
                    optimize=True,
                    solar_cost_per_kw_dc=1200.0,
                    solar_cost_per_kw_ac=None,
                ),
            )

    def test_grid_only_optimization_skips_solar_cost_check(self) -> None:
        """Grid-only BESS optimization doesn't require solar costs."""
        request = BESSRequest(
            site=Location(**_SITE_KWARGS),
            system=SystemDesign(**_SYSTEM_KWARGS),
            bess=BESSConfig(
                power_mw=5.0, duration_hr=4.0, grid_only_charging=True
            ),
            ftm=FTMConfig(dispatch_mode="ftm", iso="pjm", lmp_zone="AEP"),
            bess_economics=BESSEconomics(
                optimize=True,
                power_min_mw=2.0,
                power_max_mw=10.0,
            ),
        )
        assert request.bess_economics.optimize is True
