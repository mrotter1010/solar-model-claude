"""Tests for API schema models in src/api/schemas/common.py."""

import pytest
from pydantic import ValidationError

from src.api.schemas.common import (
    BESSConfig,
    BESSEconomics,
    BillConfig,
    BuildabilityConfig,
    FTMConfig,
    Location,
    Losses,
    LossSummary,
    ProductionSummary,
    ResourceOverrides,
    SystemDesign,
)


# ===================================================================
# Location
# ===================================================================


class TestLocation:
    """Tests for the Location model."""

    def test_minimal_required(self) -> None:
        loc = Location(name="Phoenix", latitude=33.45, longitude=-112.07)
        assert loc.name == "Phoenix"
        assert loc.customer == "API"
        assert loc.latitude == 33.45
        assert loc.longitude == -112.07
        assert loc.run_name is None

    def test_all_fields(self) -> None:
        loc = Location(
            name="Phoenix",
            customer="Acme",
            latitude=33.45,
            longitude=-112.07,
            run_name="run_001",
        )
        assert loc.customer == "Acme"
        assert loc.run_name == "run_001"

    def test_run_name_none_by_default(self) -> None:
        """run_name=None signals adapter should auto-generate it."""
        loc = Location(name="Test", latitude=0.0, longitude=0.0)
        assert loc.run_name is None

    def test_latitude_out_of_range_high(self) -> None:
        with pytest.raises(ValidationError, match="latitude"):
            Location(name="Bad", latitude=91.0, longitude=0.0)

    def test_latitude_out_of_range_low(self) -> None:
        with pytest.raises(ValidationError, match="latitude"):
            Location(name="Bad", latitude=-91.0, longitude=0.0)

    def test_longitude_out_of_range(self) -> None:
        with pytest.raises(ValidationError, match="longitude"):
            Location(name="Bad", latitude=0.0, longitude=181.0)

    def test_boundary_values(self) -> None:
        loc = Location(name="Edge", latitude=90.0, longitude=-180.0)
        assert loc.latitude == 90.0
        assert loc.longitude == -180.0

    def test_missing_name_raises(self) -> None:
        with pytest.raises(ValidationError):
            Location(latitude=33.0, longitude=-112.0)


# ===================================================================
# SystemDesign
# ===================================================================


class TestSystemDesign:
    """Tests for the SystemDesign model."""

    @pytest.fixture()
    def minimal(self) -> dict:
        return {
            "dc_capacity_mw": 13.0,
            "ac_capacity_mw": 10.0,
            "module": "CSI Solar Co. Ltd. CS3U-355P",
            "inverter": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
            "racking": "tracker",
            "gcr": 0.35,
        }

    def test_minimal_required(self, minimal: dict) -> None:
        sd = SystemDesign(**minimal)
        assert sd.dc_capacity_mw == 13.0
        assert sd.ac_capacity_mw == 10.0
        assert sd.ac_poi_mw is None
        assert sd.racking == "tracker"
        assert sd.tilt is None
        assert sd.azimuth == 180.0
        assert sd.module_orientation == "portrait"
        assert sd.num_modules == 1
        assert sd.ground_clearance_m == 1.5
        assert sd.bifacial is False
        assert sd.gcr == 0.35

    def test_all_fields(self, minimal: dict) -> None:
        sd = SystemDesign(
            **minimal,
            ac_poi_mw=9.0,
            tilt=25.0,
            azimuth=200.0,
            module_orientation="landscape",
            num_modules=2,
            ground_clearance_m=2.0,
            bifacial=True,
        )
        assert sd.ac_poi_mw == 9.0
        assert sd.tilt == 25.0
        assert sd.azimuth == 200.0
        assert sd.module_orientation == "landscape"
        assert sd.num_modules == 2
        assert sd.ground_clearance_m == 2.0
        assert sd.bifacial is True

    def test_racking_case_insensitive(self, minimal: dict) -> None:
        minimal["racking"] = "Tracker"
        sd = SystemDesign(**minimal)
        assert sd.racking == "tracker"

    def test_racking_fixed_accepted(self, minimal: dict) -> None:
        minimal["racking"] = "FIXED"
        sd = SystemDesign(**minimal)
        assert sd.racking == "fixed"

    def test_racking_invalid_rejected(self, minimal: dict) -> None:
        minimal["racking"] = "hybrid"
        with pytest.raises(ValidationError, match="racking"):
            SystemDesign(**minimal)

    def test_module_orientation_case_insensitive(self, minimal: dict) -> None:
        sd = SystemDesign(**minimal, module_orientation="Landscape")
        assert sd.module_orientation == "landscape"

    def test_module_orientation_invalid_rejected(self, minimal: dict) -> None:
        with pytest.raises(ValidationError, match="module_orientation"):
            SystemDesign(**minimal, module_orientation="diagonal")

    def test_dc_capacity_zero_rejected(self, minimal: dict) -> None:
        minimal["dc_capacity_mw"] = 0.0
        with pytest.raises(ValidationError, match="dc_capacity_mw"):
            SystemDesign(**minimal)

    def test_dc_capacity_negative_rejected(self, minimal: dict) -> None:
        minimal["dc_capacity_mw"] = -1.0
        with pytest.raises(ValidationError, match="dc_capacity_mw"):
            SystemDesign(**minimal)

    def test_gcr_zero_rejected(self, minimal: dict) -> None:
        minimal["gcr"] = 0.0
        with pytest.raises(ValidationError, match="gcr"):
            SystemDesign(**minimal)

    def test_gcr_one_rejected(self, minimal: dict) -> None:
        minimal["gcr"] = 1.0
        with pytest.raises(ValidationError, match="gcr"):
            SystemDesign(**minimal)

    def test_tilt_out_of_range(self, minimal: dict) -> None:
        with pytest.raises(ValidationError, match="tilt"):
            SystemDesign(**minimal, tilt=91.0)

    def test_azimuth_out_of_range(self, minimal: dict) -> None:
        with pytest.raises(ValidationError, match="azimuth"):
            SystemDesign(**minimal, azimuth=361.0)

    def test_num_modules_out_of_range(self, minimal: dict) -> None:
        with pytest.raises(ValidationError, match="num_modules"):
            SystemDesign(**minimal, num_modules=3)

    def test_ground_clearance_zero_rejected(self, minimal: dict) -> None:
        with pytest.raises(ValidationError, match="ground_clearance_m"):
            SystemDesign(**minimal, ground_clearance_m=0.0)

    def test_ac_poi_mw_zero_rejected(self, minimal: dict) -> None:
        with pytest.raises(ValidationError, match="ac_poi_mw"):
            SystemDesign(**minimal, ac_poi_mw=0.0)


# ===================================================================
# Losses
# ===================================================================


class TestLosses:
    """Tests for the Losses model."""

    def test_all_defaults(self) -> None:
        losses = Losses()
        assert losses.shading_pct == 0.0
        assert losses.dc_wiring_pct == 2.0
        assert losses.ac_wiring_pct == 0.5
        assert losses.transformer_pct == 1.0
        assert losses.degradation_pct == 0.5
        assert losses.availability_pct == 2.5
        assert losses.mismatch_pct == 2.0
        assert losses.lid_pct == 1.5

    def test_custom_values(self) -> None:
        losses = Losses(shading_pct=5.0, dc_wiring_pct=3.0)
        assert losses.shading_pct == 5.0
        assert losses.dc_wiring_pct == 3.0

    def test_negative_rejected(self) -> None:
        with pytest.raises(ValidationError, match="shading_pct"):
            Losses(shading_pct=-1.0)

    def test_over_100_rejected(self) -> None:
        with pytest.raises(ValidationError, match="dc_wiring_pct"):
            Losses(dc_wiring_pct=101.0)

    def test_boundary_zero(self) -> None:
        losses = Losses(shading_pct=0.0)
        assert losses.shading_pct == 0.0

    def test_boundary_100(self) -> None:
        losses = Losses(shading_pct=100.0)
        assert losses.shading_pct == 100.0


# ===================================================================
# ResourceOverrides
# ===================================================================


class TestResourceOverrides:
    """Tests for the ResourceOverrides model."""

    def test_all_defaults(self) -> None:
        ro = ResourceOverrides()
        assert ro.resource_file_path is None
        assert ro.ground_truth_file is None

    def test_set_paths(self) -> None:
        ro = ResourceOverrides(
            resource_file_path="/data/solcast.csv",
            ground_truth_file="/data/gt.csv",
        )
        assert ro.resource_file_path == "/data/solcast.csv"
        assert ro.ground_truth_file == "/data/gt.csv"


# ===================================================================
# BuildabilityConfig
# ===================================================================


class TestBuildabilityConfig:
    """Tests for the BuildabilityConfig model."""

    def test_all_defaults(self) -> None:
        bc = BuildabilityConfig()
        assert bc.kmz_file_path is None
        assert bc.analysis_radius_km is None

    def test_kmz_only(self) -> None:
        bc = BuildabilityConfig(kmz_file_path="/path/to/file.kmz")
        assert bc.kmz_file_path == "/path/to/file.kmz"
        assert bc.analysis_radius_km is None

    def test_radius_only(self) -> None:
        bc = BuildabilityConfig(analysis_radius_km=5.0)
        assert bc.analysis_radius_km == 5.0
        assert bc.kmz_file_path is None

    def test_mutual_exclusivity(self) -> None:
        with pytest.raises(ValidationError, match="Cannot specify both"):
            BuildabilityConfig(
                kmz_file_path="/path/to/file.kmz",
                analysis_radius_km=5.0,
            )

    def test_radius_must_be_positive(self) -> None:
        with pytest.raises(ValidationError, match="analysis_radius_km"):
            BuildabilityConfig(analysis_radius_km=-1.0)

    def test_radius_zero_rejected(self) -> None:
        with pytest.raises(ValidationError, match="analysis_radius_km"):
            BuildabilityConfig(analysis_radius_km=0.0)


# ===================================================================
# BillConfig
# ===================================================================


class TestBillConfig:
    """Tests for the BillConfig model."""

    def test_rate_file_with_load_file(self) -> None:
        bc = BillConfig(
            rate_file_path="/rates/sce.json",
            load_profile_path="/loads/profile.csv",
        )
        assert bc.rate_file_path == "/rates/sce.json"
        assert bc.load_profile_path == "/loads/profile.csv"

    def test_openei_with_load_type(self) -> None:
        bc = BillConfig(
            utility_name="SCE",
            tariff_name="TOU-8",
            load_type="MediumOffice",
            annual_consumption_kwh=500000.0,
        )
        assert bc.utility_name == "SCE"
        assert bc.tariff_name == "TOU-8"
        assert bc.load_type == "MediumOffice"
        assert bc.annual_consumption_kwh == 500000.0

    def test_no_rate_source_raises(self) -> None:
        with pytest.raises(ValidationError, match="No rate source"):
            BillConfig(load_profile_path="/loads/profile.csv")

    def test_both_rate_sources_raises(self) -> None:
        with pytest.raises(ValidationError, match="Cannot specify both"):
            BillConfig(
                rate_file_path="/rates/sce.json",
                utility_name="SCE",
                tariff_name="TOU-8",
                load_profile_path="/loads/profile.csv",
            )

    def test_no_load_source_raises(self) -> None:
        with pytest.raises(ValidationError, match="No load source"):
            BillConfig(rate_file_path="/rates/sce.json")

    def test_both_load_sources_raises(self) -> None:
        with pytest.raises(ValidationError, match="Cannot specify both"):
            BillConfig(
                rate_file_path="/rates/sce.json",
                load_profile_path="/loads/profile.csv",
                load_type="MediumOffice",
            )

    def test_load_file_with_consumption_raises(self) -> None:
        with pytest.raises(ValidationError, match="Cannot specify"):
            BillConfig(
                rate_file_path="/rates/sce.json",
                load_profile_path="/loads/profile.csv",
                annual_consumption_kwh=500000.0,
            )

    def test_load_file_with_peak_demand_raises(self) -> None:
        with pytest.raises(ValidationError, match="Cannot specify"):
            BillConfig(
                rate_file_path="/rates/sce.json",
                load_profile_path="/loads/profile.csv",
                peak_demand_kw=100.0,
            )

    def test_load_type_without_consumption_raises(self) -> None:
        with pytest.raises(ValidationError, match="annual_consumption_kwh"):
            BillConfig(
                rate_file_path="/rates/sce.json",
                load_type="MediumOffice",
            )

    def test_utility_without_tariff_raises(self) -> None:
        with pytest.raises(ValidationError, match="Both utility_name"):
            BillConfig(
                utility_name="SCE",
                load_profile_path="/loads/profile.csv",
            )

    def test_tariff_without_utility_raises(self) -> None:
        with pytest.raises(ValidationError, match="Both utility_name"):
            BillConfig(
                tariff_name="TOU-8",
                load_profile_path="/loads/profile.csv",
            )

    def test_negative_consumption_rejected(self) -> None:
        with pytest.raises(ValidationError, match="annual_consumption_kwh"):
            BillConfig(
                rate_file_path="/rates/sce.json",
                load_type="MediumOffice",
                annual_consumption_kwh=-100.0,
            )

    def test_zero_peak_demand_rejected(self) -> None:
        with pytest.raises(ValidationError, match="peak_demand_kw"):
            BillConfig(
                rate_file_path="/rates/sce.json",
                load_type="MediumOffice",
                annual_consumption_kwh=500000.0,
                peak_demand_kw=0.0,
            )

    def test_openei_with_load_file_and_peak_demand(self) -> None:
        bc = BillConfig(
            utility_name="SCE",
            tariff_name="TOU-8",
            load_type="MediumOffice",
            annual_consumption_kwh=500000.0,
            peak_demand_kw=150.0,
        )
        assert bc.peak_demand_kw == 150.0


# ===================================================================
# BESSConfig
# ===================================================================


class TestBESSConfig:
    """Tests for the BESSConfig model."""

    def test_all_defaults(self) -> None:
        bc = BESSConfig()
        assert bc.power_mw is None
        assert bc.duration_hr is None
        assert bc.rte_pct == 88.0
        assert bc.min_soc_pct == 10.0
        assert bc.max_soc_pct == 90.0
        assert bc.strategy == "global"
        assert bc.installed_cost_per_kwh == 275.0
        assert bc.cycles_warranty == 5000
        assert bc.solar_only_charging is False
        assert bc.grid_only_charging is False

    def test_all_fields(self) -> None:
        bc = BESSConfig(
            power_mw=5.0,
            duration_hr=4.0,
            rte_pct=90.0,
            min_soc_pct=15.0,
            max_soc_pct=85.0,
            strategy="peak_shaving",
            installed_cost_per_kwh=300.0,
            cycles_warranty=6000,
            solar_only_charging=True,
        )
        assert bc.power_mw == 5.0
        assert bc.duration_hr == 4.0
        assert bc.rte_pct == 90.0
        assert bc.strategy == "peak_shaving"

    def test_soc_range_invalid(self) -> None:
        with pytest.raises(ValidationError, match="min_soc_pct"):
            BESSConfig(min_soc_pct=90.0, max_soc_pct=10.0)

    def test_soc_range_equal_invalid(self) -> None:
        with pytest.raises(ValidationError, match="min_soc_pct"):
            BESSConfig(min_soc_pct=50.0, max_soc_pct=50.0)

    def test_charging_mutual_exclusivity(self) -> None:
        with pytest.raises(ValidationError, match="mutually exclusive"):
            BESSConfig(solar_only_charging=True, grid_only_charging=True)

    def test_strategy_tou_arbitrage(self) -> None:
        bc = BESSConfig(strategy="tou_arbitrage")
        assert bc.strategy == "tou_arbitrage"

    def test_strategy_invalid(self) -> None:
        with pytest.raises(ValidationError, match="strategy"):
            BESSConfig(strategy="random")

    def test_rte_below_50_rejected(self) -> None:
        with pytest.raises(ValidationError, match="rte_pct"):
            BESSConfig(rte_pct=49.0)

    def test_rte_above_100_rejected(self) -> None:
        with pytest.raises(ValidationError, match="rte_pct"):
            BESSConfig(rte_pct=101.0)

    def test_power_zero_rejected(self) -> None:
        with pytest.raises(ValidationError, match="power_mw"):
            BESSConfig(power_mw=0.0)

    def test_duration_negative_rejected(self) -> None:
        with pytest.raises(ValidationError, match="duration_hr"):
            BESSConfig(duration_hr=-1.0)

    def test_installed_cost_zero_rejected(self) -> None:
        with pytest.raises(ValidationError, match="installed_cost_per_kwh"):
            BESSConfig(installed_cost_per_kwh=0.0)

    def test_cycles_warranty_zero_rejected(self) -> None:
        with pytest.raises(ValidationError, match="cycles_warranty"):
            BESSConfig(cycles_warranty=0)

    def test_strategy_case_insensitive(self) -> None:
        bc = BESSConfig(strategy="Peak_Shaving")
        assert bc.strategy == "peak_shaving"


# ===================================================================
# BESSEconomics
# ===================================================================


class TestBESSEconomics:
    """Tests for the BESSEconomics model."""

    def test_all_defaults(self) -> None:
        be = BESSEconomics()
        assert be.discount_rate_pct == 7.0
        assert be.project_lifetime_years == 25
        assert be.rate_escalation_pct == 2.0
        assert be.solar_cost_per_kw_dc is None
        assert be.solar_cost_per_kw_ac is None
        assert be.solar_opex_per_kw_dc_year == 0.0
        assert be.bess_opex_per_kw_year == 0.0
        assert be.power_min_mw is None
        assert be.power_max_mw is None
        assert be.duration_min_hr == 2.0
        assert be.duration_max_hr == 5.0
        assert be.optimize is False

    def test_all_fields(self) -> None:
        be = BESSEconomics(
            discount_rate_pct=8.0,
            project_lifetime_years=30,
            rate_escalation_pct=3.0,
            solar_cost_per_kw_dc=900.0,
            solar_cost_per_kw_ac=150.0,
            solar_opex_per_kw_dc_year=10.0,
            bess_opex_per_kw_year=5.0,
            power_min_mw=1.0,
            power_max_mw=10.0,
            duration_min_hr=1.0,
            duration_max_hr=8.0,
            optimize=True,
        )
        assert be.discount_rate_pct == 8.0
        assert be.optimize is True

    def test_duration_range_invalid(self) -> None:
        with pytest.raises(ValidationError, match="duration_min_hr"):
            BESSEconomics(duration_min_hr=5.0, duration_max_hr=2.0)

    def test_duration_range_equal_invalid(self) -> None:
        with pytest.raises(ValidationError, match="duration_min_hr"):
            BESSEconomics(duration_min_hr=3.0, duration_max_hr=3.0)

    def test_discount_rate_zero_rejected(self) -> None:
        with pytest.raises(ValidationError, match="discount_rate_pct"):
            BESSEconomics(discount_rate_pct=0.0)

    def test_project_lifetime_zero_rejected(self) -> None:
        with pytest.raises(ValidationError, match="project_lifetime_years"):
            BESSEconomics(project_lifetime_years=0)


# ===================================================================
# FTMConfig
# ===================================================================


class TestFTMConfig:
    """Tests for the FTMConfig model."""

    def test_all_defaults(self) -> None:
        fc = FTMConfig()
        assert fc.dispatch_mode == "btm"
        assert fc.iso is None
        assert fc.lmp_zone is None
        assert fc.lmp_node_ids is None
        assert fc.lmp_market == "DAY_AHEAD_HOURLY"
        assert fc.lmp_year is None
        assert fc.ancillary_revenue_per_kw_year == 0.0

    def test_ftm_mode(self) -> None:
        fc = FTMConfig(
            dispatch_mode="ftm",
            iso="pjm",
            lmp_zone="AEP",
            lmp_market="REAL_TIME_HOURLY",
            lmp_year=2024,
            ancillary_revenue_per_kw_year=15.0,
        )
        assert fc.dispatch_mode == "ftm"
        assert fc.iso == "pjm"
        assert fc.lmp_zone == "AEP"
        assert fc.lmp_market == "REAL_TIME_HOURLY"
        assert fc.lmp_year == 2024
        assert fc.ancillary_revenue_per_kw_year == 15.0

    def test_dispatch_mode_case_insensitive(self) -> None:
        fc = FTMConfig(dispatch_mode="FTM")
        assert fc.dispatch_mode == "ftm"

    def test_dispatch_mode_invalid(self) -> None:
        with pytest.raises(ValidationError, match="dispatch_mode"):
            FTMConfig(dispatch_mode="wholesale")

    def test_iso_case_insensitive(self) -> None:
        fc = FTMConfig(iso="ERCOT")
        assert fc.iso == "ercot"

    def test_iso_caiso(self) -> None:
        fc = FTMConfig(iso="CAISO")
        assert fc.iso == "caiso"

    def test_iso_invalid(self) -> None:
        with pytest.raises(ValidationError, match="iso"):
            FTMConfig(iso="nyiso")

    def test_lmp_market_invalid(self) -> None:
        with pytest.raises(ValidationError, match="lmp_market"):
            FTMConfig(lmp_market="REAL_TIME_5MIN")

    def test_ancillary_revenue_negative_rejected(self) -> None:
        with pytest.raises(ValidationError, match="ancillary_revenue"):
            FTMConfig(ancillary_revenue_per_kw_year=-1.0)

    def test_ancillary_revenue_zero_accepted(self) -> None:
        fc = FTMConfig(ancillary_revenue_per_kw_year=0.0)
        assert fc.ancillary_revenue_per_kw_year == 0.0


# ===================================================================
# ProductionSummary (response)
# ===================================================================


class TestProductionSummary:
    """Tests for the ProductionSummary response model."""

    def test_construct(self) -> None:
        ps = ProductionSummary(
            annual_energy_mwh=25000.0,
            capacity_factor_dc=0.20,
            capacity_factor_ac=0.254,
            specific_yield_kwh_per_kwp=1650.0,
            performance_ratio=0.82,
        )
        assert ps.annual_energy_mwh == 25000.0
        assert ps.capacity_factor_dc == 0.20
        assert ps.capacity_factor_ac == 0.254
        assert ps.specific_yield_kwh_per_kwp == 1650.0
        assert ps.performance_ratio == 0.82

    def test_missing_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            ProductionSummary(
                annual_energy_mwh=25000.0,
                capacity_factor_dc=0.20,
            )


# ===================================================================
# LossSummary (response)
# ===================================================================


class TestLossSummary:
    """Tests for the LossSummary response model."""

    def test_construct(self) -> None:
        ls = LossSummary(
            shading_pct=3.0,
            dc_wiring_pct=2.0,
            ac_wiring_pct=0.5,
            transformer_pct=1.0,
            mismatch_pct=2.0,
            lid_pct=1.5,
            availability_pct=2.5,
            subhourly_clipping_pct=0.3,
            inverter_efficiency_pct=2.1,
        )
        assert ls.shading_pct == 3.0
        assert ls.subhourly_clipping_pct == 0.3
        assert ls.inverter_efficiency_pct == 2.1

    def test_missing_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            LossSummary(shading_pct=3.0)
