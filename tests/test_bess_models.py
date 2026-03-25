"""Tests for BESS dispatch models and SiteConfig BESS field validation."""

import math
from pathlib import Path

import pytest

from src.bess.models import BESSConfig, BESSBillComparison, DispatchResult, HourlyDispatch
from src.config.loader import load_config
from src.config.schema import SiteConfig


# ---------------------------------------------------------------------------
# Helper: base SiteConfig kwargs
# ---------------------------------------------------------------------------

def _base_kwargs() -> dict:
    """Return minimal valid SiteConfig kwargs (no BESS, no bill)."""
    return {
        "run_name": "BessTest",
        "site_name": "BessTestSite",
        "customer": "TestCustomer",
        "latitude": 33.45,
        "longitude": -112.07,
        "dc_size_mw": 13.0,
        "ac_installed_mw": 10.0,
        "ac_poi_mw": 10.0,
        "racking": "tracker",
        "tilt": 55.0,
        "azimuth": 180.0,
        "module_orientation": "portrait",
        "number_of_modules": 1,
        "ground_clearance_height_m": 1.5,
        "panel_model": "CSI Solar Co. Ltd. CS3U-355P",
        "bifacial": False,
        "inverter_model": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
        "gcr": 0.35,
        "shading_percent": 0.0,
        "dc_wiring_loss_percent": 2.0,
        "ac_wiring_loss_percent": 1.0,
        "transformer_losses_percent": 1.0,
        "degradation_percent": 0.5,
        "availability_percent": 2.5,
        "module_mismatch_percent": 1.0,
        "lid_percent": 1.5,
    }


def _bess_kwargs(**overrides: object) -> dict:
    """Return SiteConfig kwargs with BESS dispatch enabled.

    Includes valid bill_calculation fields (required for BESS dispatch).
    """
    kw = _base_kwargs()
    kw.update({
        "bess_dispatch_required": True,
        "bess_power_mw": 5.0,
        "bess_duration_hr": 4.0,
        "bill_calculation": True,
        "utility_name": "Test Utility",
        "tariff_name": "Test Rate",
        "load_type": "MediumOffice",
        "annual_consumption_kwh": 500000.0,
    })
    kw.update(overrides)
    return kw


# ===========================================================================
# BESSConfig.from_site_config
# ===========================================================================


class TestBESSConfigFromSiteConfig:
    """Test BESSConfig.from_site_config() derived value calculations."""

    def test_derived_values_5mw_4hr(self) -> None:
        """Verify all derived values for a 5 MW / 4 hr battery with defaults."""
        site = SiteConfig(**_bess_kwargs())
        config = BESSConfig.from_site_config(site)

        # Power and capacity
        assert config.bess_power_kw == 5000.0
        assert config.bess_duration_hr == 4.0
        assert config.capacity_kwh == 20000.0

        # SOC bounds
        assert config.min_soc_kwh == 2000.0   # 20000 * 10%
        assert config.max_soc_kwh == 18000.0   # 20000 * 90%
        assert config.usable_capacity_kwh == 16000.0  # 20000 * 80%

        # Efficiency (sqrt of 0.88)
        expected_eff = math.sqrt(0.88)
        assert config.charge_efficiency == pytest.approx(expected_eff, rel=1e-6)
        assert config.discharge_efficiency == pytest.approx(expected_eff, rel=1e-6)

        # Degradation cost: (275 * 20000) / (5000 * 20000 * 2) = 0.0275
        assert config.degradation_cost_per_kwh == pytest.approx(0.0275, rel=1e-6)

        # Strategy
        assert config.strategy == "global"

    def test_defaults_flow_through(self) -> None:
        """Verify default RTE/SOC/cost/cycles values are used when not overridden."""
        site = SiteConfig(**_bess_kwargs())

        # Confirm defaults on SiteConfig
        assert site.bess_rte_percent == 88.0
        assert site.bess_min_soc_percent == 10.0
        assert site.bess_max_soc_percent == 90.0
        assert site.bess_installed_cost_per_kwh == 275.0
        assert site.bess_cycles_warranty == 5000
        assert site.bess_strategy == "global"

        config = BESSConfig.from_site_config(site)
        assert config.charge_efficiency == pytest.approx(math.sqrt(0.88), rel=1e-6)
        assert config.min_soc_kwh == pytest.approx(2000.0)
        assert config.max_soc_kwh == pytest.approx(18000.0)

    def test_custom_rte_and_soc(self) -> None:
        """Verify non-default RTE and SOC values propagate correctly."""
        site = SiteConfig(**_bess_kwargs(
            bess_rte_percent=92.0,
            bess_min_soc_percent=20.0,
            bess_max_soc_percent=80.0,
        ))
        config = BESSConfig.from_site_config(site)

        assert config.charge_efficiency == pytest.approx(math.sqrt(0.92), rel=1e-6)
        assert config.min_soc_kwh == pytest.approx(4000.0)   # 20000 * 20%
        assert config.max_soc_kwh == pytest.approx(16000.0)   # 20000 * 80%
        assert config.usable_capacity_kwh == pytest.approx(12000.0)  # 20000 * 60%

    def test_custom_cost_and_cycles(self) -> None:
        """Verify custom cost and cycle warranty affect degradation cost."""
        site = SiteConfig(**_bess_kwargs(
            bess_installed_cost_per_kwh=200.0,
            bess_cycles_warranty=4000,
        ))
        config = BESSConfig.from_site_config(site)

        # (200 * 20000) / (4000 * 20000 * 2) = 0.025
        assert config.degradation_cost_per_kwh == pytest.approx(0.025, rel=1e-6)


# ===========================================================================
# SiteConfig BESS validation — error cases
# ===========================================================================


class TestBESSValidationErrors:
    """Test SiteConfig model_validator catches invalid BESS configurations."""

    def test_missing_bess_power_mw(self) -> None:
        """bess_dispatch_required=True with missing bess_power_mw raises."""
        with pytest.raises(Exception, match="BESS Power.*must be > 0"):
            SiteConfig(**_bess_kwargs(bess_power_mw=None))

    def test_missing_bess_duration_hr(self) -> None:
        """bess_dispatch_required=True with missing bess_duration_hr raises."""
        with pytest.raises(Exception, match="BESS Duration.*must be > 0"):
            SiteConfig(**_bess_kwargs(bess_duration_hr=None))

    def test_min_soc_gte_max_soc(self) -> None:
        """bess_min_soc >= bess_max_soc raises ValueError."""
        with pytest.raises(Exception, match="Min SOC.*must be less than.*Max SOC"):
            SiteConfig(**_bess_kwargs(
                bess_min_soc_percent=90.0,
                bess_max_soc_percent=90.0,
            ))

    def test_min_soc_greater_than_max_soc(self) -> None:
        """bess_min_soc > bess_max_soc raises ValueError."""
        with pytest.raises(Exception, match="Min SOC.*must be less than.*Max SOC"):
            SiteConfig(**_bess_kwargs(
                bess_min_soc_percent=95.0,
                bess_max_soc_percent=10.0,
            ))

    def test_bill_calculation_false_raises(self) -> None:
        """bess_dispatch_required=True with bill_calculation=False raises."""
        with pytest.raises(Exception, match="Bill Calculation must be True"):
            SiteConfig(**_bess_kwargs(bill_calculation=False))

    def test_invalid_strategy(self) -> None:
        """Invalid bess_strategy raises ValueError."""
        with pytest.raises(Exception, match="BESS Strategy must be one of"):
            SiteConfig(**_bess_kwargs(bess_strategy="invalid_strategy"))

    def test_rte_below_50_raises(self) -> None:
        """bess_rte_percent below 50 raises ValueError."""
        with pytest.raises(Exception, match="BESS RTE must be between"):
            SiteConfig(**_bess_kwargs(bess_rte_percent=40.0))

    def test_rte_above_100_raises(self) -> None:
        """bess_rte_percent above 100 raises ValueError."""
        with pytest.raises(Exception, match="BESS RTE must be between"):
            SiteConfig(**_bess_kwargs(bess_rte_percent=105.0))


# ===========================================================================
# SiteConfig BESS validation — happy path
# ===========================================================================


class TestBESSValidationSkipped:
    """Test that BESS validation is skipped when dispatch is disabled."""

    def test_dispatch_false_skips_all_bess_validation(self) -> None:
        """When bess_dispatch_required=False, no BESS fields are validated."""
        kw = _base_kwargs()
        # Leave all BESS fields at defaults (no power, no duration)
        config = SiteConfig(**kw)

        assert config.bess_dispatch_required is False
        assert config.bess_power_mw is None
        assert config.bess_duration_hr is None
        assert config.bess_rte_percent == 88.0
        assert config.bess_strategy == "global"

    def test_dispatch_false_with_partial_bess_fields(self) -> None:
        """Partial BESS fields don't raise when dispatch is disabled."""
        kw = _base_kwargs()
        kw["bess_power_mw"] = 5.0
        # bess_duration_hr intentionally missing
        config = SiteConfig(**kw)
        assert config.bess_dispatch_required is False
        assert config.bess_power_mw == 5.0


# ===========================================================================
# bess_dispatch_required field_validator coercion
# ===========================================================================


class TestBESSDispatchRequiredCoercion:
    """Test field_validator coercion of bess_dispatch_required from CSV strings."""

    def test_true_string(self) -> None:
        """'TRUE' string coerces to True."""
        kw = _bess_kwargs(bess_dispatch_required="TRUE")
        config = SiteConfig(**kw)
        assert config.bess_dispatch_required is True

    def test_false_string(self) -> None:
        """'FALSE' string coerces to False."""
        kw = _base_kwargs()
        kw["bess_dispatch_required"] = "FALSE"
        config = SiteConfig(**kw)
        assert config.bess_dispatch_required is False

    def test_empty_string(self) -> None:
        """Empty string coerces to False."""
        kw = _base_kwargs()
        kw["bess_dispatch_required"] = ""
        config = SiteConfig(**kw)
        assert config.bess_dispatch_required is False

    def test_one_string(self) -> None:
        """'1' string coerces to True."""
        kw = _bess_kwargs(bess_dispatch_required="1")
        config = SiteConfig(**kw)
        assert config.bess_dispatch_required is True

    def test_zero_string(self) -> None:
        """'0' string coerces to False."""
        kw = _base_kwargs()
        kw["bess_dispatch_required"] = "0"
        config = SiteConfig(**kw)
        assert config.bess_dispatch_required is False

    def test_none_coerces_to_false(self) -> None:
        """None (CSV NaN) coerces to False."""
        kw = _base_kwargs()
        kw["bess_dispatch_required"] = None
        config = SiteConfig(**kw)
        assert config.bess_dispatch_required is False


# ===========================================================================
# CSV backward compatibility
# ===========================================================================


class TestCSVBackwardCompatibility:
    """Existing CSV templates parse without errors after BESS field changes."""

    @pytest.fixture(params=[
        "EnergyAnalyticsInputsSingleRowTest-Sheet1_test.csv",
        "tests/fixtures/single_row_test.csv",
        "tests/fixtures/multi_row_test.csv",
    ])
    def csv_path(self, request: pytest.FixtureRequest) -> Path:
        """Parametrized fixture for test CSV paths."""
        path = Path(request.param)
        if not path.exists():
            pytest.skip(f"CSV not found: {path}")
        return path

    def test_csv_parses_without_errors(self, csv_path: Path) -> None:
        """CSV loads and validates successfully with new BESS field types."""
        configs = load_config(csv_path)
        assert len(configs) >= 1

        for site in configs:
            # bess_dispatch_required should be False (blank CSV cell)
            assert site.bess_dispatch_required is False
            # bess_optimization_required is now bool, default False
            assert site.bess_optimization_required is False
            # New BESS fields should have defaults
            assert site.bess_power_mw is None
            assert site.bess_duration_hr is None
            assert site.bess_rte_percent == 88.0
            assert site.bess_strategy == "global"
