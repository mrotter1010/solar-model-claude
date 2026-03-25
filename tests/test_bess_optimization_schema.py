"""Tests for BESS sizing optimization schema fields and validation."""

import pytest

from src.config.loader import COLUMN_MAP
from src.config.schema import SiteConfig


# ---------------------------------------------------------------------------
# Helper: base SiteConfig kwargs
# ---------------------------------------------------------------------------


def _base_kwargs() -> dict:
    """Return minimal valid SiteConfig kwargs (no BESS, no bill)."""
    return {
        "run_name": "OptTest",
        "site_name": "OptTestSite",
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


def _optimization_kwargs(**overrides: object) -> dict:
    """Return SiteConfig kwargs with BESS optimization enabled.

    Includes valid bill_calculation fields (required for BESS optimization)
    and solar cost fields (required for non-grid-only optimization).
    """
    kw = _base_kwargs()
    kw.update({
        "bess_dispatch_required": True,
        "bess_optimization_required": True,
        "bill_calculation": True,
        "utility_name": "Test Utility",
        "tariff_name": "Test Rate",
        "load_type": "MediumOffice",
        "annual_consumption_kwh": 500000.0,
        "solar_cost_per_kw_dc": 1200.0,
        "solar_cost_per_kw_ac": 200.0,
    })
    kw.update(overrides)
    return kw


# ===========================================================================
# bess_optimization_required coercion
# ===========================================================================


class TestBessOptimizationRequiredCoercion:
    """Test field_validator coercion for bess_optimization_required."""

    @pytest.mark.parametrize(
        "input_val,expected",
        [
            (None, False),
            ("", False),
            (0.0, False),
            (False, False),
        ],
        ids=["None→False", "empty→False", "float_0.0→False", "bool_False→False"],
    )
    def test_coercion_false(self, input_val: object, expected: bool) -> None:
        """bess_optimization_required should coerce falsy inputs to False."""
        kw = _base_kwargs()
        kw["bess_optimization_required"] = input_val
        site = SiteConfig(**kw)
        assert site.bess_optimization_required is expected

    @pytest.mark.parametrize(
        "input_val",
        ["TRUE", "YES", "1", 1.0, True],
        ids=["TRUE", "YES", "str_1", "float_1.0", "bool_True"],
    )
    def test_coercion_true(self, input_val: object) -> None:
        """bess_optimization_required should coerce truthy inputs to True.

        Uses full optimization kwargs so the model validator passes.
        """
        kw = _optimization_kwargs(bess_optimization_required=input_val)
        site = SiteConfig(**kw)
        assert site.bess_optimization_required is True


# ===========================================================================
# Cross-validation: optimization requires dispatch
# ===========================================================================


class TestOptimizationRequiresDispatch:
    """Optimization=True requires dispatch=True."""

    def test_optimization_without_dispatch_raises(self) -> None:
        """bess_optimization_required=True with dispatch=False → ValueError."""
        kw = _base_kwargs()
        kw["bess_optimization_required"] = True
        kw["bess_dispatch_required"] = False
        with pytest.raises(ValueError, match="BESS Dispatch Required must be True"):
            SiteConfig(**kw)


# ===========================================================================
# Cross-validation: optimization requires bill_calculation
# ===========================================================================


class TestOptimizationRequiresBillCalculation:
    """Optimization=True requires bill_calculation=True."""

    def test_optimization_without_bill_raises(self) -> None:
        """bess_optimization_required=True with bill_calculation=False → ValueError."""
        kw = _base_kwargs()
        kw["bess_dispatch_required"] = True
        kw["bess_optimization_required"] = True
        kw["bill_calculation"] = False
        with pytest.raises(ValueError, match="Bill Calculation must be True"):
            SiteConfig(**kw)


# ===========================================================================
# Cross-validation: solar cost fields required for non-grid-only
# ===========================================================================


class TestOptimizationRequiresSolarCosts:
    """Non-grid-only optimization requires solar cost fields."""

    def test_missing_solar_cost_dc_raises(self) -> None:
        """Missing solar_cost_per_kw_dc with optimization=True → ValueError."""
        kw = _optimization_kwargs(solar_cost_per_kw_dc=None)
        with pytest.raises(ValueError, match="Solar Cost.*DC.*required"):
            SiteConfig(**kw)

    def test_missing_solar_cost_ac_raises(self) -> None:
        """Missing solar_cost_per_kw_ac with optimization=True → ValueError."""
        kw = _optimization_kwargs(solar_cost_per_kw_ac=None)
        with pytest.raises(ValueError, match="Solar Cost.*AC.*required"):
            SiteConfig(**kw)

    def test_grid_only_skips_solar_cost_check(self) -> None:
        """Grid-only optimization does NOT require solar costs."""
        kw = _optimization_kwargs(
            bess_grid_only_charging=True,
            bess_solar_only_charging=False,
            solar_cost_per_kw_dc=None,
            solar_cost_per_kw_ac=None,
            bess_power_min_mw=1.0,
            bess_power_max_mw=10.0,
        )
        site = SiteConfig(**kw)
        assert site.solar_cost_per_kw_dc is None
        assert site.solar_cost_per_kw_ac is None


# ===========================================================================
# Cross-validation: grid-only requires power bounds
# ===========================================================================


class TestGridOnlyOptimizationRequiresPowerBounds:
    """Grid-only optimization requires bess_power_min/max_mw."""

    def test_missing_power_min_raises(self) -> None:
        """Missing bess_power_min_mw with grid-only optimization → ValueError."""
        kw = _optimization_kwargs(
            bess_grid_only_charging=True,
            bess_solar_only_charging=False,
            solar_cost_per_kw_dc=None,
            solar_cost_per_kw_ac=None,
            bess_power_min_mw=None,
            bess_power_max_mw=10.0,
        )
        with pytest.raises(ValueError, match="BESS Power Min.*required"):
            SiteConfig(**kw)

    def test_missing_power_max_raises(self) -> None:
        """Missing bess_power_max_mw with grid-only optimization → ValueError."""
        kw = _optimization_kwargs(
            bess_grid_only_charging=True,
            bess_solar_only_charging=False,
            solar_cost_per_kw_dc=None,
            solar_cost_per_kw_ac=None,
            bess_power_min_mw=1.0,
            bess_power_max_mw=None,
        )
        with pytest.raises(ValueError, match="BESS Power Max.*required"):
            SiteConfig(**kw)


# ===========================================================================
# Range validation: power min < max, duration min < max
# ===========================================================================


class TestRangeValidation:
    """BESS sizing bounds must have min < max."""

    def test_power_min_ge_max_raises(self) -> None:
        """bess_power_min_mw >= bess_power_max_mw → ValueError."""
        kw = _optimization_kwargs(
            bess_power_min_mw=10.0,
            bess_power_max_mw=5.0,
        )
        with pytest.raises(ValueError, match="BESS Power Min.*less than"):
            SiteConfig(**kw)

    def test_power_min_equals_max_raises(self) -> None:
        """bess_power_min_mw == bess_power_max_mw → ValueError."""
        kw = _optimization_kwargs(
            bess_power_min_mw=5.0,
            bess_power_max_mw=5.0,
        )
        with pytest.raises(ValueError, match="BESS Power Min.*less than"):
            SiteConfig(**kw)

    def test_duration_min_ge_max_raises(self) -> None:
        """bess_duration_min_hr >= bess_duration_max_hr → ValueError."""
        kw = _optimization_kwargs(
            bess_duration_min_hr=6.0,
            bess_duration_max_hr=4.0,
        )
        with pytest.raises(ValueError, match="BESS Duration Min.*less than"):
            SiteConfig(**kw)

    def test_duration_min_equals_max_raises(self) -> None:
        """bess_duration_min_hr == bess_duration_max_hr → ValueError."""
        kw = _optimization_kwargs(
            bess_duration_min_hr=4.0,
            bess_duration_max_hr=4.0,
        )
        with pytest.raises(ValueError, match="BESS Duration Min.*less than"):
            SiteConfig(**kw)


# ===========================================================================
# Financial field validation with optimization
# ===========================================================================


class TestFinancialFieldValidation:
    """Financial fields must be valid when optimization is enabled."""

    def test_discount_rate_zero_raises(self) -> None:
        """discount_rate_pct <= 0 with optimization=True → ValueError."""
        kw = _optimization_kwargs(discount_rate_pct=0.0)
        with pytest.raises(ValueError, match="Discount Rate.*must be > 0"):
            SiteConfig(**kw)

    def test_discount_rate_negative_raises(self) -> None:
        """Negative discount_rate_pct with optimization=True → ValueError."""
        kw = _optimization_kwargs(discount_rate_pct=-1.0)
        with pytest.raises(ValueError, match="Discount Rate.*must be > 0"):
            SiteConfig(**kw)

    def test_project_lifetime_zero_raises(self) -> None:
        """project_lifetime_years <= 0 with optimization=True → ValueError."""
        kw = _optimization_kwargs(project_lifetime_years=0)
        with pytest.raises(ValueError, match="Project Lifetime.*must be > 0"):
            SiteConfig(**kw)

    def test_project_lifetime_negative_raises(self) -> None:
        """Negative project_lifetime_years with optimization=True → ValueError."""
        kw = _optimization_kwargs(project_lifetime_years=-5)
        with pytest.raises(ValueError, match="Project Lifetime.*must be > 0"):
            SiteConfig(**kw)


# ===========================================================================
# Optimization allows dummy power/duration
# ===========================================================================


class TestOptimizationDummyDefaults:
    """Optimization mode allows bess_power_mw=0, bess_duration_hr=0."""

    def test_zero_power_and_duration_accepted(self) -> None:
        """optimization=True with power=0, duration=0 should NOT raise."""
        kw = _optimization_kwargs(
            bess_power_mw=0.0,
            bess_duration_hr=0.0,
        )
        site = SiteConfig(**kw)
        assert site.bess_power_mw == 0.0
        assert site.bess_duration_hr == 0.0

    def test_none_power_and_duration_set_to_zero(self) -> None:
        """optimization=True with power=None, duration=None → set to 0.0."""
        kw = _optimization_kwargs(
            bess_power_mw=None,
            bess_duration_hr=None,
        )
        site = SiteConfig(**kw)
        assert site.bess_power_mw == 0.0
        assert site.bess_duration_hr == 0.0

    def test_positive_power_and_duration_preserved(self) -> None:
        """optimization=True with positive power/duration preserves values."""
        kw = _optimization_kwargs(
            bess_power_mw=5.0,
            bess_duration_hr=4.0,
        )
        site = SiteConfig(**kw)
        assert site.bess_power_mw == 5.0
        assert site.bess_duration_hr == 4.0


# ===========================================================================
# New field defaults
# ===========================================================================


class TestNewFieldDefaults:
    """All new fields have correct defaults when not provided."""

    def test_financial_defaults(self) -> None:
        """Financial fields default correctly."""
        site = SiteConfig(**_base_kwargs())
        assert site.discount_rate_pct == 7.0
        assert site.project_lifetime_years == 25
        assert site.rate_escalation_pct == 2.0

    def test_solar_cost_defaults(self) -> None:
        """Solar cost fields default to None."""
        site = SiteConfig(**_base_kwargs())
        assert site.solar_cost_per_kw_dc is None
        assert site.solar_cost_per_kw_ac is None
        assert site.solar_opex_per_kw_dc_year == 0.0

    def test_bess_sizing_defaults(self) -> None:
        """BESS sizing fields default correctly."""
        site = SiteConfig(**_base_kwargs())
        assert site.bess_power_min_mw is None
        assert site.bess_power_max_mw is None
        assert site.bess_duration_min_hr == 2.0
        assert site.bess_duration_max_hr == 5.0
        assert site.bess_opex_per_kw_year == 0.0

    def test_bess_optimization_required_default(self) -> None:
        """bess_optimization_required defaults to False."""
        site = SiteConfig(**_base_kwargs())
        assert site.bess_optimization_required is False

    def test_float_defaults_coerce_from_none(self) -> None:
        """New float fields coerce None (CSV empty cell) to defaults."""
        kw = _base_kwargs()
        kw["discount_rate_pct"] = None
        kw["project_lifetime_years"] = None
        kw["rate_escalation_pct"] = None
        kw["solar_opex_per_kw_dc_year"] = None
        kw["bess_opex_per_kw_year"] = None
        kw["bess_duration_min_hr"] = None
        kw["bess_duration_max_hr"] = None
        site = SiteConfig(**kw)
        assert site.discount_rate_pct == 7.0
        assert site.project_lifetime_years == 25
        assert site.rate_escalation_pct == 2.0
        assert site.solar_opex_per_kw_dc_year == 0.0
        assert site.bess_opex_per_kw_year == 0.0
        assert site.bess_duration_min_hr == 2.0
        assert site.bess_duration_max_hr == 5.0

    def test_float_defaults_coerce_from_empty_string(self) -> None:
        """New float fields coerce empty strings to defaults."""
        kw = _base_kwargs()
        kw["discount_rate_pct"] = ""
        kw["rate_escalation_pct"] = "  "
        kw["bess_opex_per_kw_year"] = ""
        site = SiteConfig(**kw)
        assert site.discount_rate_pct == 7.0
        assert site.rate_escalation_pct == 2.0
        assert site.bess_opex_per_kw_year == 0.0

    def test_optional_float_coerce_from_empty(self) -> None:
        """Optional float fields coerce empty strings to None."""
        kw = _base_kwargs()
        kw["bess_power_min_mw"] = ""
        kw["bess_power_max_mw"] = "  "
        kw["solar_cost_per_kw_dc"] = ""
        kw["solar_cost_per_kw_ac"] = None
        site = SiteConfig(**kw)
        assert site.bess_power_min_mw is None
        assert site.bess_power_max_mw is None
        assert site.solar_cost_per_kw_dc is None
        assert site.solar_cost_per_kw_ac is None


# ===========================================================================
# COLUMN_MAP coverage
# ===========================================================================


class TestColumnMapEntries:
    """COLUMN_MAP has entries for all 11 new optimization columns."""

    @pytest.mark.parametrize(
        "csv_name,field_name",
        [
            ("Discount Rate (%)", "discount_rate_pct"),
            ("Project Lifetime (years)", "project_lifetime_years"),
            ("Rate Escalation (%)", "rate_escalation_pct"),
            ("Solar Cost ($/kW DC)", "solar_cost_per_kw_dc"),
            ("Solar Cost ($/kW AC)", "solar_cost_per_kw_ac"),
            ("Solar O&M ($/kW-DC/yr)", "solar_opex_per_kw_dc_year"),
            ("BESS O&M ($/kW/yr)", "bess_opex_per_kw_year"),
            ("BESS Power Min (MW)", "bess_power_min_mw"),
            ("BESS Power Max (MW)", "bess_power_max_mw"),
            ("BESS Duration Min (hr)", "bess_duration_min_hr"),
            ("BESS Duration Max (hr)", "bess_duration_max_hr"),
        ],
    )
    def test_column_map_entry(self, csv_name: str, field_name: str) -> None:
        """COLUMN_MAP maps CSV column name to SiteConfig field."""
        assert csv_name in COLUMN_MAP
        assert COLUMN_MAP[csv_name] == field_name

    def test_column_map_total_count(self) -> None:
        """COLUMN_MAP should have 63 entries (52 original + 11 new)."""
        assert len(COLUMN_MAP) == 63
