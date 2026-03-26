"""Tests for FTM/wholesale dispatch fields in SiteConfig (M14e)."""

import pytest

from src.config.schema import SiteConfig


def _make_site_config(**overrides: object) -> SiteConfig:
    """Create a SiteConfig with sensible defaults, overridable per test."""
    defaults = {
        "run_name": "test_ftm_run",
        "site_name": "Test FTM Site",
        "customer": "Test Customer",
        "latitude": 33.45,
        "longitude": -112.07,
        "dc_size_mw": 13.0,
        "ac_installed_mw": 10.0,
        "ac_poi_mw": 10.0,
        "racking": "tracker",
        "tilt": 60.0,
        "azimuth": 180.0,
        "module_orientation": "portrait",
        "number_of_modules": 1,
        "ground_clearance_height_m": 1.5,
        "panel_model": "CSI Solar Co. Ltd. CS3U-355P",
        "bifacial": True,
        "inverter_model": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
        "gcr": 0.35,
        "shading_percent": 3.0,
        "dc_wiring_loss_percent": 2.0,
        "ac_wiring_loss_percent": 1.0,
        "transformer_losses_percent": 1.5,
        "degradation_percent": 0.5,
        "availability_percent": 2.0,
        "module_mismatch_percent": 1.0,
        "lid_percent": 1.5,
    }
    defaults.update(overrides)
    return SiteConfig(**defaults)


# BESS fields needed when bess_dispatch_required=True (satisfies existing validators)
_BESS_DISPATCH_FIELDS = {
    "bess_dispatch_required": True,
    "bess_power_mw": 5.0,
    "bess_duration_hr": 2.0,
    "bess_rte_percent": 88.0,
    "bess_min_soc_percent": 10.0,
    "bess_max_soc_percent": 90.0,
    "bess_strategy": "global",
    "bess_installed_cost_per_kwh": 275.0,
    "bess_cycles_warranty": 5000,
    "bill_calculation": True,
    "rate_file_path": "tests/fixtures/rates/sample_rate.json",
    "load_type": "MediumOffice",
    "annual_consumption_kwh": 2_000_000.0,
}


class TestDefaultValues:
    """Default FTM field values for backward compatibility."""

    def test_dispatch_mode_defaults_to_btm(self) -> None:
        """dispatch_mode should default to 'btm' when not provided."""
        config = _make_site_config()
        assert config.dispatch_mode == "btm"

    def test_iso_defaults_to_none(self) -> None:
        """ISO should default to None when not provided."""
        config = _make_site_config()
        assert config.iso is None

    def test_lmp_zone_defaults_to_none(self) -> None:
        """LMP Zone should default to None when not provided."""
        config = _make_site_config()
        assert config.lmp_zone is None

    def test_lmp_node_ids_defaults_to_none(self) -> None:
        """LMP Node IDs should default to None when not provided."""
        config = _make_site_config()
        assert config.lmp_node_ids is None

    def test_lmp_market_defaults_to_day_ahead(self) -> None:
        """LMP Market should default to DAY_AHEAD_HOURLY."""
        config = _make_site_config()
        assert config.lmp_market == "DAY_AHEAD_HOURLY"

    def test_lmp_year_defaults_to_none(self) -> None:
        """LMP Year should default to None when not provided."""
        config = _make_site_config()
        assert config.lmp_year is None

    def test_ancillary_revenue_defaults_to_zero(self) -> None:
        """Ancillary revenue should default to 0.0."""
        config = _make_site_config()
        assert config.ancillary_revenue_per_kw_year == 0.0


class TestValidFtmConfig:
    """Valid FTM configurations should pass validation."""

    def test_valid_ftm_with_all_fields(self) -> None:
        """FTM config with all LMP fields set should pass validation."""
        config = _make_site_config(
            **_BESS_DISPATCH_FIELDS,
            dispatch_mode="ftm",
            iso="pjm",
            lmp_zone="COMED",
            lmp_node_ids="12345",
            lmp_market="DAY_AHEAD_HOURLY",
            lmp_year=2024,
            ancillary_revenue_per_kw_year=5.0,
        )
        assert config.dispatch_mode == "ftm"
        assert config.iso == "pjm"
        assert config.lmp_zone == "COMED"
        assert config.lmp_node_ids == "12345"
        assert config.lmp_market == "DAY_AHEAD_HOURLY"
        assert config.lmp_year == 2024
        assert config.ancillary_revenue_per_kw_year == 5.0

    def test_valid_ftm_with_real_time_market(self) -> None:
        """FTM config with REAL_TIME_HOURLY market should pass."""
        config = _make_site_config(
            **_BESS_DISPATCH_FIELDS,
            dispatch_mode="ftm",
            iso="ercot",
            lmp_market="REAL_TIME_HOURLY",
        )
        assert config.lmp_market == "REAL_TIME_HOURLY"

    def test_valid_ftm_minimal(self) -> None:
        """FTM config with only required fields should pass."""
        config = _make_site_config(
            **_BESS_DISPATCH_FIELDS,
            dispatch_mode="ftm",
        )
        assert config.dispatch_mode == "ftm"
        assert config.iso is None
        assert config.lmp_zone is None


class TestDispatchModeValidation:
    """dispatch_mode must be 'btm' or 'ftm'."""

    def test_reject_hybrid(self) -> None:
        """dispatch_mode='hybrid' should be rejected."""
        with pytest.raises(ValueError, match="Dispatch Mode must be 'btm' or 'ftm'"):
            _make_site_config(dispatch_mode="hybrid")

    def test_reject_wholesale(self) -> None:
        """dispatch_mode='wholesale' should be rejected."""
        with pytest.raises(ValueError, match="Dispatch Mode must be 'btm' or 'ftm'"):
            _make_site_config(dispatch_mode="wholesale")

    def test_reject_empty_string_uses_default(self) -> None:
        """Empty string should coerce to default 'btm'."""
        config = _make_site_config(dispatch_mode="")
        assert config.dispatch_mode == "btm"

    def test_case_insensitive_ftm(self) -> None:
        """'FTM' uppercase should normalize to 'ftm'."""
        config = _make_site_config(
            **_BESS_DISPATCH_FIELDS,
            dispatch_mode="FTM",
        )
        assert config.dispatch_mode == "ftm"

    def test_case_insensitive_btm(self) -> None:
        """'BTM' uppercase should normalize to 'btm'."""
        config = _make_site_config(dispatch_mode="BTM")
        assert config.dispatch_mode == "btm"


class TestFtmRequiresBessDispatch:
    """dispatch_mode='ftm' requires bess_dispatch_required=True."""

    def test_ftm_without_bess_dispatch_raises(self) -> None:
        """FTM mode without bess_dispatch_required=True should fail."""
        with pytest.raises(
            ValueError,
            match="BESS Dispatch Required must be True when Dispatch Mode is 'ftm'",
        ):
            _make_site_config(dispatch_mode="ftm", bess_dispatch_required=False)

    def test_ftm_without_bess_dispatch_default_raises(self) -> None:
        """FTM mode with default bess_dispatch_required (False) should fail."""
        with pytest.raises(
            ValueError,
            match="BESS Dispatch Required must be True when Dispatch Mode is 'ftm'",
        ):
            _make_site_config(dispatch_mode="ftm")


class TestIsoValidation:
    """ISO field validation and normalization."""

    def test_pjm_normalization(self) -> None:
        """'PJM' should normalize to 'pjm'."""
        config = _make_site_config(iso="PJM")
        assert config.iso == "pjm"

    def test_ercot_normalization(self) -> None:
        """'Ercot' should normalize to 'ercot'."""
        config = _make_site_config(iso="Ercot")
        assert config.iso == "ercot"

    def test_caiso_normalization(self) -> None:
        """'CAISO' should normalize to 'caiso'."""
        config = _make_site_config(iso="CAISO")
        assert config.iso == "caiso"

    def test_reject_nyiso(self) -> None:
        """'nyiso' should be rejected (not in allowed list)."""
        with pytest.raises(ValueError, match="ISO must be one of"):
            _make_site_config(iso="nyiso")

    def test_reject_miso(self) -> None:
        """'miso' should be rejected (not in allowed list)."""
        with pytest.raises(ValueError, match="ISO must be one of"):
            _make_site_config(iso="miso")

    def test_none_iso_allowed(self) -> None:
        """None ISO should be valid (optional field)."""
        config = _make_site_config(iso=None)
        assert config.iso is None

    def test_empty_string_iso_becomes_none(self) -> None:
        """Empty string ISO should coerce to None."""
        config = _make_site_config(iso="")
        assert config.iso is None


class TestLmpMarketValidation:
    """lmp_market field validation."""

    def test_day_ahead_hourly_valid(self) -> None:
        """DAY_AHEAD_HOURLY should be accepted."""
        config = _make_site_config(lmp_market="DAY_AHEAD_HOURLY")
        assert config.lmp_market == "DAY_AHEAD_HOURLY"

    def test_real_time_hourly_valid(self) -> None:
        """REAL_TIME_HOURLY should be accepted."""
        config = _make_site_config(lmp_market="REAL_TIME_HOURLY")
        assert config.lmp_market == "REAL_TIME_HOURLY"

    def test_reject_real_time_5min(self) -> None:
        """REAL_TIME_5MIN should be rejected."""
        with pytest.raises(ValueError, match="LMP Market must be one of"):
            _make_site_config(lmp_market="REAL_TIME_5MIN")

    def test_reject_day_ahead_15min(self) -> None:
        """DAY_AHEAD_15MIN should be rejected."""
        with pytest.raises(ValueError, match="LMP Market must be one of"):
            _make_site_config(lmp_market="DAY_AHEAD_15MIN")

    def test_empty_string_uses_default(self) -> None:
        """Empty string should coerce to default DAY_AHEAD_HOURLY."""
        config = _make_site_config(lmp_market="")
        assert config.lmp_market == "DAY_AHEAD_HOURLY"


class TestAncillaryRevenueValidation:
    """ancillary_revenue_per_kw_year must be >= 0."""

    def test_positive_value_valid(self) -> None:
        """Positive ancillary revenue should be accepted."""
        config = _make_site_config(ancillary_revenue_per_kw_year=10.0)
        assert config.ancillary_revenue_per_kw_year == 10.0

    def test_zero_valid(self) -> None:
        """Zero ancillary revenue should be accepted."""
        config = _make_site_config(ancillary_revenue_per_kw_year=0.0)
        assert config.ancillary_revenue_per_kw_year == 0.0

    def test_negative_raises(self) -> None:
        """Negative ancillary revenue should be rejected."""
        with pytest.raises(ValueError, match="Ancillary Revenue.*must be >= 0"):
            _make_site_config(ancillary_revenue_per_kw_year=-5.0)

    def test_empty_string_uses_default(self) -> None:
        """Empty string should coerce to default 0.0."""
        config = _make_site_config(ancillary_revenue_per_kw_year="")
        assert config.ancillary_revenue_per_kw_year == 0.0


class TestBtmIgnoresLmpFields:
    """BTM mode should not raise errors for missing/default LMP fields."""

    def test_btm_no_lmp_fields(self) -> None:
        """BTM config with no LMP fields should pass validation."""
        config = _make_site_config(dispatch_mode="btm")
        assert config.dispatch_mode == "btm"
        assert config.iso is None
        assert config.lmp_zone is None
        assert config.lmp_node_ids is None

    def test_btm_with_lmp_fields_set(self) -> None:
        """BTM config with LMP fields set should still pass (ignored)."""
        config = _make_site_config(
            dispatch_mode="btm",
            iso="pjm",
            lmp_zone="COMED",
            lmp_market="REAL_TIME_HOURLY",
        )
        assert config.dispatch_mode == "btm"
        assert config.iso == "pjm"  # stored but not validated as required


class TestBackwardCompatibility:
    """Existing BTM configs without FTM fields should work unchanged."""

    def test_existing_config_no_ftm_fields(self) -> None:
        """SiteConfig without any FTM fields should default correctly."""
        config = _make_site_config()
        assert config.dispatch_mode == "btm"
        assert config.iso is None
        assert config.lmp_zone is None
        assert config.lmp_node_ids is None
        assert config.lmp_market == "DAY_AHEAD_HOURLY"
        assert config.lmp_year is None
        assert config.ancillary_revenue_per_kw_year == 0.0

    def test_existing_bess_config_still_works(self) -> None:
        """Existing BESS dispatch config should work without FTM fields."""
        config = _make_site_config(**_BESS_DISPATCH_FIELDS)
        assert config.bess_dispatch_required is True
        assert config.dispatch_mode == "btm"
        assert config.iso is None

    def test_existing_btm_plain_config(self) -> None:
        """Plain config (no BESS, no FTM) should pass unchanged."""
        config = _make_site_config(
            bess_dispatch_required=False,
            bess_optimization_required=False,
        )
        assert config.dispatch_mode == "btm"
        assert config.bess_dispatch_required is False
