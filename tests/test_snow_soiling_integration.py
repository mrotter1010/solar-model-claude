"""Tests for snow and soiling integration points in configurator and data extractor."""

import pytest

from src.config.schema import SiteConfig
from src.pysam_integration.model_configurator import ModelConfigurator
from src.reporting.data_extractor import extract_loss_waterfall


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DEFAULT_MODULE = "CSI Solar Co. Ltd. CS3U-355P"
DEFAULT_INVERTER = "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]"


def _make_site_config(**overrides: object) -> SiteConfig:
    """Create a SiteConfig with sensible defaults, overridable per test."""
    defaults = {
        "run_name": "test_run",
        "site_name": "Test Site",
        "customer": "Test Customer",
        "latitude": 33.45,
        "longitude": -112.07,
        "dc_size_mw": 100.0,
        "ac_installed_mw": 80.0,
        "ac_poi_mw": 75.0,
        "racking": "tracker",
        "tilt": 60.0,
        "azimuth": 180.0,
        "module_orientation": "portrait",
        "number_of_modules": 1,
        "ground_clearance_height_m": 1.5,
        "panel_model": DEFAULT_MODULE,
        "bifacial": True,
        "inverter_model": DEFAULT_INVERTER,
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


def _make_loss_data(**overrides: object) -> dict:
    """Create a loss_data dict with non-zero DC losses for waterfall testing."""
    data = {
        "annual_dc_nominal": 35795452.0,
        "annual_dc_gross": 32400224.0,
        "annual_dc_net": 30965547.0,
        "annual_ac_gross": 30284794.0,
        "annual_energy": 28935577.0,
        "annual_poa_shading_loss_percent": 1.53,
        "annual_poa_soiling_loss_percent": 5.0,
        "annual_poa_cover_loss_percent": 0.68,
        "annual_dc_module_loss_percent": 9.48,
        "annual_dc_mismatch_loss_percent": 1.5,
        "annual_dc_diodes_loss_percent": 0.5,
        "annual_dc_wiring_loss_percent": 1.5,
        "annual_dc_nameplate_loss_percent": 1.0,
        "annual_dc_tracking_loss_percent": 0.0,
        "annual_bifacial_electrical_mismatch_percent": 0.17,
        "annual_ac_inv_clip_loss_percent": 0.69,
        "annual_ac_inv_eff_loss_percent": 1.41,
        "annual_ac_wiring_loss_percent": 1.5,
        "annual_xfmr_loss_percent": 0.0,
        "annual_ac_perf_adj_loss_percent": 3.0,
        "annual_poa_rear_gain_percent": 3.32,
        "capacity_factor": 25.41,
        "capacity_factor_ac": 32.34,
        "kwh_per_kw": 2225.81,
        "performance_ratio": 0.7676,
        "monthly_energy": [
            1800000.0, 2000000.0, 2400000.0, 2600000.0,
            2900000.0, 3100000.0, 3200000.0, 3000000.0,
            2700000.0, 2400000.0, 1900000.0, 1800000.0,
        ],
        "monthly_poa_eff": [
            8800000.0, 10100000.0, 15100000.0, 20500000.0,
            23000000.0, 20900000.0, 21200000.0, 18900000.0,
            18200000.0, 15100000.0, 11200000.0, 9800000.0,
        ],
        "avg_daytime_ghi_wm2": 490.5,
        "annual_ghi_kwh_m2": 2131.8,
    }
    data.update(overrides)
    return data


# ---------------------------------------------------------------------------
# Model Configurator — dynamic soiling
# ---------------------------------------------------------------------------


def test_dynamic_soiling_passed() -> None:
    """Monthly soiling list is applied to model when provided."""
    configurator = ModelConfigurator()
    site_config = _make_site_config()
    soiling = [1.0, 1.5, 2.0, 2.5, 3.0, 3.0, 3.0, 2.5, 2.0, 1.5, 1.0, 1.0]

    result = configurator.configure_model(site_config, monthly_soiling=soiling)

    assert list(result.model.Losses.subarray1_soiling) == soiling


def test_no_soiling_uses_default() -> None:
    """Without monthly_soiling, model defaults to 5.0% for all 12 months."""
    configurator = ModelConfigurator()
    site_config = _make_site_config()

    result = configurator.configure_model(site_config)

    assert list(result.model.Losses.subarray1_soiling) == [5.0] * 12


def test_wrong_length_soiling_falls_back_to_default() -> None:
    """Wrong-length soiling list is rejected and falls back to 5.0% default."""
    configurator = ModelConfigurator()
    site_config = _make_site_config()

    result = configurator.configure_model(site_config, monthly_soiling=[1.0, 2.0])

    assert list(result.model.Losses.subarray1_soiling) == [5.0] * 12


def test_snow_model_enabled() -> None:
    """Snow model is enabled with Townsend/Powers slide coefficient."""
    configurator = ModelConfigurator()
    site_config = _make_site_config()

    result = configurator.configure_model(site_config)

    assert result.model.Losses.en_snow_model == 1
    assert result.model.Losses.snow_slide_coefficient == pytest.approx(1.97)


# ---------------------------------------------------------------------------
# Waterfall Data Extractor — snow sub-loss
# ---------------------------------------------------------------------------


def test_snow_loss_present_in_waterfall() -> None:
    """Non-zero snow loss appears as a sub-loss in the POA & Module Losses step."""
    loss_data = _make_loss_data(annual_dc_snow_loss_percent=3.5)

    steps = extract_loss_waterfall(loss_data)

    poa_step = next(s for s in steps if s["label"] == "POA & Module Losses")
    snow_sub = next(
        (s for s in poa_step["sub_losses"] if s["label"] == "Snow"), None
    )
    assert snow_sub is not None
    assert snow_sub["percent"] == pytest.approx(3.5)


def test_snow_loss_zero_filtered_out() -> None:
    """Zero snow loss is filtered out by _filter_sub_losses."""
    loss_data = _make_loss_data(annual_dc_snow_loss_percent=0.0)

    steps = extract_loss_waterfall(loss_data)

    poa_step = next(s for s in steps if s["label"] == "POA & Module Losses")
    snow_labels = [s["label"] for s in poa_step["sub_losses"]]
    assert "Snow" not in snow_labels


def test_snow_loss_missing_key_no_crash() -> None:
    """Missing annual_dc_snow_loss_percent key does not crash; Snow not shown."""
    loss_data = _make_loss_data()
    # Verify the key is not present
    assert "annual_dc_snow_loss_percent" not in loss_data

    steps = extract_loss_waterfall(loss_data)

    dc_step = next(s for s in steps if s["label"] == "DC Losses")
    snow_labels = [s["label"] for s in dc_step["sub_losses"]]
    assert "Snow" not in snow_labels
