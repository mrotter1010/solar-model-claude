"""Tests for data extractor — transforms loss_data into chart/table structures."""

import pytest

from src.reporting.data_extractor import (
    extract_loss_waterfall,
    extract_monthly_production,
    extract_site_summary,
    get_us_state_from_coords,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_loss_data(**overrides: object) -> dict:
    """Create a loss_data dict matching Phoenix inventory values."""
    data = {
        # Annual energy totals (kWh)
        "annual_dc_nominal": 35795452.0,
        "annual_dc_gross": 32400224.0,
        "annual_dc_net": 30965547.0,
        "annual_ac_gross": 30284794.0,
        "annual_energy": 28935577.0,
        # Annual loss percentages (%)
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
        # Key scalars
        "capacity_factor": 25.41,
        "capacity_factor_ac": 32.34,
        "kwh_per_kw": 2225.81,
        "performance_ratio": 0.7676,
        # Monthly arrays (12-element lists, approximate kWh)
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
        # GHI metrics
        "avg_daytime_ghi_wm2": 490.5,
        "annual_ghi_kwh_m2": 2131.8,
    }
    data.update(overrides)
    return data


def _make_site_config(**overrides: object) -> dict:
    """Create a site_config dict matching Phoenix test site."""
    config = {
        "site_name": "SiteTest_Phoenix",
        "customer": "TestCustomer",
        "latitude": 33.483,
        "longitude": -112.073,
        "dc_size_mw": 13.0,
        "ac_installed_mw": 10.0,
        "ac_poi_mw": 10.0,
        "racking": "tracker",
        "tilt": 60.0,
        "azimuth": 180.0,
        "gcr": 0.34,
        "panel_model": "CSI Solar Co. Ltd. CS3U-355P",
        "number_of_modules": 2,
        "bifacial": True,
        "inverter_model": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
    }
    config.update(overrides)
    return config


# ---------------------------------------------------------------------------
# extract_monthly_production
# ---------------------------------------------------------------------------


class TestExtractMonthlyProduction:
    """Tests for monthly production extraction."""

    def test_returns_12_months(self) -> None:
        """Output contains exactly 12 month labels."""
        result = extract_monthly_production(_make_loss_data())

        assert result is not None
        assert len(result["months"]) == 12
        assert result["months"][0] == "Jan"
        assert result["months"][11] == "Dec"

    def test_values_are_mwh_not_kwh(self) -> None:
        """Energy values are converted from kWh to MWh (divided by 1000)."""
        loss_data = _make_loss_data()
        result = extract_monthly_production(loss_data)

        assert result is not None
        # First month: 1800000 kWh → 1800.0 MWh
        assert result["energy_mwh"][0] == pytest.approx(1800.0)
        # Last month: 1800000 kWh → 1800.0 MWh
        assert result["energy_mwh"][11] == pytest.approx(1800.0)
        assert len(result["energy_mwh"]) == 12

    def test_returns_none_if_monthly_energy_missing(self) -> None:
        """Returns None when monthly_energy key is absent."""
        loss_data = _make_loss_data()
        del loss_data["monthly_energy"]

        result = extract_monthly_production(loss_data)

        assert result is None

    def test_returns_none_if_wrong_length(self) -> None:
        """Returns None when monthly_energy has wrong number of elements."""
        loss_data = _make_loss_data(monthly_energy=[1000.0] * 6)

        result = extract_monthly_production(loss_data)

        assert result is None


# ---------------------------------------------------------------------------
# extract_loss_waterfall
# ---------------------------------------------------------------------------


class TestExtractLossWaterfall:
    """Tests for loss waterfall dataset generation."""

    def test_first_step_is_start(self) -> None:
        """First step has type 'start' with energy equal to nominal DC."""
        steps = extract_loss_waterfall(_make_loss_data())

        assert steps[0]["type"] == "start"
        assert steps[0]["label"] == "Nominal DC Energy"
        assert steps[0]["energy_kwh"] == pytest.approx(35795452.0)

    def test_last_step_is_end(self) -> None:
        """Last step has type 'end' with energy equal to annual_energy."""
        steps = extract_loss_waterfall(_make_loss_data())

        assert steps[-1]["type"] == "end"
        assert steps[-1]["label"] == "Net AC Energy"
        assert steps[-1]["energy_kwh"] == pytest.approx(28935577.0)

    def test_all_middle_steps_are_loss_type(self) -> None:
        """All steps between start and end have type 'loss'."""
        steps = extract_loss_waterfall(_make_loss_data())

        middle_steps = steps[1:-1]
        assert len(middle_steps) >= 1
        for step in middle_steps:
            assert step["type"] == "loss", f"Step '{step['label']}' has type '{step['type']}'"

    def test_sub_losses_are_non_empty(self) -> None:
        """Each loss step has at least one sub_loss entry."""
        steps = extract_loss_waterfall(_make_loss_data())

        loss_steps = [s for s in steps if s["type"] == "loss"]
        for step in loss_steps:
            assert len(step["sub_losses"]) > 0, (
                f"Step '{step['label']}' has empty sub_losses"
            )

    def test_no_zero_sub_losses(self) -> None:
        """Sub_losses with percent rounding to 0.0 are filtered out."""
        steps = extract_loss_waterfall(_make_loss_data())

        for step in steps:
            for sub in step["sub_losses"]:
                assert round(sub["percent"], 1) != 0.0, (
                    f"Step '{step['label']}' has zero sub_loss: {sub['label']}"
                )

    def test_energy_deltas_are_positive(self) -> None:
        """All loss step energy_kwh values are positive (energy removed)."""
        steps = extract_loss_waterfall(_make_loss_data())

        loss_steps = [s for s in steps if s["type"] == "loss"]
        for step in loss_steps:
            assert step["energy_kwh"] > 0, (
                f"Step '{step['label']}' has non-positive delta: {step['energy_kwh']}"
            )

    def test_expected_loss_groups_present(self) -> None:
        """All four loss groups appear for Phoenix test data."""
        steps = extract_loss_waterfall(_make_loss_data())

        labels = [s["label"] for s in steps]
        assert "POA & Module Losses" in labels
        assert "DC Losses" in labels
        assert "Inverter Losses" in labels
        assert "AC Losses & Availability" in labels

    def test_bifacial_gain_is_negative_sub_loss(self) -> None:
        """Bifacial gain appears as a negative percent in POA & Module Losses."""
        steps = extract_loss_waterfall(_make_loss_data())

        poa_step = next(s for s in steps if s["label"] == "POA & Module Losses")
        bifacial_sub = next(
            s for s in poa_step["sub_losses"] if s["label"] == "Bifacial Gain"
        )
        assert bifacial_sub["percent"] < 0

    def test_zero_loss_group_skipped(self) -> None:
        """A loss group with zero energy delta is omitted."""
        loss_data = _make_loss_data(
            # Set dc_gross == dc_net → DC Losses group has 0 delta
            annual_dc_gross=30965547.0,
            annual_dc_net=30965547.0,
            # Zero out all DC loss percentages too
            annual_dc_mismatch_loss_percent=0.0,
            annual_bifacial_electrical_mismatch_percent=0.0,
            annual_dc_diodes_loss_percent=0.0,
            annual_dc_wiring_loss_percent=0.0,
            annual_dc_tracking_loss_percent=0.0,
            annual_dc_nameplate_loss_percent=0.0,
        )
        steps = extract_loss_waterfall(loss_data)

        labels = [s["label"] for s in steps]
        assert "DC Losses" not in labels


# ---------------------------------------------------------------------------
# extract_site_summary
# ---------------------------------------------------------------------------


class TestExtractSiteSummary:
    """Tests for site summary table extraction."""

    def test_all_expected_keys_present(self) -> None:
        """Returned dict contains all required report keys."""
        result = extract_site_summary(_make_site_config(), _make_loss_data())

        expected_keys = {
            "site_name", "customer", "latitude", "longitude", "state",
            "dc_capacity_mw", "ac_capacity_mw", "dc_ac_ratio",
            "racking", "tilt", "azimuth", "gcr",
            "module_model", "module_count", "bifacial",
            "inverter_model", "annual_energy_mwh",
            "capacity_factor_pct", "specific_yield_kwh_kwp",
            "performance_ratio_pct", "avg_daytime_ghi_wm2",
            "annual_ghi_kwh_m2", "weather_year",
        }
        assert set(result.keys()) == expected_keys

    def test_dc_ac_ratio_reasonable(self) -> None:
        """DC/AC ratio is between 1.0 and 2.0 for typical sites."""
        result = extract_site_summary(_make_site_config(), _make_loss_data())

        assert 1.0 <= result["dc_ac_ratio"] <= 2.0

    def test_annual_energy_in_mwh(self) -> None:
        """annual_energy_mwh is correctly converted from kWh."""
        result = extract_site_summary(_make_site_config(), _make_loss_data())

        # 28935577 kWh / 1000 ≈ 28935.6 MWh
        assert result["annual_energy_mwh"] == pytest.approx(28935.6, rel=1e-3)

    def test_performance_ratio_as_percent(self) -> None:
        """performance_ratio is converted from decimal to percent."""
        result = extract_site_summary(_make_site_config(), _make_loss_data())

        # 0.7676 × 100 = 76.76%
        assert result["performance_ratio_pct"] == pytest.approx(76.76, rel=1e-3)

    def test_field_mapping_from_site_config(self) -> None:
        """Fields map correctly from SiteConfig field names."""
        result = extract_site_summary(_make_site_config(), _make_loss_data())

        assert result["site_name"] == "SiteTest_Phoenix"
        assert result["customer"] == "TestCustomer"
        assert result["latitude"] == pytest.approx(33.483)
        assert result["longitude"] == pytest.approx(-112.073)
        assert result["dc_capacity_mw"] == pytest.approx(13.0)
        assert result["ac_capacity_mw"] == pytest.approx(10.0)
        assert result["racking"] == "tracker"
        assert result["module_model"] == "CSI Solar Co. Ltd. CS3U-355P"
        assert result["bifacial"] is True
        assert result["module_count"] == 2
        assert result["weather_year"] == "2023"

    def test_state_from_coordinates(self) -> None:
        """Phoenix coordinates resolve to Arizona."""
        result = extract_site_summary(_make_site_config(), _make_loss_data())

        assert result["state"] == "Arizona"

    def test_ghi_fields_present(self) -> None:
        """GHI metrics are included from loss_data."""
        result = extract_site_summary(_make_site_config(), _make_loss_data())

        assert result["avg_daytime_ghi_wm2"] == pytest.approx(490.5)
        assert result["annual_ghi_kwh_m2"] == pytest.approx(2131.8)


# ---------------------------------------------------------------------------
# get_us_state_from_coords
# ---------------------------------------------------------------------------


class TestGetUsStateFromCoords:
    """Tests for US state bounding-box lookup."""

    def test_phoenix_arizona(self) -> None:
        """Phoenix, AZ coordinates return Arizona."""
        assert get_us_state_from_coords(33.48, -112.07) == "Arizona"

    def test_no_match_returns_unknown(self) -> None:
        """Coordinates outside US return Unknown."""
        assert get_us_state_from_coords(0.0, 0.0) == "Unknown"

    def test_dallas_texas(self) -> None:
        """Dallas, TX coordinates return Texas."""
        assert get_us_state_from_coords(32.78, -96.80) == "Texas"

    def test_miami_florida(self) -> None:
        """Miami, FL coordinates return Florida."""
        assert get_us_state_from_coords(25.76, -80.19) == "Florida"
