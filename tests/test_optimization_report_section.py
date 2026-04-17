"""Tests for optimization report section row builder."""

import pytest

from src.optimization.report_section import build_optimization_summary_rows


# ---------------------------------------------------------------------------
# Fixtures — realistic optimization result dicts per mode
# ---------------------------------------------------------------------------

def _base_sweep_metadata(**overrides):
    """Build a base sweep_metadata dict with sensible defaults."""
    meta = {
        "mode": "production",
        "racking": "Single-Axis Tracker",
        "buildable_acres": 150.0,
        "utilization_factor": 0.6,
        "module_power_w": 355.0,
        "module_area_m2": 1.94,
        "gcr_range": [0.30, 0.55],
        "dcac_range": [1.10, 1.40],
        "total_combinations": 48,
        "failed_combinations": 0,
        "itc_pct": None,
        "discount_rate_pct": None,
        "project_lifetime_years": None,
        "degradation_pct": None,
        "solar_cost_per_kw_dc": None,
        "solar_opex_per_kw_dc_year": None,
        "energy_price_per_kwh": None,
        "energy_cost_escalator_pct": None,
        "retain_hourly": False,
    }
    meta.update(overrides)
    return meta


def _winner_point(**overrides):
    """Build a sweep result point dict with sensible defaults."""
    point = {
        "gcr": 0.50,
        "dcac_ratio": 1.20,
        "mw_dc": 33.3,
        "mw_ac": 27.8,
        "num_modules": 93_800,
        "annual_energy_kwh": 70_439_000.0,
        "annual_energy_mwh": 70_439.0,
        "capacity_factor_ac": 26.75,
        "clipping_correction_pct": 0.35,
        "capex_total": None,
        "opex_per_year": None,
        "net_capex": None,
        "lcoe_per_mwh": None,
        "lcoe_per_kwh": None,
        "annual_revenue_year1": None,
        "npv": None,
        "simple_payback_years": None,
        "irr": None,
        "failed": False,
        "error": None,
    }
    point.update(overrides)
    return point


def _production_mode_data():
    """Optimization result for production mode."""
    return {
        "mode": "production",
        "winners": {
            "max_production": _winner_point(
                gcr=0.50, dcac_ratio=1.20,
                mw_dc=33.3, mw_ac=27.8,
                annual_energy_mwh=70_439.0,
                capacity_factor_ac=26.75,
            ),
            "max_yield": _winner_point(
                gcr=0.30, dcac_ratio=1.10,
                mw_dc=20.0, mw_ac=18.2,
                annual_energy_mwh=42_500.0,
                capacity_factor_ac=28.10,
            ),
        },
        "sweep_metadata": _base_sweep_metadata(mode="production"),
    }


def _lcoe_mode_data():
    """Optimization result for LCOE mode."""
    return {
        "mode": "lcoe",
        "winners": {
            "max_production": _winner_point(
                annual_energy_mwh=70_439.0,
                capacity_factor_ac=26.75,
            ),
            "max_yield": _winner_point(
                gcr=0.30, dcac_ratio=1.10,
                mw_dc=20.0, mw_ac=18.2,
                annual_energy_mwh=42_500.0,
                capacity_factor_ac=28.10,
            ),
            "lcoe": _winner_point(
                gcr=0.45, dcac_ratio=1.25,
                mw_dc=30.0, mw_ac=24.0,
                lcoe_per_mwh=42.61,
                lcoe_per_kwh=0.04261,
                net_capex=28_500_000.0,
                capex_total=30_000_000.0,
            ),
        },
        "sweep_metadata": _base_sweep_metadata(
            mode="lcoe",
            itc_pct=30.0,
            solar_cost_per_kw_dc=1000.0,
        ),
    }


def _npv_mode_data():
    """Optimization result for NPV mode."""
    return {
        "mode": "npv",
        "winners": {
            "max_production": _winner_point(
                annual_energy_mwh=70_439.0,
                capacity_factor_ac=26.75,
            ),
            "max_yield": _winner_point(
                gcr=0.30, dcac_ratio=1.10,
                mw_dc=20.0, mw_ac=18.2,
                annual_energy_mwh=42_500.0,
                capacity_factor_ac=28.10,
            ),
            "lcoe": _winner_point(
                gcr=0.45, dcac_ratio=1.25,
                mw_dc=30.0, mw_ac=24.0,
                lcoe_per_mwh=42.61,
                net_capex=28_500_000.0,
            ),
            "npv": _winner_point(
                gcr=0.45, dcac_ratio=1.30,
                mw_dc=31.0, mw_ac=23.8,
                npv=5_200_000.0,
                irr=8.45,
                simple_payback_years=11.2,
            ),
        },
        "sweep_metadata": _base_sweep_metadata(
            mode="npv",
            itc_pct=30.0,
            solar_cost_per_kw_dc=1000.0,
            energy_price_per_kwh=0.06,
        ),
    }


def _solar_bess_mode_data():
    """Optimization result for solar_bess mode."""
    return {
        "mode": "solar_bess",
        "winners": {
            "max_production": _winner_point(
                annual_energy_mwh=70_439.0,
                capacity_factor_ac=26.75,
            ),
            "max_yield": _winner_point(
                gcr=0.30, dcac_ratio=1.10,
                mw_dc=20.0, mw_ac=18.2,
                annual_energy_mwh=42_500.0,
                capacity_factor_ac=28.10,
            ),
            "lcoe": _winner_point(
                gcr=0.45, dcac_ratio=1.25,
                mw_dc=30.0, mw_ac=24.0,
                lcoe_per_mwh=42.61,
                net_capex=28_500_000.0,
            ),
            "solar_only_npv": _winner_point(
                gcr=0.45, dcac_ratio=1.30,
                mw_dc=31.0, mw_ac=23.8,
                npv=5_200_000.0,
                irr=8.45,
                simple_payback_years=11.2,
            ),
            "solar_bess_npv": {
                "solar_gcr": 0.50,
                "solar_dcac": 1.20,
                "bess_power_mw": 4.2,
                "bess_duration_hr": 4,
                "bess_capacity_mwh": 16.8,
                "bess_capex": 6_720_000.0,
                "bess_opex_per_year": 33_600.0,
                "bess_annual_revenue": 840_000.0,
                "bess_npv": 1_800_000.0,
                "solar_npv": 5_100_000.0,
                "combined_npv": 6_900_000.0,
                "dispatch_mode": "btm",
                "charging_mode": "solar_only",
                "failed": False,
            },
        },
        "bess_adds_value": True,
        "bess_winner_detail": {
            "solar": _winner_point(
                gcr=0.50, dcac_ratio=1.20,
                mw_dc=33.3, mw_ac=27.8,
            ),
            "bess": {
                "power_mw": 4.2,
                "duration_hr": 4,
                "capacity_mwh": 16.8,
                "capex": 6_720_000.0,
                "opex_per_year": 33_600.0,
                "annual_revenue": 840_000.0,
                "npv": 1_800_000.0,
                "dispatch_mode": "btm",
                "charging_mode": "solar_only",
            },
            "combined": {
                "npv": 6_900_000.0,
                "solar_npv": 5_100_000.0,
                "bess_npv": 1_800_000.0,
                "total_capex": 40_020_000.0,
                "total_opex": 183_600.0,
            },
        },
        "sweep_metadata": _base_sweep_metadata(
            mode="solar_bess",
            itc_pct=30.0,
            solar_cost_per_kw_dc=1000.0,
            energy_price_per_kwh=0.06,
        ),
    }


# ---------------------------------------------------------------------------
# Tests — production mode
# ---------------------------------------------------------------------------

class TestProductionMode:
    """Tests for production mode optimization summary rows."""

    def test_returns_list_of_rows(self):
        rows = build_optimization_summary_rows(_production_mode_data())
        assert isinstance(rows, list)
        assert all(isinstance(r, list) and len(r) == 2 for r in rows)

    def test_header_row(self):
        rows = build_optimization_summary_rows(_production_mode_data())
        assert rows[0] == ["Layout Optimization Summary", ""]

    def test_common_metadata_rows(self):
        rows = build_optimization_summary_rows(_production_mode_data())
        labels = [r[0] for r in rows]
        assert "Optimization Mode" in labels
        assert "Racking" in labels
        assert "Buildable Acres" in labels
        assert "Configurations Evaluated" in labels

    def test_mode_label(self):
        rows = build_optimization_summary_rows(_production_mode_data())
        mode_row = next(r for r in rows if r[0] == "Optimization Mode")
        assert mode_row[1] == "Production"

    def test_max_production_rows_present(self):
        rows = build_optimization_summary_rows(_production_mode_data())
        labels = [r[0] for r in rows]
        assert "Max Production \u2014 GCR / DC:AC" in labels
        assert "Max Production \u2014 Capacity" in labels
        assert "Max Production \u2014 Annual Energy" in labels
        assert "Max Production \u2014 Capacity Factor" in labels

    def test_max_yield_rows_present(self):
        rows = build_optimization_summary_rows(_production_mode_data())
        labels = [r[0] for r in rows]
        assert "Max Yield \u2014 GCR / DC:AC" in labels
        assert "Max Yield \u2014 Capacity" in labels
        assert "Max Yield \u2014 Annual Energy" in labels
        assert "Max Yield \u2014 Capacity Factor" in labels

    def test_no_lcoe_or_npv_rows(self):
        rows = build_optimization_summary_rows(_production_mode_data())
        labels = [r[0] for r in rows]
        assert not any("LCOE" in lbl for lbl in labels)
        assert not any("NPV" in lbl for lbl in labels)
        assert not any("BESS" in lbl for lbl in labels)

    def test_row_count(self):
        """Production mode: header + 4 meta + 4 max_prod + 4 max_yield = 13."""
        rows = build_optimization_summary_rows(_production_mode_data())
        assert len(rows) == 13


# ---------------------------------------------------------------------------
# Tests — LCOE mode
# ---------------------------------------------------------------------------

class TestLcoeMode:
    """Tests for LCOE mode optimization summary rows."""

    def test_lcoe_rows_present(self):
        rows = build_optimization_summary_rows(_lcoe_mode_data())
        labels = [r[0] for r in rows]
        assert "LCOE Winner \u2014 GCR / DC:AC" in labels
        assert "LCOE Winner \u2014 Capacity" in labels
        assert "LCOE Winner \u2014 LCOE" in labels
        assert "LCOE Winner \u2014 Net CapEx" in labels

    def test_mode_label(self):
        rows = build_optimization_summary_rows(_lcoe_mode_data())
        mode_row = next(r for r in rows if r[0] == "Optimization Mode")
        assert mode_row[1] == "LCOE"

    def test_lcoe_format(self):
        rows = build_optimization_summary_rows(_lcoe_mode_data())
        lcoe_row = next(r for r in rows if r[0] == "LCOE Winner \u2014 LCOE")
        assert lcoe_row[1] == "$42.61/MWh"

    def test_net_capex_format(self):
        rows = build_optimization_summary_rows(_lcoe_mode_data())
        capex_row = next(r for r in rows if r[0] == "LCOE Winner \u2014 Net CapEx")
        assert capex_row[1] == "$28,500,000"

    def test_no_npv_rows(self):
        rows = build_optimization_summary_rows(_lcoe_mode_data())
        labels = [r[0] for r in rows]
        assert not any("NPV Winner" in lbl for lbl in labels)

    def test_row_count(self):
        """LCOE mode: header + 4 meta + 4 max_prod + 4 max_yield + 4 lcoe = 17."""
        rows = build_optimization_summary_rows(_lcoe_mode_data())
        assert len(rows) == 17


# ---------------------------------------------------------------------------
# Tests — NPV mode
# ---------------------------------------------------------------------------

class TestNpvMode:
    """Tests for NPV mode optimization summary rows."""

    def test_npv_rows_present(self):
        rows = build_optimization_summary_rows(_npv_mode_data())
        labels = [r[0] for r in rows]
        assert "NPV Winner \u2014 GCR / DC:AC" in labels
        assert "NPV Winner \u2014 NPV" in labels
        assert "NPV Winner \u2014 IRR" in labels
        assert "NPV Winner \u2014 Payback" in labels

    def test_mode_label(self):
        rows = build_optimization_summary_rows(_npv_mode_data())
        mode_row = next(r for r in rows if r[0] == "Optimization Mode")
        assert mode_row[1] == "NPV"

    def test_npv_format(self):
        rows = build_optimization_summary_rows(_npv_mode_data())
        npv_row = next(r for r in rows if r[0] == "NPV Winner \u2014 NPV")
        assert npv_row[1] == "$5,200,000"

    def test_irr_format(self):
        rows = build_optimization_summary_rows(_npv_mode_data())
        irr_row = next(r for r in rows if r[0] == "NPV Winner \u2014 IRR")
        assert irr_row[1] == "8.45%"

    def test_payback_format(self):
        rows = build_optimization_summary_rows(_npv_mode_data())
        pb_row = next(r for r in rows if r[0] == "NPV Winner \u2014 Payback")
        assert pb_row[1] == "11.2 years"

    def test_lcoe_rows_also_present(self):
        """NPV mode includes LCOE winner rows too."""
        rows = build_optimization_summary_rows(_npv_mode_data())
        labels = [r[0] for r in rows]
        assert "LCOE Winner \u2014 LCOE" in labels

    def test_row_count(self):
        """NPV mode: header + 4 meta + 4 max_prod + 4 max_yield + 4 lcoe + 4 npv = 21."""
        rows = build_optimization_summary_rows(_npv_mode_data())
        assert len(rows) == 21


# ---------------------------------------------------------------------------
# Tests — solar_bess mode
# ---------------------------------------------------------------------------

class TestSolarBessMode:
    """Tests for solar_bess mode optimization summary rows."""

    def test_bess_adds_value_row(self):
        rows = build_optimization_summary_rows(_solar_bess_mode_data())
        bav_row = next(r for r in rows if r[0] == "BESS Adds Value")
        assert bav_row[1] == "Yes"

    def test_bess_adds_value_no(self):
        data = _solar_bess_mode_data()
        data["bess_adds_value"] = False
        rows = build_optimization_summary_rows(data)
        bav_row = next(r for r in rows if r[0] == "BESS Adds Value")
        assert bav_row[1] == "No"

    def test_mode_label(self):
        rows = build_optimization_summary_rows(_solar_bess_mode_data())
        mode_row = next(r for r in rows if r[0] == "Optimization Mode")
        assert mode_row[1] == "Solar + BESS"

    def test_bess_winner_rows_present(self):
        rows = build_optimization_summary_rows(_solar_bess_mode_data())
        labels = [r[0] for r in rows]
        assert "BESS Winner \u2014 Solar Config" in labels
        assert "BESS Winner \u2014 BESS Size" in labels
        assert "BESS Winner \u2014 Combined NPV" in labels
        assert "BESS Winner \u2014 Solar NPV" in labels
        assert "BESS Winner \u2014 BESS NPV" in labels

    def test_bess_size_format(self):
        rows = build_optimization_summary_rows(_solar_bess_mode_data())
        size_row = next(r for r in rows if r[0] == "BESS Winner \u2014 BESS Size")
        assert "4.2 MW" in size_row[1]
        assert "4hr" in size_row[1]
        assert "16.8 MWh" in size_row[1]

    def test_combined_npv_format(self):
        rows = build_optimization_summary_rows(_solar_bess_mode_data())
        npv_row = next(r for r in rows if r[0] == "BESS Winner \u2014 Combined NPV")
        assert npv_row[1] == "$6,900,000"

    def test_solar_only_npv_prefix(self):
        """In solar_bess mode the NPV winner label says 'Solar-Only NPV Winner'."""
        rows = build_optimization_summary_rows(_solar_bess_mode_data())
        labels = [r[0] for r in rows]
        assert any("Solar-Only NPV Winner" in lbl for lbl in labels)
        assert not any(
            lbl.startswith("NPV Winner") for lbl in labels
        )

    def test_solar_config_format(self):
        rows = build_optimization_summary_rows(_solar_bess_mode_data())
        cfg_row = next(r for r in rows if r[0] == "BESS Winner \u2014 Solar Config")
        assert "GCR 0.50" in cfg_row[1]
        assert "DC:AC 1.20" in cfg_row[1]

    def test_no_bess_detail_when_none(self):
        """If bess_winner_detail is None, BESS Winner rows are absent."""
        data = _solar_bess_mode_data()
        data["bess_winner_detail"] = None
        rows = build_optimization_summary_rows(data)
        labels = [r[0] for r in rows]
        assert not any("BESS Winner" in lbl for lbl in labels)
        # But BESS Adds Value should still be present
        assert "BESS Adds Value" in labels


# ---------------------------------------------------------------------------
# Tests — formatting
# ---------------------------------------------------------------------------

class TestFormatting:
    """Tests for number formatting in output rows."""

    def test_large_numbers_have_commas(self):
        rows = build_optimization_summary_rows(_production_mode_data())
        energy_row = next(
            r for r in rows if r[0] == "Max Production \u2014 Annual Energy"
        )
        assert "," in energy_row[1]
        assert "70,439 MWh" == energy_row[1]

    def test_dollars_have_prefix(self):
        rows = build_optimization_summary_rows(_lcoe_mode_data())
        lcoe_row = next(r for r in rows if r[0] == "LCOE Winner \u2014 LCOE")
        assert lcoe_row[1].startswith("$")

    def test_percentages_have_suffix(self):
        rows = build_optimization_summary_rows(_production_mode_data())
        cf_row = next(
            r for r in rows
            if r[0] == "Max Production \u2014 Capacity Factor"
        )
        assert cf_row[1].endswith("%")

    def test_gcr_dcac_format(self):
        rows = build_optimization_summary_rows(_production_mode_data())
        gcr_row = next(
            r for r in rows
            if r[0] == "Max Production \u2014 GCR / DC:AC"
        )
        assert gcr_row[1] == "0.50 / 1.20"

    def test_capacity_format(self):
        rows = build_optimization_summary_rows(_production_mode_data())
        cap_row = next(
            r for r in rows
            if r[0] == "Max Production \u2014 Capacity"
        )
        assert cap_row[1] == "33.3 MW DC / 27.8 MW AC"

    def test_buildable_acres_comma_format(self):
        data = _production_mode_data()
        data["sweep_metadata"]["buildable_acres"] = 1_500.0
        rows = build_optimization_summary_rows(data)
        acres_row = next(r for r in rows if r[0] == "Buildable Acres")
        assert acres_row[1] == "1,500"

    def test_negative_npv_format(self):
        """Negative NPV values use parentheses notation."""
        data = _npv_mode_data()
        data["winners"]["npv"]["npv"] = -500_000.0
        rows = build_optimization_summary_rows(data)
        npv_row = next(r for r in rows if r[0] == "NPV Winner \u2014 NPV")
        assert npv_row[1] == "($500,000)"
