"""Tests for the batch execution engine.

All analysis functions are mocked — no real PySAM, buildability, or
optimization runs. Tests verify field mapping, dispatch routing,
fail-forward behavior, progress callbacks, and result extraction.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.batch.executor import (
    BatchResult,
    RowResult,
    _build_economic_kwargs,
    _build_optimization_site_config,
    _build_sweep_kwargs,
    _coerce_bool,
    _coerce_float,
    _coerce_int,
    _extract_buildability_results,
    _extract_optimization_bess_results,
    _extract_optimization_results,
    _extract_production_results,
    _run_buildability,
    _run_optimization,
    _run_optimization_bess,
    _run_production,
    execute_batch,
    map_row_to_buildability_config,
    map_row_to_site_config,
    DEFAULT_LOSSES,
    DEFAULT_MODULE,
    DEFAULT_INVERTER,
)
from src.buildability.models import BuildabilityResult
from src.config.schema import SiteConfig


# ── Fixtures ───────────────────────────────────────────────────────────


@pytest.fixture
def production_row() -> dict:
    """A minimal valid production batch row."""
    return {
        "name": "Test Site Alpha",
        "latitude": 33.45,
        "longitude": -112.07,
        "analysis_type": "production",
        "racking": "tracker",
        "gcr": 0.40,
        "dc_capacity_mw": 5.0,
        "ac_capacity_mw": 4.0,
        "module": "CSI Solar Co. Ltd. CS3U-355P",
        "inverter": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
        "tilt": 55.0,
        "azimuth": 180.0,
        "bifacial": "FALSE",
    }


@pytest.fixture
def production_row_defaults() -> dict:
    """A production row relying on defaults for optional fields."""
    return {
        "name": "Defaults Site",
        "latitude": 39.74,
        "longitude": -104.99,
        "analysis_type": "production",
        "racking": "fixed",
        "gcr": 0.35,
        "dc_capacity_mw": 10.0,
        "ac_capacity_mw": 8.0,
    }


@pytest.fixture
def buildability_row() -> dict:
    """A minimal valid buildability batch row."""
    return {
        "name": "Build Site",
        "latitude": 33.45,
        "longitude": -112.07,
        "analysis_type": "buildability",
    }


@pytest.fixture
def optimization_row() -> dict:
    """A minimal valid optimization batch row."""
    return {
        "name": "Opt Site",
        "latitude": 33.45,
        "longitude": -112.07,
        "analysis_type": "optimization",
        "buildable_acres": 100.0,
        "racking": "tracker",
    }


@pytest.fixture
def optimization_bess_row() -> dict:
    """A minimal valid optimization_bess batch row."""
    return {
        "name": "BESS Site",
        "latitude": 39.95,
        "longitude": -75.16,
        "analysis_type": "optimization_bess",
        "buildable_acres": 80.0,
        "dispatch_mode": "ftm",
        "racking": "tracker",
    }


@pytest.fixture
def mock_buildability_result() -> BuildabilityResult:
    """A realistic BuildabilityResult for mock returns."""
    return BuildabilityResult(
        latitude=33.45,
        longitude=-112.07,
        radius_km=1.0,
        polygon_wkt="POLYGON((-112 33, -112 34, -111 34, -111 33, -112 33))",
        total_area_acres=500.0,
        buildable_acres=350.0,
        soft_exclusion_acres=100.0,
        hard_exclusion_acres=50.0,
        unclassified_acres=0.0,
        buildable_pct=70.0,
        soft_exclusion_pct=20.0,
        hard_exclusion_pct=10.0,
        class_breakdown=[
            {"code": 71, "name": "Grassland/Herbaceous", "percent": 45.0},
            {"code": 52, "name": "Shrub/Scrub", "percent": 25.0},
        ],
        slope_stats={
            "mean_degrees": 3.5,
            "pct_below_tracker_limit": 85.0,
            "pct_below_fixed_tilt_limit": 95.0,
        },
        nlcd_metadata={},
        dem_metadata={},
    )


@pytest.fixture
def mock_pipeline_result() -> dict:
    """A realistic pipeline result for mock returns."""
    return {
        "total_sites": 1,
        "successful": 1,
        "failed": 0,
        "timeseries_files": [Path("outputs/test.csv")],
        "summaries": [
            {
                "site_name": "Test Site Alpha",
                "run_name": "Test Site Alpha",
                "dc_size_mw": 5.0,
                "ac_installed_mw": 4.0,
                "ac_poi_mw": 4.0,
                "annual_energy_mwh": 10500.0,
                "net_capacity_factor": 0.254,
                "specific_yield": 1680.0,
                "performance_ratio": 0.82,
                "panel_model": "CSI Solar Co. Ltd. CS3U-355P",
                "inverter_model": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
                "racking": "tracker",
                "tilt": 55.0,
                "azimuth": 180.0,
            }
        ],
        "error_files": [],
        "report_files": [],
    }


@pytest.fixture
def mock_optimization_result() -> dict:
    """A realistic optimization result for mock returns."""
    return {
        "mode": "production",
        "winners": {
            "max_production": {
                "gcr": 0.45,
                "dcac_ratio": 1.30,
                "mw_dc": 12.5,
                "mw_ac": 9.6,
                "annual_energy_kwh": 21000000,
                "annual_energy_mwh": 21000,
                "capacity_factor_ac": 0.249,
                "lcoe_per_mwh": None,
                "npv": None,
            },
            "max_yield": {
                "gcr": 0.30,
                "dcac_ratio": 1.15,
                "mw_dc": 8.0,
                "mw_ac": 7.0,
                "annual_energy_kwh": 15000000,
                "annual_energy_mwh": 15000,
                "capacity_factor_ac": 0.265,
                "lcoe_per_mwh": None,
                "npv": None,
            },
        },
        "sweep_results": [],
        "sweep_metadata": {
            "buildable_acres": 100.0,
            "mode": "production",
        },
    }


@pytest.fixture
def mock_bess_optimization_result() -> dict:
    """A realistic solar+BESS optimization result for mock returns."""
    return {
        "mode": "solar_bess",
        "winners": {
            "max_production": {
                "gcr": 0.45,
                "dcac_ratio": 1.30,
                "mw_dc": 12.5,
                "mw_ac": 9.6,
                "annual_energy_mwh": 21000,
                "capacity_factor_ac": 0.249,
            },
            "max_yield": {
                "gcr": 0.30,
                "dcac_ratio": 1.15,
                "mw_dc": 8.0,
                "mw_ac": 7.0,
                "annual_energy_mwh": 15000,
                "capacity_factor_ac": 0.265,
            },
            "solar_only_npv": {
                "gcr": 0.40,
                "dcac_ratio": 1.25,
                "mw_dc": 10.0,
                "mw_ac": 8.0,
                "annual_energy_kwh": 18000000,
                "annual_energy_mwh": 18000,
                "capacity_factor_ac": 0.257,
                "lcoe_per_mwh": 35.0,
                "npv": 2500000,
            },
            "solar_bess_npv": {
                "solar_gcr": 0.40,
                "solar_dcac": 1.25,
                "bess_power_mw": 2.0,
                "bess_duration_hr": 4.0,
                "combined_npv": 3200000,
            },
        },
        "bess_adds_value": True,
        "bess_winner_detail": {
            "solar": {
                "gcr": 0.40,
                "dcac_ratio": 1.25,
                "mw_dc": 10.0,
                "mw_ac": 8.0,
            },
            "bess": {
                "power_mw": 2.0,
                "duration_hr": 4.0,
                "capacity_mwh": 8.0,
                "capex": 2200000,
                "opex_per_year": 0.0,
                "annual_revenue": 150000,
                "npv": 700000,
                "dispatch_mode": "ftm",
                "charging_mode": "solar_and_grid",
            },
            "combined": {
                "npv": 3200000,
                "solar_npv": 2500000,
                "bess_npv": 700000,
                "total_capex": 12200000,
                "total_opex": 100000,
            },
        },
        "solar_sweep_results": [],
        "bess_sweep_results": [],
        "sweep_metadata": {
            "buildable_acres": 80.0,
            "mode": "solar_bess",
        },
    }


# ── Type coercion tests ───────────────────────────────────────────────


class TestCoerceBool:
    """Tests for _coerce_bool helper."""

    def test_bool_true(self) -> None:
        assert _coerce_bool(True) is True

    def test_bool_false(self) -> None:
        assert _coerce_bool(False) is False

    def test_string_true(self) -> None:
        assert _coerce_bool("TRUE") is True
        assert _coerce_bool("true") is True
        assert _coerce_bool("Yes") is True
        assert _coerce_bool("1") is True

    def test_string_false(self) -> None:
        assert _coerce_bool("FALSE") is False
        assert _coerce_bool("false") is False
        assert _coerce_bool("No") is False
        assert _coerce_bool("0") is False

    def test_int_values(self) -> None:
        assert _coerce_bool(1) is True
        assert _coerce_bool(0) is False

    def test_none_returns_false(self) -> None:
        assert _coerce_bool(None) is False


class TestCoerceFloat:
    """Tests for _coerce_float helper."""

    def test_valid_float(self) -> None:
        assert _coerce_float(3.14) == 3.14

    def test_string_float(self) -> None:
        assert _coerce_float("2.5") == 2.5

    def test_none_returns_default(self) -> None:
        assert _coerce_float(None, 1.0) == 1.0

    def test_none_returns_none(self) -> None:
        assert _coerce_float(None) is None

    def test_invalid_returns_default(self) -> None:
        assert _coerce_float("abc", 0.0) == 0.0


class TestCoerceInt:
    """Tests for _coerce_int helper."""

    def test_valid_int(self) -> None:
        assert _coerce_int(25) == 25

    def test_float_truncates(self) -> None:
        assert _coerce_int(25.0) == 25

    def test_string_int(self) -> None:
        assert _coerce_int("30") == 30

    def test_none_returns_default(self) -> None:
        assert _coerce_int(None, 25) == 25


# ── Field mapping tests ───────────────────────────────────────────────


class TestMapRowToSiteConfig:
    """Tests for map_row_to_site_config."""

    def test_production_row_maps_correctly(self, production_row: dict) -> None:
        """Verify all field name translations for a production row."""
        config = map_row_to_site_config(production_row)

        # Field name translations
        assert config.run_name == "Test Site Alpha"
        assert config.site_name == "Test Site Alpha"
        assert config.customer == "Batch"
        assert config.dc_size_mw == 5.0  # dc_capacity_mw → dc_size_mw
        assert config.ac_installed_mw == 4.0  # ac_capacity_mw → ac_installed_mw
        assert config.ac_poi_mw == 4.0  # defaults to ac_capacity_mw
        assert config.panel_model == "CSI Solar Co. Ltd. CS3U-355P"  # module → panel_model
        assert config.inverter_model == "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]"

        # Direct-mapped fields
        assert config.latitude == 33.45
        assert config.longitude == -112.07
        assert config.racking == "tracker"
        assert config.gcr == 0.40
        assert config.tilt == 55.0
        assert config.azimuth == 180.0
        assert config.bifacial is False

        # Fixed defaults (not in template)
        assert config.module_orientation == "portrait"
        assert config.number_of_modules == 1
        assert config.ground_clearance_height_m == 1.5

    def test_default_losses_applied(self, production_row_defaults: dict) -> None:
        """When loss columns are missing, defaults from template docs apply."""
        config = map_row_to_site_config(production_row_defaults)

        assert config.shading_percent == 0.0
        assert config.dc_wiring_loss_percent == 2.0
        assert config.ac_wiring_loss_percent == 0.5
        assert config.transformer_losses_percent == 1.0
        assert config.degradation_percent == 0.5
        assert config.availability_percent == 2.5
        assert config.module_mismatch_percent == 2.0
        assert config.lid_percent == 1.5

    def test_custom_losses_override_defaults(self, production_row: dict) -> None:
        """User-specified loss values override defaults."""
        production_row["shading_pct"] = 3.0
        production_row["dc_wiring_pct"] = 1.5
        config = map_row_to_site_config(production_row)

        assert config.shading_percent == 3.0
        assert config.dc_wiring_loss_percent == 1.5
        # Others still default
        assert config.ac_wiring_loss_percent == 0.5

    def test_default_equipment_when_missing(self, production_row_defaults: dict) -> None:
        """Default module/inverter names applied when not specified."""
        config = map_row_to_site_config(production_row_defaults)

        assert config.panel_model == DEFAULT_MODULE
        assert config.inverter_model == DEFAULT_INVERTER

    def test_default_tilt_tracker(self, production_row_defaults: dict) -> None:
        """Tracker racking defaults to tilt=60."""
        production_row_defaults["racking"] = "tracker"
        config = map_row_to_site_config(production_row_defaults)
        assert config.tilt == 60.0

    def test_default_tilt_fixed(self, production_row_defaults: dict) -> None:
        """Fixed racking defaults to tilt=25."""
        config = map_row_to_site_config(production_row_defaults)
        assert config.tilt == 25.0  # production_row_defaults has racking=fixed

    def test_bifacial_string_true(self, production_row: dict) -> None:
        """Bifacial 'TRUE' string coerced to bool True."""
        production_row["bifacial"] = "TRUE"
        config = map_row_to_site_config(production_row)
        assert config.bifacial is True

    def test_returns_valid_site_config(self, production_row: dict) -> None:
        """The returned object is a valid SiteConfig instance."""
        config = map_row_to_site_config(production_row)
        assert isinstance(config, SiteConfig)


class TestMapRowToBuildabilityConfig:
    """Tests for map_row_to_buildability_config."""

    def test_maps_lat_lon(self, buildability_row: dict) -> None:
        config = map_row_to_buildability_config(buildability_row)
        assert config.latitude == 33.45
        assert config.longitude == -112.07

    def test_no_boundary_defaults(self, buildability_row: dict) -> None:
        """BuildabilityConfig should have no kmz/radius by default."""
        config = map_row_to_buildability_config(buildability_row)
        assert config.kmz_file_path is None
        assert config.radius_km is None


class TestBuildOptimizationSiteConfig:
    """Tests for _build_optimization_site_config."""

    def test_placeholder_values(self, optimization_row: dict) -> None:
        """Optimization base config has placeholder gcr/dc/ac values."""
        config = _build_optimization_site_config(optimization_row)

        # Placeholders — optimizer will override these
        assert config.gcr == 0.40
        assert config.dc_size_mw == 1.0
        assert config.ac_installed_mw == 1.0
        assert config.ac_poi_mw == 1.0

    def test_real_fields_mapped(self, optimization_row: dict) -> None:
        """Real fields like lat/lon/racking are mapped correctly."""
        config = _build_optimization_site_config(optimization_row)
        assert config.latitude == 33.45
        assert config.longitude == -112.07
        assert config.racking == "tracker"


# ── Sweep and economic kwargs tests ────────────────────────────────────


class TestBuildSweepKwargs:
    """Tests for _build_sweep_kwargs."""

    def test_no_overrides(self) -> None:
        """No sweep params → empty dict."""
        kwargs = _build_sweep_kwargs({"name": "test"})
        assert "gcr_range" not in kwargs
        assert "dcac_range" not in kwargs

    def test_gcr_range(self) -> None:
        kwargs = _build_sweep_kwargs({"gcr_min": 0.30, "gcr_max": 0.50})
        assert kwargs["gcr_range"] == (0.30, 0.50)

    def test_dcac_range(self) -> None:
        kwargs = _build_sweep_kwargs({"dcac_min": 1.20, "dcac_max": 1.50})
        assert kwargs["dcac_range"] == (1.20, 1.50)

    def test_partial_gcr_ignored(self) -> None:
        """If only gcr_min is set (no gcr_max), no gcr_range is created."""
        kwargs = _build_sweep_kwargs({"gcr_min": 0.30})
        assert "gcr_range" not in kwargs

    def test_utilization_factor(self) -> None:
        kwargs = _build_sweep_kwargs({"utilization_factor": 0.90})
        assert kwargs["utilization_factor"] == 0.90


class TestBuildEconomicKwargs:
    """Tests for _build_economic_kwargs."""

    def test_all_present(self) -> None:
        """All six economic params present → all included."""
        row = {
            "solar_cost_per_kw_dc": 950,
            "solar_opex_per_kw_dc_year": 12,
            "discount_rate_pct": 8.0,
            "project_lifetime_years": 30,
            "degradation_pct": 0.5,
            "itc_pct": 30.0,
        }
        kwargs = _build_economic_kwargs(row, require_defaults=False)
        assert kwargs["solar_cost_per_kw_dc"] == 950
        assert kwargs["project_lifetime_years"] == 30

    def test_none_present_no_defaults(self) -> None:
        """No economic params and require_defaults=False → empty dict."""
        kwargs = _build_economic_kwargs({}, require_defaults=False)
        assert kwargs == {}

    def test_partial_present_no_defaults(self) -> None:
        """Partial economic params and require_defaults=False → empty dict."""
        row = {"solar_cost_per_kw_dc": 950}
        kwargs = _build_economic_kwargs(row, require_defaults=False)
        assert kwargs == {}

    def test_defaults_applied(self) -> None:
        """require_defaults=True fills in missing economic params."""
        kwargs = _build_economic_kwargs({}, require_defaults=True)
        assert kwargs["solar_cost_per_kw_dc"] == 1000.0
        assert kwargs["discount_rate_pct"] == 7.0
        assert kwargs["project_lifetime_years"] == 25
        assert kwargs["energy_price_per_kwh"] == 0.04

    def test_revenue_params_included(self) -> None:
        """Energy price + escalator included when present."""
        row = {
            "solar_cost_per_kw_dc": 950,
            "solar_opex_per_kw_dc_year": 12,
            "discount_rate_pct": 8.0,
            "project_lifetime_years": 30,
            "degradation_pct": 0.5,
            "itc_pct": 30.0,
            "energy_price_per_kwh": 0.05,
            "energy_cost_escalator_pct": 2.5,
        }
        kwargs = _build_economic_kwargs(row, require_defaults=False)
        assert kwargs["energy_price_per_kwh"] == 0.05
        assert kwargs["energy_cost_escalator_pct"] == 2.5


# ── Result extraction tests ───────────────────────────────────────────


class TestExtractBuildabilityResults:
    """Tests for _extract_buildability_results."""

    def test_extracts_key_metrics(
        self, mock_buildability_result: BuildabilityResult
    ) -> None:
        extracted = _extract_buildability_results(mock_buildability_result)

        assert extracted["buildable_acres"] == 350.0
        assert extracted["buildable_pct"] == 70.0
        assert extracted["total_acres"] == 500.0
        assert extracted["dominant_land_cover"] == "Grassland/Herbaceous"
        assert extracted["mean_slope"] == 3.5
        assert extracted["tracker_suitable_pct"] == 85.0


class TestExtractProductionResults:
    """Tests for _extract_production_results."""

    def test_extracts_key_metrics(
        self, mock_pipeline_result: dict, production_row: dict
    ) -> None:
        site_config = map_row_to_site_config(production_row)
        extracted = _extract_production_results(mock_pipeline_result, site_config)

        assert extracted["annual_energy_mwh"] == 10500.0
        assert extracted["capacity_factor_ac_pct"] == 25.4
        assert extracted["specific_yield_kwh_per_kw"] == 1680.0
        assert extracted["dc_capacity_mw"] == 5.0
        assert extracted["ac_capacity_mw"] == 4.0
        assert extracted["gcr"] == 0.40
        assert extracted["dcac_ratio"] == 1.25

    def test_raises_on_empty_summaries(self, production_row: dict) -> None:
        site_config = map_row_to_site_config(production_row)
        with pytest.raises(ValueError, match="no summaries"):
            _extract_production_results({"summaries": []}, site_config)


class TestExtractOptimizationResults:
    """Tests for _extract_optimization_results."""

    def test_production_mode(self, mock_optimization_result: dict) -> None:
        extracted = _extract_optimization_results(mock_optimization_result)

        assert extracted["winner_type"] == "production"
        assert extracted["winner_gcr"] == 0.45
        assert extracted["winner_dcac"] == 1.30
        assert extracted["winner_mw_dc"] == 12.5
        assert extracted["winner_annual_mwh"] == 21000
        assert extracted["buildable_acres"] == 100.0

    def test_npv_mode_selects_npv_winner(self) -> None:
        result = {
            "mode": "npv",
            "winners": {
                "max_production": {"gcr": 0.45, "npv": 100},
                "npv": {"gcr": 0.40, "dcac_ratio": 1.25, "npv": 2500000,
                        "mw_dc": 10, "mw_ac": 8, "annual_energy_mwh": 18000,
                        "capacity_factor_ac": 0.257, "lcoe_per_mwh": 35},
            },
            "sweep_metadata": {"buildable_acres": 50},
        }
        extracted = _extract_optimization_results(result)
        assert extracted["winner_type"] == "npv"
        assert extracted["winner_npv"] == 2500000


class TestExtractOptimizationBessResults:
    """Tests for _extract_optimization_bess_results."""

    def test_extracts_bess_metrics(
        self, mock_bess_optimization_result: dict
    ) -> None:
        extracted = _extract_optimization_bess_results(
            mock_bess_optimization_result
        )

        assert extracted["bess_adds_value"] is True
        assert extracted["bess_power_mw"] == 2.0
        assert extracted["bess_duration_hr"] == 4.0
        assert extracted["bess_npv"] == 700000
        assert extracted["combined_npv"] == 3200000
        assert extracted["winner_npv"] == 2500000

    def test_no_bess_detail(self) -> None:
        """When no BESS combos succeeded, BESS fields are None."""
        result = {
            "mode": "solar_bess",
            "winners": {
                "solar_only_npv": {"gcr": 0.40, "npv": 100},
            },
            "bess_adds_value": False,
            "bess_winner_detail": None,
            "sweep_metadata": {"buildable_acres": 80},
        }
        extracted = _extract_optimization_bess_results(result)
        assert extracted["bess_adds_value"] is False
        assert extracted["bess_power_mw"] is None
        assert extracted["combined_npv"] is None


# ── Dispatch function tests (mocked) ──────────────────────────────────


class TestRunBuildability:
    """Tests for _run_buildability dispatch."""

    @patch("src.batch.executor.run_buildability_analysis")
    def test_calls_analysis_and_extracts(
        self,
        mock_analysis: MagicMock,
        buildability_row: dict,
        mock_buildability_result: BuildabilityResult,
        tmp_path: Path,
    ) -> None:
        mock_analysis.return_value = mock_buildability_result

        result = _run_buildability(buildability_row, tmp_path)

        mock_analysis.assert_called_once()
        assert result["buildable_acres"] == 350.0
        assert result["total_acres"] == 500.0


class TestRunProduction:
    """Tests for _run_production dispatch."""

    @patch("src.batch.executor.SolarModelingPipeline")
    def test_calls_pipeline_and_extracts(
        self,
        mock_pipeline_cls: MagicMock,
        production_row: dict,
        mock_pipeline_result: dict,
        tmp_path: Path,
    ) -> None:
        mock_pipeline = MagicMock()
        mock_pipeline.run_from_configs.return_value = mock_pipeline_result
        mock_pipeline_cls.return_value = mock_pipeline

        result = _run_production(production_row, tmp_path)

        mock_pipeline.run_from_configs.assert_called_once()
        assert result["annual_energy_mwh"] == 10500.0
        assert result["dcac_ratio"] == 1.25

    @patch("src.batch.executor.SolarModelingPipeline")
    def test_raises_on_failure(
        self,
        mock_pipeline_cls: MagicMock,
        production_row: dict,
        tmp_path: Path,
    ) -> None:
        mock_pipeline = MagicMock()
        mock_pipeline.run_from_configs.return_value = {
            "total_sites": 1,
            "successful": 0,
            "failed": 1,
            "summaries": [],
            "error_files": [],
            "report_files": [],
            "timeseries_files": [],
        }
        mock_pipeline_cls.return_value = mock_pipeline

        with pytest.raises(RuntimeError, match="failed"):
            _run_production(production_row, tmp_path)


class TestRunOptimization:
    """Tests for _run_optimization dispatch."""

    @patch("src.batch.executor.run_climate_data_pipeline", side_effect=lambda configs: (configs, {}, {}))
    @patch("src.batch.executor.optimize_solar")
    def test_calls_optimizer_and_extracts(
        self,
        mock_optimizer: MagicMock,
        mock_climate: MagicMock,
        optimization_row: dict,
        mock_optimization_result: dict,
        tmp_path: Path,
    ) -> None:
        mock_optimizer.return_value = mock_optimization_result

        result = _run_optimization(optimization_row, tmp_path)

        mock_climate.assert_called_once()
        mock_optimizer.assert_called_once()
        call_kwargs = mock_optimizer.call_args
        assert call_kwargs.kwargs["buildable_acres"] == 100.0
        assert call_kwargs.kwargs["racking"] == "tracker"
        assert result["winner_gcr"] == 0.45

    @patch("src.batch.executor.run_climate_data_pipeline", side_effect=lambda configs: (configs, {}, {}))
    @patch("src.batch.executor.optimize_solar")
    def test_sweep_range_overrides(
        self,
        mock_optimizer: MagicMock,
        mock_climate: MagicMock,
        optimization_row: dict,
        mock_optimization_result: dict,
        tmp_path: Path,
    ) -> None:
        optimization_row["gcr_min"] = 0.30
        optimization_row["gcr_max"] = 0.55
        optimization_row["dcac_min"] = 1.20
        optimization_row["dcac_max"] = 1.50
        mock_optimizer.return_value = mock_optimization_result

        _run_optimization(optimization_row, tmp_path)

        call_kwargs = mock_optimizer.call_args.kwargs
        assert call_kwargs["gcr_range"] == (0.30, 0.55)
        assert call_kwargs["dcac_range"] == (1.20, 1.50)


class TestRunOptimizationBess:
    """Tests for _run_optimization_bess dispatch."""

    @patch("src.batch.executor.run_climate_data_pipeline", side_effect=lambda configs: (configs, {}, {}))
    @patch("src.lmp.client.fetch_lmp")
    @patch("src.lmp.zone_mapper.resolve_pricing_zone")
    @patch("src.batch.executor.optimize_solar_bess")
    def test_calls_bess_optimizer(
        self,
        mock_optimizer: MagicMock,
        mock_resolve: MagicMock,
        mock_fetch: MagicMock,
        mock_climate: MagicMock,
        optimization_bess_row: dict,
        mock_bess_optimization_result: dict,
        tmp_path: Path,
    ) -> None:
        mock_zone = MagicMock()
        mock_zone.iso = "pjm"
        mock_zone.zone_name = "PJM_MID_ATLANTIC"
        mock_resolve.return_value = mock_zone
        mock_fetch.return_value = MagicMock()
        mock_optimizer.return_value = mock_bess_optimization_result

        result = _run_optimization_bess(optimization_bess_row, tmp_path)

        mock_resolve.assert_called_once()
        mock_fetch.assert_called_once()
        mock_optimizer.assert_called_once()
        assert result["bess_adds_value"] is True
        assert result["combined_npv"] == 3200000


# ── execute_batch tests ────────────────────────────────────────────────


class TestExecuteBatch:
    """Tests for the main execute_batch function."""

    @patch("src.batch.executor._run_buildability")
    def test_single_buildability_row(
        self,
        mock_handler: MagicMock,
        buildability_row: dict,
        tmp_path: Path,
    ) -> None:
        """A single buildability row dispatches correctly."""
        mock_handler.return_value = {"buildable_acres": 350.0}

        result = execute_batch([buildability_row], output_base_dir=tmp_path)

        assert isinstance(result, BatchResult)
        assert result.total_rows == 1
        assert result.succeeded == 1
        assert result.failed == 0
        assert len(result.row_results) == 1
        assert result.row_results[0].status == "success"
        assert result.row_results[0].results == {"buildable_acres": 350.0}

    @patch("src.batch.executor._run_production")
    @patch("src.batch.executor._run_buildability")
    def test_mixed_type_batch(
        self,
        mock_build: MagicMock,
        mock_prod: MagicMock,
        buildability_row: dict,
        production_row: dict,
        tmp_path: Path,
    ) -> None:
        """Rows with different analysis_types dispatch to different handlers."""
        mock_build.return_value = {"buildable_acres": 350.0}
        mock_prod.return_value = {"annual_energy_mwh": 10500.0}

        result = execute_batch(
            [buildability_row, production_row], output_base_dir=tmp_path
        )

        assert result.total_rows == 2
        assert result.succeeded == 2
        assert result.failed == 0
        mock_build.assert_called_once()
        mock_prod.assert_called_once()
        assert result.row_results[0].analysis_type == "buildability"
        assert result.row_results[1].analysis_type == "production"

    @patch("src.batch.executor._run_production")
    @patch("src.batch.executor._run_buildability")
    def test_fail_forward(
        self,
        mock_build: MagicMock,
        mock_prod: MagicMock,
        buildability_row: dict,
        production_row: dict,
        tmp_path: Path,
    ) -> None:
        """One row failing does not block subsequent rows."""
        mock_build.side_effect = RuntimeError("NLCD fetch failed")
        mock_prod.return_value = {"annual_energy_mwh": 10500.0}

        result = execute_batch(
            [buildability_row, production_row], output_base_dir=tmp_path
        )

        assert result.total_rows == 2
        assert result.succeeded == 1
        assert result.failed == 1
        assert result.row_results[0].status == "failed"
        assert "NLCD fetch failed" in result.row_results[0].error
        assert result.row_results[1].status == "success"

    @patch("src.batch.executor._run_buildability")
    def test_progress_callback_called(
        self,
        mock_handler: MagicMock,
        buildability_row: dict,
        tmp_path: Path,
    ) -> None:
        """Progress callback is invoked after each row."""
        mock_handler.return_value = {"buildable_acres": 350.0}
        callback = MagicMock()

        execute_batch(
            [buildability_row],
            output_base_dir=tmp_path,
            progress_callback=callback,
        )

        callback.assert_called_once_with(1, 1, "Build Site", "success")

    @patch("src.batch.executor._run_production")
    @patch("src.batch.executor._run_buildability")
    def test_progress_callback_multiple_rows(
        self,
        mock_build: MagicMock,
        mock_prod: MagicMock,
        buildability_row: dict,
        production_row: dict,
        tmp_path: Path,
    ) -> None:
        """Callback called for each row with correct index and total."""
        mock_build.return_value = {"buildable_acres": 350.0}
        mock_prod.return_value = {"annual_energy_mwh": 10500.0}
        callback = MagicMock()

        execute_batch(
            [buildability_row, production_row],
            output_base_dir=tmp_path,
            progress_callback=callback,
        )

        assert callback.call_count == 2
        callback.assert_any_call(1, 2, "Build Site", "success")
        callback.assert_any_call(2, 2, "Test Site Alpha", "success")

    @patch("src.batch.executor._run_buildability")
    def test_callback_exception_does_not_abort(
        self,
        mock_handler: MagicMock,
        buildability_row: dict,
        tmp_path: Path,
    ) -> None:
        """A callback that raises does not abort the batch."""
        mock_handler.return_value = {"buildable_acres": 350.0}
        callback = MagicMock(side_effect=ValueError("callback error"))

        # Should not raise
        result = execute_batch(
            [buildability_row],
            output_base_dir=tmp_path,
            progress_callback=callback,
        )

        assert result.succeeded == 1

    def test_run_id_generation(self, tmp_path: Path) -> None:
        """Run ID is a 12-character hex string."""
        result = execute_batch([], output_base_dir=tmp_path)

        assert len(result.run_id) == 12
        int(result.run_id, 16)  # validates hex

    def test_output_directory_created(self, tmp_path: Path) -> None:
        """Output directory is created under output_base_dir/{run_id}."""
        result = execute_batch([], output_base_dir=tmp_path)

        assert result.output_dir.exists()
        assert result.output_dir.parent == tmp_path

    @patch("src.batch.executor._run_buildability")
    def test_per_row_output_dirs_created(
        self,
        mock_handler: MagicMock,
        buildability_row: dict,
        tmp_path: Path,
    ) -> None:
        """Each row gets its own output subdirectory."""
        mock_handler.return_value = {"buildable_acres": 350.0}

        result = execute_batch([buildability_row], output_base_dir=tmp_path)

        row_dirs = list(result.output_dir.iterdir())
        assert len(row_dirs) == 1
        assert "Build Site" in row_dirs[0].name

    def test_unknown_analysis_type_fails_gracefully(
        self, tmp_path: Path
    ) -> None:
        """An unknown analysis_type records a failure, not a crash."""
        row = {
            "name": "Bad Row",
            "latitude": 33.45,
            "longitude": -112.07,
            "analysis_type": "nonsense",
        }

        result = execute_batch([row], output_base_dir=tmp_path)

        assert result.failed == 1
        assert result.row_results[0].status == "failed"
        assert "Unknown analysis_type" in result.row_results[0].error

    def test_empty_batch(self, tmp_path: Path) -> None:
        """An empty batch returns a valid BatchResult with zero rows."""
        result = execute_batch([], output_base_dir=tmp_path)

        assert result.total_rows == 0
        assert result.succeeded == 0
        assert result.failed == 0
        assert result.row_results == []

    @patch("src.batch.executor._run_buildability")
    def test_runtime_recorded(
        self,
        mock_handler: MagicMock,
        buildability_row: dict,
        tmp_path: Path,
    ) -> None:
        """Row runtime is recorded in the RowResult."""
        mock_handler.return_value = {"buildable_acres": 350.0}

        result = execute_batch([buildability_row], output_base_dir=tmp_path)

        assert result.row_results[0].runtime_seconds >= 0
        assert result.total_runtime_seconds >= 0

    @patch("src.batch.executor._run_optimization")
    @patch("src.batch.executor._run_production")
    @patch("src.batch.executor._run_buildability")
    def test_four_type_dispatch(
        self,
        mock_build: MagicMock,
        mock_prod: MagicMock,
        mock_opt: MagicMock,
        buildability_row: dict,
        production_row: dict,
        optimization_row: dict,
        tmp_path: Path,
    ) -> None:
        """Verify all four analysis types dispatch to correct handlers."""
        mock_build.return_value = {"buildable_acres": 100}
        mock_prod.return_value = {"annual_energy_mwh": 500}
        mock_opt.return_value = {"winner_gcr": 0.40}

        rows = [buildability_row, production_row, optimization_row]
        result = execute_batch(rows, output_base_dir=tmp_path)

        assert result.succeeded == 3
        mock_build.assert_called_once()
        mock_prod.assert_called_once()
        mock_opt.assert_called_once()

    def test_default_output_base_dir(self) -> None:
        """When output_base_dir is None, defaults to outputs/api/."""
        result = execute_batch([])

        assert "outputs/api" in str(result.output_dir)
        # Clean up the created directory
        result.output_dir.rmdir()


class TestRowResultDataclass:
    """Tests for RowResult dataclass structure."""

    def test_success_result(self) -> None:
        rr = RowResult(
            row_index=1,
            name="Test",
            analysis_type="buildability",
            status="success",
            error=None,
            runtime_seconds=1.5,
            results={"buildable_acres": 100},
        )
        assert rr.status == "success"
        assert rr.error is None
        assert rr.results is not None

    def test_failed_result(self) -> None:
        rr = RowResult(
            row_index=2,
            name="Bad",
            analysis_type="production",
            status="failed",
            error="Something went wrong",
            runtime_seconds=0.1,
            results=None,
        )
        assert rr.status == "failed"
        assert rr.results is None


class TestBatchResultDataclass:
    """Tests for BatchResult dataclass structure."""

    def test_structure(self, tmp_path: Path) -> None:
        br = BatchResult(
            run_id="abc123def456",
            total_rows=3,
            succeeded=2,
            failed=1,
            total_runtime_seconds=10.5,
            row_results=[],
            output_dir=tmp_path,
        )
        assert br.run_id == "abc123def456"
        assert br.total_rows == 3
        assert br.succeeded + br.failed == br.total_rows
