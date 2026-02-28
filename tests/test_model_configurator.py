"""Tests for PySAM model configurator."""

import math
from pathlib import Path

import pytest

from src.config.schema import SiteConfig
from src.pysam_integration.exceptions import (
    InverterNotFoundError,
    ModuleNotFoundError,
    ValidationError,
)
from src.pysam_integration.model_configurator import ModelConfigurator, PySAMModelConfig


# -- Fixtures --


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
        "panel_model": "Canadian Solar CS3U-355P",
        "bifacial": True,
        "inverter_model": "SMA America: Sunny Central 2500-EV-US (800V)",
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


@pytest.fixture
def site_config() -> SiteConfig:
    """Default tracker site config."""
    return _make_site_config()


@pytest.fixture
def configurator() -> ModelConfigurator:
    """ModelConfigurator with default CEC database."""
    return ModelConfigurator()


# -- Test: Successful configuration (tracker site) --


class TestSuccessfulConfiguration:
    """Test successful model configuration for tracker sites."""

    def test_configure_tracker_site(
        self, configurator: ModelConfigurator, site_config: SiteConfig
    ) -> None:
        """Verify a tracker site produces a valid PySAMModelConfig."""
        result = configurator.configure_model(site_config)

        # Returns correct type with expected metadata
        assert isinstance(result, PySAMModelConfig)
        assert result.site_config is site_config
        assert result.module_params.name == "Canadian Solar CS3U-355P"
        assert result.inverter_params.name == "SMA America: Sunny Central 2500-EV-US (800V)"

        # DC/AC ratio calculated correctly
        expected_ratio = 100.0 / 80.0
        assert result.dc_ac_ratio == pytest.approx(expected_ratio, rel=1e-3)

        # System capacity set in kW
        assert result.model.SystemDesign.system_capacity == pytest.approx(100_000.0)

        # Tracking mode = 1 (tracker)
        assert result.model.SystemDesign.subarray1_track_mode == 1

        # Shading mode set to standard (non-linear)
        assert result.model.Shading.subarray1_shade_mode == 1

        # Terrain slope set to flat, south-facing
        assert result.model.SystemDesign.subarray1_slope_tilt == 0.0
        assert result.model.SystemDesign.subarray1_slope_azm == 180.0

        # String sizing is populated (not stubbed zeros)
        assert result.model.SystemDesign.subarray1_nstrings > 0
        assert result.model.SystemDesign.subarray1_modules_per_string > 0
        assert result.string_config is not None
        assert result.string_config.nstrings == result.model.SystemDesign.subarray1_nstrings
        assert result.string_config.modules_per_string == result.model.SystemDesign.subarray1_modules_per_string

    def test_configure_sets_weather_file(self, configurator: ModelConfigurator) -> None:
        """Verify weather file path is set when provided."""
        weather_path = Path("/tmp/test_weather.csv")
        config = _make_site_config(weather_file_path=weather_path)
        result = configurator.configure_model(config)

        assert result.model.SolarResource.solar_resource_file == str(weather_path)


# -- Test: Monthly albedo from weather file --


class TestMonthlyAlbedo:
    """Test monthly albedo calculation from weather file."""

    def test_albedo_set_from_weather_file(
        self, configurator: ModelConfigurator, tmp_path: Path
    ) -> None:
        """Albedo is aggregated from hourly weather data to 12 monthly values."""
        # Build a minimal PySAM-format weather file with 2 header rows
        weather_file = tmp_path / "weather.csv"
        header1 = "Station,City,State,Country,Latitude,Longitude,Time Zone,Elevation"
        header2 = "id,city,state,country,33.45,-112.07,-7,331"

        # Generate 8760 hourly rows with varying albedo by month
        rows = []
        for month in range(1, 13):
            days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1]
            hours_in_month = days_in_month * 24
            albedo_val = 0.15 + (month - 1) * 0.01  # 0.15 to 0.26
            for h in range(hours_in_month):
                day = h // 24 + 1
                hour = h % 24
                rows.append(f"2024,{month},{day},{hour},0,0,0,0,0,0,{albedo_val:.2f}")

        col_header = "Year,Month,Day,Hour,DNI,DHI,GHI,Temperature,Wind Speed,Pressure,Surface Albedo"
        content = f"{header1}\n{header2}\n{col_header}\n" + "\n".join(rows) + "\n"
        weather_file.write_text(content)

        config = _make_site_config(weather_file_path=weather_file)
        result = configurator.configure_model(config)

        # Verify 12 monthly albedo values set on model
        albedo = list(result.model.SolarResource.albedo)
        assert len(albedo) == 12

        # January albedo should be ~0.15, December ~0.26
        assert albedo[0] == pytest.approx(0.15, abs=0.001)
        assert albedo[11] == pytest.approx(0.26, abs=0.001)

        # All values should increase month over month
        for i in range(11):
            assert albedo[i] < albedo[i + 1]

    def test_albedo_fallback_on_missing_file(
        self, configurator: ModelConfigurator
    ) -> None:
        """When weather file can't be read, falls back to 0.2 for all months."""
        config = _make_site_config(
            weather_file_path=Path("/nonexistent/weather.csv")
        )
        result = configurator.configure_model(config)

        albedo = list(result.model.SolarResource.albedo)
        assert len(albedo) == 12
        assert all(a == pytest.approx(0.2) for a in albedo)


# -- Test: DC/AC ratio validation --


class TestDCAcRatioValidation:
    """Test DC/AC ratio boundary validation."""

    def test_dc_ac_ratio_exceeds_max_raises_error(
        self, configurator: ModelConfigurator
    ) -> None:
        """DC/AC ratio > 2.0 should raise ValidationError."""
        # DC=100 MW, AC=40 MW → ratio=2.5
        config = _make_site_config(dc_size_mw=100.0, ac_installed_mw=40.0)

        with pytest.raises(ValidationError, match="DC/AC ratio 2.50 exceeds maximum"):
            configurator.configure_model(config)

    def test_dc_ac_ratio_at_boundary_ok(
        self, configurator: ModelConfigurator
    ) -> None:
        """DC/AC ratio exactly 2.0 should pass validation."""
        config = _make_site_config(dc_size_mw=100.0, ac_installed_mw=50.0)
        result = configurator.configure_model(config)
        assert result.dc_ac_ratio == pytest.approx(2.0)


# -- Test: Fixed racking --


class TestFixedRacking:
    """Test fixed-racking array configuration."""

    def test_fixed_racking_sets_tilt(self, configurator: ModelConfigurator) -> None:
        """Fixed racking should set tilt directly, not rotlim."""
        config = _make_site_config(racking="fixed", tilt=25.0)
        result = configurator.configure_model(config)

        assert result.model.SystemDesign.subarray1_track_mode == 0
        assert result.model.SystemDesign.subarray1_tilt == 25.0


# -- Test: Tracker racking --


class TestTrackerRacking:
    """Test tracker-racking array configuration."""

    def test_tracker_racking_sets_rotlim(self, configurator: ModelConfigurator) -> None:
        """Tracker racking should set rotation limit from tilt field."""
        config = _make_site_config(racking="tracker", tilt=60.0)
        result = configurator.configure_model(config)

        assert result.model.SystemDesign.subarray1_track_mode == 1
        assert result.model.SystemDesign.subarray1_rotlim == 60.0
        assert result.model.SystemDesign.subarray1_tilt == 0

    def test_tracker_sets_ground_clearance(
        self, configurator: ModelConfigurator
    ) -> None:
        """Tracker should set ground clearance height."""
        config = _make_site_config(
            racking="tracker", ground_clearance_height_m=2.0
        )
        result = configurator.configure_model(config)

        assert result.model.CECPerformanceModelWithModuleDatabase.cec_ground_clearance_height == 2.0


# -- Test: Bifacial vs monofacial --


class TestBifaciality:
    """Test bifaciality factor configuration."""

    def test_bifacial_module_sets_factor(
        self, configurator: ModelConfigurator
    ) -> None:
        """Bifacial=True should set bifaciality to 0.7."""
        config = _make_site_config(bifacial=True)
        result = configurator.configure_model(config)

        cec = result.model.CECPerformanceModelWithModuleDatabase
        assert cec.cec_bifaciality == pytest.approx(0.7)
        assert cec.cec_is_bifacial == 1

    def test_monofacial_module_sets_zero(
        self, configurator: ModelConfigurator
    ) -> None:
        """Bifacial=False should set bifaciality to 0.0."""
        config = _make_site_config(bifacial=False)
        result = configurator.configure_model(config)

        cec = result.model.CECPerformanceModelWithModuleDatabase
        assert cec.cec_bifaciality == pytest.approx(0.0)
        assert cec.cec_is_bifacial == 0


# -- Test: Availability conversion --


class TestAvailabilityConversion:
    """Test CSV downtime % → PySAM availability % conversion."""

    def test_availability_conversion(self, configurator: ModelConfigurator) -> None:
        """CSV availability_percent=2.0 (downtime) → PySAM adjust_constant=98.0."""
        config = _make_site_config(availability_percent=2.0)
        result = configurator.configure_model(config)

        # PySAM availability = 100 - downtime
        assert result.model.AdjustmentFactors.adjust_constant == pytest.approx(98.0)

    def test_zero_downtime(self, configurator: ModelConfigurator) -> None:
        """Zero downtime → 100% availability."""
        config = _make_site_config(availability_percent=0.0)
        result = configurator.configure_model(config)

        assert result.model.AdjustmentFactors.adjust_constant == pytest.approx(100.0)


# -- Test: Loss parameters --


class TestLossParameters:
    """Test all loss parameters are mapped correctly."""

    def test_all_losses_mapped(self, configurator: ModelConfigurator) -> None:
        """Verify DC/AC wiring, transformer, mismatch losses are set."""
        config = _make_site_config(
            dc_wiring_loss_percent=2.5,
            ac_wiring_loss_percent=1.5,
            transformer_losses_percent=2.0,
            module_mismatch_percent=1.2,
        )
        result = configurator.configure_model(config)

        # DC and AC wiring
        assert result.model.Losses.subarray1_dcwiring_loss == pytest.approx(2.5)
        assert result.model.Losses.acwiring_loss == pytest.approx(1.5)

        # Transformer 80/20 split
        assert result.model.Losses.transformer_load_loss == pytest.approx(1.6)  # 2.0 * 0.8
        assert result.model.Losses.transformer_no_load_loss == pytest.approx(0.4)  # 2.0 * 0.2

        # Mismatch
        assert result.model.Losses.subarray1_mismatch_loss == pytest.approx(1.2)


# -- Test: CEC database errors --


class TestCECDatabaseErrors:
    """Test equipment lookup failures propagate correctly."""

    def test_module_not_found(self, configurator: ModelConfigurator) -> None:
        """Invalid module name raises ModuleNotFoundError."""
        config = _make_site_config(panel_model="Nonexistent Module XYZ-9999")

        with pytest.raises(ModuleNotFoundError):
            configurator.configure_model(config)

    def test_inverter_not_found(self, configurator: ModelConfigurator) -> None:
        """Invalid inverter name raises InverterNotFoundError."""
        config = _make_site_config(inverter_model="Nonexistent Inverter ABC-9999")

        with pytest.raises(InverterNotFoundError):
            configurator.configure_model(config)


# -- Test: Inverter count calculation --


class TestInverterCount:
    """Test inverter count is ceiling-rounded correctly."""

    def test_inverter_count_exact_division(
        self, configurator: ModelConfigurator
    ) -> None:
        """When AC capacity divides evenly by Paco, count is exact."""
        # SMA Sunny Central 2500: Paco = 2,500,000 W
        # ac_installed_mw = 5.0 → 5,000,000 W / 2,500,000 = 2 inverters
        config = _make_site_config(
            dc_size_mw=7.5,
            ac_installed_mw=5.0,
        )
        result = configurator.configure_model(config)
        assert result.inverter_count == 2

    def test_inverter_count_ceil_rounding(
        self, configurator: ModelConfigurator
    ) -> None:
        """When AC capacity doesn't divide evenly, count is ceiling-rounded."""
        # ac_installed_mw = 6.0 → 6,000,000 W / 2,500,000 = 2.4 → 3
        config = _make_site_config(
            dc_size_mw=9.0,
            ac_installed_mw=6.0,
        )
        result = configurator.configure_model(config)
        assert result.inverter_count == math.ceil(6_000_000 / 2_500_000)
        assert result.inverter_count == 3


# -- Test: Module orientation --


class TestModuleOrientation:
    """Test module orientation mapping."""

    def test_portrait_orientation(self, configurator: ModelConfigurator) -> None:
        """Portrait orientation → subarray1_mod_orient = 0."""
        config = _make_site_config(module_orientation="portrait")
        result = configurator.configure_model(config)
        assert result.model.Layout.subarray1_mod_orient == 0

    def test_landscape_orientation(self, configurator: ModelConfigurator) -> None:
        """Landscape orientation → subarray1_mod_orient = 1."""
        config = _make_site_config(module_orientation="landscape")
        result = configurator.configure_model(config)
        assert result.model.Layout.subarray1_mod_orient == 1


# -- Test: String configuration integration --


class TestStringConfigIntegration:
    """Test string calculator integration in model configurator."""

    def test_string_config_populated(
        self, configurator: ModelConfigurator, site_config: SiteConfig
    ) -> None:
        """string_config is populated with valid values."""
        result = configurator.configure_model(site_config)

        sc = result.string_config
        assert sc is not None
        assert sc.nstrings >= 1
        assert 10 <= sc.modules_per_string <= 40
        assert sc.total_modules == sc.nstrings * sc.modules_per_string
        assert sc.deviation_pct <= 2.0
        assert sc.dc_size_mw_target == site_config.dc_size_mw

    def test_string_config_varies_with_dc_size(
        self, configurator: ModelConfigurator
    ) -> None:
        """Different DC sizes produce different string configs."""
        config_small = _make_site_config(dc_size_mw=10.0, ac_installed_mw=8.0)
        config_large = _make_site_config(dc_size_mw=100.0, ac_installed_mw=80.0)

        result_small = configurator.configure_model(config_small)
        result_large = configurator.configure_model(config_large)

        assert result_small.string_config.total_modules < result_large.string_config.total_modules


# -- Artifact writer --


class TestArtifactWriter:
    """Write test summary to outputs/test_results/ for manual inspection."""

    def test_write_model_config_artifact(
        self, configurator: ModelConfigurator, site_config: SiteConfig
    ) -> None:
        """Write a summary of the configured model for inspection."""
        result = configurator.configure_model(site_config)

        output_dir = Path("outputs/test_results")
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / "model_config_test.txt"
        lines = [
            "=== PySAM Model Configuration Test Results ===",
            "",
            f"Site: {site_config.site_name}",
            f"DC Size: {site_config.dc_size_mw} MW",
            f"AC Size: {site_config.ac_installed_mw} MW",
            f"DC/AC Ratio: {result.dc_ac_ratio:.3f}",
            f"Inverter Count: {result.inverter_count}",
            "",
            "--- Module ---",
            f"Name: {result.module_params.name}",
            f"Pmax: {result.module_params.pmax} W",
            f"Efficiency: {result.module_params.efficiency:.3f}",
            "",
            "--- Inverter ---",
            f"Name: {result.inverter_params.name}",
            f"Paco: {result.inverter_params.paco} W",
            "",
            "--- Array Configuration ---",
            f"Tracking Mode: {result.model.SystemDesign.subarray1_track_mode}",
            f"Tilt: {result.model.SystemDesign.subarray1_tilt}",
            f"Azimuth: {result.model.SystemDesign.subarray1_azimuth}",
            f"GCR: {result.model.SystemDesign.subarray1_gcr}",
            f"Bifaciality: {result.model.CECPerformanceModelWithModuleDatabase.cec_bifaciality}",
            f"Strings: {result.string_config.nstrings}",
            f"Modules/String: {result.string_config.modules_per_string}",
            f"Total Modules: {result.string_config.total_modules}",
            f"String Deviation: {result.string_config.deviation_pct:.4f}%",
            "",
            "--- Losses ---",
            f"DC Wiring: {result.model.Losses.subarray1_dcwiring_loss}%",
            f"AC Wiring: {result.model.Losses.acwiring_loss}%",
            f"Transformer Load: {result.model.Losses.transformer_load_loss}%",
            f"Transformer No-Load: {result.model.Losses.transformer_no_load_loss}%",
            f"Availability (adjust_constant): {result.model.AdjustmentFactors.adjust_constant}%",
            f"Mismatch: {result.model.Losses.subarray1_mismatch_loss}%",
            "",
            "=== END ===",
        ]

        output_path.write_text("\n".join(lines))
        assert output_path.exists()
