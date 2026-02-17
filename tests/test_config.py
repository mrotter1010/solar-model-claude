"""Tests for config validation framework."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.config.loader import get_unique_locations, load_config
from src.config.schema import SiteConfig

FIXTURES_DIR = Path(__file__).parent / "fixtures"
TEST_RESULTS_DIR = Path(__file__).parent.parent / "outputs" / "test_results"


@pytest.fixture(autouse=True)
def ensure_output_dir() -> None:
    """Ensure test results directory exists."""
    TEST_RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# --- Valid CSV Loading ---


def test_load_single_row_csv() -> None:
    """Load a single-row CSV and verify all fields are parsed correctly."""
    # Arrange
    csv_path = FIXTURES_DIR / "single_row_test.csv"

    # Act
    configs = load_config(csv_path)

    # Assert - exactly one site loaded
    assert len(configs) == 1
    site = configs[0]
    assert site.run_name == "Run_001"
    assert site.site_name == "Phoenix Solar Farm"
    assert site.customer == "SunCorp"
    assert site.latitude == pytest.approx(33.4484)
    assert site.longitude == pytest.approx(-112.074)
    assert site.dc_size_mw == pytest.approx(250.0)
    assert site.racking == "tracker"
    assert site.module_orientation == "portrait"
    assert site.bifacial is True
    assert site.gcr == pytest.approx(0.35)
    assert site.number_of_modules == 2

    # Write parsed config for manual inspection
    output = site.model_dump()
    output_path = TEST_RESULTS_DIR / "test_config_single_row.json"
    output_path.write_text(json.dumps(output, indent=2))


def test_load_multi_row_csv() -> None:
    """Load a multi-row CSV and verify all sites are parsed."""
    # Arrange
    csv_path = FIXTURES_DIR / "multi_row_test.csv"

    # Act
    configs = load_config(csv_path)

    # Assert - three sites loaded with correct names
    assert len(configs) == 3
    site_names = [c.site_name for c in configs]
    assert site_names == ["Phoenix Solar Farm", "Tucson Array", "Vegas Flats"]

    # Verify different racking types parsed correctly
    assert configs[0].racking == "tracker"
    assert configs[1].racking == "fixed"
    assert configs[2].racking == "tracker"

    # Write all parsed configs for manual inspection
    output = [c.model_dump() for c in configs]
    output_path = TEST_RESULTS_DIR / "test_config_multi_row.json"
    output_path.write_text(json.dumps(output, indent=2))


# --- BESS Fields ---


def test_bess_fields_are_optional() -> None:
    """BESS fields should be None when not provided in CSV."""
    configs = load_config(FIXTURES_DIR / "single_row_test.csv")
    site = configs[0]

    # BESS fields are empty in the CSV, should be None
    assert site.bess_dispatch_required is None
    assert site.bess_optimization_required is None


# --- Properties ---


def test_system_capacity_kw_conversion() -> None:
    """system_capacity_kw should convert MW to kW."""
    configs = load_config(FIXTURES_DIR / "single_row_test.csv")
    site = configs[0]

    # 250 MW should be 250000 kW
    assert site.system_capacity_kw == pytest.approx(250_000.0)


def test_tracking_mode_tracker() -> None:
    """Tracker racking should return tracking_mode=1."""
    configs = load_config(FIXTURES_DIR / "single_row_test.csv")
    site = configs[0]

    assert site.racking == "tracker"
    assert site.tracking_mode == 1


def test_tracking_mode_fixed() -> None:
    """Fixed racking should return tracking_mode=0."""
    configs = load_config(FIXTURES_DIR / "multi_row_test.csv")
    # Tucson Array is fixed
    site = configs[1]

    assert site.racking == "fixed"
    assert site.tracking_mode == 0


def test_location_tuple() -> None:
    """location property should return (lat, lon) tuple."""
    configs = load_config(FIXTURES_DIR / "single_row_test.csv")
    site = configs[0]

    lat, lon = site.location
    assert lat == pytest.approx(33.4484)
    assert lon == pytest.approx(-112.074)


# --- Unique Locations ---


def test_get_unique_locations() -> None:
    """Extract unique locations from multi-site config."""
    configs = load_config(FIXTURES_DIR / "multi_row_test.csv")

    # Act
    unique = get_unique_locations(configs)

    # Assert - 3 different locations in fixture
    assert len(unique) == 3
    # All should be (lat, lon) tuples
    for loc in unique:
        assert len(loc) == 2


def test_get_unique_locations_deduplicates() -> None:
    """Duplicate locations should be deduplicated."""
    configs = load_config(FIXTURES_DIR / "multi_row_test.csv")

    # Manually duplicate a location
    configs.append(configs[0])  # Phoenix appears twice

    unique = get_unique_locations(configs)

    # Should still be 3 unique locations even with 4 sites
    assert len(unique) == 3


# --- Validation Errors ---


def test_invalid_latitude_too_high() -> None:
    """Latitude > 90 should fail validation."""
    with pytest.raises(ValidationError, match="less than or equal to 90"):
        SiteConfig(
            run_name="Test",
            site_name="Bad Site",
            customer="Test",
            latitude=91.0,
            longitude=-112.0,
            dc_size_mw=100.0,
            ac_installed_mw=80.0,
            ac_poi_mw=75.0,
            racking="fixed",
            tilt=25.0,
            azimuth=180.0,
            module_orientation="portrait",
            number_of_modules=1,
            ground_clearance_height_m=1.0,
            panel_model="Test Panel",
            bifacial=False,
            inverter_model="Test Inverter",
            gcr=0.4,
            shading_percent=0.0,
            dc_wiring_loss_percent=0.0,
            ac_wiring_loss_percent=0.0,
            transformer_losses_percent=0.0,
            degradation_percent=0.0,
            availability_percent=0.0,
            module_mismatch_percent=0.0,
            lid_percent=0.0,
        )


def test_invalid_racking_type() -> None:
    """Racking must be 'fixed' or 'tracker'."""
    with pytest.raises(ValidationError, match="Racking must be"):
        SiteConfig(
            run_name="Test",
            site_name="Bad Site",
            customer="Test",
            latitude=33.0,
            longitude=-112.0,
            dc_size_mw=100.0,
            ac_installed_mw=80.0,
            ac_poi_mw=75.0,
            racking="rooftop",
            tilt=25.0,
            azimuth=180.0,
            module_orientation="portrait",
            number_of_modules=1,
            ground_clearance_height_m=1.0,
            panel_model="Test Panel",
            bifacial=False,
            inverter_model="Test Inverter",
            gcr=0.4,
            shading_percent=0.0,
            dc_wiring_loss_percent=0.0,
            ac_wiring_loss_percent=0.0,
            transformer_losses_percent=0.0,
            degradation_percent=0.0,
            availability_percent=0.0,
            module_mismatch_percent=0.0,
            lid_percent=0.0,
        )


def test_negative_dc_size_rejected() -> None:
    """DC size must be greater than 0."""
    with pytest.raises(ValidationError, match="greater than 0"):
        SiteConfig(
            run_name="Test",
            site_name="Bad Site",
            customer="Test",
            latitude=33.0,
            longitude=-112.0,
            dc_size_mw=-10.0,
            ac_installed_mw=80.0,
            ac_poi_mw=75.0,
            racking="fixed",
            tilt=25.0,
            azimuth=180.0,
            module_orientation="portrait",
            number_of_modules=1,
            ground_clearance_height_m=1.0,
            panel_model="Test Panel",
            bifacial=False,
            inverter_model="Test Inverter",
            gcr=0.4,
            shading_percent=0.0,
            dc_wiring_loss_percent=0.0,
            ac_wiring_loss_percent=0.0,
            transformer_losses_percent=0.0,
            degradation_percent=0.0,
            availability_percent=0.0,
            module_mismatch_percent=0.0,
            lid_percent=0.0,
        )


def test_gcr_out_of_range() -> None:
    """GCR must be between 0 and 1 (exclusive)."""
    with pytest.raises(ValidationError, match="less than 1"):
        SiteConfig(
            run_name="Test",
            site_name="Bad Site",
            customer="Test",
            latitude=33.0,
            longitude=-112.0,
            dc_size_mw=100.0,
            ac_installed_mw=80.0,
            ac_poi_mw=75.0,
            racking="fixed",
            tilt=25.0,
            azimuth=180.0,
            module_orientation="portrait",
            number_of_modules=1,
            ground_clearance_height_m=1.0,
            panel_model="Test Panel",
            bifacial=False,
            inverter_model="Test Inverter",
            gcr=1.5,
            shading_percent=0.0,
            dc_wiring_loss_percent=0.0,
            ac_wiring_loss_percent=0.0,
            transformer_losses_percent=0.0,
            degradation_percent=0.0,
            availability_percent=0.0,
            module_mismatch_percent=0.0,
            lid_percent=0.0,
        )


def test_invalid_module_orientation() -> None:
    """Module orientation must be 'portrait' or 'landscape'."""
    with pytest.raises(ValidationError, match="Module Orientation must be"):
        SiteConfig(
            run_name="Test",
            site_name="Bad Site",
            customer="Test",
            latitude=33.0,
            longitude=-112.0,
            dc_size_mw=100.0,
            ac_installed_mw=80.0,
            ac_poi_mw=75.0,
            racking="fixed",
            tilt=25.0,
            azimuth=180.0,
            module_orientation="diagonal",
            number_of_modules=1,
            ground_clearance_height_m=1.0,
            panel_model="Test Panel",
            bifacial=False,
            inverter_model="Test Inverter",
            gcr=0.4,
            shading_percent=0.0,
            dc_wiring_loss_percent=0.0,
            ac_wiring_loss_percent=0.0,
            transformer_losses_percent=0.0,
            degradation_percent=0.0,
            availability_percent=0.0,
            module_mismatch_percent=0.0,
            lid_percent=0.0,
        )


def test_config_file_not_found() -> None:
    """Loading a nonexistent CSV should raise ConfigValidationError."""
    from src.utils.exceptions import ConfigValidationError

    with pytest.raises(ConfigValidationError, match="Config file not found"):
        load_config(Path("/nonexistent/path.csv"))


def test_case_insensitive_racking() -> None:
    """Racking should accept any case and normalize to lowercase."""
    configs = load_config(FIXTURES_DIR / "single_row_test.csv")
    # CSV has "Tracker" (capitalized), should be normalized to "tracker"
    assert configs[0].racking == "tracker"


def test_case_insensitive_module_orientation() -> None:
    """Module orientation should accept any case and normalize to lowercase."""
    configs = load_config(FIXTURES_DIR / "single_row_test.csv")
    # CSV has "Portrait" (capitalized), should be normalized to "portrait"
    assert configs[0].module_orientation == "portrait"
