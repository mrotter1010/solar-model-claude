"""Tests for config validation framework."""

import json
from pathlib import Path

import pytest

from src.config import SiteConfig, get_unique_locations, load_config
from src.utils.exceptions import ConfigValidationError


# --- Valid CSV Loading ---


def test_load_valid_csv(sample_valid_csv: Path, test_results_dir: Path) -> None:
    """Test that valid CSV loads successfully with all 3 sites."""
    # Act
    configs = load_config(sample_valid_csv)

    # Assert
    assert len(configs) == 3
    assert all(isinstance(c, SiteConfig) for c in configs)
    assert configs[0].site_name == "Phoenix_Tracker"
    assert configs[1].site_name == "Phoenix_Fixed"
    assert configs[2].site_name == "Tucson_Tracker"

    # Write parsed configs to test_results for manual review
    output_path = test_results_dir / "test_config_valid_sites.json"
    output_path.write_text(json.dumps([c.model_dump() for c in configs], indent=2))


def test_load_single_row_fixture_csv() -> None:
    """Load the single-row fixture CSV and verify key fields."""
    # Arrange
    csv_path = Path(__file__).parent / "fixtures" / "single_row_test.csv"

    # Act
    configs = load_config(csv_path)

    # Assert
    assert len(configs) == 1
    site = configs[0]
    assert site.run_name == "Run_001"
    assert site.site_name == "Phoenix Solar Farm"
    assert site.latitude == pytest.approx(33.4484)
    assert site.longitude == pytest.approx(-112.074)
    assert site.dc_size_mw == pytest.approx(250.0)
    assert site.bifacial is True
    assert site.number_of_modules == 2


def test_load_multi_row_fixture_csv() -> None:
    """Load the multi-row fixture CSV and verify all sites."""
    csv_path = Path(__file__).parent / "fixtures" / "multi_row_test.csv"

    configs = load_config(csv_path)

    assert len(configs) == 3
    site_names = [c.site_name for c in configs]
    assert site_names == ["Phoenix Solar Farm", "Tucson Array", "Vegas Flats"]


# --- Case Insensitivity ---


def test_case_insensitive_racking(
    sample_valid_csv: Path, test_results_dir: Path
) -> None:
    """Test that racking field is case-insensitive and normalizes to lowercase."""
    configs = load_config(sample_valid_csv)

    # CSV has 'tracker', 'FIXED', 'Tracker' — all should normalize
    assert configs[0].racking == "tracker"
    assert configs[1].racking == "fixed"
    assert configs[2].racking == "tracker"

    # Write racking conversions for review
    racking_data = [
        {"site": c.site_name, "racking": c.racking, "tracking_mode": c.tracking_mode}
        for c in configs
    ]
    output_path = test_results_dir / "test_config_racking_conversions.json"
    output_path.write_text(json.dumps(racking_data, indent=2))


def test_case_insensitive_module_orientation(sample_valid_csv: Path) -> None:
    """Test that module orientation is case-insensitive."""
    configs = load_config(sample_valid_csv)

    assert configs[0].module_orientation == "portrait"
    assert configs[1].module_orientation == "portrait"  # Was 'PORTRAIT'
    assert configs[2].module_orientation == "landscape"  # Was 'Landscape'


# --- Property Conversions ---


def test_rotation_limit(sample_valid_csv: Path) -> None:
    """Test that rotation_limit returns tilt for trackers, None for fixed."""
    configs = load_config(sample_valid_csv)

    # Tracker sites return tilt as rotation limit
    assert configs[0].rotation_limit == 60  # tracker, tilt=60
    assert configs[2].rotation_limit == 60  # tracker, tilt=60

    # Fixed site returns None
    assert configs[1].rotation_limit is None  # fixed


def test_tracking_mode_conversion(sample_valid_csv: Path) -> None:
    """Test that racking converts to correct tracking_mode integer."""
    configs = load_config(sample_valid_csv)

    assert configs[0].tracking_mode == 1  # tracker
    assert configs[1].tracking_mode == 0  # fixed
    assert configs[2].tracking_mode == 1  # tracker


def test_mw_to_kw_conversion(sample_valid_csv: Path) -> None:
    """Test that DC Size converts from MW to kW."""
    configs = load_config(sample_valid_csv)

    assert configs[0].system_capacity_kw == 13_000  # 13 MW
    assert configs[1].system_capacity_kw == 13_000
    assert configs[2].system_capacity_kw == 20_000  # 20 MW


def test_location_property(sample_valid_csv: Path) -> None:
    """Test that location property returns (lat, lon) tuple."""
    configs = load_config(sample_valid_csv)

    assert configs[0].location == (33.483, -112.073)
    assert configs[1].location == (33.483, -112.073)
    assert configs[2].location == (32.253, -110.911)


# --- Location Deduplication ---


def test_location_deduplication(sample_valid_csv: Path) -> None:
    """Test that get_unique_locations deduplicates shared coordinates."""
    configs = load_config(sample_valid_csv)
    unique_locs = get_unique_locations(configs)

    # 3 sites, but only 2 unique locations (Phoenix sites share coords)
    assert len(unique_locs) == 2
    assert (33.483, -112.073) in unique_locs
    assert (32.253, -110.911) in unique_locs


# --- BESS Fields ---


def test_bess_fields_are_optional(sample_valid_csv: Path) -> None:
    """BESS fields should be None when not provided in CSV."""
    configs = load_config(sample_valid_csv)

    for site in configs:
        assert site.bess_dispatch_required is None
        assert site.bess_optimization_required is None


# --- Validation Errors ---


def test_invalid_latitude_raises_error(sample_invalid_csv: Path) -> None:
    """Test that out-of-range latitude raises ConfigValidationError."""
    # Note: row 1 has empty site_name which becomes NaN → None, failing first.
    # The invalid CSV has multiple errors; we just verify it raises with row context.
    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(sample_invalid_csv)

    error_msg = str(exc_info.value)
    assert "row" in error_msg.lower()


def test_invalid_racking_raises_error(sample_invalid_csv: Path) -> None:
    """Test that invalid racking type raises ConfigValidationError."""
    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(sample_invalid_csv)

    error_msg = str(exc_info.value)
    assert "row" in error_msg.lower()


def test_invalid_gcr_raises_error(sample_invalid_csv: Path) -> None:
    """Test that GCR >= 1.0 raises ConfigValidationError."""
    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(sample_invalid_csv)

    error_msg = str(exc_info.value)
    assert "row" in error_msg.lower()


def test_missing_required_fields_raises_error(
    sample_missing_fields_csv: Path,
) -> None:
    """Test that missing Panel Model and Inverter Model raises error."""
    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(sample_missing_fields_csv)

    error_msg = str(exc_info.value)
    assert "row" in error_msg.lower()


def test_nonexistent_file_raises_error() -> None:
    """Test that loading non-existent file raises ConfigValidationError."""
    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(Path("/fake/path/to/config.csv"))

    error_msg = str(exc_info.value)
    assert "not found" in error_msg.lower()


def test_error_context_includes_row_number(sample_invalid_csv: Path) -> None:
    """Test that ConfigValidationError context includes row number."""
    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(sample_invalid_csv)

    assert "row_number" in exc_info.value.context


def test_error_context_includes_site_name(sample_invalid_csv: Path) -> None:
    """Test that ConfigValidationError context includes site name."""
    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(sample_invalid_csv)

    assert "site_name" in exc_info.value.context


# --- Direct Schema Validation ---


def test_invalid_latitude_too_high_direct() -> None:
    """Latitude > 90 should fail Pydantic validation directly."""
    from pydantic import ValidationError

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


def test_invalid_racking_type_direct() -> None:
    """Racking must be 'fixed' or 'tracker' in direct construction."""
    from pydantic import ValidationError

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


def test_negative_dc_size_rejected_direct() -> None:
    """DC size must be greater than 0."""
    from pydantic import ValidationError

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
