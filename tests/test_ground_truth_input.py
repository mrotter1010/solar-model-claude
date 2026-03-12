"""Tests for ground truth data file input column and bias correction report text.

Tests the new ground_truth_data_file field on SiteConfig, validation of
ground truth CSV contents, and bias correction methodology text in the
PDF report narrative.
"""

import json
from pathlib import Path

import pandas as pd
import pytest

from src.config.loader import load_config
from src.config.schema import (
    SiteConfig,
    validate_ground_truth_file_contents,
)
from src.reporting.data_extractor import (
    extract_loss_waterfall,
    extract_site_summary,
    generate_narrative,
)
from src.utils.exceptions import ConfigValidationError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SINGLE_ROW_CSV = FIXTURES_DIR / "single_row_test.csv"

# Minimal valid SiteConfig kwargs for tests that create SiteConfig directly
_BASE_SITE_KWARGS = {
    "run_name": "Test1",
    "site_name": "TestSite",
    "customer": "TestCo",
    "latitude": 33.45,
    "longitude": -112.07,
    "dc_size_mw": 13.0,
    "ac_installed_mw": 10.0,
    "ac_poi_mw": 10.0,
    "racking": "tracker",
    "tilt": 60.0,
    "azimuth": 180.0,
    "module_orientation": "portrait",
    "number_of_modules": 2,
    "ground_clearance_height_m": 1.5,
    "panel_model": "Test Panel",
    "bifacial": True,
    "inverter_model": "Test Inverter",
    "gcr": 0.34,
    "shading_percent": 1.0,
    "dc_wiring_loss_percent": 1.5,
    "ac_wiring_loss_percent": 1.5,
    "transformer_losses_percent": 0.0,
    "degradation_percent": 0.3,
    "availability_percent": 98.0,
    "module_mismatch_percent": 1.5,
    "lid_percent": 1.0,
}


def _make_loss_data(**overrides: object) -> dict:
    """Create a loss_data dict matching Phoenix values."""
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
        "monthly_poa_eff": [15000000.0] * 12,
        "avg_daytime_ghi_wm2": 490.5,
        "annual_ghi_kwh_m2": 2131.8,
    }
    data.update(overrides)
    return data


def _make_site_config_dict(**overrides: object) -> dict:
    """Create a site_config dict for reporting functions."""
    config = {
        "site_name": "TestSite_Phoenix",
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
        "data_source": "nsrdb",
    }
    config.update(overrides)
    return config


def _write_ground_truth_csv(path: Path, columns: list[str] | None = None) -> None:
    """Write a valid ground truth CSV file."""
    if columns is None:
        columns = ["year", "month", "ghi_kwh_m2_day", "dni_kwh_m2_day"]
    rows = []
    for month in range(1, 13):
        rows.append({
            "year": 2022,
            "month": month,
            "ghi_kwh_m2_day": 5.0 + month * 0.2,
            "dni_kwh_m2_day": 6.0 + month * 0.3,
        })
    df = pd.DataFrame(rows, columns=columns)
    df.to_csv(path, index=False)


# ---------------------------------------------------------------------------
# a. ground_truth_data_file is optional, defaults to None
# ---------------------------------------------------------------------------


class TestGroundTruthFieldOptional:
    """Test that ground_truth_data_file is optional and defaults to None."""

    def test_defaults_to_none(self) -> None:
        """SiteConfig without ground_truth_data_file defaults to None."""
        config = SiteConfig(**_BASE_SITE_KWARGS)
        assert config.ground_truth_data_file is None

    def test_csv_without_column_loads(self) -> None:
        """Loading a CSV that lacks the Ground Truth column succeeds."""
        configs = load_config(SINGLE_ROW_CSV)
        assert len(configs) == 1
        assert configs[0].ground_truth_data_file is None

    def test_empty_string_becomes_none(self) -> None:
        """An empty string value is converted to None."""
        config = SiteConfig(ground_truth_data_file="", **_BASE_SITE_KWARGS)
        assert config.ground_truth_data_file is None

    def test_none_value_stays_none(self) -> None:
        """Explicit None value stays None."""
        config = SiteConfig(ground_truth_data_file=None, **_BASE_SITE_KWARGS)
        assert config.ground_truth_data_file is None


# ---------------------------------------------------------------------------
# b. Valid CSV path passes validation
# ---------------------------------------------------------------------------


class TestGroundTruthValidPath:
    """Test that a valid ground truth CSV path passes validation."""

    def test_valid_csv_path_accepted(self, tmp_path: Path) -> None:
        """Existing .csv file is accepted by the validator."""
        gt_file = tmp_path / "ground_truth.csv"
        _write_ground_truth_csv(gt_file)

        config = SiteConfig(
            ground_truth_data_file=str(gt_file), **_BASE_SITE_KWARGS
        )
        assert config.ground_truth_data_file == gt_file

    def test_path_object_accepted(self, tmp_path: Path) -> None:
        """Path object (not just string) is accepted."""
        gt_file = tmp_path / "ground_truth.csv"
        _write_ground_truth_csv(gt_file)

        config = SiteConfig(
            ground_truth_data_file=gt_file, **_BASE_SITE_KWARGS
        )
        assert config.ground_truth_data_file == gt_file

    def test_csv_with_column_loads(self, tmp_path: Path) -> None:
        """Loading a CSV that includes the Ground Truth column works."""
        gt_file = tmp_path / "ground_truth.csv"
        _write_ground_truth_csv(gt_file)

        # Create a CSV with the Ground Truth Data File column
        csv_path = tmp_path / "input.csv"
        df = pd.read_csv(SINGLE_ROW_CSV)
        df["Ground Truth Data File"] = str(gt_file)
        df.to_csv(csv_path, index=False)

        configs = load_config(csv_path)
        assert len(configs) == 1
        assert configs[0].ground_truth_data_file == gt_file


# ---------------------------------------------------------------------------
# c. Non-existent path fails validation
# ---------------------------------------------------------------------------


class TestGroundTruthInvalidPath:
    """Test that invalid paths fail validation."""

    def test_nonexistent_file_raises(self) -> None:
        """A path to a non-existent file raises ValueError."""
        with pytest.raises(ValueError, match="Ground truth data file not found"):
            SiteConfig(
                ground_truth_data_file="/nonexistent/path/file.csv",
                **_BASE_SITE_KWARGS,
            )

    def test_non_csv_extension_raises(self, tmp_path: Path) -> None:
        """A file without .csv extension raises ValueError."""
        txt_file = tmp_path / "data.txt"
        txt_file.write_text("some data")

        with pytest.raises(ValueError, match="must be a .csv file"):
            SiteConfig(
                ground_truth_data_file=str(txt_file), **_BASE_SITE_KWARGS
            )

    def test_directory_raises(self, tmp_path: Path) -> None:
        """A directory path raises ValueError."""
        with pytest.raises(ValueError, match="not a file"):
            SiteConfig(
                ground_truth_data_file=str(tmp_path), **_BASE_SITE_KWARGS
            )


# ---------------------------------------------------------------------------
# d. File without required columns fails validation
# ---------------------------------------------------------------------------


class TestGroundTruthContentValidation:
    """Test validate_ground_truth_file_contents column checks."""

    def test_valid_file_passes(self, tmp_path: Path) -> None:
        """A file with all required columns passes validation."""
        gt_file = tmp_path / "valid.csv"
        _write_ground_truth_csv(gt_file)

        df = validate_ground_truth_file_contents(gt_file)
        assert len(df) == 12
        assert "ghi_kwh_m2_day" in df.columns
        assert "dni_kwh_m2_day" in df.columns

    def test_missing_ghi_column_raises(self, tmp_path: Path) -> None:
        """File missing ghi_kwh_m2_day column raises ConfigValidationError."""
        gt_file = tmp_path / "missing_ghi.csv"
        df = pd.DataFrame({
            "year": [2022],
            "month": [1],
            "dni_kwh_m2_day": [6.0],
        })
        df.to_csv(gt_file, index=False)

        with pytest.raises(ConfigValidationError, match="missing required columns"):
            validate_ground_truth_file_contents(gt_file)

    def test_missing_multiple_columns_raises(self, tmp_path: Path) -> None:
        """File missing multiple required columns lists them all."""
        gt_file = tmp_path / "bad.csv"
        df = pd.DataFrame({"year": [2022], "extra_col": [1.0]})
        df.to_csv(gt_file, index=False)

        with pytest.raises(ConfigValidationError, match="missing required columns") as exc_info:
            validate_ground_truth_file_contents(gt_file)

        # Error context should list the missing columns
        assert "missing_columns" in exc_info.value.context

    def test_extra_columns_ok(self, tmp_path: Path) -> None:
        """Extra columns beyond the required ones are allowed."""
        gt_file = tmp_path / "extra.csv"
        df = pd.DataFrame({
            "year": [2022],
            "month": [6],
            "ghi_kwh_m2_day": [6.5],
            "dni_kwh_m2_day": [7.0],
            "notes": ["summer"],
        })
        df.to_csv(gt_file, index=False)

        result = validate_ground_truth_file_contents(gt_file)
        assert len(result) == 1

    def test_corrupt_file_raises(self, tmp_path: Path) -> None:
        """A corrupt binary file raises ConfigValidationError (missing columns)."""
        gt_file = tmp_path / "corrupt.csv"
        gt_file.write_bytes(b"\x00\x01\x02\x03")

        with pytest.raises(ConfigValidationError, match="missing required columns"):
            validate_ground_truth_file_contents(gt_file)


# ---------------------------------------------------------------------------
# e. PDF report includes bias correction text for NSRDB runs
# ---------------------------------------------------------------------------


class TestBiasCorrectionNarrativeNSRDB:
    """Test that narrative includes bias correction text for NSRDB runs."""

    def _build_narrative(self, **loss_overrides: object) -> dict:
        """Build narrative with NSRDB data source and bias correction metadata."""
        site_config = _make_site_config_dict(data_source="nsrdb")
        loss_data = _make_loss_data(
            bias_correction_applied=True,
            bias_correction_model_version="1.0.0",
            mean_ghi_correction_factor=0.965,
            mean_dni_correction_factor=0.950,
            **loss_overrides,
        )
        site_summary = extract_site_summary(site_config, loss_data)
        waterfall_data = extract_loss_waterfall(loss_data)
        return generate_narrative(site_summary, loss_data, waterfall_data)

    def test_nsrdb_bias_text_present(self) -> None:
        """solar_resource includes NSRDB bias correction methodology text."""
        result = self._build_narrative()
        assert "NSRDB Bias Correction" in result["solar_resource"]

    def test_contains_correction_factors(self) -> None:
        """Narrative includes the actual correction factor values."""
        result = self._build_narrative()
        solar = result["solar_resource"]
        # GHI: 0.965 - 1 = -0.035 → formatted as -3.5%
        assert "GHI=-3.5%" in solar
        # DNI: 0.950 - 1 = -0.05 → formatted as -5.0%
        assert "DNI=-5.0%" in solar

    def test_contains_model_version(self) -> None:
        """Narrative includes model version string."""
        result = self._build_narrative()
        assert "1.0.0" in result["solar_resource"]

    def test_contains_station_count(self) -> None:
        """Narrative mentions 20 CONUS stations."""
        result = self._build_narrative()
        assert "20 CONUS stations" in result["solar_resource"]

    def test_contains_network_names(self) -> None:
        """Narrative mentions the ground station networks."""
        result = self._build_narrative()
        solar = result["solar_resource"]
        assert "SURFRAD" in solar
        assert "SOLRAD" in solar
        assert "MIDC" in solar


# ---------------------------------------------------------------------------
# f. PDF report includes Solcast text for Solcast runs
# ---------------------------------------------------------------------------


class TestBiasCorrectionNarrativeSolcast:
    """Test that narrative includes appropriate text for Solcast runs."""

    def _build_narrative(self, **loss_overrides: object) -> dict:
        """Build narrative with Solcast data source (no bias correction)."""
        site_config = _make_site_config_dict(data_source="solcast")
        loss_data = _make_loss_data(**loss_overrides)
        site_summary = extract_site_summary(site_config, loss_data)
        waterfall_data = extract_loss_waterfall(loss_data)
        return generate_narrative(site_summary, loss_data, waterfall_data)

    def test_solcast_text_present(self) -> None:
        """solar_resource includes Solcast resource data text."""
        result = self._build_narrative()
        assert "Solcast TMY P50" in result["solar_resource"]

    def test_no_bias_correction_text(self) -> None:
        """solar_resource mentions no bias correction for Solcast."""
        result = self._build_narrative()
        assert "No additional bias correction" in result["solar_resource"]

    def test_no_nsrdb_bias_section(self) -> None:
        """Solcast narrative does NOT contain NSRDB bias correction section."""
        result = self._build_narrative()
        assert "NSRDB Bias Correction" not in result["solar_resource"]

    def test_nsrdb_without_bias_has_no_section(self) -> None:
        """NSRDB run that didn't get bias correction has no section."""
        site_config = _make_site_config_dict(data_source="nsrdb")
        loss_data = _make_loss_data()  # No bias keys
        site_summary = extract_site_summary(site_config, loss_data)
        waterfall_data = extract_loss_waterfall(loss_data)
        result = generate_narrative(site_summary, loss_data, waterfall_data)

        assert "NSRDB Bias Correction" not in result["solar_resource"]
        assert "Solcast TMY P50" not in result["solar_resource"]


# ---------------------------------------------------------------------------
# Output artifact
# ---------------------------------------------------------------------------


class TestGroundTruthOutputArtifact:
    """Write test results to outputs/test_results/ for manual inspection."""

    def test_write_artifact(self, tmp_path: Path) -> None:
        """Write ground truth test results for manual inspection."""
        gt_file = tmp_path / "ground_truth.csv"
        _write_ground_truth_csv(gt_file)

        config = SiteConfig(
            ground_truth_data_file=str(gt_file), **_BASE_SITE_KWARGS
        )

        artifact_dir = Path("outputs/test_results")
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = artifact_dir / "test_ground_truth_input.json"

        artifact = {
            "ground_truth_data_file": str(config.ground_truth_data_file),
            "field_is_optional": True,
            "default_value": "None",
        }
        artifact_path.write_text(json.dumps(artifact, indent=2))
