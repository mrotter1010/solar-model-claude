"""Tests for batch input validation."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.batch.template import TEMPLATE_COLUMNS
from src.batch.validator import (
    MAX_BATCH_ROWS,
    ValidationError,
    ValidationResult,
    validate_batch_input,
)


# ── Helpers ────────────────────────────────────────────────────────────


def _write_csv(path: Path, rows: list[dict]) -> Path:
    """Write a list of row dicts to a CSV file."""
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)
    return path


def _write_xlsx(path: Path, rows: list[dict], include_desc_row: bool = False) -> Path:
    """Write a list of row dicts to an Excel file with a Template sheet."""
    df = pd.DataFrame(rows)
    if include_desc_row:
        desc = {col: f"Description for {col}" for col in df.columns}
        # Make latitude description non-numeric to trigger skip logic
        if "latitude" in desc:
            desc["latitude"] = "* Latitude (-90 to 90)"
        desc_df = pd.DataFrame([desc])
        df = pd.concat([desc_df, df], ignore_index=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Template", index=False)
    return path


def _mock_cec_db() -> MagicMock:
    """Create a mock CECDatabase that accepts any equipment name."""
    db = MagicMock()
    db.get_module_params.return_value = MagicMock()
    db.get_inverter_params.return_value = MagicMock()
    return db


def _buildability_row(name: str = "Site A", lat: float = 33.45, lon: float = -112.07) -> dict:
    return {"name": name, "latitude": lat, "longitude": lon, "analysis_type": "buildability"}


def _production_row(name: str = "Site B", lat: float = 39.74, lon: float = -104.99) -> dict:
    return {
        "name": name,
        "latitude": lat,
        "longitude": lon,
        "analysis_type": "production",
        "racking": "tracker",
        "gcr": 0.40,
        "dc_capacity_mw": 5.0,
        "ac_capacity_mw": 4.0,
    }


def _optimization_row(name: str = "Site C", lat: float = 30.27, lon: float = -97.74) -> dict:
    return {
        "name": name,
        "latitude": lat,
        "longitude": lon,
        "analysis_type": "optimization",
        "buildable_acres": 50.0,
    }


def _optimization_bess_row(name: str = "Site D", lat: float = 39.95, lon: float = -75.16) -> dict:
    return {
        "name": name,
        "latitude": lat,
        "longitude": lon,
        "analysis_type": "optimization_bess",
        "buildable_acres": 80.0,
        "dispatch_mode": "ftm",
    }


# ── Valid input tests ──────────────────────────────────────────────────


class TestValidInputs:
    """Tests for well-formed input files that should pass validation."""

    def test_valid_buildability_batch(self, tmp_path: Path) -> None:
        """Buildability rows need only name, lat, lon, analysis_type."""
        csv_file = _write_csv(tmp_path / "batch.csv", [_buildability_row()])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert result.is_valid
        assert len(result.valid_rows) == 1
        assert len(result.errors) == 0

    def test_valid_production_batch(self, tmp_path: Path) -> None:
        """Production rows with all required fields pass validation."""
        csv_file = _write_csv(tmp_path / "batch.csv", [_production_row()])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert result.is_valid
        assert len(result.valid_rows) == 1
        assert len(result.errors) == 0

    def test_valid_optimization_batch(self, tmp_path: Path) -> None:
        """Optimization rows with buildable_acres pass validation."""
        csv_file = _write_csv(tmp_path / "batch.csv", [_optimization_row()])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert result.is_valid
        assert len(result.valid_rows) == 1

    def test_valid_optimization_bess_batch(self, tmp_path: Path) -> None:
        """optimization_bess rows with FTM dispatch pass validation."""
        csv_file = _write_csv(tmp_path / "batch.csv", [_optimization_bess_row()])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert result.is_valid
        assert len(result.valid_rows) == 1

    def test_valid_mixed_type_batch(self, tmp_path: Path) -> None:
        """A batch mixing all four analysis types passes validation."""
        rows = [
            _buildability_row("Site 1"),
            _production_row("Site 2"),
            _optimization_row("Site 3"),
            _optimization_bess_row("Site 4"),
        ]
        csv_file = _write_csv(tmp_path / "batch.csv", rows)
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert result.is_valid
        assert len(result.valid_rows) == 4
        assert result.row_count == 4

    def test_25_rows_at_limit(self, tmp_path: Path) -> None:
        """Exactly 25 rows should be accepted."""
        rows = [_buildability_row(f"Site {i}") for i in range(25)]
        csv_file = _write_csv(tmp_path / "batch.csv", rows)
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert result.is_valid
        assert len(result.valid_rows) == 25


# ── Missing required fields ───────────────────────────────────────────


class TestMissingRequiredFields:
    """Tests for missing required fields (universal and per-type)."""

    @pytest.mark.parametrize("field", ["name", "latitude", "longitude", "analysis_type"])
    def test_missing_universal_required_field(self, tmp_path: Path, field: str) -> None:
        """Each universally required field generates an error when missing."""
        row = _buildability_row()
        del row[field]
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == field for e in result.errors)

    def test_production_missing_gcr(self, tmp_path: Path) -> None:
        """Production row without gcr generates an error."""
        row = _production_row()
        del row["gcr"]
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        error = next(e for e in result.errors if e.column == "gcr")
        assert "required" in error.message.lower()

    def test_production_missing_racking(self, tmp_path: Path) -> None:
        """Production row without racking generates an error."""
        row = _production_row()
        del row["racking"]
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "racking" for e in result.errors)

    def test_production_missing_dc_capacity(self, tmp_path: Path) -> None:
        """Production row without dc_capacity_mw generates an error."""
        row = _production_row()
        del row["dc_capacity_mw"]
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "dc_capacity_mw" for e in result.errors)

    def test_production_missing_ac_capacity(self, tmp_path: Path) -> None:
        """Production row without ac_capacity_mw generates an error."""
        row = _production_row()
        del row["ac_capacity_mw"]
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "ac_capacity_mw" for e in result.errors)

    def test_optimization_missing_buildable_acres(self, tmp_path: Path) -> None:
        """Optimization row without buildable_acres generates an error."""
        row = _optimization_row()
        del row["buildable_acres"]
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "buildable_acres" for e in result.errors)

    def test_optimization_bess_missing_dispatch_mode(self, tmp_path: Path) -> None:
        """optimization_bess row without dispatch_mode generates an error."""
        row = _optimization_bess_row()
        del row["dispatch_mode"]
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "dispatch_mode" for e in result.errors)


# ── Invalid values ─────────────────────────────────────────────────────


class TestInvalidValues:
    """Tests for out-of-range or wrongly-typed values."""

    def test_invalid_analysis_type(self, tmp_path: Path) -> None:
        """Unknown analysis_type generates an error."""
        row = _buildability_row()
        row["analysis_type"] = "unknown_type"
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        error = next(e for e in result.errors if e.column == "analysis_type")
        assert "unknown_type" in error.message

    def test_latitude_out_of_range(self, tmp_path: Path) -> None:
        """Latitude outside -90 to 90 is rejected."""
        row = _buildability_row(lat=91.0)
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "latitude" for e in result.errors)

    def test_latitude_negative_out_of_range(self, tmp_path: Path) -> None:
        """Latitude below -90 is rejected."""
        row = _buildability_row(lat=-91.0)
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "latitude" for e in result.errors)

    def test_longitude_out_of_range(self, tmp_path: Path) -> None:
        """Longitude outside -180 to 180 is rejected."""
        row = _buildability_row(lon=181.0)
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "longitude" for e in result.errors)

    def test_invalid_gcr_too_low(self, tmp_path: Path) -> None:
        """GCR below 0.1 is rejected."""
        row = _production_row()
        row["gcr"] = 0.05
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "gcr" for e in result.errors)

    def test_invalid_gcr_too_high(self, tmp_path: Path) -> None:
        """GCR above 0.9 is rejected."""
        row = _production_row()
        row["gcr"] = 0.95
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "gcr" for e in result.errors)

    def test_invalid_racking_value(self, tmp_path: Path) -> None:
        """Invalid racking value is rejected."""
        row = _production_row()
        row["racking"] = "rooftop"
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "racking" for e in result.errors)

    def test_negative_dc_capacity(self, tmp_path: Path) -> None:
        """Negative dc_capacity_mw is rejected."""
        row = _production_row()
        row["dc_capacity_mw"] = -1.0
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "dc_capacity_mw" for e in result.errors)

    def test_zero_buildable_acres(self, tmp_path: Path) -> None:
        """Zero buildable_acres is rejected (must be > 0)."""
        row = _optimization_row()
        row["buildable_acres"] = 0.0
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "buildable_acres" for e in result.errors)

    def test_invalid_bifacial_value(self, tmp_path: Path) -> None:
        """Non-boolean bifacial value is rejected."""
        row = _production_row()
        row["bifacial"] = "maybe"
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "bifacial" for e in result.errors)

    def test_tilt_out_of_range(self, tmp_path: Path) -> None:
        """Tilt above 90 is rejected."""
        row = _production_row()
        row["tilt"] = 100
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "tilt" for e in result.errors)

    def test_pct_field_over_100(self, tmp_path: Path) -> None:
        """Percentage field above 100 is rejected."""
        row = _production_row()
        row["shading_pct"] = 150
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "shading_pct" for e in result.errors)

    def test_negative_cost_field(self, tmp_path: Path) -> None:
        """Negative cost field is rejected."""
        row = _optimization_row()
        row["solar_cost_per_kw_dc"] = -100
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "solar_cost_per_kw_dc" for e in result.errors)

    def test_project_lifetime_not_integer(self, tmp_path: Path) -> None:
        """Non-integer project_lifetime_years is rejected."""
        row = _optimization_row()
        row["project_lifetime_years"] = 25.5
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "project_lifetime_years" for e in result.errors)

    def test_project_lifetime_out_of_range(self, tmp_path: Path) -> None:
        """project_lifetime_years outside 1-50 is rejected."""
        row = _optimization_row()
        row["project_lifetime_years"] = 60
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "project_lifetime_years" for e in result.errors)

    def test_invalid_charging_mode(self, tmp_path: Path) -> None:
        """Invalid charging_mode is rejected."""
        row = _optimization_bess_row()
        row["charging_mode"] = "wind_only"
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(e.column == "charging_mode" for e in result.errors)


# ── Row limit ──────────────────────────────────────────────────────────


class TestRowLimit:
    """Tests for the 25-row batch limit."""

    def test_row_limit_exceeded(self, tmp_path: Path) -> None:
        """26 data rows exceeds the limit and produces an error."""
        rows = [_buildability_row(f"Site {i}") for i in range(26)]
        csv_file = _write_csv(tmp_path / "batch.csv", rows)
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert any(str(MAX_BATCH_ROWS) in e.message for e in result.errors)
        # No valid rows because we reject the whole batch
        assert len(result.valid_rows) == 0


# ── BTM rejection ─────────────────────────────────────────────────────


class TestBTMRejection:
    """Tests for BTM BESS dispatch mode rejection."""

    def test_btm_dispatch_mode_rejected(self, tmp_path: Path) -> None:
        """dispatch_mode='btm' generates a specific error message."""
        row = _optimization_bess_row()
        row["dispatch_mode"] = "btm"
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        btm_errors = [e for e in result.errors if e.column == "dispatch_mode"]
        assert len(btm_errors) == 1
        assert "BTM BESS is not supported in batch v1" in btm_errors[0].message


# ── Equipment validation ──────────────────────────────────────────────


class TestEquipmentValidation:
    """Tests for CEC module/inverter name validation."""

    def test_invalid_module_name_with_suggestions(self, tmp_path: Path) -> None:
        """Invalid module name produces error with fuzzy suggestions."""
        from src.pysam_integration.exceptions import ModuleNotFoundError as MNF

        db = MagicMock()
        db.get_module_params.side_effect = MNF(
            "Fake Module XYZ",
            ["Real Module ABC", "Real Module DEF"],
        )
        db.get_inverter_params.return_value = MagicMock()

        row = _production_row()
        row["module"] = "Fake Module XYZ"
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=db)

        assert not result.is_valid
        mod_errors = [e for e in result.errors if e.column == "module"]
        assert len(mod_errors) == 1
        assert "not found" in mod_errors[0].message
        assert "Real Module ABC" in mod_errors[0].message

    def test_invalid_inverter_name_with_suggestions(self, tmp_path: Path) -> None:
        """Invalid inverter name produces error with fuzzy suggestions."""
        from src.pysam_integration.exceptions import InverterNotFoundError as INF

        db = MagicMock()
        db.get_module_params.return_value = MagicMock()
        db.get_inverter_params.side_effect = INF(
            "Fake Inverter 999",
            ["Sungrow SG250HX", "SMA STP-50"],
        )

        row = _production_row()
        row["inverter"] = "Fake Inverter 999"
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=db)

        assert not result.is_valid
        inv_errors = [e for e in result.errors if e.column == "inverter"]
        assert len(inv_errors) == 1
        assert "not found" in inv_errors[0].message
        assert "Sungrow SG250HX" in inv_errors[0].message

    def test_valid_equipment_no_errors(self, tmp_path: Path) -> None:
        """Valid equipment names (mock accepts) produce no equipment errors."""
        row = _production_row()
        row["module"] = "Some Valid Module"
        row["inverter"] = "Some Valid Inverter"
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert result.is_valid
        assert not any(e.column in ("module", "inverter") for e in result.errors)


# ── Description row skipping ──────────────────────────────────────────


class TestDescriptionRowSkipping:
    """Tests for skipping the description row from the template."""

    def test_xlsx_with_description_row(self, tmp_path: Path) -> None:
        """Excel file with a description row in row 2 is handled correctly."""
        data_rows = [_buildability_row("Phoenix"), _production_row("Denver")]
        xlsx_file = _write_xlsx(
            tmp_path / "batch.xlsx", data_rows, include_desc_row=True
        )
        result = validate_batch_input(xlsx_file, cec_db=_mock_cec_db())

        # Should have 2 valid data rows, not 3 (description row skipped)
        assert result.row_count == 2
        assert len(result.valid_rows) == 2
        assert result.is_valid

    def test_csv_without_description_row(self, tmp_path: Path) -> None:
        """CSV without a description row works normally."""
        csv_file = _write_csv(tmp_path / "batch.csv", [_buildability_row()])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert result.row_count == 1
        assert result.is_valid


# ── Warning generation ─────────────────────────────────────────────────


class TestWarnings:
    """Tests for non-fatal warning generation."""

    def test_high_gcr_tracker_warning(self, tmp_path: Path) -> None:
        """GCR > 0.65 for tracker produces a warning (not error)."""
        row = _production_row()
        row["gcr"] = 0.70
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        # Valid but with warnings
        assert result.is_valid
        gcr_warnings = [w for w in result.warnings if w.column == "gcr"]
        assert len(gcr_warnings) == 1
        assert "unusually high" in gcr_warnings[0].message

    def test_high_gcr_fixed_no_warning(self, tmp_path: Path) -> None:
        """GCR 0.70 for fixed racking is NOT unusual (threshold is 0.75)."""
        row = _production_row()
        row["gcr"] = 0.70
        row["racking"] = "fixed"
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert result.is_valid
        gcr_warnings = [w for w in result.warnings if w.column == "gcr"]
        assert len(gcr_warnings) == 0

    def test_high_gcr_fixed_warning(self, tmp_path: Path) -> None:
        """GCR > 0.75 for fixed racking produces a warning."""
        row = _production_row()
        row["gcr"] = 0.80
        row["racking"] = "fixed"
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert result.is_valid
        gcr_warnings = [w for w in result.warnings if w.column == "gcr"]
        assert len(gcr_warnings) == 1

    def test_unusual_dcac_ratio_low(self, tmp_path: Path) -> None:
        """DC/AC ratio below 1.0 produces a warning."""
        row = _production_row()
        row["dc_capacity_mw"] = 3.0
        row["ac_capacity_mw"] = 4.0  # ratio = 0.75
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert result.is_valid
        ratio_warnings = [w for w in result.warnings if "DC/AC ratio" in w.message]
        assert len(ratio_warnings) == 1

    def test_unusual_dcac_ratio_high(self, tmp_path: Path) -> None:
        """DC/AC ratio above 1.8 produces a warning."""
        row = _production_row()
        row["dc_capacity_mw"] = 10.0
        row["ac_capacity_mw"] = 4.0  # ratio = 2.5
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert result.is_valid
        ratio_warnings = [w for w in result.warnings if "DC/AC ratio" in w.message]
        assert len(ratio_warnings) == 1

    def test_very_large_dc_capacity_warning(self, tmp_path: Path) -> None:
        """DC capacity > 100 MW produces a warning."""
        row = _production_row()
        row["dc_capacity_mw"] = 150.0
        row["ac_capacity_mw"] = 120.0
        csv_file = _write_csv(tmp_path / "batch.csv", [row])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert result.is_valid
        dc_warnings = [w for w in result.warnings if w.column == "dc_capacity_mw"]
        assert len(dc_warnings) == 1
        assert "very large" in dc_warnings[0].message.lower()


# ── Edge cases ─────────────────────────────────────────────────────────


class TestEdgeCases:
    """Tests for edge cases and file format handling."""

    def test_empty_csv_file(self, tmp_path: Path) -> None:
        """CSV with only headers (no data rows) produces an error."""
        csv_file = tmp_path / "empty.csv"
        pd.DataFrame(columns=TEMPLATE_COLUMNS).to_csv(csv_file, index=False)
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert result.row_count == 0
        assert any("no data rows" in e.message.lower() for e in result.errors)

    def test_empty_xlsx_file(self, tmp_path: Path) -> None:
        """Excel with only headers (no data rows) produces an error."""
        xlsx_file = tmp_path / "empty.xlsx"
        with pd.ExcelWriter(xlsx_file, engine="openpyxl") as writer:
            pd.DataFrame(columns=TEMPLATE_COLUMNS).to_excel(
                writer, sheet_name="Template", index=False
            )
        result = validate_batch_input(xlsx_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        assert result.row_count == 0

    def test_xlsx_format_accepted(self, tmp_path: Path) -> None:
        """Excel (.xlsx) format is read and validated correctly."""
        xlsx_file = _write_xlsx(tmp_path / "batch.xlsx", [_buildability_row()])
        result = validate_batch_input(xlsx_file, cec_db=_mock_cec_db())

        assert result.is_valid
        assert len(result.valid_rows) == 1

    def test_csv_format_accepted(self, tmp_path: Path) -> None:
        """CSV (.csv) format is read and validated correctly."""
        csv_file = _write_csv(tmp_path / "batch.csv", [_buildability_row()])
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert result.is_valid
        assert len(result.valid_rows) == 1

    def test_unsupported_extension_raises(self, tmp_path: Path) -> None:
        """Non-CSV/Excel file extension raises ValueError."""
        txt_file = tmp_path / "batch.txt"
        txt_file.write_text("name,latitude\n")
        with pytest.raises(ValueError, match="Unsupported file extension"):
            validate_batch_input(txt_file, cec_db=_mock_cec_db())

    def test_collects_all_errors_across_rows(self, tmp_path: Path) -> None:
        """Errors from multiple rows are all collected (not fail-fast)."""
        rows = [
            {"name": "A", "latitude": 999, "longitude": -112, "analysis_type": "buildability"},
            {"name": "B", "latitude": 33, "longitude": 999, "analysis_type": "buildability"},
        ]
        csv_file = _write_csv(tmp_path / "batch.csv", rows)
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert not result.is_valid
        # Each row contributes at least one error
        row_2_errors = [e for e in result.errors if e.row == 2]
        row_3_errors = [e for e in result.errors if e.row == 3]
        assert len(row_2_errors) >= 1
        assert len(row_3_errors) >= 1

    def test_duplicate_names_warning(self, tmp_path: Path) -> None:
        """Duplicate site names produce warnings (not errors)."""
        rows = [
            _buildability_row("Same Name"),
            _buildability_row("Same Name"),
        ]
        csv_file = _write_csv(tmp_path / "batch.csv", rows)
        result = validate_batch_input(csv_file, cec_db=_mock_cec_db())

        assert result.is_valid
        dupe_warnings = [w for w in result.warnings if "Duplicate" in w.message]
        assert len(dupe_warnings) >= 1

    def test_valid_bifacial_values(self, tmp_path: Path) -> None:
        """Bool-like bifacial values (true, false, 1, 0, yes, no) are accepted."""
        for val in [True, False, 1, 0, "true", "false", "yes", "no"]:
            row = _production_row(f"Site_{val}")
            row["bifacial"] = val
            csv_file = _write_csv(tmp_path / f"batch_{val}.csv", [row])
            result = validate_batch_input(csv_file, cec_db=_mock_cec_db())
            assert not any(
                e.column == "bifacial" for e in result.errors
            ), f"bifacial={val!r} should be valid"

    def test_xlsx_without_template_sheet(self, tmp_path: Path) -> None:
        """Excel file without 'Template' sheet falls back to first sheet."""
        xlsx_file = tmp_path / "batch.xlsx"
        with pd.ExcelWriter(xlsx_file, engine="openpyxl") as writer:
            pd.DataFrame([_buildability_row()]).to_excel(
                writer, sheet_name="Data", index=False
            )
        result = validate_batch_input(xlsx_file, cec_db=_mock_cec_db())

        assert result.is_valid
        assert len(result.valid_rows) == 1
