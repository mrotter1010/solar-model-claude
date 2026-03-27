"""Tests for the pipeline runner: CSV shim mechanics and response extraction."""

import csv
from pathlib import Path

import pytest

from src.api.runner import (
    _FIELD_TO_COLUMN,
    _LOSS_KEY_MAP,
    _format_csv_value,
    extract_production_response,
)
from src.config.loader import COLUMN_MAP
from src.config.schema import SiteConfig


def _make_site_config(**overrides: object) -> SiteConfig:
    """Build a SiteConfig with sensible defaults for testing."""
    defaults = {
        "run_name": "test_run_001",
        "site_name": "TestSite",
        "customer": "API",
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
        "bifacial": False,
        "inverter_model": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
        "gcr": 0.35,
        "shading_percent": 0.0,
        "dc_wiring_loss_percent": 2.0,
        "ac_wiring_loss_percent": 0.5,
        "transformer_losses_percent": 1.0,
        "degradation_percent": 0.5,
        "availability_percent": 2.5,
        "module_mismatch_percent": 2.0,
        "lid_percent": 1.5,
        "report": True,
    }
    defaults.update(overrides)
    return SiteConfig(**defaults)


class TestReverseColumnMap:
    """Verify reverse COLUMN_MAP generation covers all CSV-mapped fields."""

    def test_all_column_map_entries_have_reverse(self) -> None:
        """Every SiteConfig field in COLUMN_MAP produces a valid reverse entry."""
        for csv_col, field_name in COLUMN_MAP.items():
            assert field_name in _FIELD_TO_COLUMN, (
                f"Field '{field_name}' (CSV: '{csv_col}') missing from reverse map"
            )
            assert _FIELD_TO_COLUMN[field_name] == csv_col

    def test_reverse_map_size_matches_column_map(self) -> None:
        """Reverse map has the same number of entries as COLUMN_MAP."""
        assert len(_FIELD_TO_COLUMN) == len(COLUMN_MAP)


class TestFormatCsvValue:
    """Test value formatting for CSV output."""

    def test_none_becomes_empty(self) -> None:
        assert _format_csv_value(None) == ""

    def test_bool_true_becomes_TRUE(self) -> None:
        assert _format_csv_value(True) == "TRUE"

    def test_bool_false_becomes_FALSE(self) -> None:
        assert _format_csv_value(False) == "FALSE"

    def test_path_becomes_string(self) -> None:
        assert _format_csv_value(Path("/tmp/test.csv")) == "/tmp/test.csv"

    def test_float_becomes_string(self) -> None:
        assert _format_csv_value(13.5) == "13.5"

    def test_int_becomes_string(self) -> None:
        assert _format_csv_value(42) == "42"

    def test_string_passthrough(self) -> None:
        assert _format_csv_value("hello") == "hello"


class TestCsvShimMechanics:
    """Test that a SiteConfig can be serialized to a CSV row and read back."""

    def test_csv_roundtrip_headers_and_values(self, tmp_path: Path) -> None:
        """Write a SiteConfig to CSV, read it back, and verify key fields."""
        config = _make_site_config()

        # Build header/value rows the same way runner.py does
        headers: list[str] = []
        values: list[str] = []
        for field_name, csv_col in _FIELD_TO_COLUMN.items():
            raw_value = getattr(config, field_name, None)
            headers.append(csv_col)
            values.append(_format_csv_value(raw_value))

        # Write to temp CSV
        csv_path = tmp_path / "test_shim.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerow(values)

        # Read back
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            row = next(reader)

        # Verify key fields round-trip correctly
        assert row["Run Name"] == "test_run_001"
        assert row["Site Name"] == "TestSite"
        assert row["Customer"] == "API"
        assert row["Latitude"] == "33.45"
        assert row["Longitude"] == "-112.07"
        assert row["DC Size (MW)"] == "13.0"
        assert row["AC Installed (MW)"] == "10.0"
        assert row["AC POI (MW)"] == "10.0"
        assert row["Racking"] == "tracker"
        assert row["Tilt"] == "60.0"
        assert row["Azimuth"] == "180.0"
        assert row["Panel Model"] == "CSI Solar Co. Ltd. CS3U-355P"
        assert row["Bifacial"] == "FALSE"
        assert row["GCR"] == "0.35"
        assert row["Report"] == "TRUE"

    def test_all_column_map_fields_present_in_csv(self, tmp_path: Path) -> None:
        """Every COLUMN_MAP column appears in the CSV header."""
        config = _make_site_config()

        headers: list[str] = []
        values: list[str] = []
        for field_name, csv_col in _FIELD_TO_COLUMN.items():
            raw_value = getattr(config, field_name, None)
            headers.append(csv_col)
            values.append(_format_csv_value(raw_value))

        csv_path = tmp_path / "test_headers.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerow(values)

        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            row = next(reader)

        for csv_col in COLUMN_MAP:
            assert csv_col in row, f"CSV column '{csv_col}' missing from output"


class TestExtractProductionResponse:
    """Test response extraction from mock pipeline results."""

    @pytest.fixture()
    def mock_results(self) -> dict:
        """Build a mock pipeline results dict matching real pipeline output."""
        loss_data = {
            # Key scalars
            "capacity_factor": 25.4,
            "capacity_factor_ac": 30.2,
            "kwh_per_kw": 1650.0,
            "performance_ratio": 0.82,
            # Monthly energy (kWh)
            "monthly_energy": [
                1500_000, 1700_000, 2100_000, 2400_000, 2700_000, 2800_000,
                2900_000, 2750_000, 2300_000, 2000_000, 1600_000, 1400_000,
            ],
            # Loss percentages
            "annual_poa_shading_loss_percent": 1.5,
            "annual_dc_wiring_loss_percent": 2.0,
            "annual_ac_wiring_loss_percent": 0.5,
            "annual_xfmr_loss_percent": 1.0,
            "annual_dc_mismatch_loss_percent": 2.0,
            "annual_dc_nameplate_loss_percent": 1.5,
            "annual_ac_perf_adj_loss_percent": 2.5,
            "annual_ac_inv_clip_loss_percent": 0.3,
            "annual_ac_inv_eff_loss_percent": 3.2,
        }
        return {
            "total_sites": 1,
            "successful": 1,
            "failed": 0,
            "timeseries_files": [Path("/tmp/out/timeseries/test_run_001_TestSite_8760.csv")],
            "summaries": [
                {
                    "site_name": "TestSite",
                    "run_name": "test_run_001",
                    "customer": "API",
                    "weather_year": "2023",
                    "dc_size_mw": 13.0,
                    "ac_installed_mw": 10.0,
                    "ac_poi_mw": 10.0,
                    "panel_model": "CSI Solar Co. Ltd. CS3U-355P",
                    "inverter_model": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
                    "racking": "tracker",
                    "tilt": 60.0,
                    "azimuth": 180.0,
                    "annual_energy_mwh": 26150.0,
                    "net_capacity_factor": 0.2984,
                    "specific_yield": 1650.0,
                    "performance_ratio": 0.82,
                    "shading_pct_applied": 0.0,
                    "simulation_timestamp": "2024-01-01T00:00:00+00:00",
                    "loss_data": loss_data,
                    "errors": [],
                },
            ],
            "error_files": [],
            "report_files": [Path("/tmp/out/reports/test_run_001.pdf")],
        }

    def test_basic_extraction(self, mock_results: dict) -> None:
        """Extract a ProductionResponse from mock results."""
        resp = extract_production_response(
            mock_results, "test_run_001", Path("/tmp/out")
        )

        assert resp.run_id == "test_run_001"
        assert resp.status == "success"
        assert resp.site_name == "TestSite"

    def test_production_metrics(self, mock_results: dict) -> None:
        """Production summary contains correct metrics."""
        resp = extract_production_response(
            mock_results, "test_run_001", Path("/tmp/out")
        )

        assert resp.production.annual_energy_mwh == 26150.0
        assert resp.production.capacity_factor_dc == 25.4
        assert resp.production.capacity_factor_ac == 30.2
        assert resp.production.specific_yield_kwh_per_kwp == 1650.0
        assert resp.production.performance_ratio == 0.82

    def test_loss_breakdown(self, mock_results: dict) -> None:
        """Loss summary maps PySAM keys correctly."""
        resp = extract_production_response(
            mock_results, "test_run_001", Path("/tmp/out")
        )

        assert resp.losses.shading_pct == 1.5
        assert resp.losses.dc_wiring_pct == 2.0
        assert resp.losses.ac_wiring_pct == 0.5
        assert resp.losses.transformer_pct == 1.0
        assert resp.losses.mismatch_pct == 2.0
        assert resp.losses.lid_pct == 1.5
        assert resp.losses.availability_pct == 2.5
        assert resp.losses.subhourly_clipping_pct == 0.3
        assert resp.losses.inverter_efficiency_pct == 3.2

    def test_monthly_production_kwh_to_mwh(self, mock_results: dict) -> None:
        """Monthly energy is converted from kWh to MWh."""
        resp = extract_production_response(
            mock_results, "test_run_001", Path("/tmp/out")
        )

        assert len(resp.monthly_production_mwh) == 12
        # First month: 1_500_000 kWh → 1500.0 MWh
        assert resp.monthly_production_mwh[0] == 1500.0
        # Last month: 1_400_000 kWh → 1400.0 MWh
        assert resp.monthly_production_mwh[11] == 1400.0

    def test_file_links(self, mock_results: dict) -> None:
        """File links contain the run_name."""
        resp = extract_production_response(
            mock_results, "test_run_001", Path("/tmp/out")
        )

        assert resp.files.report_url == "/analyses/test_run_001/report"
        assert resp.files.timeseries_url == "/analyses/test_run_001/timeseries"

    def test_missing_run_name_raises(self, mock_results: dict) -> None:
        """ValueError raised when run_name not found in summaries."""
        with pytest.raises(ValueError, match="not found"):
            extract_production_response(
                mock_results, "nonexistent_run", Path("/tmp/out")
            )

    def test_missing_loss_data_defaults_to_zero(self) -> None:
        """Missing loss_data keys default to 0.0."""
        results = {
            "summaries": [
                {
                    "run_name": "run_no_losses",
                    "site_name": "NoLossSite",
                    "annual_energy_mwh": 100.0,
                    "ac_installed_mw": 1.0,
                    "specific_yield": 1500.0,
                    "performance_ratio": 0.80,
                    "loss_data": {},
                },
            ],
        }
        resp = extract_production_response(results, "run_no_losses", Path("/tmp"))

        assert resp.losses.shading_pct == 0.0
        assert resp.losses.dc_wiring_pct == 0.0
        assert resp.losses.inverter_efficiency_pct == 0.0
        assert resp.monthly_production_mwh == [0.0] * 12

    def test_ac_cf_fallback_when_zero(self) -> None:
        """AC capacity factor is computed from annual energy when loss_data value is 0."""
        results = {
            "summaries": [
                {
                    "run_name": "run_cf_fallback",
                    "site_name": "FallbackSite",
                    "annual_energy_mwh": 8760.0,
                    "ac_installed_mw": 10.0,
                    "specific_yield": 1500.0,
                    "performance_ratio": 0.80,
                    "loss_data": {
                        "capacity_factor": 20.0,
                        "capacity_factor_ac": 0.0,
                    },
                },
            ],
        }
        resp = extract_production_response(results, "run_cf_fallback", Path("/tmp"))

        # 8760 MWh / (10 MW * 8760 h) * 100 = 10.0%
        assert resp.production.capacity_factor_ac == 10.0

    def test_loss_key_map_covers_all_loss_summary_fields(self) -> None:
        """Every LossSummary field has a mapping in _LOSS_KEY_MAP."""
        from src.api.schemas.common import LossSummary

        loss_summary_fields = set(LossSummary.model_fields.keys())
        mapped_fields = set(_LOSS_KEY_MAP.values())
        assert loss_summary_fields == mapped_fields, (
            f"Unmapped: {loss_summary_fields - mapped_fields}, "
            f"Extra: {mapped_fields - loss_summary_fields}"
        )
