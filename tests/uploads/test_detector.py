"""Tests for src.uploads.detector — file type auto-detection."""

import json

import pytest

from src.uploads.detector import DetectionResult, detect_file_type


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _csv_with_rows(n_data_rows: int) -> bytes:
    """Build a minimal CSV with a header + *n_data_rows* data rows."""
    lines = ["hour,kwh"]
    for i in range(n_data_rows):
        lines.append(f"{i},{100.0 + i}")
    return "\n".join(lines).encode("utf-8")


def _valid_rate_dict() -> dict:
    """Return a minimal dict that has all required RateSchedule keys."""
    return {
        "utility_name": "Test Utility",
        "tariff_name": "Test Tariff",
        "energyratestructure": [[{"rate": 0.10}]],
        "energyweekdayschedule": [[0] * 24 for _ in range(12)],
        "energyweekendschedule": [[0] * 24 for _ in range(12)],
    }


# ---------------------------------------------------------------------------
# KMZ detection
# ---------------------------------------------------------------------------


class TestKmzDetection:
    """KMZ files are detected by extension alone."""

    def test_kmz_lowercase(self) -> None:
        result = detect_file_type("boundary.kmz", b"fake kmz bytes")
        assert result.detected_type == "kmz"
        assert result.confidence == "high"

    def test_kmz_uppercase(self) -> None:
        result = detect_file_type("SITE.KMZ", b"fake kmz bytes")
        assert result.detected_type == "kmz"
        assert result.confidence == "high"

    def test_kmz_mixed_case(self) -> None:
        result = detect_file_type("Site.Kmz", b"\x00\x01")
        assert result.detected_type == "kmz"
        assert result.confidence == "high"


# ---------------------------------------------------------------------------
# Load profile detection (CSV with 8760 rows)
# ---------------------------------------------------------------------------


class TestLoadProfileDetection:
    """CSV files with 8,760-8,784 data rows are detected as load profiles."""

    def test_csv_8760_rows(self) -> None:
        content = _csv_with_rows(8_760)
        result = detect_file_type("load.csv", content)
        assert result.detected_type == "load_profile"
        assert result.confidence == "high"
        assert "8,760" in result.message

    def test_csv_8784_rows_leap_year(self) -> None:
        content = _csv_with_rows(8_784)
        result = detect_file_type("load_leap.csv", content)
        assert result.detected_type == "load_profile"
        assert result.confidence == "high"

    def test_csv_100_rows_unknown(self) -> None:
        """CSV with too few rows should not match load profile."""
        content = _csv_with_rows(100)
        result = detect_file_type("short.csv", content)
        assert result.detected_type == "unknown"
        assert result.confidence == "low"

    def test_csv_20000_rows_unknown(self) -> None:
        """CSV with too many rows should not match load profile."""
        content = _csv_with_rows(20_000)
        result = detect_file_type("big.csv", content)
        assert result.detected_type == "unknown"
        assert result.confidence == "low"

    def test_csv_empty_content(self) -> None:
        """Empty CSV body is unknown."""
        result = detect_file_type("empty.csv", b"")
        assert result.detected_type == "unknown"

    def test_csv_header_only(self) -> None:
        """CSV with header but no data rows is unknown."""
        result = detect_file_type("header.csv", b"hour,kwh\n")
        assert result.detected_type == "unknown"


# ---------------------------------------------------------------------------
# Rate schedule detection (JSON with required keys)
# ---------------------------------------------------------------------------


class TestRateScheduleDetection:
    """JSON files with all required RateSchedule keys are detected."""

    def test_valid_rate_json(self) -> None:
        content = json.dumps(_valid_rate_dict()).encode()
        result = detect_file_type("rate.json", content)
        assert result.detected_type == "rate_schedule"
        assert result.confidence == "high"
        assert "Test Utility" in result.message

    def test_json_missing_required_keys(self) -> None:
        """JSON missing energyratestructure should be unknown."""
        data = {"utility_name": "X", "tariff_name": "Y"}
        content = json.dumps(data).encode()
        result = detect_file_type("partial.json", content)
        assert result.detected_type == "unknown"
        assert result.confidence == "low"

    def test_invalid_json_syntax(self) -> None:
        """Malformed JSON should be unknown."""
        result = detect_file_type("bad.json", b"not json {{{")
        assert result.detected_type == "unknown"
        assert result.confidence == "low"

    def test_json_array_not_object(self) -> None:
        """JSON array (not object) should be unknown."""
        content = json.dumps([1, 2, 3]).encode()
        result = detect_file_type("array.json", content)
        assert result.detected_type == "unknown"

    def test_json_uppercase_extension(self) -> None:
        content = json.dumps(_valid_rate_dict()).encode()
        result = detect_file_type("RATE.JSON", content)
        assert result.detected_type == "rate_schedule"
        assert result.confidence == "high"


# ---------------------------------------------------------------------------
# Unknown / unsupported extensions
# ---------------------------------------------------------------------------


class TestUnknownDetection:
    """Files with unrecognized extensions fall through to unknown."""

    @pytest.mark.parametrize(
        "filename",
        ["document.pdf", "readme.docx", "image.png", "noext"],
    )
    def test_unsupported_extensions(self, filename: str) -> None:
        result = detect_file_type(filename, b"some content")
        assert result.detected_type == "unknown"
        assert result.confidence == "low"
        assert "not automatically recognized" in result.message

    @pytest.mark.parametrize("filename", ["spreadsheet.xlsx", "data.xls"])
    def test_excel_without_batch_headers_is_unknown(self, filename: str) -> None:
        result = detect_file_type(filename, b"some content")
        assert result.detected_type == "unknown"
        assert result.confidence == "low"
        assert "does not match batch input format" in result.message


# ---------------------------------------------------------------------------
# DetectionResult model
# ---------------------------------------------------------------------------


class TestDetectionResultModel:
    """DetectionResult Pydantic model serializes correctly."""

    def test_serialization(self) -> None:
        r = DetectionResult(
            detected_type="kmz", confidence="high", message="test"
        )
        d = r.model_dump()
        assert d == {
            "detected_type": "kmz",
            "confidence": "high",
            "message": "test",
        }
