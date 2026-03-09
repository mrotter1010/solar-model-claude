"""Tests for Solcast TMY SAM CSV file parser and validator."""

import json
from pathlib import Path

import pytest

from src.climate.solcast_parser import parse_solcast_file
from src.utils.exceptions import ClimateDataError

FIXTURES_DIR = Path(__file__).parent / "fixtures"
FULL_FIXTURE = FIXTURES_DIR / "synthetic_solcast_tmy_phoenix.csv"
MINIMAL_FIXTURE = FIXTURES_DIR / "synthetic_solcast_tmy_phoenix_minimal.csv"

# Expected coordinates from the synthetic fixtures
PHOENIX_LAT = 33.483
PHOENIX_LON = -112.073


# --- Valid File Parsing ---


def test_parse_full_solcast_file(test_results_dir: Path) -> None:
    """Full Solcast fixture should parse with all metadata and optional columns."""
    # Act
    result = parse_solcast_file(FULL_FIXTURE, PHOENIX_LAT, PHOENIX_LON)

    # Assert — return structure
    assert result["weather_file"] == FULL_FIXTURE
    assert result["has_snow_depth"] is True
    assert result["has_albedo"] is True

    # Assert — metadata
    meta = result["metadata"]
    assert meta["latitude"] == pytest.approx(33.483)
    assert meta["longitude"] == pytest.approx(-112.073)
    assert meta["time_zone"] == pytest.approx(-7)
    assert meta["elevation"] == pytest.approx(343)
    assert meta["source"] == "Solcast"

    # Write result to test_results for review
    output = {
        "weather_file": str(result["weather_file"]),
        "metadata": result["metadata"],
        "has_snow_depth": result["has_snow_depth"],
        "has_albedo": result["has_albedo"],
    }
    output_path = test_results_dir / "test_solcast_parser_full.json"
    output_path.write_text(json.dumps(output, indent=2))


def test_parse_minimal_solcast_file(test_results_dir: Path) -> None:
    """Minimal Solcast fixture should parse without optional columns."""
    # Act
    result = parse_solcast_file(MINIMAL_FIXTURE, PHOENIX_LAT, PHOENIX_LON)

    # Assert — optional columns absent
    assert result["weather_file"] == MINIMAL_FIXTURE
    assert result["has_snow_depth"] is False
    assert result["has_albedo"] is False

    # Assert — metadata still correct
    meta = result["metadata"]
    assert meta["latitude"] == pytest.approx(33.483)
    assert meta["source"] == "Solcast"

    # Write result to test_results for review
    output = {
        "weather_file": str(result["weather_file"]),
        "metadata": result["metadata"],
        "has_snow_depth": result["has_snow_depth"],
        "has_albedo": result["has_albedo"],
    }
    output_path = test_results_dir / "test_solcast_parser_minimal.json"
    output_path.write_text(json.dumps(output, indent=2))


# --- Lat/Lon Validation ---


def test_latlon_within_tolerance() -> None:
    """File should parse when expected coords are within tolerance."""
    result = parse_solcast_file(FULL_FIXTURE, 33.48, -112.07)
    assert result["metadata"]["latitude"] == pytest.approx(33.483)


def test_latlon_mismatch_raises_error() -> None:
    """Expected coords far from file coords should raise ClimateDataError."""
    with pytest.raises(ClimateDataError, match="from expected site location") as exc_info:
        parse_solcast_file(FULL_FIXTURE, 40.0, -105.0)

    assert "distance" in exc_info.value.context


def test_latlon_mismatch_error_message_format() -> None:
    """Error message should include file coords, expected coords, and tolerance."""
    with pytest.raises(ClimateDataError) as exc_info:
        parse_solcast_file(FULL_FIXTURE, 40.0, -105.0, lat_lon_tolerance=0.5)

    msg = str(exc_info.value)
    assert "33.483" in msg
    assert "-112.073" in msg
    assert "40.0" in msg
    assert "0.5" in msg


def test_custom_tolerance() -> None:
    """Custom tolerance should allow larger coordinate differences."""
    # Default tolerance (0.1) would reject this, but 1.0 should accept
    result = parse_solcast_file(FULL_FIXTURE, 33.5, -112.0, lat_lon_tolerance=1.0)
    assert result["metadata"]["latitude"] == pytest.approx(33.483)


# --- Missing Required Columns ---


def test_missing_required_column_raises_error(tmp_path: Path) -> None:
    """File missing a required column (DNI) should raise ClimateDataError."""
    csv_path = tmp_path / "bad_columns.csv"
    csv_path.write_text(
        "Latitude,Longitude,Time Zone,Elevation\n"
        "33.483,-112.073,-7,343\n"
        "Year,Month,Day,Hour,Minute,GHI,DHI,Tdry,Wspd\n"
        + "2024,1,1,0,30,0,0,10,2.0\n" * 8760
    )

    with pytest.raises(ClimateDataError, match="missing required columns") as exc_info:
        parse_solcast_file(csv_path, PHOENIX_LAT, PHOENIX_LON)

    assert "DNI" in exc_info.value.context["missing_columns"]


def test_missing_multiple_columns_lists_all(tmp_path: Path) -> None:
    """All missing required columns should be listed in the error."""
    csv_path = tmp_path / "very_bad_columns.csv"
    csv_path.write_text(
        "Latitude,Longitude,Time Zone,Elevation\n"
        "33.483,-112.073,-7,343\n"
        "Year,Month,Day,Hour,Minute,GHI\n"
        + "2024,1,1,0,30,100\n" * 8760
    )

    with pytest.raises(ClimateDataError) as exc_info:
        parse_solcast_file(csv_path, PHOENIX_LAT, PHOENIX_LON)

    missing = exc_info.value.context["missing_columns"]
    assert "DNI" in missing
    assert "DHI" in missing
    assert "Temperature" in missing
    assert "Wind Speed" in missing


# --- Wrong Row Count ---


def test_wrong_row_count_raises_error(tmp_path: Path) -> None:
    """File with wrong number of data rows should raise ClimateDataError."""
    csv_path = tmp_path / "bad_rows.csv"
    csv_path.write_text(
        "Latitude,Longitude,Time Zone,Elevation\n"
        "33.483,-112.073,-7,343\n"
        "Year,Month,Day,Hour,Minute,GHI,DNI,DHI,Tdry,Wspd\n"
        + "2024,1,1,0,30,0,0,0,10,2.0\n" * 100
    )

    with pytest.raises(ClimateDataError, match="Expected 8760") as exc_info:
        parse_solcast_file(csv_path, PHOENIX_LAT, PHOENIX_LON)

    assert exc_info.value.context["data_row_count"] == 100


# --- Missing/Invalid Header Metadata ---


def test_missing_header_labels_raises_error(tmp_path: Path) -> None:
    """File with missing header metadata labels should raise ClimateDataError."""
    csv_path = tmp_path / "bad_header.csv"
    csv_path.write_text(
        "Latitude,Longitude\n"
        "33.483,-112.073\n"
        "Year,Month,Day,Hour,Minute,GHI,DNI,DHI,Tdry,Wspd\n"
        + "2024,1,1,0,30,0,0,0,10,2.0\n" * 8760
    )

    with pytest.raises(ClimateDataError, match="missing required labels"):
        parse_solcast_file(csv_path, PHOENIX_LAT, PHOENIX_LON)


def test_invalid_header_values_raises_error(tmp_path: Path) -> None:
    """File with non-numeric header values should raise ClimateDataError."""
    csv_path = tmp_path / "bad_values.csv"
    csv_path.write_text(
        "Latitude,Longitude,Time Zone,Elevation\n"
        "not_a_number,-112.073,-7,343\n"
        "Year,Month,Day,Hour,Minute,GHI,DNI,DHI,Tdry,Wspd\n"
        + "2024,1,1,0,30,0,0,0,10,2.0\n" * 8760
    )

    with pytest.raises(ClimateDataError, match="invalid numeric values"):
        parse_solcast_file(csv_path, PHOENIX_LAT, PHOENIX_LON)


def test_empty_header_raises_error(tmp_path: Path) -> None:
    """File with empty first row should raise ClimateDataError."""
    csv_path = tmp_path / "empty_header.csv"
    csv_path.write_text(
        "\n"
        "\n"
        "Year,Month,Day,Hour,Minute,GHI,DNI,DHI,Tdry,Wspd\n"
        + "2024,1,1,0,30,0,0,0,10,2.0\n" * 8760
    )

    with pytest.raises(ClimateDataError, match="header is empty"):
        parse_solcast_file(csv_path, PHOENIX_LAT, PHOENIX_LON)


# --- File Access Errors ---


def test_nonexistent_file_raises_error() -> None:
    """Nonexistent file path should raise ClimateDataError."""
    with pytest.raises(ClimateDataError, match="not found"):
        parse_solcast_file(
            Path("/fake/nonexistent/solcast.csv"), PHOENIX_LAT, PHOENIX_LON
        )


def test_directory_path_raises_error(tmp_path: Path) -> None:
    """Directory path (not a file) should raise ClimateDataError."""
    with pytest.raises(ClimateDataError, match="not a file"):
        parse_solcast_file(tmp_path, PHOENIX_LAT, PHOENIX_LON)


# --- Source Field ---


def test_source_defaults_to_solcast_when_absent(tmp_path: Path) -> None:
    """When Source column is absent from header, default to 'Solcast'."""
    csv_path = tmp_path / "no_source.csv"
    csv_path.write_text(
        "Latitude,Longitude,Time Zone,Elevation\n"
        "33.483,-112.073,-7,343\n"
        "Year,Month,Day,Hour,Minute,GHI,DNI,DHI,Tdry,Wspd\n"
        + "2024,1,1,0,30,0,0,0,10,2.0\n" * 8760
    )

    result = parse_solcast_file(csv_path, PHOENIX_LAT, PHOENIX_LON)
    assert result["metadata"]["source"] == "Solcast"


# --- Column Alias Case Insensitivity ---


def test_column_aliases_case_insensitive(tmp_path: Path) -> None:
    """Column matching should be case-insensitive."""
    csv_path = tmp_path / "mixed_case.csv"
    csv_path.write_text(
        "Latitude,Longitude,Time Zone,Elevation\n"
        "33.483,-112.073,-7,343\n"
        "Year,Month,Day,Hour,Minute,ghi,Dni,dHI,TDRY,WSPD,SNOW DEPTH,ALB\n"
        + "2024,1,1,0,30,0,0,0,10,2.0,0,0.2\n" * 8760
    )

    result = parse_solcast_file(csv_path, PHOENIX_LAT, PHOENIX_LON)
    assert result["has_snow_depth"] is True
    assert result["has_albedo"] is True
