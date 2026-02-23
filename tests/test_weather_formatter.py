"""Tests for the weather data formatter."""

import json
from pathlib import Path

import pandas as pd
import pytest

from src.climate.weather_formatter import REQUIRED_COLUMNS, WeatherFormatter
from src.utils.exceptions import ClimateDataError

# Realistic NSRDB CSV with 2-row metadata header
SAMPLE_NSRDB_CSV = """Source,Location ID,City,State,Country,Latitude,Longitude,Time Zone,Elevation
NSRDB,123456,Phoenix,AZ,United States,33.45,-111.98,-7,337
Year,Month,Day,Hour,Minute,GHI,DNI,DHI,Temperature,Wind Speed,Surface Albedo
2024,1,1,0,0,0,0,0,8.5,2.1,0.18
2024,1,1,1,0,0,0,0,7.9,1.8,0.18
2024,1,1,2,0,0,0,0,7.2,1.5,0.18
2024,1,1,7,0,120,350,45,12.3,3.2,0.19
2024,1,1,12,0,650,800,120,18.7,4.1,0.20
"""

# CSV missing required columns
INCOMPLETE_CSV = """Source,Location ID,City
NSRDB,123456,Phoenix
Year,Month,Day,Hour,Minute,GHI,DNI
2024,1,1,0,0,0,0
"""


class TestFormatForPysam:
    """Tests for NSRDB CSV parsing and PySAM formatting."""

    def test_skips_metadata_rows(self) -> None:
        """Parser skips the 2-row NSRDB metadata header correctly."""
        formatter = WeatherFormatter()
        df = formatter.format_for_pysam(SAMPLE_NSRDB_CSV, lat=33.45, lon=-111.98)

        # Should have 5 data rows (not metadata rows)
        assert len(df) == 5

        # First row should be actual data, not metadata
        assert df.iloc[0]["Year"] == 2024
        assert df.iloc[0]["Month"] == 1

    def test_required_columns_present(self) -> None:
        """Output DataFrame contains all required PySAM columns."""
        formatter = WeatherFormatter()
        df = formatter.format_for_pysam(SAMPLE_NSRDB_CSV, lat=33.45, lon=-111.98)

        for col in REQUIRED_COLUMNS:
            assert col in df.columns, f"Missing required column: {col}"

    def test_precipitation_column_added(self) -> None:
        """Precipitation column is added as all zeros."""
        formatter = WeatherFormatter()
        df = formatter.format_for_pysam(SAMPLE_NSRDB_CSV, lat=33.45, lon=-111.98)

        assert "Precipitation" in df.columns
        assert (df["Precipitation"] == 0).all()

    def test_data_values_preserved(self) -> None:
        """Original NSRDB data values are preserved in the output."""
        formatter = WeatherFormatter()
        df = formatter.format_for_pysam(SAMPLE_NSRDB_CSV, lat=33.45, lon=-111.98)

        # Check a midday row with non-zero irradiance
        noon_row = df[df["Hour"] == 12].iloc[0]
        assert noon_row["GHI"] == 650
        assert noon_row["DNI"] == 800
        assert noon_row["DHI"] == 120
        assert noon_row["Temperature"] == pytest.approx(18.7)
        assert noon_row["Wind Speed"] == pytest.approx(4.1)

    def test_missing_columns_raises_error(self) -> None:
        """Raises ClimateDataError when required columns are absent."""
        formatter = WeatherFormatter()

        with pytest.raises(ClimateDataError) as exc_info:
            formatter.format_for_pysam(INCOMPLETE_CSV, lat=33.45, lon=-111.98)

        # Should report which columns are missing
        assert "missing_columns" in exc_info.value.context
        missing = exc_info.value.context["missing_columns"]
        assert "DHI" in missing
        assert "Temperature" in missing

    def test_dataframe_column_order(self) -> None:
        """Output columns follow expected PySAM order with Precipitation last."""
        formatter = WeatherFormatter()
        df = formatter.format_for_pysam(SAMPLE_NSRDB_CSV, lat=33.45, lon=-111.98)

        columns = list(df.columns)
        # Precipitation should be last (added column)
        assert columns[-1] == "Precipitation"
        # First 5 should be time columns
        assert columns[:5] == ["Year", "Month", "Day", "Hour", "Minute"]


class TestSaveToCsv:
    """Tests for saving PySAM-formatted data to CSV files."""

    def test_save_creates_file(self, tmp_path: Path) -> None:
        """Saves a CSV file at the specified path."""
        formatter = WeatherFormatter()
        df = formatter.format_for_pysam(SAMPLE_NSRDB_CSV, lat=33.45, lon=-111.98)

        filepath = tmp_path / "weather.csv"
        formatter.save_to_csv(df, filepath, lat=33.45, lon=-111.98)

        assert filepath.exists()

    def test_save_includes_header(self, tmp_path: Path) -> None:
        """Output CSV includes PySAM metadata header rows."""
        formatter = WeatherFormatter()
        df = formatter.format_for_pysam(SAMPLE_NSRDB_CSV, lat=33.45, lon=-111.98)

        filepath = tmp_path / "weather.csv"
        formatter.save_to_csv(df, filepath, lat=33.45, lon=-111.98)

        lines = filepath.read_text().splitlines()
        # First line is metadata header
        assert "Latitude" in lines[0]
        assert "Longitude" in lines[0]
        # Second line has the coordinate values
        assert "33.45" in lines[1]
        assert "-111.98" in lines[1]
        # Third line is column names
        assert "Year" in lines[2]

    def test_save_creates_parent_dirs(self, tmp_path: Path) -> None:
        """Creates parent directories if they don't exist."""
        formatter = WeatherFormatter()
        df = formatter.format_for_pysam(SAMPLE_NSRDB_CSV, lat=33.45, lon=-111.98)

        filepath = tmp_path / "nested" / "dir" / "weather.csv"
        formatter.save_to_csv(df, filepath, lat=33.45, lon=-111.98)

        assert filepath.exists()

    def test_save_roundtrip(self, tmp_path: Path, test_results_dir: Path) -> None:
        """Saved CSV can be read back and matches original DataFrame."""
        formatter = WeatherFormatter()
        df_original = formatter.format_for_pysam(
            SAMPLE_NSRDB_CSV, lat=33.45, lon=-111.98
        )

        filepath = tmp_path / "roundtrip.csv"
        formatter.save_to_csv(df_original, filepath, lat=33.45, lon=-111.98)

        # Read back, skipping the 2-row PySAM header
        df_loaded = pd.read_csv(filepath, skiprows=2)

        assert list(df_loaded.columns) == list(df_original.columns)
        assert len(df_loaded) == len(df_original)

        # Save test output for manual inspection
        output_path = test_results_dir / "test_weather_formatter_roundtrip.csv"
        filepath_content = filepath.read_text()
        output_path.write_text(filepath_content)

        # Also save the DataFrame as JSON for inspection
        json_path = test_results_dir / "test_weather_formatter_pysam_df.json"
        json_path.write_text(
            json.dumps(
                {
                    "columns": list(df_original.columns),
                    "row_count": len(df_original),
                    "sample_row": df_original.iloc[0].to_dict(),
                },
                indent=2,
                default=str,
            )
        )
