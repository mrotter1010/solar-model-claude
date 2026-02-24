"""Formatter for converting NSRDB CSV data to PySAM-compatible format."""

from io import StringIO
from pathlib import Path

import pandas as pd

from src.utils.exceptions import ClimateDataError
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Columns required from NSRDB data (after header skip)
REQUIRED_COLUMNS = [
    "Year",
    "Month",
    "Day",
    "Hour",
    "Minute",
    "GHI",
    "DNI",
    "DHI",
    "Temperature",
    "Wind Speed",
    "Surface Albedo",
]

# Mapping from NSRDB column names to PySAM-expected names
NSRDB_TO_PYSAM = {
    "Year": "Year",
    "Month": "Month",
    "Day": "Day",
    "Hour": "Hour",
    "Minute": "Minute",
    "GHI": "GHI",
    "DNI": "DNI",
    "DHI": "DHI",
    "Temperature": "Temperature",
    "Wind Speed": "Wind Speed",
    "Surface Albedo": "Surface Albedo",
    "Pressure": "Pressure",
    "Dew Point": "Dew Point",
}


class WeatherFormatter:
    """Converts NSRDB CSV data into PySAM-compatible DataFrames and files."""

    def format_for_pysam(
        self,
        nsrdb_csv: str,
        lat: float,
        lon: float,
        precipitation: pd.Series | None = None,
    ) -> pd.DataFrame:
        """Parse NSRDB CSV and format for PySAM consumption.

        NSRDB CSVs have a 2-row metadata header (source info + units) before
        the actual column names and data rows.

        Args:
            nsrdb_csv: Raw CSV string from NSRDB API.
            lat: Site latitude (for logging/error context).
            lon: Site longitude (for logging/error context).
            precipitation: Optional hourly precipitation series from NCEI.
                If provided and length matches, replaces the default zeros.

        Returns:
            DataFrame with PySAM-compatible columns including Precipitation.

        Raises:
            ClimateDataError: If required columns are missing from the data.
        """
        logger.info(f"Formatting NSRDB data for PySAM: ({lat}, {lon})")

        # Skip 2-row metadata header
        df = pd.read_csv(StringIO(nsrdb_csv), skiprows=2)

        # Validate required columns are present
        missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            logger.error(
                f"Missing required columns for ({lat}, {lon}): {missing}"
            )
            raise ClimateDataError(
                "NSRDB data missing required columns",
                context={
                    "location": (lat, lon),
                    "missing_columns": missing,
                },
            )

        # Select and order PySAM columns
        pysam_columns = [
            col for col in REQUIRED_COLUMNS if col in df.columns
        ]
        result = df[pysam_columns].copy()

        # Add Precipitation column (required by PySAM but not in NSRDB)
        if precipitation is not None and len(precipitation) == len(result):
            result["Precipitation"] = precipitation.values
        else:
            if precipitation is not None:
                logger.warning(
                    f"Precipitation length mismatch for ({lat}, {lon}): "
                    f"expected {len(result)}, got {len(precipitation)}. "
                    f"Falling back to zeros."
                )
            result["Precipitation"] = 0

        logger.info(
            f"Formatted {len(result)} rows for PySAM from ({lat}, {lon})"
        )
        return result

    def save_to_csv(
        self, df: pd.DataFrame, filepath: Path, lat: float, lon: float
    ) -> None:
        """Save formatted weather data with PySAM-compatible header.

        Args:
            df: PySAM-formatted DataFrame from format_for_pysam.
            filepath: Output file path.
            lat: Site latitude for header metadata.
            lon: Site longitude for header metadata.
        """
        filepath.parent.mkdir(parents=True, exist_ok=True)

        # PySAM expects a metadata header row before column names
        header_line = f"Latitude,Longitude,Time Zone,Elevation"
        # Use placeholder values for timezone and elevation
        values_line = f"{lat},{lon},0,0"

        with filepath.open("w") as f:
            f.write(header_line + "\n")
            f.write(values_line + "\n")
            df.to_csv(f, index=False)

        logger.info(f"Saved PySAM weather file: {filepath}")
