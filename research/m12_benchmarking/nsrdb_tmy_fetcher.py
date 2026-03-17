"""Standalone NSRDB TMY fetcher for M12 benchmarking research.

Fetches TMY (Typical Meteorological Year) data from the NSRDB GOES TMY v4.0.0
endpoint. This is a SEPARATE endpoint from the single-year GOES Aggregated v4
used in src/climate/nsrdb_client.py.

This is a research utility — it does NOT modify or integrate with the
production pipeline.

Usage:
    python research/m12_benchmarking/nsrdb_tmy_fetcher.py
"""

import os
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

NSRDB_TMY_URL = (
    "https://developer.nrel.gov/api/nsrdb/v2/solar/"
    "nsrdb-GOES-tmy-v4-0-0-download.csv"
)

# Same credential defaults as src/climate/nsrdb_client.py
DEFAULT_API_KEY = "y1zAp5Hghami0SWXdi0xhc6kcvfWZhpliZoApVzB"
DEFAULT_EMAIL = "rotter.mich@gmail.com"

# Same attributes as WEATHER_ATTRIBUTES in src/climate/nsrdb_client.py
WEATHER_ATTRIBUTES = [
    "ghi",
    "dni",
    "dhi",
    "air_temperature",
    "wind_speed",
    "surface_albedo",
]


def fetch_nsrdb_tmy(lat: float, lon: float, output_dir: Path) -> Path:
    """Fetch TMY data from NSRDB GOES TMY v4.0.0 and save to CSV.

    Args:
        lat: Latitude of the site.
        lon: Longitude of the site.
        output_dir: Directory to save the output CSV.

    Returns:
        Path to the saved CSV file.

    Raises:
        RuntimeError: On HTTP errors or unexpected responses.
    """
    api_key = os.environ.get("NSRDB_API_KEY", DEFAULT_API_KEY)
    email = os.environ.get("NSRDB_API_EMAIL", DEFAULT_EMAIL)

    # NSRDB uses WKT format: POINT(lon lat) — longitude first
    wkt = f"POINT({lon} {lat})"

    params = {
        "api_key": api_key,
        "email": email,
        "wkt": wkt,
        "names": "tmy",
        "attributes": ",".join(WEATHER_ATTRIBUTES),
        "interval": "60",
        "leap_day": "true",
        "utc": "false",
    }

    print(f"Fetching NSRDB TMY data for ({lat}, {lon})...")
    response = requests.get(NSRDB_TMY_URL, params=params, timeout=120)

    if response.status_code != 200:
        raise RuntimeError(
            f"NSRDB TMY request failed with HTTP {response.status_code}: "
            f"{response.text[:500]}"
        )

    print(f"Response: {response.status_code}, {len(response.text)} chars")

    # Save raw CSV
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{lat}_{lon}_tmy.csv"
    output_path.write_text(response.text)
    print(f"Saved to: {output_path}")

    return output_path


def print_summary(csv_path: Path) -> None:
    """Print summary statistics of the fetched TMY CSV."""
    # NSRDB CSVs have a 2-row metadata header before column names
    df = pd.read_csv(csv_path, skiprows=2)

    print(f"\n{'='*60}")
    print(f"TMY Summary: {csv_path.name}")
    print(f"{'='*60}")
    print(f"Rows:          {len(df)}")
    print(f"Columns:       {list(df.columns)}")

    if "Year" in df.columns:
        years = df["Year"].unique()
        print(f"Years present: {sorted(years)} ({len(years)} unique)")
        print(f"Date range:    {df['Year'].min()}-{df['Month'].iloc[0]:02d}-{df['Day'].iloc[0]:02d}"
              f" to {df['Year'].iloc[-1]}-{df['Month'].iloc[-1]:02d}-{df['Day'].iloc[-1]:02d}")

    if "GHI" in df.columns:
        print(f"GHI mean:      {df['GHI'].mean():.1f} W/m2")
        print(f"GHI max:       {df['GHI'].max():.1f} W/m2")

    if "DNI" in df.columns:
        print(f"DNI mean:      {df['DNI'].mean():.1f} W/m2")
        print(f"DNI max:       {df['DNI'].max():.1f} W/m2")

    if "DHI" in df.columns:
        print(f"DHI mean:      {df['DHI'].mean():.1f} W/m2")

    if "Temperature" in df.columns:
        print(f"Temp mean:     {df['Temperature'].mean():.1f} C")
        print(f"Temp range:    {df['Temperature'].min():.1f} to {df['Temperature'].max():.1f} C")

    if "Wind Speed" in df.columns:
        print(f"Wind mean:     {df['Wind Speed'].mean():.1f} m/s")

    if "Surface Albedo" in df.columns:
        print(f"Albedo mean:   {df['Surface Albedo'].mean():.3f}")

    # Check for expected 8760 hourly rows
    expected_rows = 8760
    if len(df) == expected_rows:
        print(f"\nRow count matches expected 8760 (365 days x 24 hours)")
    else:
        print(f"\nWARNING: Expected {expected_rows} rows, got {len(df)}")

    # Print metadata header
    header_df = pd.read_csv(csv_path, nrows=1)
    print(f"\nMetadata header columns: {list(header_df.columns)}")
    print(f"Metadata values:         {list(header_df.iloc[0])}")


if __name__ == "__main__":
    output = Path("research/m12_benchmarking/output")
    csv_path = fetch_nsrdb_tmy(lat=33.45, lon=-111.95, output_dir=output)
    print_summary(csv_path)
