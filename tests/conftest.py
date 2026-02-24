"""Shared pytest fixtures for solar model tests."""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from src.config.schema import SiteConfig


@pytest.fixture()
def temp_output_dir(tmp_path: Path) -> Path:
    """Create temporary output directory that cleans up after tests.

    Args:
        tmp_path: pytest built-in temporary directory fixture.

    Returns:
        Path to temporary output directory.
    """
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()
    (output_dir / "logs").mkdir()
    (output_dir / "test_results").mkdir()
    return output_dir


@pytest.fixture()
def test_results_dir() -> Path:
    """Create test results directory for manual inspection of test outputs.

    This directory persists between test runs to allow review of intermediate
    results from data processing tests.

    Returns:
        Path to outputs/test_results/ directory.
    """
    results_dir = Path("outputs/test_results")
    results_dir.mkdir(parents=True, exist_ok=True)
    return results_dir


@pytest.fixture()
def sample_valid_csv(tmp_path: Path) -> Path:
    """Create a temporary CSV with 3 valid site configurations.

    Two sites share the same lat/lon to test location deduplication.

    Returns:
        Path to temporary CSV file.
    """
    csv_path = tmp_path / "valid_config.csv"

    data = {
        "Run Name": ["Test1", "Test2", "Test3"],
        "Site Name": ["Phoenix_Tracker", "Phoenix_Fixed", "Tucson_Tracker"],
        "Customer": ["TestCo", "TestCo", "TestCo"],
        "Latitude": [33.483, 33.483, 32.253],
        "Longitude": [-112.073, -112.073, -110.911],
        "BESS Dispatch Required": [None, None, None],
        "BESS Optimization Required": [None, None, None],
        "DC Size (MW)": [13, 13, 20],
        "AC Installed (MW)": [10, 10, 16],
        "AC POI (MW) ": [10, 10, 16],  # Note: trailing space in column name
        "Racking": ["tracker", "FIXED", "Tracker"],  # Test case insensitivity
        "Tilt": [60, 25, 60],
        "Azimuth": [180, 180, 180],
        "Module Orientation": ["portrait", "PORTRAIT", "Landscape"],
        "Number of Modules": [2, 2, 1],
        "Ground Clearance Height (m)": [1.8, 1.0, 1.5],
        "Panel Model": [
            "SunPower SPR-310-WHT-U",
            "SunPower SPR-310-WHT-U",
            "Canadian Solar CS3W-410P",
        ],
        "Bifacial": [True, True, False],
        "Inverter Model": ["iPower SHO 5-2", "iPower SHO 5-2", "SMA SC-2750"],
        "GCR": [0.34, 0.55, 0.40],
        "Shading (%)": [1, 1, 2],
        "DC Wiring Loss (%) ": [1.5, 1.5, 1.5],  # Note: space before %
        "AC Wiring Loss (%) ": [1.5, 1.5, 1.5],
        "Transformer Losses (%)": [0, 0, 1],
        "Degradation (%)": [0.3, 0.3, 0.5],
        "Availability (%)": [98, 98, 97],
        "Module Mismatch (%)": [1.5, 1.5, 2.0],
        "LID(%)": [1, 1, 1.5],
    }

    df = pd.DataFrame(data)
    df.to_csv(csv_path, index=False)

    return csv_path


@pytest.fixture()
def sample_invalid_csv(tmp_path: Path) -> Path:
    """Create a temporary CSV with validation errors.

    Errors include:
    - Row 2: Empty site name (still valid for str)
    - Row 3: Out of range latitude (95.0) and GCR >= 1.0
    - Row 4: Invalid racking type

    Returns:
        Path to temporary CSV file.
    """
    csv_path = tmp_path / "invalid_config.csv"

    data = {
        "Run Name": ["BadSite1", "BadSite2", "BadSite3"],
        "Site Name": ["", "OutOfRange", "BadRacking"],
        "Customer": ["TestCo", "TestCo", "TestCo"],
        "Latitude": [33.483, 95.0, 32.253],  # Row 2: lat > 90
        "Longitude": [-112.073, -112.073, -110.911],
        "BESS Dispatch Required": [None, None, None],
        "BESS Optimization Required": [None, None, None],
        "DC Size (MW)": [13, 13, 20],
        "AC Installed (MW)": [10, 10, 16],
        "AC POI (MW) ": [10, 10, 16],
        "Racking": ["tracker", "fixed", "invalid_type"],  # Row 3: bad racking
        "Tilt": [0, 25, 0],
        "Azimuth": [180, 180, 180],
        "Module Orientation": ["portrait", "portrait", "landscape"],
        "Number of Modules": [2, 2, 1],
        "Ground Clearance Height (m)": [1.8, 1.0, 1.5],
        "Panel Model": [
            "SunPower SPR-310-WHT-U",
            "SunPower SPR-310-WHT-U",
            "Canadian Solar CS3W-410P",
        ],
        "Bifacial": [True, True, False],
        "Inverter Model": ["iPower SHO 5-2", "iPower SHO 5-2", "SMA SC-2750"],
        "GCR": [0.34, 1.5, 0.40],  # Row 2: GCR >= 1.0
        "Shading (%)": [1, 1, 2],
        "DC Wiring Loss (%) ": [1.5, 1.5, 1.5],
        "AC Wiring Loss (%) ": [1.5, 1.5, 1.5],
        "Transformer Losses (%)": [0, 0, 1],
        "Degradation (%)": [0.3, 0.3, 0.5],
        "Availability (%)": [98, 98, 97],
        "Module Mismatch (%)": [1.5, 1.5, 2.0],
        "LID(%)": [1, 1, 1.5],
    }

    df = pd.DataFrame(data)
    df.to_csv(csv_path, index=False)

    return csv_path


@pytest.fixture()
def sample_missing_fields_csv(tmp_path: Path) -> Path:
    """Create a CSV missing required fields to test validation.

    Missing: Panel Model and Inverter Model columns.

    Returns:
        Path to temporary CSV file.
    """
    csv_path = tmp_path / "missing_fields.csv"

    data = {
        "Run Name": ["MissingData"],
        "Site Name": ["IncompleteSite"],
        "Customer": ["TestCo"],
        "Latitude": [33.483],
        "Longitude": [-112.073],
        "DC Size (MW)": [13],
        "AC Installed (MW)": [10],
        "AC POI (MW) ": [10],
        "Racking": ["tracker"],
        "Tilt": [0],
        "Azimuth": [180],
        "Module Orientation": ["portrait"],
        "Number of Modules": [2],
        "Ground Clearance Height (m)": [1.8],
        # Panel Model: MISSING
        "Bifacial": [True],
        # Inverter Model: MISSING
        "GCR": [0.34],
        "Shading (%)": [1],
        "DC Wiring Loss (%) ": [1.5],
        "AC Wiring Loss (%) ": [1.5],
        "Transformer Losses (%)": [0],
        "Degradation (%)": [0.3],
        "Availability (%)": [98],
        "Module Mismatch (%)": [1.5],
        "LID(%)": [1],
    }

    df = pd.DataFrame(data)
    df.to_csv(csv_path, index=False)

    return csv_path


def generate_mock_nsrdb_csv(
    lat: float = 33.45,
    lon: float = -111.98,
    year: int = 2024,
    num_hours: int = 24,
) -> str:
    """Generate a realistic NSRDB CSV string with 2-row header + hourly data.

    Args:
        lat: Site latitude for the metadata header.
        lon: Site longitude for the metadata header.
        year: Data year for the time columns.
        num_hours: Number of hourly data rows to generate.

    Returns:
        CSV string matching NSRDB API response format.
    """
    # Row 1: metadata labels
    header1 = (
        "Source,Location ID,City,State,Country,Latitude,Longitude,"
        "Time Zone,Elevation"
    )
    # Row 2: metadata values
    header2 = f"NSRDB,123456,TestCity,AZ,United States,{lat},{lon},-7,337"

    # Column names row
    columns = (
        "Year,Month,Day,Hour,Minute,GHI,DNI,DHI,"
        "Temperature,Wind Speed,Surface Albedo"
    )

    # Generate hourly data rows
    rows = []
    hour = 0
    month = 1
    day = 1
    for i in range(num_hours):
        # Simple solar pattern: GHI peaks at noon
        if 6 <= hour <= 18:
            ghi = int(100 + 500 * max(0, 1 - abs(hour - 12) / 6))
            dni = int(ghi * 1.2)
            dhi = int(ghi * 0.2)
        else:
            ghi, dni, dhi = 0, 0, 0

        temp = round(15.0 + 10.0 * max(0, 1 - abs(hour - 14) / 10), 1)
        wind = round(2.0 + 1.5 * (hour % 5) / 5, 1)
        albedo = 0.18

        rows.append(
            f"{year},{month},{day},{hour},0,{ghi},{dni},{dhi},"
            f"{temp},{wind},{albedo}"
        )

        hour += 1
        if hour >= 24:
            hour = 0
            day += 1
            if day > 28:
                day = 1
                month += 1
                if month > 12:
                    month = 1

    lines = [header1, header2, columns] + rows
    return "\n".join(lines) + "\n"


@pytest.fixture()
def mock_nsrdb_response() -> str:
    """Generate a realistic 120-row NSRDB CSV response for testing.

    Returns:
        CSV string with 2-row header + 120 hourly data rows.
    """
    return generate_mock_nsrdb_csv(num_hours=120)


@pytest.fixture()
def sample_cache_files(tmp_path: Path) -> dict[str, Path]:
    """Create temp cache files at known locations and dates.

    Returns:
        Dict mapping descriptive names to cache file paths.
    """
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    today = datetime.now(timezone.utc).strftime("%Y%m%d")

    files = {}
    # Phoenix cache file (fresh)
    phoenix = cache_dir / f"nsrdb_33.45_-111.98_{today}.csv"
    phoenix.write_text(generate_mock_nsrdb_csv(33.45, -111.98))
    files["phoenix"] = phoenix

    # Tucson cache file (fresh)
    tucson = cache_dir / f"nsrdb_32.22_-110.97_{today}.csv"
    tucson.write_text(generate_mock_nsrdb_csv(32.22, -110.97))
    files["tucson"] = tucson

    # Flagstaff cache file (stale — 400 days old)
    flagstaff = cache_dir / "nsrdb_35.2_-111.65_20240101.csv"
    flagstaff.write_text(generate_mock_nsrdb_csv(35.2, -111.65))
    files["flagstaff_stale"] = flagstaff

    files["cache_dir"] = cache_dir
    return files


@pytest.fixture()
def test_sites() -> list[SiteConfig]:
    """Return 5 SiteConfig objects at Phoenix, Tucson, and Flagstaff.

    3 unique locations:
    - Phoenix (33.45, -111.98): 2 sites (tracker + fixed)
    - Tucson (32.22, -110.97): 2 sites (tracker + fixed)
    - Flagstaff (35.20, -111.65): 1 site

    Returns:
        List of 5 SiteConfig objects.
    """
    base = {
        "customer": "TestCo",
        "dc_size_mw": 10.0,
        "ac_installed_mw": 8.0,
        "ac_poi_mw": 8.0,
        "tilt": 25.0,
        "azimuth": 180.0,
        "module_orientation": "portrait",
        "number_of_modules": 2,
        "ground_clearance_height_m": 1.5,
        "panel_model": "Test Panel",
        "bifacial": True,
        "inverter_model": "Test Inverter",
        "gcr": 0.35,
        "shading_percent": 1.0,
        "dc_wiring_loss_percent": 1.5,
        "ac_wiring_loss_percent": 1.5,
        "transformer_losses_percent": 0.0,
        "degradation_percent": 0.3,
        "availability_percent": 98.0,
        "module_mismatch_percent": 1.5,
        "lid_percent": 1.0,
    }
    return [
        SiteConfig(
            run_name="PHX1", site_name="Phoenix_Tracker",
            latitude=33.45, longitude=-111.98, racking="tracker",
            **base,
        ),
        SiteConfig(
            run_name="PHX2", site_name="Phoenix_Fixed",
            latitude=33.45, longitude=-111.98, racking="fixed",
            **base,
        ),
        SiteConfig(
            run_name="TUS1", site_name="Tucson_Tracker",
            latitude=32.22, longitude=-110.97, racking="tracker",
            **base,
        ),
        SiteConfig(
            run_name="TUS2", site_name="Tucson_Fixed",
            latitude=32.22, longitude=-110.97, racking="fixed",
            **base,
        ),
        SiteConfig(
            run_name="FLG1", site_name="Flagstaff_Tracker",
            latitude=35.20, longitude=-111.65, racking="tracker",
            **base,
        ),
    ]


@pytest.fixture()
def climate_results_dir() -> Path:
    """Create and return the climate test results directory.

    Returns:
        Path to outputs/test_results/climate/ directory.
    """
    results_dir = Path("outputs/test_results/climate")
    results_dir.mkdir(parents=True, exist_ok=True)
    return results_dir
