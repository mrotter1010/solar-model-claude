"""Shared pytest fixtures for solar model tests."""

from pathlib import Path

import pandas as pd
import pytest


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
        "Tilt": [0, 25, 0],
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
