"""Tests for BESS solar-only and grid-only charging mode fields."""

import json
from pathlib import Path

import pandas as pd
import pytest

from src.config.schema import SiteConfig
from src.config.loader import load_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _base_kwargs() -> dict:
    """Return minimal valid SiteConfig kwargs (no BESS, no bill)."""
    return {
        "run_name": "ChargingTest",
        "site_name": "ChargingTestSite",
        "customer": "TestCustomer",
        "latitude": 33.45,
        "longitude": -112.07,
        "dc_size_mw": 13.0,
        "ac_installed_mw": 10.0,
        "ac_poi_mw": 10.0,
        "racking": "tracker",
        "tilt": 55.0,
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
        "ac_wiring_loss_percent": 1.0,
        "transformer_losses_percent": 1.0,
        "degradation_percent": 0.5,
        "availability_percent": 2.5,
        "module_mismatch_percent": 1.0,
        "lid_percent": 1.5,
    }


def _bess_kwargs(**overrides: object) -> dict:
    """Return SiteConfig kwargs with BESS dispatch enabled.

    Includes valid bill_calculation fields (required for BESS dispatch).
    """
    kw = _base_kwargs()
    kw.update({
        "bess_dispatch_required": True,
        "bess_power_mw": 5.0,
        "bess_duration_hr": 4.0,
        "bill_calculation": True,
        "utility_name": "Test Utility",
        "tariff_name": "Test Rate",
        "load_type": "MediumOffice",
        "annual_consumption_kwh": 500000.0,
    })
    kw.update(overrides)
    return kw


# ---------------------------------------------------------------------------
# Default values
# ---------------------------------------------------------------------------


class TestDefaults:
    """Both charging mode fields default to False when not specified."""

    def test_solar_only_defaults_false(self) -> None:
        """bess_solar_only_charging defaults to False."""
        config = SiteConfig(**_base_kwargs())
        assert config.bess_solar_only_charging is False

    def test_grid_only_defaults_false(self) -> None:
        """bess_grid_only_charging defaults to False."""
        config = SiteConfig(**_base_kwargs())
        assert config.bess_grid_only_charging is False


# ---------------------------------------------------------------------------
# Bool coercion
# ---------------------------------------------------------------------------


class TestBoolCoercion:
    """Field validators coerce various truthy/falsy inputs to bool."""

    @pytest.mark.parametrize("value", [1, "true", "True", "TRUE", "yes", "Yes", "YES", "1"])
    def test_solar_only_truthy(self, value: object) -> None:
        """Truthy values coerce to True for bess_solar_only_charging."""
        # solar_only=True requires dispatch_required=True + valid BESS config
        config = SiteConfig(**_bess_kwargs(bess_solar_only_charging=value))
        assert config.bess_solar_only_charging is True

    @pytest.mark.parametrize("value", [0, "false", "False", "FALSE", "no", "No", "NO", "0", None, ""])
    def test_solar_only_falsy(self, value: object) -> None:
        """Falsy values coerce to False for bess_solar_only_charging."""
        config = SiteConfig(**_base_kwargs(), bess_solar_only_charging=value)
        assert config.bess_solar_only_charging is False

    @pytest.mark.parametrize("value", [1, "true", "True", "TRUE", "yes", "Yes", "YES", "1"])
    def test_grid_only_truthy(self, value: object) -> None:
        """Truthy values coerce to True for bess_grid_only_charging."""
        # grid_only=True requires dispatch_required=True + valid BESS config
        config = SiteConfig(**_bess_kwargs(bess_grid_only_charging=value))
        assert config.bess_grid_only_charging is True

    @pytest.mark.parametrize("value", [0, "false", "False", "FALSE", "no", "No", "NO", "0", None, ""])
    def test_grid_only_falsy(self, value: object) -> None:
        """Falsy values coerce to False for bess_grid_only_charging."""
        config = SiteConfig(**_base_kwargs(), bess_grid_only_charging=value)
        assert config.bess_grid_only_charging is False


# ---------------------------------------------------------------------------
# Mutual exclusivity
# ---------------------------------------------------------------------------


class TestMutualExclusivity:
    """Setting both charging modes to True raises ValueError."""

    def test_both_true_raises(self) -> None:
        """Both solar_only and grid_only True is an error."""
        with pytest.raises(ValueError, match="mutually exclusive"):
            SiteConfig(**_bess_kwargs(
                bess_solar_only_charging=True,
                bess_grid_only_charging=True,
            ))


# ---------------------------------------------------------------------------
# Dispatch required dependency
# ---------------------------------------------------------------------------


class TestDispatchRequired:
    """Charging modes require bess_dispatch_required=True."""

    def test_solar_only_without_dispatch_raises(self) -> None:
        """solar_only=True without dispatch_required raises ValueError."""
        kw = _base_kwargs()
        kw["bess_solar_only_charging"] = True
        kw["bess_dispatch_required"] = False
        with pytest.raises(ValueError, match="BESS Dispatch Required must be True.*Solar Only"):
            SiteConfig(**kw)

    def test_grid_only_without_dispatch_raises(self) -> None:
        """grid_only=True without dispatch_required raises ValueError."""
        kw = _base_kwargs()
        kw["bess_grid_only_charging"] = True
        kw["bess_dispatch_required"] = False
        with pytest.raises(ValueError, match="BESS Dispatch Required must be True.*Grid Only"):
            SiteConfig(**kw)


# ---------------------------------------------------------------------------
# Valid configurations (happy path)
# ---------------------------------------------------------------------------


class TestValidConfigurations:
    """Charging modes work correctly with valid BESS dispatch config."""

    def test_solar_only_with_dispatch(self) -> None:
        """solar_only=True passes with dispatch_required=True and valid BESS."""
        config = SiteConfig(**_bess_kwargs(bess_solar_only_charging=True))
        assert config.bess_solar_only_charging is True
        assert config.bess_grid_only_charging is False
        assert config.bess_dispatch_required is True

    def test_grid_only_with_dispatch(self) -> None:
        """grid_only=True passes with dispatch_required=True and valid BESS."""
        config = SiteConfig(**_bess_kwargs(bess_grid_only_charging=True))
        assert config.bess_grid_only_charging is True
        assert config.bess_solar_only_charging is False
        assert config.bess_dispatch_required is True

    def test_neither_charging_mode_with_dispatch(self) -> None:
        """Both False with dispatch enabled is valid (default charging)."""
        config = SiteConfig(**_bess_kwargs())
        assert config.bess_solar_only_charging is False
        assert config.bess_grid_only_charging is False
        assert config.bess_dispatch_required is True


# ---------------------------------------------------------------------------
# CSV loading
# ---------------------------------------------------------------------------


class TestCSVLoading:
    """Verify new fields load correctly from CSV via the loader."""

    def test_csv_with_solar_only_charging(self, tmp_path: Path) -> None:
        """CSV with BESS Solar Only Charging column loads and maps correctly."""
        csv_path = tmp_path / "solar_only.csv"

        data = {
            "Run Name": ["SolarOnly1"],
            "Site Name": ["SolarOnlySite"],
            "Customer": ["TestCo"],
            "Latitude": [33.45],
            "Longitude": [-112.07],
            "DC Size (MW)": [13.0],
            "AC Installed (MW)": [10.0],
            "AC POI (MW)": [10.0],
            "Racking": ["tracker"],
            "Tilt": [55.0],
            "Azimuth": [180.0],
            "Module Orientation": ["portrait"],
            "Number of Modules": [1],
            "Ground Clearance Height (m)": [1.5],
            "Panel Model": ["CSI Solar Co. Ltd. CS3U-355P"],
            "Bifacial": [False],
            "Inverter Model": ["Sungrow Power Supply Co - Ltd : SG250HX-US [800V]"],
            "GCR": [0.35],
            "Shading (%)": [0.0],
            "DC Wiring Loss (%)": [2.0],
            "AC Wiring Loss (%)": [1.0],
            "Transformer Losses (%)": [1.0],
            "Degradation (%)": [0.5],
            "Availability (%)": [2.5],
            "Module Mismatch (%)": [1.0],
            "LID(%)": [1.5],
            # BESS dispatch config
            "BESS Dispatch Required": ["TRUE"],
            "BESS Power (MW)": [5.0],
            "BESS Duration (hr)": [4.0],
            "BESS Solar Only Charging": ["TRUE"],
            "BESS Grid Only Charging": ["FALSE"],
            # Bill calculation config (required for BESS dispatch)
            "Bill Calculation": ["TRUE"],
            "Utility Name": ["Test Utility"],
            "Tariff Name": ["Test Rate"],
            "Load Type": ["MediumOffice"],
            "Annual Consumption (kWh)": [500000.0],
        }

        df = pd.DataFrame(data)
        df.to_csv(csv_path, index=False)

        configs = load_config(csv_path)

        assert len(configs) == 1
        assert configs[0].bess_solar_only_charging is True
        assert configs[0].bess_grid_only_charging is False

    def test_csv_with_grid_only_charging(self, tmp_path: Path) -> None:
        """CSV with BESS Grid Only Charging column loads and maps correctly."""
        csv_path = tmp_path / "grid_only.csv"

        data = {
            "Run Name": ["GridOnly1"],
            "Site Name": ["GridOnlySite"],
            "Customer": ["TestCo"],
            "Latitude": [33.45],
            "Longitude": [-112.07],
            "DC Size (MW)": [13.0],
            "AC Installed (MW)": [10.0],
            "AC POI (MW)": [10.0],
            "Racking": ["tracker"],
            "Tilt": [55.0],
            "Azimuth": [180.0],
            "Module Orientation": ["portrait"],
            "Number of Modules": [1],
            "Ground Clearance Height (m)": [1.5],
            "Panel Model": ["CSI Solar Co. Ltd. CS3U-355P"],
            "Bifacial": [False],
            "Inverter Model": ["Sungrow Power Supply Co - Ltd : SG250HX-US [800V]"],
            "GCR": [0.35],
            "Shading (%)": [0.0],
            "DC Wiring Loss (%)": [2.0],
            "AC Wiring Loss (%)": [1.0],
            "Transformer Losses (%)": [1.0],
            "Degradation (%)": [0.5],
            "Availability (%)": [2.5],
            "Module Mismatch (%)": [1.0],
            "LID(%)": [1.5],
            # BESS dispatch config
            "BESS Dispatch Required": ["TRUE"],
            "BESS Power (MW)": [5.0],
            "BESS Duration (hr)": [4.0],
            "BESS Solar Only Charging": ["FALSE"],
            "BESS Grid Only Charging": ["TRUE"],
            # Bill calculation config (required for BESS dispatch)
            "Bill Calculation": ["TRUE"],
            "Utility Name": ["Test Utility"],
            "Tariff Name": ["Test Rate"],
            "Load Type": ["MediumOffice"],
            "Annual Consumption (kWh)": [500000.0],
        }

        df = pd.DataFrame(data)
        df.to_csv(csv_path, index=False)

        configs = load_config(csv_path)

        assert len(configs) == 1
        assert configs[0].bess_solar_only_charging is False
        assert configs[0].bess_grid_only_charging is True

    def test_csv_without_charging_columns(self, tmp_path: Path) -> None:
        """CSV without charging columns defaults both to False."""
        csv_path = tmp_path / "no_charging.csv"

        data = {
            "Run Name": ["NoBess1"],
            "Site Name": ["NoChargingSite"],
            "Customer": ["TestCo"],
            "Latitude": [33.45],
            "Longitude": [-112.07],
            "DC Size (MW)": [13.0],
            "AC Installed (MW)": [10.0],
            "AC POI (MW)": [10.0],
            "Racking": ["tracker"],
            "Tilt": [55.0],
            "Azimuth": [180.0],
            "Module Orientation": ["portrait"],
            "Number of Modules": [1],
            "Ground Clearance Height (m)": [1.5],
            "Panel Model": ["CSI Solar Co. Ltd. CS3U-355P"],
            "Bifacial": [False],
            "Inverter Model": ["Sungrow Power Supply Co - Ltd : SG250HX-US [800V]"],
            "GCR": [0.35],
            "Shading (%)": [0.0],
            "DC Wiring Loss (%)": [2.0],
            "AC Wiring Loss (%)": [1.0],
            "Transformer Losses (%)": [1.0],
            "Degradation (%)": [0.5],
            "Availability (%)": [2.5],
            "Module Mismatch (%)": [1.0],
            "LID(%)": [1.5],
        }

        df = pd.DataFrame(data)
        df.to_csv(csv_path, index=False)

        configs = load_config(csv_path)

        assert len(configs) == 1
        assert configs[0].bess_solar_only_charging is False
        assert configs[0].bess_grid_only_charging is False


# ---------------------------------------------------------------------------
# Test output for manual inspection
# ---------------------------------------------------------------------------


class TestOutputs:
    """Write test results for manual inspection."""

    def test_write_summary(self, test_results_dir: Path) -> None:
        """Write a summary of charging mode test configurations."""
        output_dir = test_results_dir / "m14b_prompt4"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Valid config with solar_only
        solar_config = SiteConfig(**_bess_kwargs(bess_solar_only_charging=True))
        # Valid config with grid_only
        grid_config = SiteConfig(**_bess_kwargs(
            run_name="GridTest",
            bess_grid_only_charging=True,
        ))
        # Default config (neither charging mode)
        default_config = SiteConfig(**_bess_kwargs(run_name="DefaultTest"))

        summary = {
            "solar_only_config": {
                "bess_solar_only_charging": solar_config.bess_solar_only_charging,
                "bess_grid_only_charging": solar_config.bess_grid_only_charging,
                "bess_dispatch_required": solar_config.bess_dispatch_required,
            },
            "grid_only_config": {
                "bess_solar_only_charging": grid_config.bess_solar_only_charging,
                "bess_grid_only_charging": grid_config.bess_grid_only_charging,
                "bess_dispatch_required": grid_config.bess_dispatch_required,
            },
            "default_config": {
                "bess_solar_only_charging": default_config.bess_solar_only_charging,
                "bess_grid_only_charging": default_config.bess_grid_only_charging,
                "bess_dispatch_required": default_config.bess_dispatch_required,
            },
        }

        output_path = output_dir / "charging_modes_test_summary.json"
        output_path.write_text(json.dumps(summary, indent=2))

        assert output_path.exists()
