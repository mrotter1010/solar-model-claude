"""Tests for bill calculation fields in SiteConfig schema."""

from pathlib import Path

import pytest

from src.config.schema import SiteConfig


# Base kwargs for a valid SiteConfig (all required fields)
def _base_kwargs() -> dict:
    """Return minimal valid SiteConfig kwargs."""
    return {
        "run_name": "TestRun",
        "site_name": "TestSite",
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


SAMPLE_RATE_PATH = "tests/fixtures/rates/sample_rate.json"


class TestBillCalculationDisabled:
    """Tests when bill_calculation is False (default)."""

    def test_all_bill_fields_ignored_when_disabled(self) -> None:
        """Cross-validation skipped when bill_calculation=False.

        Both rate_file and utility_name set, both load_profile and load_type set,
        invalid load_type — none of this triggers model-level validation errors
        because bill_calculation is False. Field-level validators (path existence,
        positive numbers) still run, so we use None for paths and valid numbers.
        """
        kw = _base_kwargs()
        kw.update({
            "bill_calculation": False,
            "utility_name": "Some Utility",
            "tariff_name": "Some Tariff",
            "load_type": "NotARealType",
            "annual_consumption_kwh": 500000.0,
            "peak_demand_kw": 100.0,
        })
        # Would fail cross-validation if bill_calculation were True
        # (no rate file + openei set, invalid load type, etc.)
        config = SiteConfig(**kw)
        assert config.bill_calculation is False

    def test_default_is_false(self) -> None:
        """bill_calculation defaults to False when not provided."""
        config = SiteConfig(**_base_kwargs())
        assert config.bill_calculation is False
        assert config.rate_file_path is None
        assert config.utility_name is None


class TestBillCalculationWithRateFile:
    """Tests when bill_calculation=True using a rate file."""

    def test_valid_rate_file_path(self, tmp_path: Path) -> None:
        """Rate file path points to an existing file."""
        kw = _base_kwargs()
        kw.update({
            "bill_calculation": True,
            "rate_file_path": SAMPLE_RATE_PATH,
            "load_type": "MediumOffice",
            "annual_consumption_kwh": 500000.0,
        })
        config = SiteConfig(**kw)
        assert config.rate_file_path == SAMPLE_RATE_PATH
        assert config.utility_name is None

    def test_rate_file_and_openei_both_set_raises(self) -> None:
        """Cannot specify both rate_file_path and utility_name."""
        kw = _base_kwargs()
        kw.update({
            "bill_calculation": True,
            "rate_file_path": SAMPLE_RATE_PATH,
            "utility_name": "APS",
            "tariff_name": "E-TOU",
            "load_type": "MediumOffice",
            "annual_consumption_kwh": 500000.0,
        })
        with pytest.raises(Exception, match="Cannot specify both Rate File Path"):
            SiteConfig(**kw)


class TestBillCalculationWithOpenEI:
    """Tests when bill_calculation=True using OpenEI lookup."""

    def test_utility_and_tariff_name(self) -> None:
        """Both utility_name and tariff_name provided."""
        kw = _base_kwargs()
        kw.update({
            "bill_calculation": True,
            "utility_name": "Arizona Public Service Co",
            "tariff_name": "E-TOU-GS",
            "load_type": "MediumOffice",
            "annual_consumption_kwh": 500000.0,
        })
        config = SiteConfig(**kw)
        assert config.utility_name == "Arizona Public Service Co"
        assert config.tariff_name == "E-TOU-GS"
        assert config.rate_file_path is None

    def test_no_rate_source_raises(self) -> None:
        """No rate file and no utility/tariff → error."""
        kw = _base_kwargs()
        kw.update({
            "bill_calculation": True,
            "load_type": "MediumOffice",
            "annual_consumption_kwh": 500000.0,
        })
        with pytest.raises(Exception, match="No rate source specified"):
            SiteConfig(**kw)


class TestBillCalculationLoadSources:
    """Tests for load source validation."""

    def test_load_profile_path(self, tmp_path: Path) -> None:
        """Load from a customer-provided CSV."""
        # Create a temp CSV with 8760 rows
        csv_path = tmp_path / "load.csv"
        csv_path.write_text("\n".join(["100.0"] * 8760) + "\n")

        kw = _base_kwargs()
        kw.update({
            "bill_calculation": True,
            "utility_name": "Test Utility",
            "tariff_name": "Test Rate",
            "load_profile_path": str(csv_path),
        })
        config = SiteConfig(**kw)
        assert config.load_profile_path == str(csv_path)
        assert config.load_type is None

    def test_load_type_and_consumption(self) -> None:
        """Typical building profile with annual consumption."""
        kw = _base_kwargs()
        kw.update({
            "bill_calculation": True,
            "utility_name": "Test Utility",
            "tariff_name": "Test Rate",
            "load_type": "SmallOffice",
            "annual_consumption_kwh": 250000.0,
        })
        config = SiteConfig(**kw)
        assert config.load_type == "SmallOffice"
        assert config.annual_consumption_kwh == 250000.0

    def test_both_load_sources_raises(self, tmp_path: Path) -> None:
        """Cannot specify both load_profile_path and load_type."""
        csv_path = tmp_path / "load.csv"
        csv_path.write_text("\n".join(["100.0"] * 8760) + "\n")

        kw = _base_kwargs()
        kw.update({
            "bill_calculation": True,
            "utility_name": "Test Utility",
            "tariff_name": "Test Rate",
            "load_profile_path": str(csv_path),
            "load_type": "MediumOffice",
        })
        with pytest.raises(Exception, match="Cannot specify both Load Profile Path"):
            SiteConfig(**kw)

    def test_no_load_source_raises(self) -> None:
        """No load file and no load type → error."""
        kw = _base_kwargs()
        kw.update({
            "bill_calculation": True,
            "utility_name": "Test Utility",
            "tariff_name": "Test Rate",
        })
        with pytest.raises(Exception, match="No load source specified"):
            SiteConfig(**kw)

    def test_invalid_load_type_raises(self) -> None:
        """Invalid building type → error listing available types."""
        kw = _base_kwargs()
        kw.update({
            "bill_calculation": True,
            "utility_name": "Test Utility",
            "tariff_name": "Test Rate",
            "load_type": "InvalidBuildingType",
            "annual_consumption_kwh": 500000.0,
        })
        with pytest.raises(Exception, match="Invalid Load Type"):
            SiteConfig(**kw)

    def test_load_type_without_consumption_raises(self) -> None:
        """Load type provided without annual consumption → error."""
        kw = _base_kwargs()
        kw.update({
            "bill_calculation": True,
            "utility_name": "Test Utility",
            "tariff_name": "Test Rate",
            "load_type": "MediumOffice",
        })
        with pytest.raises(Exception, match="Annual Consumption.*required"):
            SiteConfig(**kw)


class TestBackwardCompatibility:
    """Tests that existing CSVs without new columns still work."""

    def test_existing_csv_loads_without_bill_fields(self, tmp_path: Path) -> None:
        """CSV without bill columns → bill_calculation defaults to False."""
        csv_content = (
            "Run Name,Site Name,Customer,Latitude,Longitude,"
            "DC Size (MW),AC Installed (MW),AC POI (MW),"
            "Racking,Tilt,Azimuth,Module Orientation,Number of Modules,"
            "Ground Clearance Height (m),Panel Model,Bifacial,Inverter Model,"
            "GCR,Shading (%),DC Wiring Loss (%),AC Wiring Loss (%),"
            "Transformer Losses (%),Degradation (%),Availability (%),"
            "Module Mismatch (%),LID(%)\n"
            "Run1,Site1,Cust1,33.45,-112.07,"
            "13.0,10.0,10.0,"
            "tracker,55,180,portrait,1,"
            "1.5,CSI Solar Co. Ltd. CS3U-355P,FALSE,"
            "Sungrow Power Supply Co - Ltd : SG250HX-US [800V],"
            "0.35,0.0,2.0,1.0,1.0,0.5,2.5,1.0,1.5\n"
        )
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content)

        from src.config.loader import load_config
        configs = load_config(csv_path)

        assert len(configs) == 1
        assert configs[0].bill_calculation is False
        assert configs[0].rate_file_path is None
        assert configs[0].utility_name is None
