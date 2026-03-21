"""Tests for load profile loading and scaling."""

import pytest
from pathlib import Path

from src.rates.load_profile import (
    get_ashrae_climate_zone,
    get_available_building_types,
    load_customer_profile,
    load_typical_profile,
)

OUTPUT_DIR = Path("outputs/test_results/load_profiles")


class TestGetAshraeClimateZone:
    """Tests for nearest-zone lookup."""

    def test_phoenix_is_2b(self) -> None:
        """Phoenix coords map to zone 2B."""
        assert get_ashrae_climate_zone(33.45, -112.07) == "2B"

    def test_chicago_is_5a(self) -> None:
        """Chicago coords map to zone 5A."""
        assert get_ashrae_climate_zone(41.88, -87.63) == "5A"

    def test_miami_is_1a(self) -> None:
        """Miami coords map to zone 1A."""
        assert get_ashrae_climate_zone(25.76, -80.19) == "1A"

    def test_denver_nearest_boulder(self) -> None:
        """Denver (39.74, -104.99) is nearest to Boulder (5B)."""
        assert get_ashrae_climate_zone(39.74, -104.99) == "5B"


class TestLoadTypicalProfile:
    """Tests for typical building profile loading and scaling."""

    def test_medium_office_phoenix_500k(self) -> None:
        """MediumOffice in Phoenix scaled to 500,000 kWh."""
        profile = load_typical_profile(
            building_type="MediumOffice",
            latitude=33.45,
            longitude=-112.07,
            annual_consumption_kwh=500_000.0,
        )

        # Verify 8760 values
        assert len(profile.hourly_kwh) == 8760

        # Verify sum ≈ 500,000 (within 1 kWh tolerance)
        assert abs(profile.annual_consumption_kwh - 500_000.0) < 1.0

        # Verify metadata
        assert profile.source == "typical"
        assert profile.building_type == "MediumOffice"
        assert profile.scale_factor is not None
        assert profile.scale_factor > 0

        # Write first 24 hours to test output
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out_path = OUTPUT_DIR / "medium_office_phoenix_500k_first24.csv"
        with open(out_path, "w") as f:
            f.write("hour,kwh\n")
            for i, v in enumerate(profile.hourly_kwh[:24]):
                f.write(f"{i},{v:.4f}\n")

    def test_invalid_building_type(self) -> None:
        """Unknown building type raises ValueError with clear message."""
        with pytest.raises(ValueError, match="Unknown building type 'InvalidType'"):
            load_typical_profile(
                building_type="InvalidType",
                latitude=33.45,
                longitude=-112.07,
                annual_consumption_kwh=500_000.0,
            )


class TestLoadCustomerProfile:
    """Tests for customer-provided CSV loading."""

    def test_simple_single_column(self, tmp_path: Path) -> None:
        """Simple single-column CSV with 8760 rows of 100.0."""
        csv_path = tmp_path / "load.csv"
        csv_path.write_text("\n".join(["100.0"] * 8760) + "\n")

        profile = load_customer_profile(csv_path)

        assert len(profile.hourly_kwh) == 8760
        assert abs(profile.annual_consumption_kwh - 876_000.0) < 0.1
        assert profile.peak_demand_kw == 100.0
        assert profile.source == "customer"

    def test_with_header(self, tmp_path: Path) -> None:
        """CSV with 'kwh' header + 8760 rows of 75.5."""
        lines = ["kwh"] + ["75.5"] * 8760
        csv_path = tmp_path / "load_header.csv"
        csv_path.write_text("\n".join(lines) + "\n")

        profile = load_customer_profile(csv_path)

        assert len(profile.hourly_kwh) == 8760
        assert abs(profile.annual_consumption_kwh - 75.5 * 8760) < 0.1
        assert profile.peak_demand_kw == 75.5

    def test_wrong_row_count(self, tmp_path: Path) -> None:
        """CSV with only 100 rows raises ValueError."""
        csv_path = tmp_path / "short.csv"
        csv_path.write_text("\n".join(["50.0"] * 100) + "\n")

        with pytest.raises(ValueError, match="exactly 8760"):
            load_customer_profile(csv_path)


class TestGetAvailableBuildingTypes:
    """Tests for building type listing."""

    def test_returns_16_types(self) -> None:
        """Should return exactly 16 building types."""
        types = get_available_building_types()
        assert len(types) == 16

    def test_contains_expected_types(self) -> None:
        """Should include MediumOffice and StandAloneRetail."""
        types = get_available_building_types()
        assert "MediumOffice" in types
        assert "StandAloneRetail" in types
