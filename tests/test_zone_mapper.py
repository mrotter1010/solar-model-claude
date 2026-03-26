"""Tests for LMP zone auto-detection (M14e)."""

import pytest

from src.config.schema import SiteConfig
from src.lmp.models import PricingZone
from src.lmp.zone_mapper import (
    get_caiso_zone,
    get_ercot_zone,
    get_iso_from_latlon,
    get_pjm_zone,
    resolve_pricing_zone,
)


def _make_site_config(**overrides: object) -> SiteConfig:
    """Create a SiteConfig with sensible defaults, overridable per test."""
    defaults = {
        "run_name": "test_zone_run",
        "site_name": "Test Zone Site",
        "customer": "Test Customer",
        "latitude": 33.45,
        "longitude": -112.07,
        "dc_size_mw": 13.0,
        "ac_installed_mw": 10.0,
        "ac_poi_mw": 10.0,
        "racking": "tracker",
        "tilt": 60.0,
        "azimuth": 180.0,
        "module_orientation": "portrait",
        "number_of_modules": 1,
        "ground_clearance_height_m": 1.5,
        "panel_model": "CSI Solar Co. Ltd. CS3U-355P",
        "bifacial": True,
        "inverter_model": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
        "gcr": 0.35,
        "shading_percent": 3.0,
        "dc_wiring_loss_percent": 2.0,
        "ac_wiring_loss_percent": 1.0,
        "transformer_losses_percent": 1.5,
        "degradation_percent": 0.5,
        "availability_percent": 2.0,
        "module_mismatch_percent": 1.0,
        "lid_percent": 1.5,
    }
    defaults.update(overrides)
    return SiteConfig(**defaults)


# BESS fields needed when dispatch_mode="ftm" (satisfies validators)
_FTM_BESS_FIELDS = {
    "dispatch_mode": "ftm",
    "bess_dispatch_required": True,
    "bess_power_mw": 5.0,
    "bess_duration_hr": 2.0,
    "bess_installed_cost_per_kwh": 275.0,
    "bess_cycles_warranty": 5000,
    "bill_calculation": True,
    "rate_file_path": "tests/fixtures/rates/sample_rate.json",
    "load_type": "MediumOffice",
    "annual_consumption_kwh": 2_000_000.0,
}


class TestGetIsoFromLatlon:
    """Bounding-box ISO auto-detection."""

    def test_philadelphia_is_pjm(self) -> None:
        """Philadelphia (39.95, -75.16) should map to PJM."""
        assert get_iso_from_latlon(39.95, -75.16) == "pjm"

    def test_houston_is_ercot(self) -> None:
        """Houston (29.76, -95.37) should map to ERCOT."""
        assert get_iso_from_latlon(29.76, -95.37) == "ercot"

    def test_los_angeles_is_caiso(self) -> None:
        """Los Angeles (34.05, -118.24) should map to CAISO."""
        assert get_iso_from_latlon(34.05, -118.24) == "caiso"

    def test_seattle_is_none(self) -> None:
        """Seattle (47.6, -122.33) falls outside all ISO bounding boxes."""
        assert get_iso_from_latlon(47.6, -122.33) is None

    def test_phoenix_is_none(self) -> None:
        """Phoenix (33.45, -112.07) is outside all ISO bounding boxes."""
        assert get_iso_from_latlon(33.45, -112.07) is None

    def test_columbus_ohio_is_pjm(self) -> None:
        """Columbus, OH (39.96, -82.99) should map to PJM."""
        assert get_iso_from_latlon(39.96, -82.99) == "pjm"


class TestGetErcotZone:
    """ERCOT load zone mapping."""

    def test_houston(self) -> None:
        """Houston (29.76, -95.37) should map to LZ_HOUSTON."""
        assert get_ercot_zone(29.76, -95.37) == "LZ_HOUSTON"

    def test_dallas(self) -> None:
        """Dallas (32.78, -96.80) should map to LZ_NORTH."""
        assert get_ercot_zone(32.78, -96.80) == "LZ_NORTH"

    def test_san_antonio(self) -> None:
        """San Antonio (29.42, -98.49) should map to LZ_CPS."""
        assert get_ercot_zone(29.42, -98.49) == "LZ_CPS"

    def test_austin(self) -> None:
        """Austin (30.27, -97.74) should map to LZ_LCRA."""
        assert get_ercot_zone(30.27, -97.74) == "LZ_LCRA"

    def test_lubbock_west_texas(self) -> None:
        """Lubbock (33.58, -101.85) should map to LZ_WEST."""
        assert get_ercot_zone(33.58, -101.85) == "LZ_WEST"

    def test_south_texas_fallback(self) -> None:
        """A point in far south Texas (27.5, -97.5) should map to LZ_SOUTH."""
        assert get_ercot_zone(27.5, -97.5) == "LZ_SOUTH"

    def test_general_fallback(self) -> None:
        """A point not matching any specific region should fall back to LZ_SOUTH."""
        # East Texas, outside specific metro boxes
        assert get_ercot_zone(31.0, -94.5) == "LZ_SOUTH"


class TestGetCaisoZone:
    """CAISO trading hub mapping."""

    def test_san_francisco_np15(self) -> None:
        """San Francisco (37.77, -122.42) should map to NP15."""
        assert get_caiso_zone(37.77, -122.42) == "NP15"

    def test_fresno_zp26(self) -> None:
        """Fresno (36.74, -119.77) should map to ZP26."""
        assert get_caiso_zone(36.74, -119.77) == "ZP26"

    def test_los_angeles_sp15(self) -> None:
        """Los Angeles (34.05, -118.24) should map to SP15."""
        assert get_caiso_zone(34.05, -118.24) == "SP15"

    def test_boundary_at_37(self) -> None:
        """Latitude exactly 37.0 should map to NP15."""
        assert get_caiso_zone(37.0, -120.0) == "NP15"

    def test_boundary_at_35_5(self) -> None:
        """Latitude exactly 35.5 should map to ZP26."""
        assert get_caiso_zone(35.5, -119.0) == "ZP26"


class TestGetPjmZone:
    """PJM zone lookup (graceful fallback when GeoJSON missing)."""

    def test_returns_none_when_geojson_missing(self) -> None:
        """get_pjm_zone should return None when GeoJSON file doesn't exist."""
        # Reset module cache to force reload attempt
        import src.lmp.zone_mapper as zm
        zm._pjm_gdf = None
        zm._pjm_lookup = None

        result = get_pjm_zone(39.95, -75.16)
        assert result is None

    def test_no_exception_when_files_missing(self) -> None:
        """get_pjm_zone should not raise when geo files are absent."""
        import src.lmp.zone_mapper as zm
        zm._pjm_gdf = None
        zm._pjm_lookup = None

        # Should not raise, just return None
        result = get_pjm_zone(40.0, -76.0)
        assert result is None


class TestResolvePricingZone:
    """Full pricing zone resolution logic."""

    def test_explicit_lmp_zone_skips_auto_detect(self) -> None:
        """When lmp_zone is set, use it directly without auto-detection."""
        config = _make_site_config(
            **_FTM_BESS_FIELDS,
            iso="pjm",
            lmp_zone="CUSTOM_ZONE",
        )
        result = resolve_pricing_zone(config)
        assert isinstance(result, PricingZone)
        assert result.iso == "pjm"
        assert result.zone_name == "CUSTOM_ZONE"
        assert result.zone_type == "zone"

    def test_iso_provided_ercot_auto_zone(self) -> None:
        """With iso='ercot' and Houston coordinates, auto-detect LZ_HOUSTON."""
        config = _make_site_config(
            **_FTM_BESS_FIELDS,
            latitude=29.76,
            longitude=-95.37,
            iso="ercot",
        )
        result = resolve_pricing_zone(config)
        assert result.iso == "ercot"
        assert result.zone_name == "LZ_HOUSTON"

    def test_iso_provided_caiso_auto_zone(self) -> None:
        """With iso='caiso' and LA coordinates, auto-detect SP15."""
        config = _make_site_config(
            **_FTM_BESS_FIELDS,
            latitude=34.05,
            longitude=-118.24,
            iso="caiso",
        )
        result = resolve_pricing_zone(config)
        assert result.iso == "caiso"
        assert result.zone_name == "SP15"
        assert result.zone_type == "hub"

    def test_full_auto_detect_ercot(self) -> None:
        """No iso or lmp_zone — auto-detect ISO and zone from lat/lon."""
        config = _make_site_config(
            **_FTM_BESS_FIELDS,
            latitude=29.76,
            longitude=-95.37,
            iso=None,
        )
        result = resolve_pricing_zone(config)
        assert result.iso == "ercot"
        assert result.zone_name == "LZ_HOUSTON"

    def test_full_auto_detect_caiso(self) -> None:
        """No iso or lmp_zone — auto-detect CAISO SP15 from LA coordinates."""
        config = _make_site_config(
            **_FTM_BESS_FIELDS,
            latitude=34.05,
            longitude=-118.24,
            iso=None,
        )
        result = resolve_pricing_zone(config)
        assert result.iso == "caiso"
        assert result.zone_name == "SP15"

    def test_raises_when_iso_unknown(self) -> None:
        """Sites outside all ISO bounding boxes should raise ValueError."""
        config = _make_site_config(
            **_FTM_BESS_FIELDS,
            latitude=47.6,
            longitude=-122.33,
            iso=None,
        )
        with pytest.raises(ValueError, match="Could not determine ISO"):
            resolve_pricing_zone(config)

    def test_explicit_lmp_zone_with_auto_iso(self) -> None:
        """lmp_zone provided but iso is None — ISO auto-detected from lat/lon."""
        config = _make_site_config(
            **_FTM_BESS_FIELDS,
            latitude=29.76,
            longitude=-95.37,
            iso=None,
            lmp_zone="MY_CUSTOM_ZONE",
        )
        result = resolve_pricing_zone(config)
        assert result.iso == "ercot"
        assert result.zone_name == "MY_CUSTOM_ZONE"
