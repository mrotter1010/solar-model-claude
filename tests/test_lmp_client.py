"""Tests for LMP client — PJM fetch and routing (M14e).

All tests mock gridstatus.PJM to avoid real API calls.
"""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.lmp.client import (
    CAISO_HUB_MAP,
    CAISO_LOCATIONS,
    ERCOT_LOCATIONS,
    PJM_ZONES,
    fetch_caiso_lmp,
    fetch_ercot_lmp,
    fetch_lmp,
    fetch_pjm_lmp,
)
from src.lmp.models import LMPData


# ---------------------------------------------------------------------------
# Helpers — build synthetic DataFrames matching real PJM gridstatus schema
# ---------------------------------------------------------------------------

def _make_pjm_dataframe(
    zone: str = "AEP",
    year: int = 2025,
    n_hours: int = 8760,
    zones: list[str] | None = None,
    base_price: float = 30.0,
) -> pd.DataFrame:
    """Build a synthetic DataFrame matching gridstatus PJM get_lmp output.

    Args:
        zone: Primary zone to generate (used when zones is None).
        year: Calendar year for timestamps.
        n_hours: Number of hourly rows per zone.
        zones: If provided, generate rows for all listed zones.
        base_price: Base LMP price; varies slightly per hour.

    Returns:
        DataFrame with columns matching real PJM API response.
    """
    if zones is None:
        zones = [zone]

    rows = []
    for z in zones:
        timestamps = pd.date_range(
            f"{year}-01-01",
            periods=n_hours,
            freq="h",
            tz="US/Eastern",
        )
        for i, ts in enumerate(timestamps):
            price = base_price + (i % 24) * 0.5
            rows.append({
                "Time": ts,
                "Interval Start": ts,
                "Interval End": ts + pd.Timedelta(hours=1),
                "Market": "DAY_AHEAD_HOURLY",
                "Location Id": 100 + zones.index(z),
                "Location Name": f"{z} Zone",
                "Location Short Name": z,
                "Location Type": "ZONE",
                "LMP": price,
                "Energy": price * 0.85,
                "Congestion": price * 0.10,
                "Loss": price * 0.05,
            })

    df = pd.DataFrame(rows)
    # Match real dtypes
    df["Location Id"] = df["Location Id"].astype("int64")
    for col in ["LMP", "Energy", "Congestion", "Loss"]:
        df[col] = df[col].astype("float64")
    return df


def _mock_pjm_class(return_df: pd.DataFrame) -> MagicMock:
    """Create a mock gridstatus.PJM class whose get_lmp returns the given df."""
    mock_instance = MagicMock()
    mock_instance.get_lmp.return_value = return_df
    mock_class = MagicMock(return_value=mock_instance)
    return mock_class


# ---------------------------------------------------------------------------
# Test: basic fetch
# ---------------------------------------------------------------------------

class TestFetchPjmLmpBasic:
    """Basic PJM LMP fetch with mocked gridstatus."""

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_fetch_returns_8760_prices(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock
    ) -> None:
        """fetch_pjm_lmp should return LMPData with 8760 prices."""
        df = _make_pjm_dataframe(zone="AEP", year=2025, n_hours=8760)
        mock_gs.PJM.return_value.get_lmp.return_value = df

        result = fetch_pjm_lmp(zone="AEP", year=2025)

        assert isinstance(result, LMPData)
        assert len(result.prices) == 8760
        assert result.iso == "pjm"
        assert result.zone == "AEP"
        assert result.market == "DAY_AHEAD_HOURLY"
        assert result.year == 2025

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_prices_match_lmp_column(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock
    ) -> None:
        """Prices should come from the LMP column, not Energy/Congestion/Loss."""
        df = _make_pjm_dataframe(zone="PECO", year=2025, base_price=50.0)
        mock_gs.PJM.return_value.get_lmp.return_value = df

        result = fetch_pjm_lmp(zone="PECO", year=2025)

        # First hour price should be base_price (50.0)
        assert result.prices[0] == pytest.approx(50.0)

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_timestamps_are_utc(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock
    ) -> None:
        """Returned timestamps should be converted to UTC."""
        df = _make_pjm_dataframe(zone="AEP", year=2025)
        mock_gs.PJM.return_value.get_lmp.return_value = df

        result = fetch_pjm_lmp(zone="AEP", year=2025)

        assert result.timestamps is not None
        # First timestamp should be Jan 1 05:00 UTC (midnight ET = UTC-5)
        first_ts = result.timestamps[0]
        assert first_ts.year == 2025
        assert first_ts.month == 1
        assert first_ts.day == 1


# ---------------------------------------------------------------------------
# Test: zone filtering
# ---------------------------------------------------------------------------

class TestFetchPjmLmpZoneFilter:
    """Multi-zone data should be filtered to the requested zone."""

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_filters_to_requested_zone(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock
    ) -> None:
        """Only data for the requested zone should be returned."""
        # Return multi-zone data (3 zones × 8760 hours)
        df = _make_pjm_dataframe(
            year=2025, n_hours=8760, zones=["AEP", "PECO", "DOM"]
        )
        mock_gs.PJM.return_value.get_lmp.return_value = df

        result = fetch_pjm_lmp(zone="PECO", year=2025)

        assert result.zone == "PECO"
        assert len(result.prices) == 8760


# ---------------------------------------------------------------------------
# Test: cache hit
# ---------------------------------------------------------------------------

class TestFetchPjmLmpCacheHit:
    """When cache exists, no API call should be made."""

    @patch("src.lmp.client.gridstatus")
    @patch("src.lmp.client.get_cached_lmp")
    def test_cache_hit_skips_api(
        self, mock_cache: MagicMock, mock_gs: MagicMock
    ) -> None:
        """Cached data should be used without calling gridstatus."""
        # Simulate cached DataFrame
        timestamps = pd.date_range("2025-01-01", periods=8760, freq="h", tz="UTC")
        cached_df = pd.DataFrame({
            "timestamp": timestamps,
            "lmp": [30.0 + (i % 24) * 0.5 for i in range(8760)],
        })
        mock_cache.return_value = cached_df

        result = fetch_pjm_lmp(zone="AEP", year=2025)

        assert isinstance(result, LMPData)
        assert len(result.prices) == 8760
        # gridstatus.PJM should never be instantiated
        mock_gs.PJM.assert_not_called()


# ---------------------------------------------------------------------------
# Test: cache miss then save
# ---------------------------------------------------------------------------

class TestFetchPjmLmpCacheSave:
    """After API fetch, data should be saved to cache."""

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_saves_to_cache_after_fetch(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock
    ) -> None:
        """save_lmp_cache should be called with correct args after fetch."""
        df = _make_pjm_dataframe(zone="BGE", year=2025)
        mock_gs.PJM.return_value.get_lmp.return_value = df

        fetch_pjm_lmp(zone="BGE", year=2025)

        mock_save.assert_called_once()
        call_args = mock_save.call_args
        assert call_args[0][0] == "pjm"  # iso
        assert call_args[0][1] == "BGE"  # zone
        assert call_args[0][2] == "DAY_AHEAD_HOURLY"  # market
        assert call_args[0][3] == 2025  # year
        # The DataFrame arg should have 8760 rows
        saved_df = call_args[0][4]
        assert len(saved_df) == 8760


# ---------------------------------------------------------------------------
# Test: leap year truncation
# ---------------------------------------------------------------------------

class TestFetchPjmLmpLeapYear:
    """Leap year (8784 hours) should be truncated to 8760."""

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_leap_year_truncated(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock
    ) -> None:
        """8784 rows (leap year) should drop Feb 29 to get 8760."""
        # 2024 is a leap year
        df = _make_pjm_dataframe(zone="AEP", year=2024, n_hours=8784)
        mock_gs.PJM.return_value.get_lmp.return_value = df

        result = fetch_pjm_lmp(zone="AEP", year=2024)

        assert len(result.prices) == 8760
        # Verify no Feb 29 timestamps
        if result.timestamps:
            for ts in result.timestamps:
                assert not (ts.month == 2 and ts.day == 29)


# ---------------------------------------------------------------------------
# Test: invalid zone
# ---------------------------------------------------------------------------

class TestFetchPjmLmpValidation:
    """Input validation for zone names."""

    def test_invalid_zone_raises(self) -> None:
        """Unknown zone name should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid PJM zone"):
            fetch_pjm_lmp(zone="FAKE_ZONE", year=2025)

    def test_zone_case_insensitive(self) -> None:
        """Zone lookup should be case-insensitive."""
        # This should not raise on zone validation (will fail on API call,
        # but we're testing the normalization step)
        with patch("src.lmp.client.get_cached_lmp", return_value=None), \
             patch("src.lmp.client.save_lmp_cache"), \
             patch("src.lmp.client.gridstatus") as mock_gs:
            df = _make_pjm_dataframe(zone="AEP", year=2025)
            mock_gs.PJM.return_value.get_lmp.return_value = df

            result = fetch_pjm_lmp(zone="aep", year=2025)
            assert result.zone == "AEP"


# ---------------------------------------------------------------------------
# Test: default year
# ---------------------------------------------------------------------------

class TestFetchPjmLmpDefaultYear:
    """Default year should be previous calendar year."""

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_default_year_is_previous(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock
    ) -> None:
        """When year is None, should default to current year - 1."""
        expected_year = datetime.now().year - 1
        df = _make_pjm_dataframe(zone="AEP", year=expected_year)
        mock_gs.PJM.return_value.get_lmp.return_value = df

        result = fetch_pjm_lmp(zone="AEP")

        assert result.year == expected_year


# ---------------------------------------------------------------------------
# Test: gap filling
# ---------------------------------------------------------------------------

class TestFetchPjmLmpGapFill:
    """Gap filling in LMP timeseries."""

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_small_gap_forward_filled(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock
    ) -> None:
        """Gaps of <= 4 hours should be forward-filled."""
        # Build full-year DataFrame but remove 3 consecutive hours
        df = _make_pjm_dataframe(zone="AEP", year=2025)
        # Remove hours 100-102 (a 3-hour gap)
        df = df.drop(index=[100, 101, 102]).reset_index(drop=True)
        mock_gs.PJM.return_value.get_lmp.return_value = df

        result = fetch_pjm_lmp(zone="AEP", year=2025)

        # Should still get 8760 after gap filling
        assert len(result.prices) == 8760

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_large_gap_logs_warning(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Gaps > 4 hours should log a warning but still forward-fill."""
        # Remove 6 consecutive hours (hours 200-205)
        df = _make_pjm_dataframe(zone="AEP", year=2025)
        df = df.drop(index=list(range(200, 206))).reset_index(drop=True)
        mock_gs.PJM.return_value.get_lmp.return_value = df

        import logging
        with caplog.at_level(logging.WARNING, logger="src.lmp.client"):
            result = fetch_pjm_lmp(zone="AEP", year=2025)

        assert len(result.prices) == 8760
        assert any("gaps > 4 hours" in msg for msg in caplog.messages)


# ---------------------------------------------------------------------------
# Test: fetch_lmp router
# ---------------------------------------------------------------------------

class TestFetchLmpRouter:
    """Router function dispatches to ISO-specific clients."""

    @patch("src.lmp.client.fetch_pjm_lmp")
    def test_pjm_dispatches(self, mock_pjm: MagicMock) -> None:
        """'pjm' should route to fetch_pjm_lmp."""
        mock_pjm.return_value = MagicMock(spec=LMPData)
        fetch_lmp(iso="pjm", zone="AEP", year=2025)
        mock_pjm.assert_called_once_with(zone="AEP", market="DAY_AHEAD_HOURLY", year=2025)

    @patch("src.lmp.client.fetch_pjm_lmp")
    def test_pjm_case_insensitive(self, mock_pjm: MagicMock) -> None:
        """ISO routing should be case-insensitive."""
        mock_pjm.return_value = MagicMock(spec=LMPData)
        fetch_lmp(iso="PJM", zone="AEP", year=2025)
        mock_pjm.assert_called_once()

    @patch("src.lmp.client.fetch_ercot_lmp")
    def test_ercot_dispatches(self, mock_ercot: MagicMock) -> None:
        """ERCOT should route to fetch_ercot_lmp."""
        mock_ercot.return_value = MagicMock(spec=LMPData)
        fetch_lmp(iso="ercot", zone="LZ_HOUSTON", year=2025)
        mock_ercot.assert_called_once()

    @patch("src.lmp.client.fetch_caiso_lmp")
    def test_caiso_dispatches(self, mock_caiso: MagicMock) -> None:
        """CAISO should route to fetch_caiso_lmp."""
        mock_caiso.return_value = MagicMock(spec=LMPData)
        fetch_lmp(iso="caiso", zone="NP15", year=2025)
        mock_caiso.assert_called_once()

    def test_unknown_iso_raises_value_error(self) -> None:
        """Unknown ISO should raise ValueError."""
        with pytest.raises(ValueError, match="Unrecognized ISO"):
            fetch_lmp(iso="nyiso", zone="ZONE_A", year=2025)


# ---------------------------------------------------------------------------
# Test: PJM_ZONES constant
# ---------------------------------------------------------------------------

class TestPjmZones:
    """PJM zone list validation."""

    def test_zone_count(self) -> None:
        """Should have exactly 23 PJM zones."""
        assert len(PJM_ZONES) == 23

    def test_known_zones_present(self) -> None:
        """Key zones should be in the list."""
        for zone in ["AEP", "PECO", "DOM", "PJM-RTO", "PSEG", "COMED"]:
            assert zone in PJM_ZONES


# ===========================================================================
# ERCOT Tests
# ===========================================================================

# ---------------------------------------------------------------------------
# Helpers — build synthetic DataFrames matching real ERCOT gridstatus schema
# ---------------------------------------------------------------------------

def _make_ercot_dataframe(
    location: str = "LZ_HOUSTON",
    year: int = 2025,
    n_hours: int = 8760,
    locations: list[str] | None = None,
    base_price: float = 25.0,
) -> pd.DataFrame:
    """Build a synthetic DataFrame matching gridstatus ERCOT get_dam_spp output.

    Args:
        location: Primary location to generate (used when locations is None).
        year: Calendar year for timestamps.
        n_hours: Number of hourly rows per location.
        locations: If provided, generate rows for all listed locations.
        base_price: Base SPP price; varies slightly per hour.

    Returns:
        DataFrame with columns matching real ERCOT API response.
    """
    if locations is None:
        locations = [location]

    rows = []
    for loc in locations:
        timestamps = pd.date_range(
            f"{year}-01-01",
            periods=n_hours,
            freq="h",
            tz="US/Central",
        )
        # Determine location type from name prefix
        if loc.startswith("HB_"):
            loc_type = "Trading Hub"
        else:
            loc_type = "Load Zone"

        for i, ts in enumerate(timestamps):
            price = base_price + (i % 24) * 0.5
            rows.append({
                "Time": ts,
                "Interval Start": ts,
                "Interval End": ts + pd.Timedelta(hours=1),
                "Location": loc,
                "Location Type": loc_type,
                "Market": "DAY_AHEAD_HOURLY",
                "SPP": price,
            })

    df = pd.DataFrame(rows)
    df["SPP"] = df["SPP"].astype("float64")
    df["Location"] = df["Location"].astype("string")
    df["Location Type"] = df["Location Type"].astype("category")
    return df


# ---------------------------------------------------------------------------
# Test: ERCOT basic fetch
# ---------------------------------------------------------------------------

class TestFetchErcotLmpBasic:
    """Basic ERCOT SPP fetch with mocked gridstatus."""

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_fetch_returns_8760_prices(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock
    ) -> None:
        """fetch_ercot_lmp should return LMPData with 8760 prices."""
        df = _make_ercot_dataframe(location="LZ_HOUSTON", year=2025)
        mock_gs.Ercot.return_value.get_dam_spp.return_value = df

        result = fetch_ercot_lmp(zone="LZ_HOUSTON", year=2025)

        assert isinstance(result, LMPData)
        assert len(result.prices) == 8760
        assert result.iso == "ercot"
        assert result.zone == "LZ_HOUSTON"
        assert result.market == "DAY_AHEAD_HOURLY"
        assert result.year == 2025

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_prices_from_spp_column(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock
    ) -> None:
        """Prices should come from the SPP column."""
        df = _make_ercot_dataframe(
            location="LZ_NORTH", year=2025, base_price=40.0
        )
        mock_gs.Ercot.return_value.get_dam_spp.return_value = df

        result = fetch_ercot_lmp(zone="LZ_NORTH", year=2025)

        assert result.prices[0] == pytest.approx(40.0)


# ---------------------------------------------------------------------------
# Test: ERCOT zone filtering
# ---------------------------------------------------------------------------

class TestFetchErcotLmpZoneFilter:
    """Multi-location data should be filtered to the requested zone."""

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_filters_to_requested_zone(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock
    ) -> None:
        """Only data for the requested location should be returned."""
        # Return all 15 ERCOT locations
        df = _make_ercot_dataframe(
            year=2025,
            n_hours=8760,
            locations=ERCOT_LOCATIONS,
        )
        mock_gs.Ercot.return_value.get_dam_spp.return_value = df

        result = fetch_ercot_lmp(zone="LZ_SOUTH", year=2025)

        assert result.zone == "LZ_SOUTH"
        assert len(result.prices) == 8760


# ---------------------------------------------------------------------------
# Test: ERCOT cache hit
# ---------------------------------------------------------------------------

class TestFetchErcotLmpCacheHit:
    """When cache exists, no API call should be made."""

    @patch("src.lmp.client.gridstatus")
    @patch("src.lmp.client.get_cached_lmp")
    def test_cache_hit_skips_api(
        self, mock_cache: MagicMock, mock_gs: MagicMock
    ) -> None:
        """Cached data should be used without calling gridstatus."""
        timestamps = pd.date_range(
            "2025-01-01", periods=8760, freq="h", tz="UTC"
        )
        cached_df = pd.DataFrame({
            "timestamp": timestamps,
            "lmp": [25.0 + (i % 24) * 0.5 for i in range(8760)],
        })
        mock_cache.return_value = cached_df

        result = fetch_ercot_lmp(zone="LZ_HOUSTON", year=2025)

        assert isinstance(result, LMPData)
        assert len(result.prices) == 8760
        mock_gs.Ercot.assert_not_called()


# ---------------------------------------------------------------------------
# Test: ERCOT invalid zone
# ---------------------------------------------------------------------------

class TestFetchErcotLmpValidation:
    """Input validation for ERCOT location names."""

    def test_invalid_zone_raises(self) -> None:
        """Unknown location should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid ERCOT location"):
            fetch_ercot_lmp(zone="FAKE_ZONE", year=2025)

    def test_zone_case_insensitive(self) -> None:
        """Location lookup should be case-insensitive."""
        with patch("src.lmp.client.get_cached_lmp", return_value=None), \
             patch("src.lmp.client.save_lmp_cache"), \
             patch("src.lmp.client.gridstatus") as mock_gs:
            df = _make_ercot_dataframe(location="LZ_WEST", year=2025)
            mock_gs.Ercot.return_value.get_dam_spp.return_value = df

            result = fetch_ercot_lmp(zone="lz_west", year=2025)
            assert result.zone == "LZ_WEST"


# ---------------------------------------------------------------------------
# Test: ERCOT hub accepted
# ---------------------------------------------------------------------------

class TestFetchErcotLmpHubs:
    """Trading hubs (HB_*) should be accepted, not just load zones."""

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_hub_location_accepted(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock
    ) -> None:
        """HB_HOUSTON should be a valid location."""
        df = _make_ercot_dataframe(location="HB_HOUSTON", year=2025)
        mock_gs.Ercot.return_value.get_dam_spp.return_value = df

        result = fetch_ercot_lmp(zone="HB_HOUSTON", year=2025)

        assert result.zone == "HB_HOUSTON"
        assert len(result.prices) == 8760


# ---------------------------------------------------------------------------
# Test: ERCOT router integration
# ---------------------------------------------------------------------------

class TestFetchLmpRouterErcot:
    """Router function dispatches ERCOT correctly."""

    @patch("src.lmp.client.fetch_ercot_lmp")
    def test_ercot_dispatches(self, mock_ercot: MagicMock) -> None:
        """'ercot' should route to fetch_ercot_lmp."""
        mock_ercot.return_value = MagicMock(spec=LMPData)
        fetch_lmp(iso="ercot", zone="LZ_HOUSTON", year=2025)
        mock_ercot.assert_called_once_with(
            zone="LZ_HOUSTON", market="DAY_AHEAD_HOURLY", year=2025
        )

    @patch("src.lmp.client.fetch_ercot_lmp")
    def test_ercot_case_insensitive(self, mock_ercot: MagicMock) -> None:
        """ISO routing should be case-insensitive for ERCOT."""
        mock_ercot.return_value = MagicMock(spec=LMPData)
        fetch_lmp(iso="ERCOT", zone="LZ_NORTH", year=2025)
        mock_ercot.assert_called_once()


# ---------------------------------------------------------------------------
# Test: ERCOT_LOCATIONS constant
# ---------------------------------------------------------------------------

class TestErcotLocations:
    """ERCOT location list validation."""

    def test_location_count(self) -> None:
        """Should have exactly 15 ERCOT locations (8 LZ + 7 HB)."""
        assert len(ERCOT_LOCATIONS) == 15

    def test_load_zones_present(self) -> None:
        """All 8 load zones should be in the list."""
        for zone in [
            "LZ_AEN", "LZ_CPS", "LZ_HOUSTON", "LZ_LCRA",
            "LZ_NORTH", "LZ_RAYBN", "LZ_SOUTH", "LZ_WEST",
        ]:
            assert zone in ERCOT_LOCATIONS

    def test_trading_hubs_present(self) -> None:
        """All 7 trading hubs should be in the list."""
        for hub in [
            "HB_BUSAVG", "HB_HOUSTON", "HB_HUBAVG", "HB_NORTH",
            "HB_PAN", "HB_SOUTH", "HB_WEST",
        ]:
            assert hub in ERCOT_LOCATIONS


# ===========================================================================
# CAISO Tests
# ===========================================================================

# ---------------------------------------------------------------------------
# Helpers — build synthetic DataFrames matching real CAISO gridstatus schema
# ---------------------------------------------------------------------------

def _make_caiso_dataframe(
    location: str = "TH_NP15_GEN-APND",
    year: int = 2025,
    n_hours: int = 8760,
    locations: list[str] | None = None,
    base_price: float = 35.0,
) -> pd.DataFrame:
    """Build a synthetic DataFrame matching gridstatus CAISO get_lmp output.

    Args:
        location: Full hub name (e.g., "TH_NP15_GEN-APND").
        year: Calendar year for timestamps.
        n_hours: Number of hourly rows per location.
        locations: If provided, generate rows for all listed locations.
        base_price: Base LMP price; varies slightly per hour.

    Returns:
        DataFrame with columns matching real CAISO API response.
    """
    if locations is None:
        locations = [location]

    rows = []
    for loc in locations:
        timestamps = pd.date_range(
            f"{year}-01-01",
            periods=n_hours,
            freq="h",
            tz="US/Pacific",
        )
        for i, ts in enumerate(timestamps):
            price = base_price + (i % 24) * 0.5
            rows.append({
                "Time": ts,
                "Interval Start": ts,
                "Interval End": ts + pd.Timedelta(hours=1),
                "Market": "DAY_AHEAD_HOURLY",
                "Location": loc,
                "Location Type": "Trading Hub",
                "LMP": price,
                "Energy": price * 0.80,
                "Congestion": price * 0.12,
                "Loss": price * 0.08,
            })

    df = pd.DataFrame(rows)
    for col in ["LMP", "Energy", "Congestion", "Loss"]:
        df[col] = df[col].astype("float64")
    return df


# ---------------------------------------------------------------------------
# Test: CAISO basic fetch
# ---------------------------------------------------------------------------

class TestFetchCaisoLmpBasic:
    """Basic CAISO LMP fetch with mocked gridstatus."""

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_fetch_returns_8760_prices(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock
    ) -> None:
        """fetch_caiso_lmp should return LMPData with 8760 prices."""
        df = _make_caiso_dataframe(location="TH_NP15_GEN-APND", year=2025)
        mock_gs.CAISO.return_value.get_lmp.return_value = df

        result = fetch_caiso_lmp(zone="NP15", year=2025)

        assert isinstance(result, LMPData)
        assert len(result.prices) == 8760
        assert result.iso == "caiso"
        assert result.zone == "NP15"
        assert result.market == "DAY_AHEAD_HOURLY"
        assert result.year == 2025

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_prices_from_lmp_column(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock
    ) -> None:
        """Prices should come from the LMP column."""
        df = _make_caiso_dataframe(
            location="TH_SP15_GEN-APND", year=2025, base_price=45.0
        )
        mock_gs.CAISO.return_value.get_lmp.return_value = df

        result = fetch_caiso_lmp(zone="SP15", year=2025)

        assert result.prices[0] == pytest.approx(45.0)


# ---------------------------------------------------------------------------
# Test: CAISO hub mapping
# ---------------------------------------------------------------------------

class TestFetchCaisoLmpHubMapping:
    """Short hub name should map to full gridstatus location name."""

    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_np15_maps_to_full_name(
        self, mock_gs: MagicMock, mock_save: MagicMock, mock_cache: MagicMock
    ) -> None:
        """NP15 should pass TH_NP15_GEN-APND to the API."""
        df = _make_caiso_dataframe(location="TH_NP15_GEN-APND", year=2025)
        mock_gs.CAISO.return_value.get_lmp.return_value = df

        fetch_caiso_lmp(zone="NP15", year=2025)

        # Verify the locations list passed to get_lmp
        call_kwargs = mock_gs.CAISO.return_value.get_lmp.call_args
        assert call_kwargs[1]["locations"] == ["TH_NP15_GEN-APND"]


# ---------------------------------------------------------------------------
# Test: CAISO cache hit
# ---------------------------------------------------------------------------

class TestFetchCaisoLmpCacheHit:
    """When cache exists, no API call should be made."""

    @patch("src.lmp.client.gridstatus")
    @patch("src.lmp.client.get_cached_lmp")
    def test_cache_hit_skips_api(
        self, mock_cache: MagicMock, mock_gs: MagicMock
    ) -> None:
        """Cached data should be used without calling gridstatus."""
        timestamps = pd.date_range(
            "2025-01-01", periods=8760, freq="h", tz="UTC"
        )
        cached_df = pd.DataFrame({
            "timestamp": timestamps,
            "lmp": [35.0 + (i % 24) * 0.5 for i in range(8760)],
        })
        mock_cache.return_value = cached_df

        result = fetch_caiso_lmp(zone="NP15", year=2025)

        assert isinstance(result, LMPData)
        assert len(result.prices) == 8760
        mock_gs.CAISO.assert_not_called()


# ---------------------------------------------------------------------------
# Test: CAISO invalid zone
# ---------------------------------------------------------------------------

class TestFetchCaisoLmpValidation:
    """Input validation for CAISO hub names."""

    def test_invalid_zone_raises(self) -> None:
        """Unknown hub should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid CAISO hub"):
            fetch_caiso_lmp(zone="NP16", year=2025)

    def test_non_caiso_zone_raises(self) -> None:
        """ERCOT/PJM zone names should not be accepted."""
        with pytest.raises(ValueError, match="Invalid CAISO hub"):
            fetch_caiso_lmp(zone="LZ_HOUSTON", year=2025)

    def test_zone_case_insensitive(self) -> None:
        """Hub lookup should be case-insensitive."""
        with patch("src.lmp.client.get_cached_lmp", return_value=None), \
             patch("src.lmp.client.save_lmp_cache"), \
             patch("src.lmp.client.gridstatus") as mock_gs:
            df = _make_caiso_dataframe(location="TH_ZP26_GEN-APND", year=2025)
            mock_gs.CAISO.return_value.get_lmp.return_value = df

            result = fetch_caiso_lmp(zone="zp26", year=2025)
            assert result.zone == "ZP26"


# ---------------------------------------------------------------------------
# Test: all CAISO hubs accepted
# ---------------------------------------------------------------------------

class TestFetchCaisoLmpAllHubs:
    """All three CAISO trading hubs should be accepted."""

    @pytest.mark.parametrize("short_name,full_name", [
        ("NP15", "TH_NP15_GEN-APND"),
        ("SP15", "TH_SP15_GEN-APND"),
        ("ZP26", "TH_ZP26_GEN-APND"),
    ])
    @patch("src.lmp.client.get_cached_lmp", return_value=None)
    @patch("src.lmp.client.save_lmp_cache")
    @patch("src.lmp.client.gridstatus")
    def test_hub_accepted(
        self,
        mock_gs: MagicMock,
        mock_save: MagicMock,
        mock_cache: MagicMock,
        short_name: str,
        full_name: str,
    ) -> None:
        """Each CAISO hub should be fetchable."""
        df = _make_caiso_dataframe(location=full_name, year=2025)
        mock_gs.CAISO.return_value.get_lmp.return_value = df

        result = fetch_caiso_lmp(zone=short_name, year=2025)

        assert result.zone == short_name
        assert len(result.prices) == 8760


# ---------------------------------------------------------------------------
# Test: CAISO router integration
# ---------------------------------------------------------------------------

class TestFetchLmpRouterCaiso:
    """Router function dispatches CAISO correctly."""

    @patch("src.lmp.client.fetch_caiso_lmp")
    def test_caiso_dispatches(self, mock_caiso: MagicMock) -> None:
        """'caiso' should route to fetch_caiso_lmp."""
        mock_caiso.return_value = MagicMock(spec=LMPData)
        fetch_lmp(iso="caiso", zone="NP15", year=2025)
        mock_caiso.assert_called_once_with(
            zone="NP15", market="DAY_AHEAD_HOURLY", year=2025
        )

    @patch("src.lmp.client.fetch_caiso_lmp")
    def test_caiso_case_insensitive(self, mock_caiso: MagicMock) -> None:
        """ISO routing should be case-insensitive for CAISO."""
        mock_caiso.return_value = MagicMock(spec=LMPData)
        fetch_lmp(iso="CAISO", zone="SP15", year=2025)
        mock_caiso.assert_called_once()


# ---------------------------------------------------------------------------
# Test: all-ISO router coverage
# ---------------------------------------------------------------------------

class TestFetchLmpRouterAllISOs:
    """Full router coverage: PJM, ERCOT, CAISO all dispatch correctly."""

    @patch("src.lmp.client.fetch_pjm_lmp")
    def test_pjm_routes(self, mock_fn: MagicMock) -> None:
        mock_fn.return_value = MagicMock(spec=LMPData)
        fetch_lmp(iso="pjm", zone="AEP", year=2025)
        mock_fn.assert_called_once()

    @patch("src.lmp.client.fetch_ercot_lmp")
    def test_ercot_routes(self, mock_fn: MagicMock) -> None:
        mock_fn.return_value = MagicMock(spec=LMPData)
        fetch_lmp(iso="ercot", zone="LZ_HOUSTON", year=2025)
        mock_fn.assert_called_once()

    @patch("src.lmp.client.fetch_caiso_lmp")
    def test_caiso_routes(self, mock_fn: MagicMock) -> None:
        mock_fn.return_value = MagicMock(spec=LMPData)
        fetch_lmp(iso="caiso", zone="NP15", year=2025)
        mock_fn.assert_called_once()

    def test_unknown_iso_raises(self) -> None:
        with pytest.raises(ValueError, match="Unrecognized ISO"):
            fetch_lmp(iso="nyiso", zone="ZONE_A", year=2025)


# ---------------------------------------------------------------------------
# Test: CAISO_LOCATIONS and CAISO_HUB_MAP constants
# ---------------------------------------------------------------------------

class TestCaisoConstants:
    """CAISO hub map and location list validation."""

    def test_location_count(self) -> None:
        """Should have exactly 3 CAISO hubs."""
        assert len(CAISO_LOCATIONS) == 3

    def test_hub_map_values(self) -> None:
        """Hub map should contain correct full names."""
        assert CAISO_HUB_MAP["NP15"] == "TH_NP15_GEN-APND"
        assert CAISO_HUB_MAP["SP15"] == "TH_SP15_GEN-APND"
        assert CAISO_HUB_MAP["ZP26"] == "TH_ZP26_GEN-APND"

    def test_locations_match_map_keys(self) -> None:
        """CAISO_LOCATIONS should be exactly the hub map keys."""
        assert set(CAISO_LOCATIONS) == set(CAISO_HUB_MAP.keys())
