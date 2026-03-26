"""Tests for LMP caching layer (M14e)."""

from pathlib import Path

import pandas as pd
import pytest

from src.lmp.cache import CACHE_DIR, _cache_filename, get_cached_lmp, save_lmp_cache


class TestCacheFilename:
    """Cache filename generation."""

    def test_filename_pattern(self) -> None:
        """Filename should be lowercase with underscores."""
        result = _cache_filename("PJM", "AEP", "DAY_AHEAD_HOURLY", 2025)
        assert result == "pjm_aep_day_ahead_hourly_2025.csv"

    def test_filename_with_spaces(self) -> None:
        """Spaces in zone names should become underscores."""
        result = _cache_filename("pjm", "MID-ATL/APS", "DAY_AHEAD_HOURLY", 2024)
        assert result == "pjm_mid-atl/aps_day_ahead_hourly_2024.csv"

    def test_filename_all_lowercase(self) -> None:
        """All parts should be lowercased."""
        result = _cache_filename("PJM", "PECO", "REAL_TIME_HOURLY", 2023)
        assert result == "pjm_peco_real_time_hourly_2023.csv"


class TestCacheMiss:
    """Cache miss behavior."""

    def test_cache_miss_returns_none(self, tmp_path: Path) -> None:
        """get_cached_lmp should return None when file doesn't exist."""
        import src.lmp.cache as cache_module

        original_dir = cache_module.CACHE_DIR
        cache_module.CACHE_DIR = tmp_path / "nonexistent"
        try:
            result = get_cached_lmp("pjm", "AEP", "DAY_AHEAD_HOURLY", 2025)
            assert result is None
        finally:
            cache_module.CACHE_DIR = original_dir


class TestCacheRoundTrip:
    """Cache save and read round-trip."""

    def test_save_and_read(self, tmp_path: Path) -> None:
        """Saved data should be readable and match original."""
        import src.lmp.cache as cache_module

        original_dir = cache_module.CACHE_DIR
        cache_module.CACHE_DIR = tmp_path
        try:
            # Create test data
            timestamps = pd.date_range("2025-01-01", periods=8760, freq="h")
            prices = [30.0 + (i % 24) * 0.5 for i in range(8760)]
            df = pd.DataFrame({"timestamp": timestamps, "lmp": prices})

            # Save
            path = save_lmp_cache("pjm", "AEP", "DAY_AHEAD_HOURLY", 2025, df)
            assert path.exists()
            assert path.name == "pjm_aep_day_ahead_hourly_2025.csv"

            # Read back
            result = get_cached_lmp("pjm", "AEP", "DAY_AHEAD_HOURLY", 2025)
            assert result is not None
            assert len(result) == 8760
            assert list(result.columns) == ["timestamp", "lmp"]
            assert result["lmp"].iloc[0] == pytest.approx(30.0)
            assert result["lmp"].iloc[1] == pytest.approx(30.5)
        finally:
            cache_module.CACHE_DIR = original_dir

    def test_save_creates_directory(self, tmp_path: Path) -> None:
        """save_lmp_cache should create the cache directory if needed."""
        import src.lmp.cache as cache_module

        original_dir = cache_module.CACHE_DIR
        cache_module.CACHE_DIR = tmp_path / "nested" / "cache"
        try:
            df = pd.DataFrame(
                {"timestamp": pd.date_range("2025-01-01", periods=10, freq="h"),
                 "lmp": [30.0] * 10}
            )
            path = save_lmp_cache("pjm", "AEP", "DAY_AHEAD_HOURLY", 2025, df)
            assert path.exists()
            assert (tmp_path / "nested" / "cache").is_dir()
        finally:
            cache_module.CACHE_DIR = original_dir
