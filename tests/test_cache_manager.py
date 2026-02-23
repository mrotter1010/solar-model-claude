"""Tests for the weather data cache manager."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.climate.cache_manager import CacheManager, _calculate_distance


class TestGetCachedFile:
    """Tests for exact-match cache lookups."""

    def test_cache_hit(self, tmp_path: Path) -> None:
        """Returns path when exact lat/lon match found and file is fresh."""
        # Arrange — create a cache file dated today
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        cache_file = cache_dir / f"nsrdb_33.45_-111.98_{today}.csv"
        cache_file.write_text("fake,data")

        manager = CacheManager(cache_dir=cache_dir)

        # Act
        result = manager.get_cached_file(lat=33.45, lon=-111.98)

        # Assert
        assert result == cache_file

    def test_cache_miss_no_file(self, tmp_path: Path) -> None:
        """Returns None when no matching file exists."""
        manager = CacheManager(cache_dir=tmp_path)
        result = manager.get_cached_file(lat=33.45, lon=-111.98)
        assert result is None

    def test_cache_miss_different_coords(self, tmp_path: Path) -> None:
        """Returns None when files exist but coords don't match."""
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        cache_file = tmp_path / f"nsrdb_40.0_-100.0_{today}.csv"
        cache_file.write_text("fake,data")

        manager = CacheManager(cache_dir=tmp_path)
        result = manager.get_cached_file(lat=33.45, lon=-111.98)
        assert result is None

    def test_stale_file_rejected(self, tmp_path: Path) -> None:
        """Returns None when cached file exceeds max_age_days."""
        # Create file dated 400 days ago
        old_date = (datetime.now(timezone.utc) - timedelta(days=400)).strftime(
            "%Y%m%d"
        )
        cache_file = tmp_path / f"nsrdb_33.45_-111.98_{old_date}.csv"
        cache_file.write_text("fake,data")

        manager = CacheManager(cache_dir=tmp_path)
        result = manager.get_cached_file(lat=33.45, lon=-111.98, max_age_days=365)
        assert result is None

    def test_fresh_file_accepted(self, tmp_path: Path) -> None:
        """Returns path when file is within max_age_days."""
        recent_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime(
            "%Y%m%d"
        )
        cache_file = tmp_path / f"nsrdb_33.45_-111.98_{recent_date}.csv"
        cache_file.write_text("fake,data")

        manager = CacheManager(cache_dir=tmp_path)
        result = manager.get_cached_file(lat=33.45, lon=-111.98, max_age_days=365)
        assert result == cache_file


class TestFindNearestCache:
    """Tests for proximity-based cache lookups."""

    def test_nearest_within_range(self, tmp_path: Path) -> None:
        """Returns nearest file when within max_distance_km."""
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        # Phoenix area — ~0.1 degree apart is ~10km
        cache_file = tmp_path / f"nsrdb_33.45_-111.98_{today}.csv"
        cache_file.write_text("data")

        manager = CacheManager(cache_dir=tmp_path)

        # Slightly different location, should be within 50km
        result = manager.find_nearest_cache(lat=33.46, lon=-111.97)
        assert result is not None
        path, distance = result
        assert path == cache_file
        assert distance < 50.0

    def test_no_match_beyond_range(self, tmp_path: Path) -> None:
        """Returns None when no cached file is within max_distance_km."""
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        # Phoenix
        cache_file = tmp_path / f"nsrdb_33.45_-111.98_{today}.csv"
        cache_file.write_text("data")

        manager = CacheManager(cache_dir=tmp_path)

        # Tucson is ~180km from Phoenix — outside 50km default
        result = manager.find_nearest_cache(lat=32.25, lon=-110.91)
        assert result is None

    def test_picks_closest_of_multiple(self, tmp_path: Path) -> None:
        """Returns the closest file when multiple are within range."""
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        far_file = tmp_path / f"nsrdb_33.50_-112.10_{today}.csv"
        far_file.write_text("data")
        near_file = tmp_path / f"nsrdb_33.46_-111.99_{today}.csv"
        near_file.write_text("data")

        manager = CacheManager(cache_dir=tmp_path)
        result = manager.find_nearest_cache(
            lat=33.45, lon=-111.98, max_distance_km=100
        )

        assert result is not None
        path, _ = result
        assert path == near_file

    def test_empty_cache_dir(self, tmp_path: Path) -> None:
        """Returns None when cache directory is empty."""
        manager = CacheManager(cache_dir=tmp_path)
        result = manager.find_nearest_cache(lat=33.45, lon=-111.98)
        assert result is None


class TestSaveWeatherData:
    """Tests for saving weather data to cache."""

    def test_save_creates_file(self, tmp_path: Path) -> None:
        """Saves data and returns path with correct filename format."""
        manager = CacheManager(cache_dir=tmp_path)
        data = "Year,Month,Day\n2024,1,1"

        result = manager.save_weather_data(lat=33.45, lon=-111.98, data=data)

        assert result.exists()
        assert result.read_text() == data
        # Verify filename format: nsrdb_{lat}_{lon}_{YYYYMMDD}.csv
        assert result.name.startswith("nsrdb_33.45_-111.98_")
        assert result.name.endswith(".csv")

    def test_save_filename_contains_date(self, tmp_path: Path) -> None:
        """Saved file name includes today's date."""
        manager = CacheManager(cache_dir=tmp_path)
        today = datetime.now(timezone.utc).strftime("%Y%m%d")

        result = manager.save_weather_data(lat=33.45, lon=-111.98, data="x")

        assert today in result.name

    def test_save_creates_cache_dir(self, tmp_path: Path) -> None:
        """CacheManager creates cache directory if it doesn't exist."""
        cache_dir = tmp_path / "nested" / "cache"
        manager = CacheManager(cache_dir=cache_dir)

        assert cache_dir.exists()


class TestCalculateDistance:
    """Tests for the Haversine distance function."""

    def test_same_point_zero_distance(self) -> None:
        """Distance between identical points is zero."""
        assert _calculate_distance(33.45, -111.98, 33.45, -111.98) == 0.0

    def test_known_city_pair_phoenix_tucson(self) -> None:
        """Phoenix to Tucson is approximately 180 km."""
        # Phoenix: 33.45, -112.07  |  Tucson: 32.22, -110.97
        distance = _calculate_distance(33.45, -112.07, 32.22, -110.97)
        assert 150 < distance < 200  # ~180 km

    def test_known_city_pair_la_sf(self) -> None:
        """Los Angeles to San Francisco is approximately 560 km."""
        # LA: 34.05, -118.24  |  SF: 37.77, -122.42
        distance = _calculate_distance(34.05, -118.24, 37.77, -122.42)
        assert 500 < distance < 620  # ~560 km

    def test_symmetry(self) -> None:
        """Distance A→B equals distance B→A."""
        d1 = _calculate_distance(33.45, -111.98, 40.71, -74.01)
        d2 = _calculate_distance(40.71, -74.01, 33.45, -111.98)
        assert d1 == pytest.approx(d2)
