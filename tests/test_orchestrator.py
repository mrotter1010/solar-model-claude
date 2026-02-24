"""Tests for the climate data orchestrator."""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.climate.cache_manager import CacheManager
from src.climate.config import ClimateConfig
from src.climate.nsrdb_client import NSRDBClient
from src.climate.orchestrator import ClimateOrchestrator
from src.climate.precipitation_client import PrecipitationClient
from src.climate.weather_formatter import WeatherFormatter
from src.config.loader import load_config
from src.config.schema import SiteConfig
from src.utils.exceptions import ClimateDataError

# Realistic NSRDB CSV with 2-row metadata header (matches test_weather_formatter)
SAMPLE_NSRDB_CSV = """Source,Location ID,City,State,Country,Latitude,Longitude,Time Zone,Elevation
NSRDB,123456,Phoenix,AZ,United States,33.45,-111.98,-7,337
Year,Month,Day,Hour,Minute,GHI,DNI,DHI,Temperature,Wind Speed,Surface Albedo
2024,1,1,0,0,0,0,0,8.5,2.1,0.18
2024,1,1,1,0,0,0,0,7.9,1.8,0.18
2024,1,1,12,0,650,800,120,18.7,4.1,0.20
"""


@pytest.fixture()
def mock_client() -> MagicMock:
    """Create a mock NSRDBClient that returns sample CSV data."""
    client = MagicMock(spec=NSRDBClient)
    client.fetch_weather_data.return_value = SAMPLE_NSRDB_CSV
    return client


@pytest.fixture()
def orchestrator(tmp_path: Path, mock_client: MagicMock) -> ClimateOrchestrator:
    """Create a ClimateOrchestrator with mock client and real cache/formatter."""
    cache_manager = CacheManager(cache_dir=tmp_path / "cache")
    formatter = WeatherFormatter()
    return ClimateOrchestrator(
        nsrdb_client=mock_client,
        cache_manager=cache_manager,
        formatter=formatter,
    )


@pytest.fixture()
def sample_sites(sample_valid_csv: Path) -> list:
    """Load site configs from the shared sample_valid_csv fixture."""
    return load_config(sample_valid_csv)


class TestClimateConfig:
    """Tests for ClimateConfig Pydantic model."""

    def test_defaults(self) -> None:
        """Default values are set correctly."""
        config = ClimateConfig()
        assert config.api_key == "DEMO_KEY"
        assert config.api_email == "demo@example.com"
        assert config.default_year == 2024
        assert config.cache_max_age_days == 365
        assert config.cache_dir == Path("data/climate")
        assert config.max_cache_distance_km == 50.0

    def test_env_override_api_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """NSRDB_API_KEY env var overrides api_key field."""
        monkeypatch.setenv("NSRDB_API_KEY", "my-secret-key")
        config = ClimateConfig()
        assert config.api_key == "my-secret-key"

    def test_env_override_email(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """NSRDB_API_EMAIL env var overrides api_email field."""
        monkeypatch.setenv("NSRDB_API_EMAIL", "user@solar.com")
        config = ClimateConfig()
        assert config.api_email == "user@solar.com"

    def test_explicit_values_kept_without_env(self) -> None:
        """Explicit constructor values are preserved when env vars not set."""
        config = ClimateConfig(api_key="explicit-key", api_email="explicit@test.com")
        assert config.api_key == "explicit-key"
        assert config.api_email == "explicit@test.com"

    def test_ncei_token_default(self) -> None:
        """Default NCEI token is set."""
        config = ClimateConfig()
        assert config.ncei_token == "WewidNCeiBHMUnnVbgNyjKxxHCSXXCad"

    def test_ncei_token_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """NCEI_API_TOKEN env var overrides ncei_token field."""
        monkeypatch.setenv("NCEI_API_TOKEN", "my-ncei-token")
        config = ClimateConfig()
        assert config.ncei_token == "my-ncei-token"

    def test_precipitation_enabled_default(self) -> None:
        """Precipitation is enabled by default."""
        config = ClimateConfig()
        assert config.precipitation_enabled is True

    def test_max_station_distance_default(self) -> None:
        """Default max station distance is 100 km."""
        config = ClimateConfig()
        assert config.max_station_distance_km == 100.0


class TestDeduplication:
    """Tests for location deduplication in fetch_climate_data."""

    def test_deduplicates_shared_locations(
        self, orchestrator: ClimateOrchestrator, sample_sites: list, mock_client: MagicMock
    ) -> None:
        """3 sites with 2 unique locations results in only 2 API calls."""
        # sample_valid_csv has Phoenix (33.483, -112.073) x2 and Tucson (32.253, -110.911) x1
        results = orchestrator.fetch_climate_data(sample_sites)

        # Should have 2 unique location entries
        assert len(results) == 2
        # Should have called API exactly 2 times (not 3)
        assert mock_client.fetch_weather_data.call_count == 2


class TestCacheHits:
    """Tests for cache hit/miss behavior."""

    def test_full_cache_hit_skips_api(
        self, tmp_path: Path, mock_client: MagicMock, sample_sites: list
    ) -> None:
        """Fresh cache files for all locations → 0 API calls."""
        # Arrange — pre-populate cache with fresh files for both locations
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        today = datetime.now(timezone.utc).strftime("%Y%m%d")

        for site in sample_sites:
            lat, lon = site.latitude, site.longitude
            cache_file = cache_dir / f"nsrdb_{lat}_{lon}_{today}.csv"
            cache_file.write_text(SAMPLE_NSRDB_CSV)

        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(mock_client, cache_manager, formatter)

        # Act
        results = orch.fetch_climate_data(sample_sites)

        # Assert — no API calls made
        assert mock_client.fetch_weather_data.call_count == 0
        assert len(results) == 2

    def test_stale_cache_triggers_api_call(
        self, tmp_path: Path, mock_client: MagicMock, sample_sites: list
    ) -> None:
        """Stale cache file (>max_age_days) triggers a fresh API call."""
        # Arrange — create a stale cache file (400 days old)
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        old_date = "20240101"  # Old enough to be stale with default 365 days

        for site in sample_sites:
            lat, lon = site.latitude, site.longitude
            cache_file = cache_dir / f"nsrdb_{lat}_{lon}_{old_date}.csv"
            cache_file.write_text(SAMPLE_NSRDB_CSV)

        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(mock_client, cache_manager, formatter)

        # Act
        results = orch.fetch_climate_data(sample_sites)

        # Assert — API should be called for stale locations
        assert mock_client.fetch_weather_data.call_count == 2
        assert len(results) == 2


class TestApiFailureHandling:
    """Tests for interactive failure handling with mocked input()."""

    def test_retry_succeeds(
        self, orchestrator: ClimateOrchestrator, mock_client: MagicMock
    ) -> None:
        """User chooses retry and second attempt succeeds."""
        # First call fails, second succeeds
        mock_client.fetch_weather_data.side_effect = [
            ClimateDataError("API error"),
            SAMPLE_NSRDB_CSV,
        ]

        with patch("builtins.input", return_value="1"):
            path = orchestrator.handle_api_failure(
                33.45, -111.98, ClimateDataError("API error")
            )

        assert path.exists()

    def test_retry_exhausted_raises(
        self, orchestrator: ClimateOrchestrator, mock_client: MagicMock
    ) -> None:
        """All 3 retries fail → raises ClimateDataError."""
        mock_client.fetch_weather_data.side_effect = ClimateDataError("API error")

        with patch("builtins.input", return_value="1"):
            with pytest.raises(ClimateDataError, match="after 3 retries"):
                orchestrator.handle_api_failure(
                    33.45, -111.98, ClimateDataError("API error")
                )

        # Should have tried 3 times
        assert mock_client.fetch_weather_data.call_count == 3

    def test_abort_raises(
        self, orchestrator: ClimateOrchestrator, mock_client: MagicMock
    ) -> None:
        """User chooses abort → raises ClimateDataError."""
        # No nearest cache available, so abort is option "2"
        with patch("builtins.input", return_value="2"):
            with pytest.raises(ClimateDataError, match="aborted by user"):
                orchestrator.handle_api_failure(
                    33.45, -111.98, ClimateDataError("API error")
                )

    def test_abort_with_nearest_cache_option(
        self, tmp_path: Path, mock_client: MagicMock
    ) -> None:
        """User chooses abort when nearest cache option is also shown (option 3)."""
        # Create a nearby cache file to make option 2 available
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        cache_file = cache_dir / f"nsrdb_33.46_-111.97_{today}.csv"
        cache_file.write_text(SAMPLE_NSRDB_CSV)

        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(mock_client, cache_manager, formatter)

        with patch("builtins.input", return_value="3"):
            with pytest.raises(ClimateDataError, match="aborted by user"):
                orch.handle_api_failure(
                    33.45, -111.98, ClimateDataError("API error")
                )


class TestNearestCacheFallback:
    """Tests for nearest cache fallback during API failures."""

    def test_nearby_cache_shown_and_used(
        self, tmp_path: Path, mock_client: MagicMock
    ) -> None:
        """Cache within 50km is offered and user selects it."""
        # Create a cache file ~1km away
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        nearby_file = cache_dir / f"nsrdb_33.46_-111.97_{today}.csv"
        nearby_file.write_text(SAMPLE_NSRDB_CSV)

        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(mock_client, cache_manager, formatter)

        # User picks option 2 (use nearest cache)
        with patch("builtins.input", return_value="2"):
            path = orch.handle_api_failure(
                33.45, -111.98, ClimateDataError("API error")
            )

        assert path == nearby_file

    def test_distant_cache_not_offered(
        self, tmp_path: Path, mock_client: MagicMock
    ) -> None:
        """Cache beyond max_cache_distance_km is not offered as fallback."""
        # Create a cache file ~180km away (Phoenix to Tucson)
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        far_file = cache_dir / f"nsrdb_32.22_-110.97_{today}.csv"
        far_file.write_text(SAMPLE_NSRDB_CSV)

        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(mock_client, cache_manager, formatter)

        # When no nearby cache, option "2" is abort (not "use nearest")
        with patch("builtins.input", return_value="2"):
            with pytest.raises(ClimateDataError, match="aborted by user"):
                orch.handle_api_failure(
                    33.45, -111.98, ClimateDataError("API error")
                )


class TestIntegration:
    """Integration test using sample_valid_csv fixture."""

    def test_full_pipeline(
        self,
        sample_valid_csv: Path,
        tmp_path: Path,
        test_results_dir: Path,
    ) -> None:
        """End-to-end: load CSV → mock API → verify output mapping."""
        # Arrange
        sites = load_config(sample_valid_csv)

        mock_client = MagicMock(spec=NSRDBClient)
        mock_client.fetch_weather_data.return_value = SAMPLE_NSRDB_CSV

        cache_dir = tmp_path / "climate_cache"
        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()

        orch = ClimateOrchestrator(mock_client, cache_manager, formatter)

        # Act
        results = orch.fetch_climate_data(sites)

        # Assert — 2 unique locations, both mapped to paths
        assert len(results) == 2
        for (lat, lon), path in results.items():
            assert path.exists(), f"Weather file missing for ({lat}, {lon})"

        # Only 2 API calls for 3 sites
        assert mock_client.fetch_weather_data.call_count == 2

        # Write results to test_results_dir for manual inspection
        output = {
            "total_sites": len(sites),
            "unique_locations": len(results),
            "api_calls": mock_client.fetch_weather_data.call_count,
            "location_mapping": {
                f"({lat}, {lon})": str(path) for (lat, lon), path in results.items()
            },
        }
        output_path = test_results_dir / "test_orchestrator_integration.json"
        output_path.write_text(json.dumps(output, indent=2))


class TestFiveSiteDeduplication:
    """Tests for 5 sites / 3 unique locations deduplication."""

    def test_five_sites_three_locations(
        self,
        test_sites: list[SiteConfig],
        tmp_path: Path,
        climate_results_dir: Path,
    ) -> None:
        """5 sites at 3 unique locations results in exactly 3 API calls."""
        mock_client = MagicMock(spec=NSRDBClient)
        mock_client.fetch_weather_data.return_value = SAMPLE_NSRDB_CSV

        cache_manager = CacheManager(cache_dir=tmp_path / "cache")
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(mock_client, cache_manager, formatter)

        results = orch.fetch_climate_data(test_sites)

        # 3 unique locations: Phoenix, Tucson, Flagstaff
        assert len(results) == 3
        assert mock_client.fetch_weather_data.call_count == 3

        # Write deduplication log for inspection
        location_map: dict[str, list[str]] = {}
        for site in test_sites:
            key = f"({site.latitude}, {site.longitude})"
            location_map.setdefault(key, []).append(site.site_name)

        log_path = climate_results_dir / "api_deduplication_log.json"
        log_path.write_text(
            json.dumps(
                {
                    "total_sites": len(test_sites),
                    "unique_locations": len(results),
                    "api_calls": mock_client.fetch_weather_data.call_count,
                    "sites_per_location": location_map,
                },
                indent=2,
            )
        )


class TestMixedCacheScenario:
    """Tests for mixed cache hit/miss/stale scenarios."""

    def test_mixed_cache_fresh_stale_uncached(
        self,
        test_sites: list[SiteConfig],
        tmp_path: Path,
    ) -> None:
        """1 fresh cache hit, 1 stale miss, 1 uncached -> 2 API calls."""
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        today = datetime.now(timezone.utc).strftime("%Y%m%d")

        # Fresh cache for Phoenix (33.45, -111.98)
        phoenix_cache = cache_dir / f"nsrdb_33.45_-111.98_{today}.csv"
        phoenix_cache.write_text(SAMPLE_NSRDB_CSV)

        # Stale cache for Tucson (32.22, -110.97) — 400 days old
        tucson_cache = cache_dir / "nsrdb_32.22_-110.97_20240101.csv"
        tucson_cache.write_text(SAMPLE_NSRDB_CSV)

        # No cache for Flagstaff (35.20, -111.65)

        mock_client = MagicMock(spec=NSRDBClient)
        mock_client.fetch_weather_data.return_value = SAMPLE_NSRDB_CSV

        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(mock_client, cache_manager, formatter)

        results = orch.fetch_climate_data(test_sites)

        # Phoenix: cache hit (0 API calls)
        # Tucson: stale cache -> API call (1)
        # Flagstaff: no cache -> API call (2)
        assert len(results) == 3
        assert mock_client.fetch_weather_data.call_count == 2


class TestApiFailureInFetchClimateData:
    """Tests for API failure path within fetch_climate_data (lines 77-82)."""

    def test_api_failure_triggers_handle_api_failure(
        self,
        test_sites: list[SiteConfig],
        tmp_path: Path,
    ) -> None:
        """API failure in fetch_climate_data calls handle_api_failure."""
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        today = datetime.now(timezone.utc).strftime("%Y%m%d")

        # Create a nearby cache file for fallback
        nearby_cache = cache_dir / f"nsrdb_33.46_-111.97_{today}.csv"
        nearby_cache.write_text(SAMPLE_NSRDB_CSV)

        mock_client = MagicMock(spec=NSRDBClient)
        # All API calls fail
        mock_client.fetch_weather_data.side_effect = ClimateDataError(
            "API temporarily unavailable"
        )

        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(mock_client, cache_manager, formatter)

        # Use only one site so we can control the flow
        single_site = [test_sites[0]]  # Phoenix (33.45, -111.98)

        # User selects "use nearest cache" (option 2) when prompted
        with patch("builtins.input", return_value="2"):
            results = orch.fetch_climate_data(single_site)

        # Should have recovered using the nearby cache
        assert len(results) == 1
        path = list(results.values())[0]
        assert path == nearby_cache

    def test_api_failure_abort_in_fetch_climate_data(
        self,
        test_sites: list[SiteConfig],
        tmp_path: Path,
    ) -> None:
        """API failure + user abort in fetch_climate_data raises ClimateDataError."""
        mock_client = MagicMock(spec=NSRDBClient)
        mock_client.fetch_weather_data.side_effect = ClimateDataError("API down")

        cache_manager = CacheManager(cache_dir=tmp_path / "cache")
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(mock_client, cache_manager, formatter)

        single_site = [test_sites[0]]

        # User selects abort (option 2 when no nearby cache)
        with patch("builtins.input", return_value="2"):
            with pytest.raises(ClimateDataError, match="aborted by user"):
                orch.fetch_climate_data(single_site)


class TestEnhancedIntegration:
    """Enhanced integration tests with artifact output."""

    def test_full_pipeline_with_artifacts(
        self,
        test_sites: list[SiteConfig],
        tmp_path: Path,
        climate_results_dir: Path,
    ) -> None:
        """End-to-end test writing comprehensive artifacts to test_results/climate/."""
        mock_client = MagicMock(spec=NSRDBClient)
        mock_client.fetch_weather_data.return_value = SAMPLE_NSRDB_CSV

        cache_dir = tmp_path / "cache"
        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(mock_client, cache_manager, formatter)

        # Act — fetch for all 5 sites
        results = orch.fetch_climate_data(test_sites)

        # Assert
        assert len(results) == 3  # 3 unique locations
        assert mock_client.fetch_weather_data.call_count == 3
        for path in results.values():
            assert path.exists()

        # Write integration summary
        summary_path = climate_results_dir / "integration_summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "total_sites": len(test_sites),
                    "unique_locations": len(results),
                    "api_calls": mock_client.fetch_weather_data.call_count,
                    "cache_hits": 0,
                    "locations": [
                        {"lat": lat, "lon": lon, "file": str(path)}
                        for (lat, lon), path in results.items()
                    ],
                },
                indent=2,
            )
        )

        # Write cache listing
        cache_files = list(cache_dir.glob("nsrdb_*.csv"))
        cache_listing = []
        for f in cache_files:
            cache_listing.append({"filename": f.name, "size_bytes": f.stat().st_size})

        listing_path = climate_results_dir / "cache_listing.json"
        listing_path.write_text(json.dumps(cache_listing, indent=2))


class TestPrecipitationIntegration:
    """Tests for precipitation client integration in the orchestrator."""

    def test_without_precip_client_backward_compatible(
        self,
        test_sites: list[SiteConfig],
        tmp_path: Path,
    ) -> None:
        """Orchestrator without precipitation client works as before."""
        mock_client = MagicMock(spec=NSRDBClient)
        mock_client.fetch_weather_data.return_value = SAMPLE_NSRDB_CSV

        cache_manager = CacheManager(cache_dir=tmp_path / "cache")
        formatter = WeatherFormatter()
        # No precipitation_client argument — backward compatible
        orch = ClimateOrchestrator(mock_client, cache_manager, formatter)

        results = orch.fetch_climate_data(test_sites)

        assert len(results) == 3
        assert mock_client.fetch_weather_data.call_count == 3

    def test_precip_client_called_on_cache_miss(
        self,
        test_sites: list[SiteConfig],
        tmp_path: Path,
    ) -> None:
        """Precipitation client called for each unique location on cache miss."""
        mock_client = MagicMock(spec=NSRDBClient)
        mock_client.fetch_weather_data.return_value = SAMPLE_NSRDB_CSV

        mock_precip = MagicMock(spec=PrecipitationClient)
        mock_precip.fetch_precipitation.return_value = None

        cache_manager = CacheManager(cache_dir=tmp_path / "cache")
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(
            mock_client, cache_manager, formatter,
            precipitation_client=mock_precip,
        )

        results = orch.fetch_climate_data(test_sites)

        # 3 unique locations, all cache misses → 3 precip calls
        assert mock_precip.fetch_precipitation.call_count == 3
        assert len(results) == 3

    def test_precip_failure_doesnt_break_pipeline(
        self,
        test_sites: list[SiteConfig],
        tmp_path: Path,
    ) -> None:
        """Precipitation failure (returns None) doesn't break the pipeline."""
        mock_client = MagicMock(spec=NSRDBClient)
        mock_client.fetch_weather_data.return_value = SAMPLE_NSRDB_CSV

        mock_precip = MagicMock(spec=PrecipitationClient)
        mock_precip.fetch_precipitation.return_value = None

        cache_manager = CacheManager(cache_dir=tmp_path / "cache")
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(
            mock_client, cache_manager, formatter,
            precipitation_client=mock_precip,
        )

        results = orch.fetch_climate_data(test_sites)

        # Pipeline completes successfully despite precip failures
        assert len(results) == 3
        for path in results.values():
            assert path.exists()

    def test_cache_hits_skip_precipitation_fetch(
        self,
        test_sites: list[SiteConfig],
        tmp_path: Path,
    ) -> None:
        """Cache hits skip precipitation fetch entirely."""
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        today = datetime.now(timezone.utc).strftime("%Y%m%d")

        # Pre-populate cache for all locations
        for site in test_sites:
            lat, lon = site.latitude, site.longitude
            cache_file = cache_dir / f"nsrdb_{lat}_{lon}_{today}.csv"
            cache_file.write_text(SAMPLE_NSRDB_CSV)

        mock_client = MagicMock(spec=NSRDBClient)
        mock_precip = MagicMock(spec=PrecipitationClient)

        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(
            mock_client, cache_manager, formatter,
            precipitation_client=mock_precip,
        )

        results = orch.fetch_climate_data(test_sites)

        # All cache hits — no API calls, no precipitation calls
        assert mock_client.fetch_weather_data.call_count == 0
        assert mock_precip.fetch_precipitation.call_count == 0
        assert len(results) == 3
