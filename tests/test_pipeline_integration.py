"""End-to-end integration tests for the climate data pipeline."""

import json
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.climate.cache_manager import CacheManager
from src.climate.nsrdb_client import NSRDBClient
from src.climate.orchestrator import ClimateOrchestrator
from src.climate.weather_formatter import WeatherFormatter
from src.config.loader import load_config
from src.pipeline import print_summary, run_climate_data_pipeline
from tests.conftest import generate_mock_nsrdb_csv

# Real CSV files in the repo
SINGLE_ROW_CSV = Path(
    "Energy Analytics Inputs Single Row Test - Sheet1.csv"
)
MULTI_ROW_CSV = Path(
    "Energy Analytics Inputs Multi Row Test - Sheet1.csv"
)


class TestSingleSitePipeline:
    """Test pipeline with a single-site CSV (Phoenix 33.483, -112.073)."""

    def test_single_site_weather_file_assigned(self, tmp_path: Path) -> None:
        """Pipeline assigns weather_file_path to the single site."""
        mock_nsrdb = MagicMock(spec=NSRDBClient)
        mock_nsrdb.fetch_weather_data.return_value = generate_mock_nsrdb_csv(
            lat=33.483, lon=-112.073, year=2024, num_hours=8760
        )

        cache_dir = tmp_path / "cache"
        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()

        orchestrator = ClimateOrchestrator(
            nsrdb_client=mock_nsrdb,
            cache_manager=cache_manager,
            formatter=formatter,
        )

        sites = load_config(SINGLE_ROW_CSV)
        location_to_file = orchestrator.fetch_climate_data(sites, year=2024)

        # Assign weather file paths
        for site in sites:
            if site.location in location_to_file:
                site.weather_file_path = location_to_file[site.location]

        assert len(sites) == 1
        assert sites[0].weather_file_path is not None
        assert sites[0].has_climate_data is True
        mock_nsrdb.fetch_weather_data.assert_called_once()

    def test_single_site_correct_location(self) -> None:
        """Single-row CSV parses the expected Phoenix coordinates."""
        sites = load_config(SINGLE_ROW_CSV)
        assert len(sites) == 1
        assert sites[0].latitude == 33.483
        assert sites[0].longitude == -112.073


class TestMultiSitePipeline:
    """Test pipeline with multi-site CSV (2 sites at same Phoenix location)."""

    def test_deduplication_single_api_call(self, tmp_path: Path) -> None:
        """Two sites at the same location should trigger only 1 API call."""
        mock_nsrdb = MagicMock(spec=NSRDBClient)
        mock_nsrdb.fetch_weather_data.return_value = generate_mock_nsrdb_csv(
            lat=33.483, lon=-112.073, year=2024, num_hours=8760
        )

        cache_dir = tmp_path / "cache"
        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()

        orchestrator = ClimateOrchestrator(
            nsrdb_client=mock_nsrdb,
            cache_manager=cache_manager,
            formatter=formatter,
        )

        sites = load_config(MULTI_ROW_CSV)
        location_to_file = orchestrator.fetch_climate_data(sites, year=2024)

        # Both sites share the same location — only 1 API call
        assert len(sites) == 2
        assert mock_nsrdb.fetch_weather_data.call_count == 1

    def test_both_sites_get_weather_path(self, tmp_path: Path) -> None:
        """Both sites at the same location get weather_file_path assigned."""
        mock_nsrdb = MagicMock(spec=NSRDBClient)
        mock_nsrdb.fetch_weather_data.return_value = generate_mock_nsrdb_csv(
            lat=33.483, lon=-112.073, year=2024, num_hours=8760
        )

        cache_dir = tmp_path / "cache"
        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()

        orchestrator = ClimateOrchestrator(
            nsrdb_client=mock_nsrdb,
            cache_manager=cache_manager,
            formatter=formatter,
        )

        sites = load_config(MULTI_ROW_CSV)
        location_to_file = orchestrator.fetch_climate_data(sites, year=2024)

        for site in sites:
            if site.location in location_to_file:
                site.weather_file_path = location_to_file[site.location]

        # Both sites should have the same weather file
        assert sites[0].weather_file_path is not None
        assert sites[1].weather_file_path is not None
        assert sites[0].weather_file_path == sites[1].weather_file_path
        assert sites[0].has_climate_data is True
        assert sites[1].has_climate_data is True


class TestCachePersistence:
    """Test that a second pipeline run uses cache instead of API."""

    def test_second_run_zero_api_calls(self, tmp_path: Path) -> None:
        """Running pipeline twice with the same cache dir skips API on second run."""
        mock_nsrdb = MagicMock(spec=NSRDBClient)
        mock_nsrdb.fetch_weather_data.return_value = generate_mock_nsrdb_csv(
            lat=33.483, lon=-112.073, year=2024, num_hours=8760
        )

        cache_dir = tmp_path / "cache"
        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()

        orchestrator = ClimateOrchestrator(
            nsrdb_client=mock_nsrdb,
            cache_manager=cache_manager,
            formatter=formatter,
        )

        sites = load_config(SINGLE_ROW_CSV)

        # First run — should call the API
        orchestrator.fetch_climate_data(sites, year=2024)
        assert mock_nsrdb.fetch_weather_data.call_count == 1

        # Second run — should use cache, no new API calls
        orchestrator.fetch_climate_data(sites, year=2024)
        assert mock_nsrdb.fetch_weather_data.call_count == 1


class TestWeatherFileFormat:
    """Validate the PySAM weather file format produced by the pipeline."""

    def test_pysam_header_format(self, tmp_path: Path) -> None:
        """PySAM weather file has Latitude/Longitude header line."""
        mock_nsrdb = MagicMock(spec=NSRDBClient)
        mock_nsrdb.fetch_weather_data.return_value = generate_mock_nsrdb_csv(
            lat=33.483, lon=-112.073, year=2024, num_hours=8760
        )

        cache_dir = tmp_path / "cache"
        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()

        orchestrator = ClimateOrchestrator(
            nsrdb_client=mock_nsrdb,
            cache_manager=cache_manager,
            formatter=formatter,
        )

        sites = load_config(SINGLE_ROW_CSV)
        location_to_file = orchestrator.fetch_climate_data(sites, year=2024)

        # Find the PySAM-formatted file
        cache_path = location_to_file[(33.483, -112.073)]
        pysam_path = cache_path.with_suffix(".pysam.csv")
        assert pysam_path.exists()

        lines = pysam_path.read_text().splitlines()

        # Line 1: header labels
        assert "Latitude" in lines[0]
        assert "Longitude" in lines[0]

        # Line 2: header values with correct coordinates
        assert "33.483" in lines[1]
        assert "-112.073" in lines[1]

    def test_pysam_correct_columns(self, tmp_path: Path) -> None:
        """PySAM weather file has the expected column set."""
        mock_nsrdb = MagicMock(spec=NSRDBClient)
        mock_nsrdb.fetch_weather_data.return_value = generate_mock_nsrdb_csv(
            lat=33.483, lon=-112.073, year=2024, num_hours=8760
        )

        cache_dir = tmp_path / "cache"
        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()

        orchestrator = ClimateOrchestrator(
            nsrdb_client=mock_nsrdb,
            cache_manager=cache_manager,
            formatter=formatter,
        )

        sites = load_config(SINGLE_ROW_CSV)
        location_to_file = orchestrator.fetch_climate_data(sites, year=2024)

        cache_path = location_to_file[(33.483, -112.073)]
        pysam_path = cache_path.with_suffix(".pysam.csv")
        lines = pysam_path.read_text().splitlines()

        # Line 3: column names
        columns = lines[2].split(",")
        expected = [
            "Year", "Month", "Day", "Hour", "Minute",
            "GHI", "DNI", "DHI", "Temperature", "Wind Speed",
            "Surface Albedo", "Precipitation",
        ]
        assert columns == expected

    def test_pysam_8760_rows(self, tmp_path: Path) -> None:
        """PySAM weather file has 8760 data rows (1 year of hourly data)."""
        mock_nsrdb = MagicMock(spec=NSRDBClient)
        mock_nsrdb.fetch_weather_data.return_value = generate_mock_nsrdb_csv(
            lat=33.483, lon=-112.073, year=2024, num_hours=8760
        )

        cache_dir = tmp_path / "cache"
        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()

        orchestrator = ClimateOrchestrator(
            nsrdb_client=mock_nsrdb,
            cache_manager=cache_manager,
            formatter=formatter,
        )

        sites = load_config(SINGLE_ROW_CSV)
        location_to_file = orchestrator.fetch_climate_data(sites, year=2024)

        cache_path = location_to_file[(33.483, -112.073)]
        pysam_path = cache_path.with_suffix(".pysam.csv")
        lines = pysam_path.read_text().splitlines()

        # 2 header lines + 1 column names line + 8760 data rows
        data_rows = len(lines) - 3
        assert data_rows == 8760


class TestPipelineSummary:
    """Test the print_summary logging output."""

    def test_summary_logs_expected_info(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """print_summary logs total sites, unique locations, and data count."""
        mock_nsrdb = MagicMock(spec=NSRDBClient)
        mock_nsrdb.fetch_weather_data.return_value = generate_mock_nsrdb_csv(
            lat=33.483, lon=-112.073, year=2024, num_hours=24
        )

        cache_dir = tmp_path / "cache"
        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()

        orchestrator = ClimateOrchestrator(
            nsrdb_client=mock_nsrdb,
            cache_manager=cache_manager,
            formatter=formatter,
        )

        sites = load_config(MULTI_ROW_CSV)
        location_to_file = orchestrator.fetch_climate_data(sites, year=2024)

        for site in sites:
            if site.location in location_to_file:
                site.weather_file_path = location_to_file[site.location]

        import logging
        with caplog.at_level(logging.INFO):
            print_summary(sites, location_to_file)

        assert "2 total sites" in caplog.text
        assert "1 unique locations" in caplog.text
        assert "2 sites with weather data assigned" in caplog.text


class TestRunClimatePipeline:
    """Test the top-level run_climate_data_pipeline function."""

    @patch("src.pipeline.ClimateConfig")
    @patch("src.pipeline.NSRDBClient")
    @patch("src.pipeline.CacheManager")
    @patch("src.pipeline.PrecipitationClient")
    def test_full_pipeline_function(
        self,
        mock_precip_cls: MagicMock,
        mock_cache_cls: MagicMock,
        mock_nsrdb_cls: MagicMock,
        mock_config_cls: MagicMock,
        tmp_path: Path,
    ) -> None:
        """run_climate_data_pipeline loads CSV, fetches data, assigns paths."""
        # Configure mock config
        mock_config = MagicMock()
        mock_config.api_key = "test-key"
        mock_config.api_email = "test@test.com"
        mock_config.cache_dir = tmp_path / "cache"
        mock_config.precipitation_enabled = False
        mock_config.cache_max_age_days = 365
        mock_config.max_cache_distance_km = 50.0
        mock_config.ncei_token = "test-token"
        mock_config_cls.return_value = mock_config

        # Configure mock NSRDB client
        mock_nsrdb = MagicMock(spec=NSRDBClient)
        mock_nsrdb.fetch_weather_data.return_value = generate_mock_nsrdb_csv(
            lat=33.483, lon=-112.073, year=2024, num_hours=24
        )
        mock_nsrdb_cls.return_value = mock_nsrdb

        # Configure mock cache manager
        mock_cache = CacheManager(cache_dir=tmp_path / "cache")
        mock_cache_cls.return_value = mock_cache

        sites = run_climate_data_pipeline(SINGLE_ROW_CSV, year=2024)

        assert len(sites) == 1
        assert sites[0].weather_file_path is not None
        assert sites[0].site_name == "SiteTest_Phoenix"


class TestIntegrationArtifacts:
    """Write test artifacts to outputs/test_results/ for inspection."""

    def test_write_integration_summary(
        self, tmp_path: Path, test_results_dir: Path
    ) -> None:
        """Write milestone 2 integration summary JSON."""
        mock_nsrdb = MagicMock(spec=NSRDBClient)
        mock_nsrdb.fetch_weather_data.return_value = generate_mock_nsrdb_csv(
            lat=33.483, lon=-112.073, year=2024, num_hours=8760
        )

        cache_dir = tmp_path / "cache"
        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()

        orchestrator = ClimateOrchestrator(
            nsrdb_client=mock_nsrdb,
            cache_manager=cache_manager,
            formatter=formatter,
        )

        # Run with multi-row CSV
        sites = load_config(MULTI_ROW_CSV)
        location_to_file = orchestrator.fetch_climate_data(sites, year=2024)

        for site in sites:
            if site.location in location_to_file:
                site.weather_file_path = location_to_file[site.location]

        # Write summary JSON
        summary = {
            "milestone": 2,
            "test": "integration",
            "total_sites": len(sites),
            "unique_locations": len(location_to_file),
            "sites_with_weather_data": sum(
                1 for s in sites if s.weather_file_path is not None
            ),
            "api_calls": mock_nsrdb.fetch_weather_data.call_count,
            "sites": [
                {
                    "site_name": s.site_name,
                    "location": list(s.location),
                    "has_climate_data": s.has_climate_data,
                    "weather_file": str(s.weather_file_path),
                }
                for s in sites
            ],
        }

        summary_path = test_results_dir / "milestone2_integration.json"
        summary_path.write_text(json.dumps(summary, indent=2))
        assert summary_path.exists()

    def test_write_sample_weather_files(
        self, tmp_path: Path, test_results_dir: Path
    ) -> None:
        """Copy generated weather CSV to test results for inspection."""
        mock_nsrdb = MagicMock(spec=NSRDBClient)
        mock_nsrdb.fetch_weather_data.return_value = generate_mock_nsrdb_csv(
            lat=33.483, lon=-112.073, year=2024, num_hours=8760
        )

        cache_dir = tmp_path / "cache"
        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()

        orchestrator = ClimateOrchestrator(
            nsrdb_client=mock_nsrdb,
            cache_manager=cache_manager,
            formatter=formatter,
        )

        sites = load_config(SINGLE_ROW_CSV)
        location_to_file = orchestrator.fetch_climate_data(sites, year=2024)

        # Copy weather files to test results
        sample_dir = test_results_dir / "climate" / "sample_weather_files"
        sample_dir.mkdir(parents=True, exist_ok=True)

        for (lat, lon), path in location_to_file.items():
            shutil.copy2(path, sample_dir / path.name)
            # Also copy PySAM version if it exists
            pysam_path = path.with_suffix(".pysam.csv")
            if pysam_path.exists():
                shutil.copy2(pysam_path, sample_dir / pysam_path.name)

        assert any(sample_dir.iterdir())

    def test_write_integration_log(
        self, tmp_path: Path, test_results_dir: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Write pipeline log output to test results."""
        mock_nsrdb = MagicMock(spec=NSRDBClient)
        mock_nsrdb.fetch_weather_data.return_value = generate_mock_nsrdb_csv(
            lat=33.483, lon=-112.073, year=2024, num_hours=24
        )

        cache_dir = tmp_path / "cache"
        cache_manager = CacheManager(cache_dir=cache_dir)
        formatter = WeatherFormatter()

        orchestrator = ClimateOrchestrator(
            nsrdb_client=mock_nsrdb,
            cache_manager=cache_manager,
            formatter=formatter,
        )

        import logging
        with caplog.at_level(logging.DEBUG):
            sites = load_config(SINGLE_ROW_CSV)
            location_to_file = orchestrator.fetch_climate_data(sites, year=2024)

            for site in sites:
                if site.location in location_to_file:
                    site.weather_file_path = location_to_file[site.location]

            print_summary(sites, location_to_file)

        log_dir = test_results_dir / "climate"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "integration_log.txt"
        log_path.write_text(caplog.text)
        assert log_path.exists()
