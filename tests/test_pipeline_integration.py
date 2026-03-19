"""End-to-end integration tests for the climate data pipeline."""

import json
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.climate.cache_manager import CacheManager
from src.climate.nsrdb_client import NSRDBClient
from src.climate.orchestrator import ClimateOrchestrator
from src.climate.weather_formatter import WeatherFormatter
from src.config.loader import load_config
from src.config.schema import SiteConfig
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
        location_results = orchestrator.fetch_climate_data(sites, year=2024)

        # NSRDB results now return weather_df + weather_metadata (no file)
        assert len(sites) == 1
        result = location_results[sites[0].location]
        assert isinstance(result["weather_df"], pd.DataFrame)
        assert isinstance(result["weather_metadata"], dict)
        assert result["data_source"] == "nsrdb"
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
        location_results = orchestrator.fetch_climate_data(sites, year=2024)

        # Both sites share the same location — single result entry
        assert len(location_results) == 1
        result = list(location_results.values())[0]
        assert isinstance(result["weather_df"], pd.DataFrame)
        assert isinstance(result["weather_metadata"], dict)

        # Both sites map to the same location key
        assert sites[0].location in location_results
        assert sites[1].location in location_results
        assert sites[0].location == sites[1].location


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
        location_results = orchestrator.fetch_climate_data(sites, year=2024)

        # Save weather_df to file using formatter (orchestrator no longer writes files)
        result = location_results[(33.483, -112.073)]
        pysam_path = tmp_path / "pysam_output.csv"
        formatter.save_to_csv(
            result["weather_df"], pysam_path, 33.483, -112.073,
            metadata=result["weather_metadata"],
        )
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
        location_results = orchestrator.fetch_climate_data(sites, year=2024)

        # Save weather_df to file using formatter (orchestrator no longer writes files)
        result = location_results[(33.483, -112.073)]
        pysam_path = tmp_path / "pysam_output.csv"
        formatter.save_to_csv(
            result["weather_df"], pysam_path, 33.483, -112.073,
            metadata=result["weather_metadata"],
        )
        lines = pysam_path.read_text().splitlines()

        # Line 3: column names
        columns = lines[2].split(",")
        expected = [
            "Year", "Month", "Day", "Hour", "Minute",
            "GHI", "DNI", "DHI", "Temperature", "Wind Speed",
            "Surface Albedo", "Snow Depth",
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
        location_results = orchestrator.fetch_climate_data(sites, year=2024)

        # Save weather_df to file using formatter (orchestrator no longer writes files)
        result = location_results[(33.483, -112.073)]
        pysam_path = tmp_path / "pysam_output.csv"
        formatter.save_to_csv(
            result["weather_df"], pysam_path, 33.483, -112.073,
            metadata=result["weather_metadata"],
        )
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
        location_results = orchestrator.fetch_climate_data(sites, year=2024)

        # Save weather_df to file and assign to sites (simulates pipeline behavior)
        for site in sites:
            if site.location in location_results:
                result = location_results[site.location]
                pysam_path = tmp_path / f"pysam_{site.latitude}_{site.longitude}.csv"
                formatter.save_to_csv(
                    result["weather_df"], pysam_path, site.latitude, site.longitude,
                    metadata=result["weather_metadata"],
                )
                site.weather_file_path = pysam_path

        import logging
        with caplog.at_level(logging.INFO):
            print_summary(sites, location_results)

        assert "2 total sites" in caplog.text
        assert "1 unique locations" in caplog.text
        assert "2 sites with weather data assigned" in caplog.text


class TestRunClimatePipeline:
    """Test the top-level run_climate_data_pipeline function."""

    @patch("src.pipeline.ClimateConfig")
    @patch("src.pipeline.NSRDBClient")
    @patch("src.pipeline.CacheManager")
    @patch("src.pipeline.fetch_era5_land_data")
    @patch("src.pipeline.get_elevation_m", return_value=340.0)
    @patch("src.pipeline.apply_bias_correction")
    def test_full_pipeline_function(
        self,
        mock_apply: MagicMock,
        mock_elev: MagicMock,
        mock_era5: MagicMock,
        mock_cache_cls: MagicMock,
        mock_nsrdb_cls: MagicMock,
        mock_config_cls: MagicMock,
        tmp_path: Path,
    ) -> None:
        """run_climate_data_pipeline loads CSV, fetches data, assigns paths."""
        # Configure mock ERA5 client (avoid real ARCO Zarr network call)
        mock_era5.return_value = {
            "snow_depth_cm": [0.0] * 8760,
            "monthly_precip_inches": [1.0] * 12,
        }

        # Configure mock config
        mock_config = MagicMock()
        mock_config.api_key = "test-key"
        mock_config.api_email = "test@test.com"
        mock_config.cache_dir = tmp_path / "cache"
        mock_config.cache_max_age_days = 365
        mock_config.max_cache_distance_km = 50.0
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

        # Mock bias correction to return identity transform + metadata
        def fake_apply(df, lat, lon, elev):
            meta = {
                "bias_correction_applied": True,
                "bias_correction_model_version": "1.0.0",
                "mean_ghi_correction_factor": 1.0,
                "mean_dni_correction_factor": 1.0,
                "monthly_ghi_factors": {str(m): 1.0 for m in range(1, 13)},
                "monthly_dni_factors": {str(m): 1.0 for m in range(1, 13)},
            }
            return df.copy(), meta
        mock_apply.side_effect = fake_apply

        sites, _soiling_lookup, _bias_lookup = run_climate_data_pipeline(
            SINGLE_ROW_CSV, year=2024,
        )

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
        location_results = orchestrator.fetch_climate_data(sites, year=2024)

        # Save weather_df to file and assign to sites (simulates pipeline behavior)
        for site in sites:
            if site.location in location_results:
                result = location_results[site.location]
                pysam_path = tmp_path / f"pysam_{site.latitude}_{site.longitude}.csv"
                if not pysam_path.exists():
                    formatter.save_to_csv(
                        result["weather_df"], pysam_path,
                        site.latitude, site.longitude,
                        metadata=result["weather_metadata"],
                    )
                site.weather_file_path = pysam_path

        # Write summary JSON
        summary = {
            "milestone": 2,
            "test": "integration",
            "total_sites": len(sites),
            "unique_locations": len(location_results),
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
        location_results = orchestrator.fetch_climate_data(sites, year=2024)

        # Save weather_df to files and copy to test results
        sample_dir = test_results_dir / "climate" / "sample_weather_files"
        sample_dir.mkdir(parents=True, exist_ok=True)

        for (lat, lon), result in location_results.items():
            pysam_path = tmp_path / f"pysam_{lat}_{lon}.csv"
            formatter.save_to_csv(
                result["weather_df"], pysam_path, lat, lon,
                metadata=result["weather_metadata"],
            )
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
            location_results = orchestrator.fetch_climate_data(sites, year=2024)

            # Save weather_df to file and assign to sites
            for site in sites:
                if site.location in location_results:
                    result = location_results[site.location]
                    pysam_path = tmp_path / f"pysam_{site.latitude}_{site.longitude}.csv"
                    formatter.save_to_csv(
                        result["weather_df"], pysam_path,
                        site.latitude, site.longitude,
                        metadata=result["weather_metadata"],
                    )
                    site.weather_file_path = pysam_path

            print_summary(sites, location_results)

        log_dir = test_results_dir / "climate"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "integration_log.txt"
        log_path.write_text(caplog.text)
        assert log_path.exists()


class TestDataSourceWiring:
    """Tests that data_source and solcast_metadata flow from orchestrator to SiteConfig."""

    SOLCAST_FIXTURE = Path(__file__).parent / "fixtures" / "synthetic_solcast_tmy_phoenix.csv"
    PHOENIX_LAT = 33.483
    PHOENIX_LON = -112.073

    def test_default_data_source_is_nsrdb(self) -> None:
        """SiteConfig defaults data_source to 'nsrdb' when not explicitly set."""
        site = SiteConfig(
            run_name="Test", site_name="TestSite", customer="TestCo",
            latitude=33.0, longitude=-112.0, dc_size_mw=10.0,
            ac_installed_mw=8.0, ac_poi_mw=8.0, racking="tracker",
            tilt=25.0, azimuth=180.0, module_orientation="portrait",
            number_of_modules=2, ground_clearance_height_m=1.5,
            panel_model="Test Panel", bifacial=True,
            inverter_model="Test Inverter", gcr=0.4,
            shading_percent=0.0, dc_wiring_loss_percent=0.0,
            ac_wiring_loss_percent=0.0, transformer_losses_percent=0.0,
            degradation_percent=0.0, availability_percent=0.0,
            module_mismatch_percent=0.0, lid_percent=0.0,
        )
        assert site.data_source == "nsrdb"
        assert site.solcast_metadata is None

    def test_nsrdb_path_assigns_nsrdb_data_source(self, tmp_path: Path) -> None:
        """NSRDB flow assigns data_source='nsrdb' via pipeline wiring."""
        mock_nsrdb = MagicMock(spec=NSRDBClient)
        mock_nsrdb.fetch_weather_data.return_value = generate_mock_nsrdb_csv(
            lat=self.PHOENIX_LAT, lon=self.PHOENIX_LON, year=2024, num_hours=8760
        )

        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(
            nsrdb_client=mock_nsrdb,
            cache_manager=CacheManager(cache_dir=tmp_path / "cache"),
            formatter=formatter,
        )

        sites = load_config(SINGLE_ROW_CSV)
        location_results = orch.fetch_climate_data(sites, year=2024)

        # Simulate pipeline assignment (save df to file, then assign)
        for site in sites:
            if site.location in location_results:
                result = location_results[site.location]
                pysam_path = tmp_path / f"pysam_{site.latitude}_{site.longitude}.csv"
                formatter.save_to_csv(
                    result["weather_df"], pysam_path,
                    site.latitude, site.longitude,
                    metadata=result["weather_metadata"],
                )
                site.weather_file_path = pysam_path
                site.data_source = result.get("data_source", "nsrdb")
                site.solcast_metadata = result.get("solcast_metadata")

        assert sites[0].data_source == "nsrdb"
        assert sites[0].solcast_metadata is None

    def test_solcast_path_assigns_solcast_data_source(self, tmp_path: Path) -> None:
        """Solcast flow assigns data_source='solcast' and solcast_metadata via pipeline wiring."""
        mock_nsrdb = MagicMock(spec=NSRDBClient)
        mock_era5 = MagicMock(return_value={
            "snow_depth_cm": [0.0] * 8760,
            "monthly_precip_inches": [1.0] * 12,
        })

        orch = ClimateOrchestrator(
            nsrdb_client=mock_nsrdb,
            cache_manager=CacheManager(cache_dir=tmp_path / "cache"),
            formatter=WeatherFormatter(),
            era5_client=mock_era5,
        )

        site = SiteConfig(
            run_name="Run_Solcast", site_name="Phoenix_Solcast", customer="TestCo",
            latitude=self.PHOENIX_LAT, longitude=self.PHOENIX_LON,
            dc_size_mw=13.0, ac_installed_mw=10.0, ac_poi_mw=10.0,
            racking="tracker", tilt=60.0, azimuth=180.0,
            module_orientation="portrait", number_of_modules=2,
            ground_clearance_height_m=1.8, panel_model="Test Panel",
            bifacial=True, inverter_model="Test Inverter", gcr=0.34,
            shading_percent=1.0, dc_wiring_loss_percent=1.5,
            ac_wiring_loss_percent=1.5, transformer_losses_percent=0.0,
            degradation_percent=0.3, availability_percent=98.0,
            module_mismatch_percent=1.5, lid_percent=1.0,
            resource_file_path=self.SOLCAST_FIXTURE,
        )

        location_results = orch.fetch_climate_data([site], year=2024)

        # Simulate pipeline assignment (same logic as run_climate_data_pipeline)
        result = location_results[site.location]
        site.weather_file_path = result["weather_file"]
        site.data_source = result.get("data_source", "nsrdb")
        site.solcast_metadata = result.get("solcast_metadata")

        assert site.data_source == "solcast"
        assert site.solcast_metadata is not None
        assert site.solcast_metadata["source"] == "Solcast"
        assert site.solcast_metadata["latitude"] == pytest.approx(self.PHOENIX_LAT)
        assert site.weather_file_path == self.SOLCAST_FIXTURE
        mock_nsrdb.fetch_weather_data.assert_not_called()
