"""Tests for Solcast resource file branching in the climate orchestrator."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.climate.cache_manager import CacheManager
from src.climate.nsrdb_client import NSRDBClient
from src.climate.orchestrator import ClimateOrchestrator
from src.climate.weather_formatter import WeatherFormatter
from src.config.schema import SiteConfig
from src.utils.exceptions import ClimateDataError

FIXTURES_DIR = Path(__file__).parent / "fixtures"
FULL_FIXTURE = FIXTURES_DIR / "synthetic_solcast_tmy_phoenix.csv"
MINIMAL_FIXTURE = FIXTURES_DIR / "synthetic_solcast_tmy_phoenix_minimal.csv"

# Coordinates matching the synthetic Solcast fixtures
PHOENIX_LAT = 33.483
PHOENIX_LON = -112.073

# Canned ERA5 response to avoid real API calls
MOCK_ERA5_DATA = {
    "snow_depth_cm": [0.0] * 8760,
    "monthly_precip_inches": [1.0] * 12,
}


def _make_site(
    *,
    run_name: str = "Run1",
    site_name: str = "Phoenix_Solcast",
    lat: float = PHOENIX_LAT,
    lon: float = PHOENIX_LON,
    resource_file_path: Path | None = None,
) -> SiteConfig:
    """Build a SiteConfig with optional resource_file_path."""
    return SiteConfig(
        run_name=run_name,
        site_name=site_name,
        customer="TestCo",
        latitude=lat,
        longitude=lon,
        dc_size_mw=13.0,
        ac_installed_mw=10.0,
        ac_poi_mw=10.0,
        racking="tracker",
        tilt=60.0,
        azimuth=180.0,
        module_orientation="portrait",
        number_of_modules=2,
        ground_clearance_height_m=1.8,
        panel_model="Test Panel",
        bifacial=True,
        inverter_model="Test Inverter",
        gcr=0.34,
        shading_percent=1.0,
        dc_wiring_loss_percent=1.5,
        ac_wiring_loss_percent=1.5,
        transformer_losses_percent=0.0,
        degradation_percent=0.3,
        availability_percent=98.0,
        module_mismatch_percent=1.5,
        lid_percent=1.0,
        resource_file_path=resource_file_path,
    )


@pytest.fixture()
def mock_nsrdb_client() -> MagicMock:
    """Mock NSRDB client that should NOT be called in Solcast tests."""
    return MagicMock(spec=NSRDBClient)


@pytest.fixture()
def mock_era5_client() -> MagicMock:
    """Mock ERA5 client returning canned precipitation data."""
    client = MagicMock()
    client.return_value = MOCK_ERA5_DATA
    return client


@pytest.fixture()
def orchestrator(
    tmp_path: Path, mock_nsrdb_client: MagicMock, mock_era5_client: MagicMock
) -> ClimateOrchestrator:
    """Orchestrator with mocked NSRDB + ERA5 clients."""
    cache_manager = CacheManager(cache_dir=tmp_path / "cache")
    formatter = WeatherFormatter()
    return ClimateOrchestrator(
        nsrdb_client=mock_nsrdb_client,
        cache_manager=cache_manager,
        formatter=formatter,
        era5_client=mock_era5_client,
    )


# --- Solcast Path: NSRDB Skipped, ERA5 Called ---


class TestSolcastBranch:
    """Tests for the Solcast resource file branch in fetch_climate_data."""

    def test_solcast_file_skips_nsrdb(
        self,
        orchestrator: ClimateOrchestrator,
        mock_nsrdb_client: MagicMock,
        mock_era5_client: MagicMock,
        test_results_dir: Path,
    ) -> None:
        """When resource_file_path is set, NSRDB is NOT called."""
        site = _make_site(resource_file_path=FULL_FIXTURE)

        results = orchestrator.fetch_climate_data([site])

        # NSRDB should never be called
        mock_nsrdb_client.fetch_weather_data.assert_not_called()
        # ERA5 should still be called for soiling
        mock_era5_client.assert_called_once()

        result = results[(PHOENIX_LAT, PHOENIX_LON)]
        assert result["data_source"] == "solcast"
        assert result["weather_file"] == FULL_FIXTURE
        assert result["solcast_metadata"]["source"] == "Solcast"
        assert result["solcast_metadata"]["latitude"] == pytest.approx(PHOENIX_LAT)
        assert result["monthly_soiling"] is not None

        # Write artifact for review
        output = {
            "weather_file": str(result["weather_file"]),
            "data_source": result["data_source"],
            "solcast_metadata": result["solcast_metadata"],
            "monthly_soiling_sample": result["monthly_soiling"][:3],
        }
        output_path = test_results_dir / "test_orchestrator_solcast_branch.json"
        output_path.write_text(json.dumps(output, indent=2))

    def test_solcast_result_has_correct_structure(
        self,
        orchestrator: ClimateOrchestrator,
    ) -> None:
        """Solcast result dict has all expected keys."""
        site = _make_site(resource_file_path=FULL_FIXTURE)
        results = orchestrator.fetch_climate_data([site])

        result = results[(PHOENIX_LAT, PHOENIX_LON)]
        assert "weather_file" in result
        assert "monthly_soiling" in result
        assert "data_source" in result
        assert "solcast_metadata" in result
        assert result["data_source"] == "solcast"

    def test_era5_unavailable_soiling_is_none(
        self,
        tmp_path: Path,
        mock_nsrdb_client: MagicMock,
    ) -> None:
        """When ERA5 returns None, monthly_soiling is None."""
        era5_returns_none = MagicMock(return_value=None)
        cache_manager = CacheManager(cache_dir=tmp_path / "cache")
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(
            nsrdb_client=mock_nsrdb_client,
            cache_manager=cache_manager,
            formatter=formatter,
            era5_client=era5_returns_none,
        )

        site = _make_site(resource_file_path=FULL_FIXTURE)
        results = orch.fetch_climate_data([site])

        result = results[(PHOENIX_LAT, PHOENIX_LON)]
        assert result["monthly_soiling"] is None


# --- NSRDB Path: data_source = "nsrdb" ---


SAMPLE_NSRDB_CSV = """Source,Location ID,City,State,Country,Latitude,Longitude,Time Zone,Elevation
NSRDB,123456,Phoenix,AZ,United States,33.45,-111.98,-7,337
Year,Month,Day,Hour,Minute,GHI,DNI,DHI,Temperature,Wind Speed,Surface Albedo
2024,1,1,0,0,0,0,0,8.5,2.1,0.18
2024,1,1,1,0,0,0,0,7.9,1.8,0.18
2024,1,1,12,0,650,800,120,18.7,4.1,0.20
"""


class TestNsrdbDataSource:
    """Tests that existing NSRDB path includes data_source key."""

    def test_nsrdb_result_has_data_source(self, tmp_path: Path) -> None:
        """NSRDB flow should return data_source='nsrdb' in result."""
        mock_client = MagicMock(spec=NSRDBClient)
        mock_client.fetch_weather_data.return_value = SAMPLE_NSRDB_CSV

        cache_manager = CacheManager(cache_dir=tmp_path / "cache")
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(mock_client, cache_manager, formatter)

        # Site with NO resource_file_path → NSRDB path
        site = _make_site(
            lat=33.45, lon=-111.98, resource_file_path=None,
        )
        results = orch.fetch_climate_data([site])

        result = results[(33.45, -111.98)]
        assert result["data_source"] == "nsrdb"
        assert isinstance(result["weather_df"], pd.DataFrame)
        assert isinstance(result["weather_metadata"], dict)
        mock_client.fetch_weather_data.assert_called_once()


# --- Snow Depth Warning Prompt ---


class TestSnowDepthWarning:
    """Tests for snow depth missing prompt when using Solcast files."""

    def test_no_snow_depth_user_proceeds(
        self,
        orchestrator: ClimateOrchestrator,
    ) -> None:
        """User types 'y' to proceed without snow losses."""
        site = _make_site(resource_file_path=MINIMAL_FIXTURE)

        with patch("builtins.input", return_value="y") as mock_input:
            results = orchestrator.fetch_climate_data([site])

        # Should have prompted the user
        mock_input.assert_called_once()
        prompt_text = mock_input.call_args[0][0]
        assert "Snow Depth" in prompt_text
        assert f"({PHOENIX_LAT}, {PHOENIX_LON})" in prompt_text

        # Should still succeed
        result = results[(PHOENIX_LAT, PHOENIX_LON)]
        assert result["data_source"] == "solcast"

    def test_no_snow_depth_user_aborts(
        self,
        orchestrator: ClimateOrchestrator,
    ) -> None:
        """User types 'n' to abort when snow depth is missing."""
        site = _make_site(resource_file_path=MINIMAL_FIXTURE)

        with patch("builtins.input", return_value="n"):
            with pytest.raises(ClimateDataError, match="User aborted"):
                orchestrator.fetch_climate_data([site])

    def test_full_file_no_snow_prompt(
        self,
        orchestrator: ClimateOrchestrator,
    ) -> None:
        """Full Solcast file with Snow Depth should NOT prompt user."""
        site = _make_site(resource_file_path=FULL_FIXTURE)

        with patch("builtins.input") as mock_input:
            results = orchestrator.fetch_climate_data([site])

        mock_input.assert_not_called()
        result = results[(PHOENIX_LAT, PHOENIX_LON)]
        assert result["data_source"] == "solcast"


# --- Mixed Sites: Solcast + NSRDB ---


class TestMixedSources:
    """Tests with both Solcast and NSRDB sites in the same batch."""

    def test_mixed_solcast_and_nsrdb_sites(
        self,
        tmp_path: Path,
        test_results_dir: Path,
    ) -> None:
        """Solcast site skips NSRDB; NSRDB site goes through normal flow."""
        # Phoenix site → Solcast
        solcast_site = _make_site(
            run_name="Run_Solcast",
            site_name="Phoenix_Solcast",
            lat=PHOENIX_LAT,
            lon=PHOENIX_LON,
            resource_file_path=FULL_FIXTURE,
        )
        # Tucson site → NSRDB (different location, no resource file)
        nsrdb_site = _make_site(
            run_name="Run_NSRDB",
            site_name="Tucson_NSRDB",
            lat=32.253,
            lon=-110.911,
            resource_file_path=None,
        )

        mock_client = MagicMock(spec=NSRDBClient)
        mock_client.fetch_weather_data.return_value = SAMPLE_NSRDB_CSV

        mock_era5 = MagicMock(return_value=MOCK_ERA5_DATA)

        cache_manager = CacheManager(cache_dir=tmp_path / "cache")
        formatter = WeatherFormatter()
        orch = ClimateOrchestrator(
            mock_client, cache_manager, formatter, era5_client=mock_era5,
        )

        results = orch.fetch_climate_data([solcast_site, nsrdb_site])

        # Phoenix → Solcast path (NSRDB not called for this location)
        phoenix_result = results[(PHOENIX_LAT, PHOENIX_LON)]
        assert phoenix_result["data_source"] == "solcast"
        assert phoenix_result["weather_file"] == FULL_FIXTURE

        # Tucson → NSRDB path
        tucson_result = results[(32.253, -110.911)]
        assert tucson_result["data_source"] == "nsrdb"
        assert isinstance(tucson_result["weather_df"], pd.DataFrame)

        # NSRDB called exactly once (Tucson only)
        mock_client.fetch_weather_data.assert_called_once()

        # ERA5 called for both locations
        assert mock_era5.call_count == 2

        # Write artifact
        output = {
            "phoenix": {
                "data_source": phoenix_result["data_source"],
                "weather_file": str(phoenix_result["weather_file"]),
            },
            "tucson": {
                "data_source": tucson_result["data_source"],
                "weather_df_rows": len(tucson_result["weather_df"]),
            },
            "nsrdb_calls": mock_client.fetch_weather_data.call_count,
            "era5_calls": mock_era5.call_count,
        }
        output_path = test_results_dir / "test_orchestrator_mixed_sources.json"
        output_path.write_text(json.dumps(output, indent=2))
