"""Tests for the include_maps feature on the buildability API endpoint."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from src.api.app import create_app
from src.buildability.models import BuildabilityResult


@pytest.fixture()
def client() -> TestClient:
    """Create a TestClient for the FastAPI app."""
    app = create_app()
    return TestClient(app)


def _mock_buildability_result(
    figure_paths: dict[str, str] | None = None,
) -> BuildabilityResult:
    """Build a mock BuildabilityResult with optional figure_paths."""
    return BuildabilityResult(
        latitude=33.45,
        longitude=-112.07,
        radius_km=1.0,
        kmz_file_path=None,
        polygon_wkt="POLYGON ((-112.08 33.44, -112.06 33.44, -112.06 33.46, -112.08 33.46, -112.08 33.44))",
        total_area_acres=600.0,
        buildable_acres=420.0,
        soft_exclusion_acres=80.0,
        hard_exclusion_acres=60.0,
        unclassified_acres=40.0,
        buildable_pct=70.0,
        soft_exclusion_pct=13.3,
        hard_exclusion_pct=10.0,
        class_breakdown=[
            {
                "code": 82,
                "name": "Cultivated Crops",
                "category": "buildable",
                "pixels": 4200,
                "acres": 350.0,
                "percent": 58.3,
            },
        ],
        slope_stats={
            "min_degrees": 0.0,
            "max_degrees": 18.5,
            "mean_degrees": 3.2,
            "median_degrees": 2.1,
            "distribution": [
                {"range": "0\u00b0 - 5\u00b0", "pixels": 5000, "percent": 70.0},
            ],
            "pct_below_tracker_limit": 91.0,
            "pct_below_fixed_tilt_limit": 98.0,
        },
        nlcd_metadata={"crs": "EPSG:4326"},
        dem_metadata={"crs": "EPSG:4326"},
        figure_paths=figure_paths or {},
    )


def _minimal_body() -> dict:
    """Minimal valid request body for POST /analyses/buildability."""
    return {
        "site": {
            "name": "TestSite",
            "latitude": 33.45,
            "longitude": -112.07,
            "run_name": "TestSite_maps_test",
        },
    }


# ---------------------------------------------------------------------------
# include_maps defaults to false
# ---------------------------------------------------------------------------


class TestIncludeMapsDefault:
    """include_maps defaults to false when omitted."""

    @patch("src.api.routes.analyses.run_buildability")
    @patch("src.api.routes.analyses.buildability_request_to_site_config")
    def test_include_maps_defaults_false(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """Omitting include_maps → defaults to false, empty figure_paths."""
        mock_config = MagicMock()
        mock_config.run_name = "TestSite_maps_test"
        mock_adapter.return_value = mock_config
        mock_run.return_value = _mock_buildability_result(figure_paths={})

        resp = client.post("/analyses/buildability", json=_minimal_body())

        assert resp.status_code == 200
        data = resp.json()
        assert data["figure_paths"] == {}
        # Verify run_buildability was called with include_maps=False
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args
        assert call_kwargs.kwargs["include_maps"] is False


# ---------------------------------------------------------------------------
# include_maps = false (explicit)
# ---------------------------------------------------------------------------


class TestIncludeMapsFalse:
    """include_maps=false preserves existing behavior."""

    @patch("src.api.routes.analyses.run_buildability")
    @patch("src.api.routes.analyses.buildability_request_to_site_config")
    def test_explicit_false_returns_empty_figure_paths(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """Explicit include_maps=false → 200, empty figure_paths."""
        mock_config = MagicMock()
        mock_config.run_name = "TestSite_maps_test"
        mock_adapter.return_value = mock_config
        mock_run.return_value = _mock_buildability_result(figure_paths={})

        body = _minimal_body()
        body["include_maps"] = False

        resp = client.post("/analyses/buildability", json=body)

        assert resp.status_code == 200
        data = resp.json()
        assert data["figure_paths"] == {}
        # Verify include_maps=False was passed through
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args
        assert call_kwargs.kwargs["include_maps"] is False


# ---------------------------------------------------------------------------
# include_maps = true
# ---------------------------------------------------------------------------


class TestIncludeMapsTrue:
    """include_maps=true passes output_dir to the analyzer."""

    @patch("src.api.routes.analyses.run_buildability")
    @patch("src.api.routes.analyses.buildability_request_to_site_config")
    def test_include_maps_true_returns_figure_paths(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """include_maps=true → figure_paths populated in response."""
        mock_config = MagicMock()
        mock_config.run_name = "TestSite_maps_test"
        mock_adapter.return_value = mock_config
        mock_run.return_value = _mock_buildability_result(
            figure_paths={
                "land_cover": "/outputs/api/TestSite_maps_test/land_cover.png",
                "slope": "/outputs/api/TestSite_maps_test/slope.png",
            }
        )

        body = _minimal_body()
        body["include_maps"] = True

        resp = client.post("/analyses/buildability", json=body)

        assert resp.status_code == 200
        data = resp.json()
        assert data["figure_paths"]["land_cover"] == (
            "/outputs/api/TestSite_maps_test/land_cover.png"
        )
        assert data["figure_paths"]["slope"] == (
            "/outputs/api/TestSite_maps_test/slope.png"
        )

    @patch("src.api.routes.analyses.run_buildability")
    @patch("src.api.routes.analyses.buildability_request_to_site_config")
    def test_include_maps_true_passes_output_dir(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """include_maps=true → output_dir kwarg is passed to runner."""
        mock_config = MagicMock()
        mock_config.run_name = "TestSite_maps_test"
        mock_adapter.return_value = mock_config
        mock_run.return_value = _mock_buildability_result()

        body = _minimal_body()
        body["include_maps"] = True

        resp = client.post("/analyses/buildability", json=body)

        assert resp.status_code == 200
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args
        assert call_kwargs.kwargs["include_maps"] is True
        assert isinstance(call_kwargs.kwargs["output_dir"], Path)
        assert call_kwargs.kwargs["output_dir"] == Path(
            "outputs/api/TestSite_maps_test"
        )


# ---------------------------------------------------------------------------
# Runner unit test: run_buildability constructs BuildabilityConfig
# ---------------------------------------------------------------------------


class TestRunBuildabilityRunner:
    """Unit tests for run_buildability in src/api/runner.py."""

    @patch("src.buildability.analyzer.run_buildability_analysis")
    def test_constructs_config_and_calls_analyzer(
        self,
        mock_analyze: MagicMock,
    ) -> None:
        """run_buildability constructs BuildabilityConfig from SiteConfig fields."""
        from src.api.runner import run_buildability

        site_config = MagicMock()
        site_config.latitude = 33.45
        site_config.longitude = -112.07
        site_config.kmz_file_path = None
        site_config.analysis_radius_km = 2.0

        mock_analyze.return_value = _mock_buildability_result()

        run_buildability(site_config, include_maps=False)

        # Verify analyzer was called once with a BuildabilityConfig (no output_dir)
        mock_analyze.assert_called_once()
        call_args = mock_analyze.call_args
        config_arg = call_args.args[0]
        assert config_arg.latitude == 33.45
        assert config_arg.longitude == -112.07
        assert config_arg.kmz_file_path is None
        assert config_arg.radius_km == 2.0
        # No output_dir when include_maps=False
        assert "output_dir" not in call_args.kwargs

    @patch("src.buildability.analyzer.run_buildability_analysis")
    def test_passes_output_dir_when_maps_enabled(
        self,
        mock_analyze: MagicMock,
    ) -> None:
        """include_maps=True → output_dir is passed to run_buildability_analysis."""
        from src.api.runner import run_buildability

        site_config = MagicMock()
        site_config.latitude = 33.45
        site_config.longitude = -112.07
        site_config.kmz_file_path = None
        site_config.analysis_radius_km = None

        mock_analyze.return_value = _mock_buildability_result()

        out = Path("/tmp/test_output")
        run_buildability(site_config, include_maps=True, output_dir=out)

        mock_analyze.assert_called_once()
        call_kwargs = mock_analyze.call_args.kwargs
        assert call_kwargs["output_dir"] == out
