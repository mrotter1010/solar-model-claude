"""Tests for the buildability API endpoint, adapter, and runner."""

import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from src.api.adapter import buildability_request_to_site_config
from src.api.app import create_app
from src.api.runner import extract_buildability_response
from src.api.schemas.common import BuildabilityConfig, Location
from src.api.schemas.requests import BuildabilityRequest
from src.buildability.models import BuildabilityResult


@pytest.fixture()
def client() -> TestClient:
    """Create a TestClient for the FastAPI app."""
    app = create_app()
    return TestClient(app)


def _mock_buildability_result() -> BuildabilityResult:
    """Build a realistic mock BuildabilityResult.

    Matches the structure returned by run_buildability_from_site():
    location, acreage, land cover classification, and slope stats.
    """
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
            {
                "code": 71,
                "name": "Grassland/Herbaceous",
                "category": "buildable",
                "pixels": 840,
                "acres": 70.0,
                "percent": 11.7,
            },
            {
                "code": 52,
                "name": "Shrub/Scrub",
                "category": "soft_exclusion",
                "pixels": 960,
                "acres": 80.0,
                "percent": 13.3,
            },
            {
                "code": 11,
                "name": "Open Water",
                "category": "hard_exclusion",
                "pixels": 720,
                "acres": 60.0,
                "percent": 10.0,
            },
        ],
        slope_stats={
            "min_degrees": 0.0,
            "max_degrees": 18.5,
            "mean_degrees": 3.2,
            "median_degrees": 2.1,
            "distribution": [
                {"range": "0\u00b0 - 5\u00b0", "pixels": 5000, "percent": 70.0},
                {"range": "5\u00b0 - 10\u00b0", "pixels": 1500, "percent": 21.0},
                {"range": "10\u00b0 - 15\u00b0", "pixels": 500, "percent": 7.0},
                {"range": "15\u00b0+", "pixels": 140, "percent": 2.0},
            ],
            "pct_below_tracker_limit": 91.0,
            "pct_below_fixed_tilt_limit": 98.0,
        },
        nlcd_metadata={"crs": "EPSG:4326"},
        dem_metadata={"crs": "EPSG:4326"},
        figure_paths={},
    )


def _minimal_buildability_body() -> dict:
    """Build a minimal valid request body for POST /analyses/buildability."""
    return {
        "site": {
            "name": "TestSite",
            "latitude": 33.45,
            "longitude": -112.07,
            "run_name": "TestSite_buildability_20240101",
        },
    }


# ---------------------------------------------------------------------------
# Adapter tests
# ---------------------------------------------------------------------------


class TestBuildabilityAdapter:
    """BuildabilityRequest → SiteConfig mapping."""

    def test_buildable_land_assessment_true(self) -> None:
        """Buildability request sets buildable_land_assessment=True."""
        request = BuildabilityRequest(
            site=Location(name="TestSite", latitude=33.45, longitude=-112.07),
        )
        config = buildability_request_to_site_config(request)
        assert config.buildable_land_assessment is True

    def test_dummy_system_values_are_valid(self) -> None:
        """Dummy system values produce a valid SiteConfig (no validation errors)."""
        request = BuildabilityRequest(
            site=Location(name="TestSite", latitude=33.45, longitude=-112.07),
        )
        config = buildability_request_to_site_config(request)

        # Verify dummy values are set and valid
        assert config.dc_size_mw == 1.0
        assert config.ac_installed_mw == 1.0
        assert config.ac_poi_mw == 1.0
        assert config.racking == "tracker"
        assert config.panel_model == "Placeholder"
        assert config.inverter_model == "Placeholder"
        assert config.gcr == 0.35

    def test_other_features_disabled(self) -> None:
        """Bill, BESS, and report are disabled for buildability."""
        request = BuildabilityRequest(
            site=Location(name="TestSite", latitude=33.45, longitude=-112.07),
        )
        config = buildability_request_to_site_config(request)

        assert config.bill_calculation is False
        assert config.bess_dispatch_required is False
        assert config.bess_optimization_required is False
        assert config.report is False

    def test_location_mapped(self) -> None:
        """Latitude and longitude flow to SiteConfig."""
        request = BuildabilityRequest(
            site=Location(name="TestSite", latitude=40.0, longitude=-75.0),
        )
        config = buildability_request_to_site_config(request)
        assert config.latitude == 40.0
        assert config.longitude == -75.0

    def test_radius_km_mapped(self) -> None:
        """analysis_radius_km from BuildabilityConfig flows to SiteConfig."""
        request = BuildabilityRequest(
            site=Location(name="TestSite", latitude=33.45, longitude=-112.07),
            buildability=BuildabilityConfig(analysis_radius_km=2.5),
        )
        config = buildability_request_to_site_config(request)
        assert config.analysis_radius_km == 2.5

    def test_kmz_file_path_mapped(self) -> None:
        """kmz_file_path from BuildabilityConfig flows to SiteConfig."""
        request = BuildabilityRequest(
            site=Location(name="TestSite", latitude=33.45, longitude=-112.07),
            buildability=BuildabilityConfig(kmz_file_path="/tmp/boundary.kmz"),
        )
        config = buildability_request_to_site_config(request)
        assert config.kmz_file_path == "/tmp/boundary.kmz"

    def test_no_buildability_config_uses_defaults(self) -> None:
        """No buildability config → both kmz and radius are None."""
        request = BuildabilityRequest(
            site=Location(name="TestSite", latitude=33.45, longitude=-112.07),
        )
        config = buildability_request_to_site_config(request)
        assert config.kmz_file_path is None
        assert config.analysis_radius_km is None

    def test_run_name_auto_generated(self) -> None:
        """run_name is auto-generated when not provided."""
        request = BuildabilityRequest(
            site=Location(name="TestSite", latitude=33.45, longitude=-112.07),
        )
        config = buildability_request_to_site_config(request)
        assert config.run_name.startswith("TestSite_")

    def test_run_name_preserved(self) -> None:
        """Explicit run_name is preserved."""
        request = BuildabilityRequest(
            site=Location(
                name="TestSite",
                latitude=33.45,
                longitude=-112.07,
                run_name="my_run",
            ),
        )
        config = buildability_request_to_site_config(request)
        assert config.run_name == "my_run"


# ---------------------------------------------------------------------------
# Runner extract tests
# ---------------------------------------------------------------------------


class TestExtractBuildabilityResponse:
    """extract_buildability_response maps BuildabilityResult to response."""

    def test_basic_fields(self) -> None:
        """Core fields are mapped correctly."""
        result = _mock_buildability_result()
        response = extract_buildability_response(result, "test_run")

        assert response.run_id == "test_run"
        assert response.status == "success"
        assert response.latitude == 33.45
        assert response.longitude == -112.07
        assert response.radius_km == 1.0
        assert response.total_area_acres == 600.0
        assert response.buildable_acres == 420.0
        assert response.buildable_pct == 70.0

    def test_class_breakdown_mapped(self) -> None:
        """class_breakdown list is mapped to LandCoverClass models."""
        result = _mock_buildability_result()
        response = extract_buildability_response(result, "test_run")

        assert len(response.class_breakdown) == 4
        first = response.class_breakdown[0]
        assert first.code == 82
        assert first.name == "Cultivated Crops"
        assert first.category == "buildable"
        assert first.acres == 350.0

    def test_slope_mapped(self) -> None:
        """slope_stats dict is mapped to SlopeSummary model."""
        result = _mock_buildability_result()
        response = extract_buildability_response(result, "test_run")

        assert response.slope.mean_degrees == 3.2
        assert response.slope.pct_below_tracker_limit == 91.0
        assert response.slope.pct_below_fixed_tilt_limit == 98.0
        assert len(response.slope.distribution) == 4


# ---------------------------------------------------------------------------
# Route tests
# ---------------------------------------------------------------------------


class TestBuildabilityEndpointValid:
    """POST /analyses/buildability with valid inputs returns 200."""

    @patch("src.api.routes.analyses.run_buildability")
    @patch("src.api.routes.analyses.buildability_request_to_site_config")
    def test_minimal_request_returns_200(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """Minimal buildability request → 200 with expected fields."""
        mock_config = MagicMock()
        mock_config.run_name = "TestSite_buildability_20240101"
        mock_adapter.return_value = mock_config
        mock_run.return_value = _mock_buildability_result()

        resp = client.post(
            "/analyses/buildability", json=_minimal_buildability_body()
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["run_id"] == "TestSite_buildability_20240101"
        assert data["latitude"] == 33.45
        assert data["longitude"] == -112.07
        assert data["total_area_acres"] == 600.0
        assert data["buildable_acres"] == 420.0
        assert data["buildable_pct"] == 70.0
        assert "class_breakdown" in data
        assert "slope" in data

    @patch("src.api.routes.analyses.run_buildability")
    @patch("src.api.routes.analyses.buildability_request_to_site_config")
    def test_slope_values(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """Slope metrics match mock data."""
        mock_config = MagicMock()
        mock_config.run_name = "TestSite_buildability_20240101"
        mock_adapter.return_value = mock_config
        mock_run.return_value = _mock_buildability_result()

        resp = client.post(
            "/analyses/buildability", json=_minimal_buildability_body()
        )
        slope = resp.json()["slope"]

        assert slope["mean_degrees"] == 3.2
        assert slope["pct_below_tracker_limit"] == 91.0
        assert slope["pct_below_fixed_tilt_limit"] == 98.0
        assert len(slope["distribution"]) == 4

    @patch("src.api.routes.analyses.run_buildability")
    @patch("src.api.routes.analyses.buildability_request_to_site_config")
    def test_with_radius(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """Request with explicit radius → 200."""
        mock_config = MagicMock()
        mock_config.run_name = "TestSite_buildability_20240101"
        mock_adapter.return_value = mock_config
        mock_run.return_value = _mock_buildability_result()

        body = _minimal_buildability_body()
        body["buildability"] = {"analysis_radius_km": 2.0}

        resp = client.post("/analyses/buildability", json=body)
        assert resp.status_code == 200


class TestBuildabilityEndpointValidation:
    """POST /analyses/buildability with invalid inputs returns 422."""

    def test_missing_site_returns_422(self, client: TestClient) -> None:
        """Request without site → 422."""
        resp = client.post("/analyses/buildability", json={})
        assert resp.status_code == 422

    def test_both_kmz_and_radius_returns_422(self, client: TestClient) -> None:
        """Request with both kmz_file_path and analysis_radius_km → 422."""
        body = _minimal_buildability_body()
        body["buildability"] = {
            "kmz_file_path": "/tmp/boundary.kmz",
            "analysis_radius_km": 2.0,
        }
        resp = client.post("/analyses/buildability", json=body)
        assert resp.status_code == 422


class TestBuildabilityEndpointError:
    """POST /analyses/buildability with analyzer failure returns 502."""

    @patch("src.api.routes.analyses.run_buildability")
    @patch("src.api.routes.analyses.buildability_request_to_site_config")
    def test_analyzer_exception_returns_502(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """Analyzer exception → 502 with error detail."""
        mock_config = MagicMock()
        mock_config.run_name = "TestSite_buildability_20240101"
        mock_adapter.return_value = mock_config
        mock_run.side_effect = RuntimeError("NLCD API timeout")

        resp = client.post(
            "/analyses/buildability", json=_minimal_buildability_body()
        )

        assert resp.status_code == 502
        data = resp.json()
        assert "NLCD API timeout" in data["detail"]


# ---------------------------------------------------------------------------
# PDF report generation tests
# ---------------------------------------------------------------------------


class TestBuildabilityPDFReport:
    """Buildability endpoint generates a PDF report downloadable via GET."""

    @patch("src.api.routes.analyses.generate_standalone_buildability_report")
    @patch("src.api.routes.analyses.run_buildability")
    @patch("src.api.routes.analyses.buildability_request_to_site_config")
    def test_generates_pdf_in_reports_dir(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        mock_pdf_gen: MagicMock,
        client: TestClient,
    ) -> None:
        """Endpoint calls generate_standalone_buildability_report with correct path."""
        mock_config = MagicMock()
        mock_config.run_name = "TestSite_buildability_pdf_test"
        mock_adapter.return_value = mock_config
        result = _mock_buildability_result()
        mock_run.return_value = result
        mock_pdf_gen.return_value = Path(
            "outputs/api/TestSite_buildability_pdf_test/reports/buildability_report.pdf"
        )

        resp = client.post(
            "/analyses/buildability", json=_minimal_buildability_body()
        )

        assert resp.status_code == 200

        # Verify PDF generator was called exactly once
        mock_pdf_gen.assert_called_once()
        call_args = mock_pdf_gen.call_args[0]

        # First arg: the BuildabilityResult
        assert call_args[0] is result

        # Second arg: path under reports/ subdirectory
        pdf_path = Path(call_args[1])
        assert pdf_path.parent.name == "reports"
        assert pdf_path.name == "buildability_report.pdf"

        # Cleanup output dir created by the endpoint
        output_dir = Path("outputs/api") / "TestSite_buildability_pdf_test"
        if output_dir.exists():
            shutil.rmtree(output_dir)

    def test_report_download_returns_200(self, client: TestClient) -> None:
        """GET /analyses/{run_id}/report returns 200 when buildability PDF exists."""
        run_id = "test_buildability_report_download"
        reports_dir = Path("outputs/api") / run_id / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        pdf_path = reports_dir / "buildability_report.pdf"
        pdf_path.write_bytes(b"%PDF-1.4 dummy content")

        try:
            resp = client.get(f"/analyses/{run_id}/report")
            assert resp.status_code == 200
            assert resp.headers["content-type"] == "application/pdf"
        finally:
            shutil.rmtree(Path("outputs/api") / run_id)
