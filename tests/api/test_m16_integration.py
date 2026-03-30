"""M16 integration tests: end-to-end API flows exercising the full M16 surface.

All tests use FastAPI TestClient with mocks — no real PySAM, climate API,
or database calls.
"""

import json
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from src.api.app import create_app
from src.config.schema import SiteConfig
from src.pipeline import SolarModelingPipeline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _schedule_12x24(value: int = 0) -> list[list[int]]:
    """Build a uniform 12x24 schedule matrix filled with a single value."""
    return [[value] * 24 for _ in range(12)]


def _minimal_rate_dict() -> dict:
    """Build a minimal valid rate schedule dict for JSON payloads."""
    return {
        "utility_name": "Integration Utility",
        "tariff_name": "Integration Tariff",
        "energyratestructure": [[{"rate": 0.12}]],
        "energyweekdayschedule": _schedule_12x24(0),
        "energyweekendschedule": _schedule_12x24(0),
    }


def _minimal_system_dict() -> dict:
    """Minimal valid SystemDesign for API requests."""
    return {
        "dc_capacity_mw": 5.0,
        "ac_capacity_mw": 4.0,
        "module": "CSI Solar Co. Ltd. CS3U-355P",
        "inverter": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
        "racking": "tracker",
        "azimuth": 180.0,
        "gcr": 0.35,
        "bifacial": False,
    }


def _minimal_site_dict() -> dict:
    """Minimal valid Location for API requests."""
    return {
        "name": "Integration Test Site",
        "latitude": 33.45,
        "longitude": -112.07,
        "run_name": "m16_integration_test",
    }


def _mock_pipeline_result(run_name: str = "m16_integration_test") -> dict:
    """Build a realistic mock result dict from SolarModelingPipeline.run_from_configs()."""
    return {
        "total_sites": 1,
        "successful": 1,
        "failed": 0,
        "timeseries_files": [],
        "error_files": [],
        "report_files": [],
        "summaries": [
            {
                "run_name": run_name,
                "site_name": "Integration Test Site",
                "annual_energy_mwh": 9500.0,
                "ac_installed_mw": 4.0,
                "specific_yield": 1900.0,
                "performance_ratio": 0.82,
                "loss_data": {
                    "capacity_factor": 21.7,
                    "capacity_factor_ac": 27.1,
                    "annual_poa_shading_loss_percent": 0.5,
                    "annual_dc_wiring_loss_percent": 2.0,
                    "annual_ac_wiring_loss_percent": 0.5,
                    "annual_xfmr_loss_percent": 1.0,
                    "annual_dc_mismatch_loss_percent": 2.0,
                    "annual_dc_nameplate_loss_percent": 1.5,
                    "annual_ac_perf_adj_loss_percent": 2.5,
                    "annual_ac_inv_clip_loss_percent": 0.3,
                    "annual_ac_inv_eff_loss_percent": 3.0,
                    "monthly_energy": [
                        600_000, 700_000, 850_000, 900_000, 950_000, 1_000_000,
                        1_050_000, 1_000_000, 900_000, 800_000, 650_000, 600_000,
                    ],
                },
                "bill_savings": {
                    "annual_bill_without_solar": 120_000.0,
                    "annual_bill_with_solar": 45_000.0,
                    "annual_savings": 75_000.0,
                    "savings_percent": 62.5,
                    "avoided_cost_per_kwh": 0.079,
                    "annual_demand_savings": 15_000.0,
                    "annual_export_kwh": 2_000_000.0,
                    "annual_export_credits": 8_000.0,
                    "nem_true_up_credit": 500.0,
                    "monthly_detail": [
                        {
                            "month": m,
                            "bill_without": 10_000.0,
                            "bill_with": 3_750.0,
                            "savings": 6_250.0,
                        }
                        for m in range(1, 13)
                    ],
                },
            }
        ],
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Create a TestClient with auth disabled."""
    monkeypatch.delenv("API_KEY", raising=False)
    app = create_app()
    return TestClient(app)


@pytest.fixture()
def upload_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> TestClient:
    """Create a TestClient with UPLOAD_DIR pointing to tmp_path and auth disabled."""
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.setattr("src.api.routes.uploads.UPLOAD_DIR", str(tmp_path))
    app = create_app()
    return TestClient(app)


# ---------------------------------------------------------------------------
# Flow A: Rate builder → inline rate → bill-savings
# ---------------------------------------------------------------------------


class TestRateBuilderToInlineBillSavings:
    """Build a rate via the API, then use it inline for bill-savings analysis."""

    def test_build_rate_returns_200_with_source_api(
        self, client: TestClient
    ) -> None:
        """POST /rates/build accepts a valid rate and stamps source='api'."""
        resp = client.post("/rates/build", json={"rate": _minimal_rate_dict()})

        assert resp.status_code == 200
        data = resp.json()
        assert data["rate"]["source"] == "api"
        assert data["rate"]["utility_name"] == "Integration Utility"

    @patch("src.api.runner.SolarModelingPipeline")
    def test_inline_rate_flows_through_bill_savings(
        self, mock_pipeline_cls: MagicMock, client: TestClient
    ) -> None:
        """Built rate used as inline bill.rate in POST /analyses/bill-savings."""
        # Step 1: Build the rate
        build_resp = client.post(
            "/rates/build", json={"rate": _minimal_rate_dict()}
        )
        assert build_resp.status_code == 200
        built_rate = build_resp.json()["rate"]

        # Step 2: Mock the pipeline to return a realistic result
        mock_pipeline = MagicMock()
        mock_pipeline.run_from_configs.return_value = _mock_pipeline_result()
        mock_pipeline_cls.return_value = mock_pipeline

        # Step 3: POST /analyses/bill-savings with inline rate
        bill_request = {
            "site": _minimal_site_dict(),
            "system": _minimal_system_dict(),
            "bill": {
                "rate": built_rate,
                "load_type": "SmallOffice",
                "annual_consumption_kwh": 500_000.0,
            },
        }
        resp = client.post("/analyses/bill-savings", json=bill_request)

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["bill_savings"]["annual_savings"] == 75_000.0
        assert data["bill_savings"]["savings_percent"] == 62.5

        # Verify pipeline was called with a list of SiteConfig
        mock_pipeline.run_from_configs.assert_called_once()
        call_args = mock_pipeline.run_from_configs.call_args
        site_configs = call_args[0][0]
        assert len(site_configs) == 1
        assert isinstance(site_configs[0], SiteConfig)
        # Verify inline rate was threaded through
        assert site_configs[0].rate_schedule is not None
        assert site_configs[0].rate_schedule.utility_name == "Integration Utility"
        assert site_configs[0].rate_schedule.source == "api"


# ---------------------------------------------------------------------------
# Flow B: Rate builder → save to disk → file reference
# ---------------------------------------------------------------------------


class TestRateBuilderSaveToDisk:
    """Build a rate, save to disk, verify the file is valid."""

    def test_save_to_disk_returns_file_path(
        self,
        client: TestClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """POST /rates/build with save_to_disk=true returns a file_path."""
        monkeypatch.chdir(tmp_path)

        body = {
            "rate": _minimal_rate_dict(),
            "save_to_disk": True,
        }
        resp = client.post("/rates/build", json=body)

        assert resp.status_code == 200
        data = resp.json()
        assert data["file_path"] is not None
        assert data["file_path"].endswith(".json")

    def test_saved_file_is_valid_json(
        self,
        client: TestClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The saved rate file contains valid JSON matching the built rate."""
        monkeypatch.chdir(tmp_path)

        body = {
            "rate": _minimal_rate_dict(),
            "save_to_disk": True,
        }
        resp = client.post("/rates/build", json=body)
        file_path = resp.json()["file_path"]

        # Read and validate the saved file
        saved = Path(file_path)
        assert saved.exists()
        content = json.loads(saved.read_text())
        assert content["utility_name"] == "Integration Utility"
        assert content["source"] == "api"
        assert len(content["energyratestructure"]) == 1

    def test_saved_file_path_usable_as_rate_file_path(
        self,
        client: TestClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Saved rate file path can be referenced as bill.rate_file_path."""
        monkeypatch.chdir(tmp_path)

        # Build and save rate
        body = {
            "rate": _minimal_rate_dict(),
            "save_to_disk": True,
        }
        resp = client.post("/rates/build", json=body)
        file_path = resp.json()["file_path"]

        # Verify the path can be loaded as a RateSchedule
        from src.rates.rate_parser import load_rate_file

        loaded_rate = load_rate_file(Path(file_path))
        assert loaded_rate.utility_name == "Integration Utility"
        assert loaded_rate.source == "api"


# ---------------------------------------------------------------------------
# Flow C: File upload → reference uploaded file
# ---------------------------------------------------------------------------


class TestFileUploadRateReference:
    """Upload a rate file, verify the returned path is usable."""

    def test_upload_rate_returns_200_with_path(
        self, upload_client: TestClient
    ) -> None:
        """POST /uploads/rate with valid rate JSON returns 200 and path."""
        rate_bytes = json.dumps(_minimal_rate_dict()).encode()
        resp = upload_client.post(
            "/uploads/rate",
            files={"file": ("test_rate.json", BytesIO(rate_bytes), "application/json")},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["file_type"] == "rate"
        assert data["filename"] == "test_rate.json"
        assert data["path"].endswith("test_rate.json")

    def test_uploaded_rate_file_exists_and_is_valid(
        self, upload_client: TestClient
    ) -> None:
        """Uploaded rate file exists on disk and is valid RateSchedule JSON."""
        rate_bytes = json.dumps(_minimal_rate_dict()).encode()
        resp = upload_client.post(
            "/uploads/rate",
            files={"file": ("check_rate.json", BytesIO(rate_bytes), "application/json")},
        )

        path = Path(resp.json()["path"])
        assert path.exists()

        content = json.loads(path.read_text())
        assert content["utility_name"] == "Integration Utility"
        assert len(content["energyratestructure"]) == 1

    def test_uploaded_path_loadable_as_rate_schedule(
        self, upload_client: TestClient
    ) -> None:
        """The returned path can be loaded by the rate parser."""
        rate_bytes = json.dumps(_minimal_rate_dict()).encode()
        resp = upload_client.post(
            "/uploads/rate",
            files={"file": ("loadable.json", BytesIO(rate_bytes), "application/json")},
        )

        from src.rates.rate_parser import load_rate_file

        path = Path(resp.json()["path"])
        rate = load_rate_file(path)
        assert rate.utility_name == "Integration Utility"
        assert rate.tariff_name == "Integration Tariff"


# ---------------------------------------------------------------------------
# Flow D: Load types endpoint
# ---------------------------------------------------------------------------


class TestLoadTypesEndpoint:
    """GET /analyses/load-types returns DOE reference building types."""

    def test_returns_200(self, client: TestClient) -> None:
        """Endpoint returns 200."""
        resp = client.get("/analyses/load-types")

        assert resp.status_code == 200

    def test_count_greater_than_zero(self, client: TestClient) -> None:
        """Response contains at least one load type."""
        data = client.get("/analyses/load-types").json()

        assert data["count"] > 0
        assert len(data["load_types"]) == data["count"]

    def test_known_types_present(self, client: TestClient) -> None:
        """Known DOE reference building types are present."""
        data = client.get("/analyses/load-types").json()
        types = data["load_types"]

        assert "SmallOffice" in types
        assert "LargeHotel" in types
        assert "Warehouse" in types
        assert "Hospital" in types

    def test_response_is_list_of_strings(self, client: TestClient) -> None:
        """All load types are strings."""
        data = client.get("/analyses/load-types").json()

        for t in data["load_types"]:
            assert isinstance(t, str)


# ---------------------------------------------------------------------------
# Flow E: Auth flow
# ---------------------------------------------------------------------------


class TestAuthFlowWithKey:
    """Auth behavior when API_KEY env var is set."""

    def test_missing_header_returns_401(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Protected endpoint without X-API-Key header returns 401."""
        monkeypatch.setenv("API_KEY", "m16-test-key")
        client = TestClient(create_app())

        resp = client.get("/analyses/load-types")

        assert resp.status_code == 401
        assert "Missing API key" in resp.json()["detail"]

    def test_wrong_key_returns_403(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Protected endpoint with wrong key returns 403."""
        monkeypatch.setenv("API_KEY", "m16-test-key")
        client = TestClient(create_app())

        resp = client.get(
            "/analyses/load-types",
            headers={"X-API-Key": "wrong-key"},
        )

        assert resp.status_code == 403
        assert "Invalid API key" in resp.json()["detail"]

    def test_correct_key_returns_200(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Protected endpoint with correct key returns 200."""
        monkeypatch.setenv("API_KEY", "m16-test-key")
        client = TestClient(create_app())

        resp = client.get(
            "/analyses/load-types",
            headers={"X-API-Key": "m16-test-key"},
        )

        assert resp.status_code == 200


class TestAuthFlowWithoutKey:
    """Auth behavior when API_KEY env var is unset."""

    def test_no_header_required(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Requests pass without header when API_KEY is unset."""
        monkeypatch.delenv("API_KEY", raising=False)
        client = TestClient(create_app())

        resp = client.get("/analyses/load-types")

        assert resp.status_code == 200

    def test_rate_build_works_without_auth(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """POST /rates/build works without auth when API_KEY unset."""
        monkeypatch.delenv("API_KEY", raising=False)
        client = TestClient(create_app())

        resp = client.post("/rates/build", json={"rate": _minimal_rate_dict()})

        assert resp.status_code == 200


class TestHealthAlwaysOpen:
    """GET /health always passes regardless of auth configuration."""

    def test_health_with_auth_enabled_no_header(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Health check passes without header when API_KEY is set."""
        monkeypatch.setenv("API_KEY", "m16-test-key")
        client = TestClient(create_app())

        resp = client.get("/health")

        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_health_with_auth_disabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Health check passes when API_KEY is unset."""
        monkeypatch.delenv("API_KEY", raising=False)
        client = TestClient(create_app())

        resp = client.get("/health")

        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# Flow F: Pipeline refactor validation
# ---------------------------------------------------------------------------


class TestPipelineRefactorRunFromConfigs:
    """Verify run_from_configs() is callable with list[SiteConfig] and run() delegates."""

    @patch("src.pipeline.run_climate_data_pipeline")
    @patch("src.pipeline.BatchSimulator")
    @patch("src.pipeline.OutputWriter")
    def test_run_from_configs_accepts_list_of_site_config(
        self,
        mock_writer_cls: MagicMock,
        mock_batch_cls: MagicMock,
        mock_climate: MagicMock,
    ) -> None:
        """run_from_configs() accepts list[SiteConfig] and returns a result dict."""
        # Mock climate pipeline
        mock_climate.return_value = ([], {}, {})

        # Mock batch simulator (returns no results)
        mock_batch = MagicMock()
        mock_batch.run_batch.return_value = ([], [])
        mock_batch_cls.return_value = mock_batch

        # Mock output writer
        mock_writer = MagicMock()
        mock_writer.write_timeseries.return_value = Path("/fake/ts.csv")
        mock_writer_cls.return_value = mock_writer

        pipeline = SolarModelingPipeline(output_dir=Path("/tmp/test"))
        result = pipeline.run_from_configs([])

        assert isinstance(result, dict)
        assert "total_sites" in result
        assert "successful" in result
        assert "failed" in result
        assert "summaries" in result

    @patch.object(SolarModelingPipeline, "run_from_configs")
    @patch("src.pipeline.load_config")
    def test_run_delegates_to_run_from_configs(
        self,
        mock_load_config: MagicMock,
        mock_run_from_configs: MagicMock,
    ) -> None:
        """run() loads CSV then delegates to run_from_configs()."""
        mock_site = MagicMock(spec=SiteConfig)
        mock_load_config.return_value = [mock_site]
        mock_run_from_configs.return_value = {"total_sites": 1}

        pipeline = SolarModelingPipeline(output_dir=Path("/tmp/test"))
        result = pipeline.run(Path("/fake/input.csv"))

        # Verify load_config was called with the CSV path
        mock_load_config.assert_called_once_with(Path("/fake/input.csv"))
        # Verify run_from_configs was called with the loaded configs
        mock_run_from_configs.assert_called_once()
        call_args = mock_run_from_configs.call_args
        assert call_args[0][0] == [mock_site]
        assert result == {"total_sites": 1}

    def test_run_from_configs_method_exists(self) -> None:
        """SolarModelingPipeline has a run_from_configs() method."""
        assert hasattr(SolarModelingPipeline, "run_from_configs")
        assert callable(getattr(SolarModelingPipeline, "run_from_configs"))

    def test_run_method_exists(self) -> None:
        """SolarModelingPipeline has a run() method."""
        assert hasattr(SolarModelingPipeline, "run")
        assert callable(getattr(SolarModelingPipeline, "run"))


# ---------------------------------------------------------------------------
# Cross-flow: rate build → upload flow consistency
# ---------------------------------------------------------------------------


class TestCrossFlowRateConsistency:
    """Built and uploaded rates produce consistent results."""

    def test_built_rate_matches_uploaded_rate(
        self,
        upload_client: TestClient,
    ) -> None:
        """A rate built via /rates/build matches the same rate uploaded."""
        rate_dict = _minimal_rate_dict()

        # Build rate via API
        build_resp = upload_client.post(
            "/rates/build", json={"rate": rate_dict}
        )
        built = build_resp.json()["rate"]

        # Upload same rate via file
        rate_bytes = json.dumps(rate_dict).encode()
        upload_resp = upload_client.post(
            "/uploads/rate",
            files={"file": ("compare.json", BytesIO(rate_bytes), "application/json")},
        )
        uploaded_path = Path(upload_resp.json()["path"])
        uploaded = json.loads(uploaded_path.read_text())

        # Core fields should match
        assert built["utility_name"] == uploaded["utility_name"]
        assert built["tariff_name"] == uploaded["tariff_name"]
        # Built rate has Pydantic defaults (adj, max) expanded; uploaded is raw.
        # Compare the core rate value from both.
        assert (
            built["energyratestructure"][0][0]["rate"]
            == uploaded["energyratestructure"][0][0]["rate"]
        )

    @patch("src.api.runner.SolarModelingPipeline")
    def test_production_endpoint_with_mocked_pipeline(
        self, mock_pipeline_cls: MagicMock, client: TestClient
    ) -> None:
        """POST /analyses/production works with mocked pipeline."""
        mock_pipeline = MagicMock()
        mock_pipeline.run_from_configs.return_value = _mock_pipeline_result()
        mock_pipeline_cls.return_value = mock_pipeline

        request_body = {
            "site": _minimal_site_dict(),
            "system": _minimal_system_dict(),
        }
        resp = client.post("/analyses/production", json=request_body)

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["production"]["annual_energy_mwh"] == 9500.0
        assert len(data["monthly_production_mwh"]) == 12
        assert data["losses"]["shading_pct"] == 0.5
