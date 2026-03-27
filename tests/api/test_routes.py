"""Tests for API routes using mocked pipeline."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from src.api.app import create_app


@pytest.fixture()
def client() -> TestClient:
    """Create a TestClient for the FastAPI app."""
    app = create_app()
    return TestClient(app)


def _mock_pipeline_results(run_name: str = "TestSite_20240101_120000") -> dict:
    """Build a realistic mock pipeline results dict.

    Matches the structure returned by SolarModelingPipeline.run():
    - total_sites, successful, failed counts
    - summaries list with one entry containing SummaryMetrics fields
    - loss_data dict with PySAM keys, monthly_energy, capacity factors
    - timeseries_files, error_files, report_files lists
    """
    loss_data = {
        # Key scalars
        "capacity_factor": 25.4,
        "capacity_factor_ac": 30.2,
        "kwh_per_kw": 1650.0,
        "performance_ratio": 0.82,
        # Annual energy totals (kWh)
        "annual_dc_nominal": 28_000_000.0,
        "annual_dc_gross": 27_500_000.0,
        "annual_dc_net": 26_500_000.0,
        "annual_ac_gross": 25_000_000.0,
        "annual_energy": 24_500_000.0,
        # Annual loss percentages
        "annual_poa_shading_loss_percent": 1.5,
        "annual_poa_soiling_loss_percent": 5.0,
        "annual_poa_cover_loss_percent": 0.0,
        "annual_dc_module_loss_percent": 1.2,
        "annual_dc_mismatch_loss_percent": 2.0,
        "annual_dc_diodes_loss_percent": 0.5,
        "annual_dc_wiring_loss_percent": 2.0,
        "annual_dc_nameplate_loss_percent": 1.5,
        "annual_dc_tracking_loss_percent": 0.0,
        "annual_bifacial_electrical_mismatch_percent": 0.0,
        "annual_ac_inv_clip_loss_percent": 0.3,
        "annual_ac_inv_eff_loss_percent": 3.2,
        "annual_ac_wiring_loss_percent": 0.5,
        "annual_xfmr_loss_percent": 1.0,
        "annual_ac_perf_adj_loss_percent": 2.5,
        "annual_poa_rear_gain_percent": 0.0,
        # Snow
        "annual_dc_snow_loss_percent": 0.0,
        "annual_snow_loss": 0.0,
        "monthly_snow_loss": [0.0] * 12,
        # Monthly arrays (kWh)
        "monthly_energy": [
            1_500_000, 1_700_000, 2_100_000, 2_400_000, 2_700_000, 2_800_000,
            2_900_000, 2_750_000, 2_300_000, 2_000_000, 1_600_000, 1_400_000,
        ],
        "monthly_poa_eff": [
            120.0, 140.0, 175.0, 200.0, 220.0, 230.0,
            235.0, 215.0, 185.0, 160.0, 125.0, 110.0,
        ],
        # GHI
        "avg_daytime_ghi_wm2": 450.0,
        "annual_ghi_kwh_m2": 2100.0,
        # Bifacial flag
        "bifacial": False,
    }
    return {
        "total_sites": 1,
        "successful": 1,
        "failed": 0,
        "timeseries_files": [Path("/tmp/out/timeseries/test_8760.csv")],
        "summaries": [
            {
                "site_name": "TestSite",
                "run_name": run_name,
                "customer": "API",
                "weather_year": "2023",
                "dc_size_mw": 13.0,
                "ac_installed_mw": 10.0,
                "ac_poi_mw": 10.0,
                "panel_model": "CSI Solar Co. Ltd. CS3U-355P",
                "inverter_model": (
                    "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]"
                ),
                "racking": "tracker",
                "tilt": 60.0,
                "azimuth": 180.0,
                "annual_energy_mwh": 24_500.0,
                "net_capacity_factor": 0.2797,
                "specific_yield": 1650.0,
                "performance_ratio": 0.82,
                "shading_pct_applied": 0.0,
                "simulation_timestamp": "2024-01-01T00:00:00+00:00",
                "loss_data": loss_data,
                "errors": [],
            },
        ],
        "error_files": [],
        "report_files": [Path("/tmp/out/reports/test.pdf")],
    }


def _minimal_request_body() -> dict:
    """Build a minimal valid request body for POST /analyses/production.

    Includes a fixed run_name so tests can match it against mock results.
    """
    return {
        "site": {
            "name": "TestSite",
            "latitude": 33.45,
            "longitude": -112.07,
            "run_name": "TestSite_20240101_120000",
        },
        "system": {
            "dc_capacity_mw": 13.0,
            "ac_capacity_mw": 10.0,
            "module": "CSI Solar Co. Ltd. CS3U-355P",
            "inverter": "Sungrow Power Supply Co - Ltd : SG250HX-US [800V]",
            "racking": "tracker",
            "gcr": 0.35,
        },
    }


class TestHealthEndpoint:
    """GET /health returns status and version."""

    def test_health_ok(self, client: TestClient) -> None:
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["version"] == "1.0.0"


class TestProductionEndpointValid:
    """POST /analyses/production with valid inputs returns 200."""

    @patch("src.api.routes.analyses.run_production")
    def test_minimal_request_returns_200(
        self, mock_run: MagicMock, client: TestClient, tmp_path: Path
    ) -> None:
        """Minimal request with only required fields → 200 + valid response."""
        mock_run.return_value = _mock_pipeline_results()

        resp = client.post("/analyses/production", json=_minimal_request_body())

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["site_name"] == "TestSite"
        assert "run_id" in data
        assert "production" in data
        assert "losses" in data
        assert "monthly_production_mwh" in data
        assert len(data["monthly_production_mwh"]) == 12
        assert "files" in data

    @patch("src.api.routes.analyses.run_production")
    def test_production_metrics_values(
        self, mock_run: MagicMock, client: TestClient
    ) -> None:
        """Response production metrics match the mock pipeline output."""
        mock_run.return_value = _mock_pipeline_results()

        resp = client.post("/analyses/production", json=_minimal_request_body())
        data = resp.json()

        prod = data["production"]
        assert prod["annual_energy_mwh"] == 24_500.0
        assert prod["capacity_factor_dc"] == 25.4
        assert prod["capacity_factor_ac"] == 30.2
        assert prod["specific_yield_kwh_per_kwp"] == 1650.0
        assert prod["performance_ratio"] == 0.82

    @patch("src.api.routes.analyses.run_production")
    def test_loss_breakdown_values(
        self, mock_run: MagicMock, client: TestClient
    ) -> None:
        """Response loss breakdown matches the mock loss_data."""
        mock_run.return_value = _mock_pipeline_results()

        resp = client.post("/analyses/production", json=_minimal_request_body())
        losses = resp.json()["losses"]

        assert losses["shading_pct"] == 1.5
        assert losses["dc_wiring_pct"] == 2.0
        assert losses["ac_wiring_pct"] == 0.5
        assert losses["transformer_pct"] == 1.0
        assert losses["mismatch_pct"] == 2.0
        assert losses["lid_pct"] == 1.5
        assert losses["availability_pct"] == 2.5
        assert losses["subhourly_clipping_pct"] == 0.3
        assert losses["inverter_efficiency_pct"] == 3.2

    @patch("src.api.routes.analyses.run_production")
    def test_all_optional_fields(
        self, mock_run: MagicMock, client: TestClient
    ) -> None:
        """Request with all optional fields → 200."""
        mock_run.return_value = _mock_pipeline_results(
            run_name="full_run_001"
        )

        body = _minimal_request_body()
        body["site"]["customer"] = "Acme Corp"
        body["site"]["run_name"] = "full_run_001"
        body["system"]["ac_poi_mw"] = 8.0
        body["system"]["tilt"] = 30.0
        body["system"]["azimuth"] = 190.0
        body["system"]["module_orientation"] = "landscape"
        body["system"]["num_modules"] = 2
        body["system"]["ground_clearance_m"] = 2.0
        body["system"]["bifacial"] = True
        body["losses"] = {
            "shading_pct": 3.0,
            "dc_wiring_pct": 1.5,
            "ac_wiring_pct": 0.8,
            "transformer_pct": 0.5,
            "degradation_pct": 0.3,
            "availability_pct": 1.0,
            "mismatch_pct": 1.2,
            "lid_pct": 0.9,
        }

        resp = client.post("/analyses/production", json=body)
        assert resp.status_code == 200
        assert resp.json()["run_id"] == "full_run_001"

    @patch("src.api.routes.analyses.run_production")
    def test_monthly_production_kwh_to_mwh(
        self, mock_run: MagicMock, client: TestClient
    ) -> None:
        """Monthly energy is converted from kWh to MWh."""
        mock_run.return_value = _mock_pipeline_results()

        resp = client.post("/analyses/production", json=_minimal_request_body())
        monthly = resp.json()["monthly_production_mwh"]

        assert len(monthly) == 12
        # Jan: 1_500_000 kWh → 1500.0 MWh
        assert monthly[0] == 1500.0
        # Jul: 2_900_000 kWh → 2900.0 MWh
        assert monthly[6] == 2900.0

    @patch("src.api.routes.analyses.run_production")
    def test_file_links_present(
        self, mock_run: MagicMock, client: TestClient
    ) -> None:
        """Response includes file links with run_name."""
        mock_run.return_value = _mock_pipeline_results()

        resp = client.post("/analyses/production", json=_minimal_request_body())
        files = resp.json()["files"]

        assert "/report" in files["report_url"]
        assert "/timeseries" in files["timeseries_url"]


class TestProductionEndpointValidation:
    """POST /analyses/production with invalid inputs returns 422."""

    def test_missing_site_returns_422(self, client: TestClient) -> None:
        """Request without site → 422."""
        body = {
            "system": {
                "dc_capacity_mw": 13.0,
                "ac_capacity_mw": 10.0,
                "module": "TestModule",
                "inverter": "TestInverter",
                "racking": "tracker",
                "gcr": 0.35,
            },
        }
        resp = client.post("/analyses/production", json=body)
        assert resp.status_code == 422

    def test_negative_dc_capacity_returns_422(self, client: TestClient) -> None:
        """Negative dc_capacity_mw → 422."""
        body = _minimal_request_body()
        body["system"]["dc_capacity_mw"] = -1.0
        resp = client.post("/analyses/production", json=body)
        assert resp.status_code == 422

    def test_missing_system_returns_422(self, client: TestClient) -> None:
        """Request without system → 422."""
        body = {
            "site": {
                "name": "TestSite",
                "latitude": 33.45,
                "longitude": -112.07,
            },
        }
        resp = client.post("/analyses/production", json=body)
        assert resp.status_code == 422

    def test_invalid_racking_returns_422(self, client: TestClient) -> None:
        """Invalid racking value → 422."""
        body = _minimal_request_body()
        body["system"]["racking"] = "rooftop"
        resp = client.post("/analyses/production", json=body)
        assert resp.status_code == 422


class TestProductionEndpointError:
    """POST /analyses/production with pipeline failure returns 500."""

    @patch("src.api.routes.analyses.run_production")
    def test_pipeline_exception_returns_500(
        self, mock_run: MagicMock, client: TestClient
    ) -> None:
        """Pipeline exception → 500 with error detail."""
        mock_run.side_effect = RuntimeError("NSRDB API timeout")

        resp = client.post("/analyses/production", json=_minimal_request_body())

        assert resp.status_code == 500
        data = resp.json()
        assert "NSRDB API timeout" in data["detail"]


# ---------------------------------------------------------------------------
# Bill savings endpoint tests
# ---------------------------------------------------------------------------


def _mock_bill_savings_results(
    run_name: str = "TestSite_20240101_120000",
) -> dict:
    """Build a mock pipeline results dict that includes bill_savings.

    Extends _mock_pipeline_results() with a bill_savings dict on the summary.
    """
    results = _mock_pipeline_results(run_name=run_name)
    results["summaries"][0]["bill_savings"] = {
        "annual_bill_without_solar": 120_000.0,
        "annual_bill_with_solar": 45_000.0,
        "annual_savings": 75_000.0,
        "savings_percent": 62.5,
        "avoided_cost_per_kwh": 0.085,
        "annual_demand_savings": 12_000.0,
        "annual_export_kwh": 3_500_000.0,
        "annual_export_credits": 8_000.0,
        "nem_true_up_credit": 2_500.0,
        "monthly_detail": [
            {
                "month": m,
                "bill_without": 10_000.0,
                "bill_with": 3_750.0,
                "savings": 6_250.0,
            }
            for m in range(1, 13)
        ],
    }
    return results


def _bill_savings_request_body() -> dict:
    """Build a minimal valid request body for POST /analyses/bill-savings."""
    body = _minimal_request_body()
    body["bill"] = {
        "rate_file_path": "/tmp/rates.json",
        "load_profile_path": "/tmp/load.csv",
    }
    return body


def _mock_site_config() -> MagicMock:
    """Build a mock SiteConfig with just enough attributes for the route."""
    mock = MagicMock()
    mock.run_name = "TestSite_20240101_120000"
    return mock


class TestBillSavingsEndpointValid:
    """POST /analyses/bill-savings with valid inputs returns 200."""

    @patch("src.api.routes.analyses.run_production")
    @patch("src.api.routes.analyses.bill_savings_request_to_site_config")
    def test_valid_request_returns_200(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """Valid bill savings request → 200 with bill_savings in response."""
        mock_adapter.return_value = _mock_site_config()
        mock_run.return_value = _mock_bill_savings_results()

        resp = client.post(
            "/analyses/bill-savings", json=_bill_savings_request_body()
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["site_name"] == "TestSite"
        assert "production" in data
        assert "losses" in data
        assert "monthly_production_mwh" in data
        assert "bill_savings" in data
        assert "monthly_bill_detail" in data
        assert "files" in data

    @patch("src.api.routes.analyses.run_production")
    @patch("src.api.routes.analyses.bill_savings_request_to_site_config")
    def test_bill_savings_values(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """Bill savings metrics match mock data."""
        mock_adapter.return_value = _mock_site_config()
        mock_run.return_value = _mock_bill_savings_results()

        resp = client.post(
            "/analyses/bill-savings", json=_bill_savings_request_body()
        )
        bs = resp.json()["bill_savings"]

        assert bs["annual_bill_without_solar"] == 120_000.0
        assert bs["annual_bill_with_solar"] == 45_000.0
        assert bs["annual_savings"] == 75_000.0
        assert bs["savings_percent"] == 62.5
        assert bs["avoided_cost_per_kwh"] == 0.085
        assert bs["annual_demand_savings"] == 12_000.0
        assert bs["annual_export_kwh"] == 3_500_000.0
        assert bs["annual_export_credits"] == 8_000.0
        assert bs["nem_true_up_credit"] == 2_500.0

    @patch("src.api.routes.analyses.run_production")
    @patch("src.api.routes.analyses.bill_savings_request_to_site_config")
    def test_monthly_bill_detail(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """Monthly bill detail has 12 entries with correct structure."""
        mock_adapter.return_value = _mock_site_config()
        mock_run.return_value = _mock_bill_savings_results()

        resp = client.post(
            "/analyses/bill-savings", json=_bill_savings_request_body()
        )
        monthly = resp.json()["monthly_bill_detail"]

        assert len(monthly) == 12
        assert monthly[0]["month"] == 1
        assert monthly[0]["bill_without"] == 10_000.0
        assert monthly[0]["bill_with"] == 3_750.0
        assert monthly[0]["savings"] == 6_250.0
        assert monthly[11]["month"] == 12


class TestBillSavingsEndpointValidation:
    """POST /analyses/bill-savings with invalid inputs returns 422."""

    def test_missing_bill_config_returns_422(self, client: TestClient) -> None:
        """Request without bill config → 422."""
        body = _minimal_request_body()
        # No "bill" key
        resp = client.post("/analyses/bill-savings", json=body)
        assert resp.status_code == 422

    def test_both_rate_sources_returns_422(self, client: TestClient) -> None:
        """Providing both rate_file_path and utility_name → 422."""
        body = _bill_savings_request_body()
        body["bill"]["utility_name"] = "Arizona Public Service"
        body["bill"]["tariff_name"] = "E-12 TOU"
        resp = client.post("/analyses/bill-savings", json=body)
        assert resp.status_code == 422

    def test_no_rate_source_returns_422(self, client: TestClient) -> None:
        """Providing neither rate source → 422."""
        body = _bill_savings_request_body()
        body["bill"] = {"load_profile_path": "/tmp/load.csv"}
        resp = client.post("/analyses/bill-savings", json=body)
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# BESS endpoint tests
# ---------------------------------------------------------------------------


def _mock_bess_btm_results(
    run_name: str = "TestSite_20240101_120000",
) -> dict:
    """Build a mock pipeline results dict for BTM BESS dispatch."""
    results = _mock_pipeline_results(run_name=run_name)
    results["summaries"][0]["bess_dispatch"] = {
        "bess_power_mw": 5.0,
        "bess_duration_hr": 4.0,
        "bess_capacity_kwh": 20_000.0,
        "bess_strategy": "global",
        "solar_only_annual_bill": 45_000.0,
        "solar_plus_bess_annual_bill": 32_000.0,
        "bess_incremental_savings": 13_000.0,
        "bess_demand_savings": 8_000.0,
        "bess_energy_savings": 5_000.0,
        "annual_cycles": 300.0,
        "annual_throughput_kwh": 6_000_000.0,
        "average_daily_cycles": 0.82,
        "capacity_utilization_pct": 72.5,
        "total_curtailed_kwh": 50_000.0,
        "estimated_annual_degradation_pct": 1.5,
        "total_export_kwh": 1_000_000.0,
        "total_export_hours": 2_500,
        "charging_source": "solar+grid",
        "solar_only_export_kwh": 500_000.0,
        "solar_only_export_credits": 15_000.0,
        "solar_plus_bess_export_kwh": 300_000.0,
        "solar_plus_bess_export_credits": 9_000.0,
        "monthly_solver_status": ["optimal"] * 12,
        "heatmap_data": [[0.0] * 24 for _ in range(12)],
    }
    results["summaries"][0]["bill_savings"] = {
        "annual_bill_without_solar": 120_000.0,
        "annual_bill_with_solar": 32_000.0,
        "annual_savings": 88_000.0,
        "savings_percent": 73.3,
        "avoided_cost_per_kwh": 0.10,
        "annual_demand_savings": 15_000.0,
        "annual_export_kwh": 3_500_000.0,
        "annual_export_credits": 8_000.0,
        "nem_true_up_credit": 2_500.0,
    }
    return results


def _mock_bess_ftm_results(
    run_name: str = "TestSite_20240101_120000",
) -> dict:
    """Build a mock pipeline results dict for FTM BESS dispatch."""
    results = _mock_pipeline_results(run_name=run_name)
    results["summaries"][0]["bess_dispatch"] = {
        "dispatch_mode": "ftm",
        "bess_power_mw": 5.0,
        "bess_duration_hr": 4.0,
        "bess_capacity_kwh": 20_000.0,
        "bess_strategy": "global",
        "ftm_revenue": 150_000.0,
        "annual_cycles": 350.0,
        "annual_throughput_kwh": 7_000_000.0,
        "average_daily_cycles": 0.96,
        "capacity_utilization_pct": 80.0,
        "total_curtailed_kwh": 30_000.0,
        "estimated_annual_degradation_pct": 1.8,
        "charging_source": "solar+grid",
        "monthly_solver_status": ["optimal"] * 12,
        "heatmap_data": [[0.0] * 24 for _ in range(12)],
    }
    results["summaries"][0]["lmp"] = {
        "iso": "pjm",
        "zone": "AEP",
        "market": "DAY_AHEAD_HOURLY",
        "year": 2023,
        "mean_lmp": 35.50,
        "min_lmp": -5.00,
        "max_lmp": 250.00,
    }
    results["summaries"][0]["ftm_economics"] = {
        "annual_solar_revenue": 200_000.0,
        "annual_bess_arbitrage_revenue": 150_000.0,
        "annual_ancillary_revenue": 25_000.0,
        "annual_gross_revenue": 375_000.0,
        "total_project_npv": 2_500_000.0,
        "bess_npv": 800_000.0,
        "solar_npv": 1_700_000.0,
        "system_lcoe_per_kwh": 0.045,
        "total_installed_cost": 15_000_000.0,
    }
    return results


def _mock_bess_sizing_results(
    run_name: str = "TestSite_20240101_120000",
) -> dict:
    """Build a mock pipeline results dict for BESS sizing optimization."""
    results = _mock_bess_btm_results(run_name=run_name)
    results["summaries"][0]["bess_sizing"] = {
        "optimal_power_mw": 7.5,
        "optimal_duration_hr": 4.0,
        "optimal_capacity_kwh": 30_000.0,
        "bess_npv": 500_000.0,
        "total_project_npv": 3_000_000.0,
        "system_lcoe_per_kwh": 0.055,
        "total_installed_cost": 20_000_000.0,
        "solar_cost": 12_000_000.0,
        "bess_cost": 8_000_000.0,
        "total_annual_savings": 200_000.0,
        "bess_incremental_savings": 50_000.0,
        "annual_production_mwh": 24_500.0,
        "lifetime_generation_mwh": 612_500.0,
        "combos_evaluated": 12,
        "project_lifetime_years": 25,
        "discount_rate_pct": 7.0,
        "rate_escalation_pct": 2.0,
        "sweep_results": [],
    }
    return results


def _bess_btm_request_body() -> dict:
    """Build a minimal valid request body for POST /analyses/bess (BTM)."""
    body = _minimal_request_body()
    body["bess"] = {
        "power_mw": 5.0,
        "duration_hr": 4.0,
    }
    body["bill"] = {
        "rate_file_path": "/tmp/rates.json",
        "load_profile_path": "/tmp/load.csv",
    }
    return body


def _bess_ftm_request_body() -> dict:
    """Build a minimal valid request body for POST /analyses/bess (FTM)."""
    body = _minimal_request_body()
    body["bess"] = {
        "power_mw": 5.0,
        "duration_hr": 4.0,
    }
    body["ftm"] = {
        "dispatch_mode": "ftm",
        "iso": "pjm",
        "lmp_zone": "AEP",
    }
    return body


def _bess_optimization_request_body() -> dict:
    """Build a minimal valid request body for POST /analyses/bess (optimization)."""
    body = _bess_btm_request_body()
    body["bess_economics"] = {
        "optimize": True,
        "solar_cost_per_kw_dc": 1200.0,
        "solar_cost_per_kw_ac": 200.0,
    }
    return body


class TestBESSEndpointBTM:
    """POST /analyses/bess with BTM request."""

    @patch("src.api.routes.analyses.run_production")
    @patch("src.api.routes.analyses.bess_request_to_site_config")
    def test_btm_returns_200(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """Valid BTM BESS request → 200 with dispatch and bill_comparison."""
        mock_adapter.return_value = _mock_site_config()
        mock_run.return_value = _mock_bess_btm_results()

        resp = client.post("/analyses/bess", json=_bess_btm_request_body())

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert "dispatch" in data
        assert "bill_comparison" in data
        assert data["bill_comparison"] is not None
        assert data["lmp"] is None
        assert data["ftm_economics"] is None
        assert data["sizing"] is None

    @patch("src.api.routes.analyses.run_production")
    @patch("src.api.routes.analyses.bess_request_to_site_config")
    def test_btm_dispatch_values(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """BTM dispatch metrics match mock data."""
        mock_adapter.return_value = _mock_site_config()
        mock_run.return_value = _mock_bess_btm_results()

        resp = client.post("/analyses/bess", json=_bess_btm_request_body())
        d = resp.json()["dispatch"]

        assert d["bess_power_mw"] == 5.0
        assert d["bess_duration_hr"] == 4.0
        assert d["bess_capacity_kwh"] == 20_000.0
        assert d["bess_strategy"] == "global"
        assert d["annual_cycles"] == 300.0
        assert d["annual_throughput_kwh"] == 6_000_000.0
        assert d["capacity_utilization_pct"] == 72.5
        assert d["total_curtailed_kwh"] == 50_000.0
        assert d["estimated_annual_degradation_pct"] == 1.5
        assert d["charging_source"] == "solar+grid"

    @patch("src.api.routes.analyses.run_production")
    @patch("src.api.routes.analyses.bess_request_to_site_config")
    def test_btm_bill_comparison_values(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """BTM bill comparison metrics match mock data."""
        mock_adapter.return_value = _mock_site_config()
        mock_run.return_value = _mock_bess_btm_results()

        resp = client.post("/analyses/bess", json=_bess_btm_request_body())
        bc = resp.json()["bill_comparison"]

        assert bc["solar_only_annual_bill"] == 45_000.0
        assert bc["solar_plus_bess_annual_bill"] == 32_000.0
        assert bc["bess_incremental_savings"] == 13_000.0
        assert bc["bess_demand_savings"] == 8_000.0
        assert bc["bess_energy_savings"] == 5_000.0

    @patch("src.api.routes.analyses.run_production")
    @patch("src.api.routes.analyses.bess_request_to_site_config")
    def test_btm_bill_savings_present(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """BTM response includes bill_savings when present in pipeline."""
        mock_adapter.return_value = _mock_site_config()
        mock_run.return_value = _mock_bess_btm_results()

        resp = client.post("/analyses/bess", json=_bess_btm_request_body())
        bs = resp.json()["bill_savings"]

        assert bs is not None
        assert bs["annual_bill_without_solar"] == 120_000.0
        assert bs["annual_savings"] == 88_000.0


class TestBESSEndpointFTM:
    """POST /analyses/bess with FTM request."""

    @patch("src.api.routes.analyses.run_production")
    @patch("src.api.routes.analyses.bess_request_to_site_config")
    def test_ftm_returns_200(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """Valid FTM BESS request → 200 with lmp and ftm_economics."""
        mock_adapter.return_value = _mock_site_config()
        mock_run.return_value = _mock_bess_ftm_results()

        resp = client.post("/analyses/bess", json=_bess_ftm_request_body())

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert "dispatch" in data
        assert data["bill_comparison"] is None
        assert data["lmp"] is not None
        assert data["ftm_economics"] is not None
        assert data["sizing"] is None
        assert data["bill_savings"] is None

    @patch("src.api.routes.analyses.run_production")
    @patch("src.api.routes.analyses.bess_request_to_site_config")
    def test_ftm_lmp_values(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """FTM LMP summary values match mock data."""
        mock_adapter.return_value = _mock_site_config()
        mock_run.return_value = _mock_bess_ftm_results()

        resp = client.post("/analyses/bess", json=_bess_ftm_request_body())
        lmp = resp.json()["lmp"]

        assert lmp["iso"] == "pjm"
        assert lmp["zone"] == "AEP"
        assert lmp["market"] == "DAY_AHEAD_HOURLY"
        assert lmp["year"] == 2023
        assert lmp["mean_lmp"] == 35.50
        assert lmp["min_lmp"] == -5.00
        assert lmp["max_lmp"] == 250.00

    @patch("src.api.routes.analyses.run_production")
    @patch("src.api.routes.analyses.bess_request_to_site_config")
    def test_ftm_economics_values(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """FTM economics values match mock data."""
        mock_adapter.return_value = _mock_site_config()
        mock_run.return_value = _mock_bess_ftm_results()

        resp = client.post("/analyses/bess", json=_bess_ftm_request_body())
        fe = resp.json()["ftm_economics"]

        assert fe["annual_solar_revenue"] == 200_000.0
        assert fe["annual_bess_arbitrage_revenue"] == 150_000.0
        assert fe["annual_ancillary_revenue"] == 25_000.0
        assert fe["annual_gross_revenue"] == 375_000.0
        assert fe["total_project_npv"] == 2_500_000.0
        assert fe["bess_npv"] == 800_000.0
        assert fe["solar_npv"] == 1_700_000.0
        assert fe["system_lcoe_per_kwh"] == 0.045
        assert fe["total_installed_cost"] == 15_000_000.0


class TestBESSEndpointOptimization:
    """POST /analyses/bess with sizing optimization request."""

    @patch("src.api.routes.analyses.run_production")
    @patch("src.api.routes.analyses.bess_request_to_site_config")
    def test_optimization_returns_200(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """Optimization request → 200 with sizing summary."""
        mock_adapter.return_value = _mock_site_config()
        mock_run.return_value = _mock_bess_sizing_results()

        resp = client.post(
            "/analyses/bess", json=_bess_optimization_request_body()
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["sizing"] is not None
        assert data["dispatch"] is not None
        assert data["bill_comparison"] is not None

    @patch("src.api.routes.analyses.run_production")
    @patch("src.api.routes.analyses.bess_request_to_site_config")
    def test_sizing_values(
        self,
        mock_adapter: MagicMock,
        mock_run: MagicMock,
        client: TestClient,
    ) -> None:
        """Sizing summary values match mock data."""
        mock_adapter.return_value = _mock_site_config()
        mock_run.return_value = _mock_bess_sizing_results()

        resp = client.post(
            "/analyses/bess", json=_bess_optimization_request_body()
        )
        sz = resp.json()["sizing"]

        assert sz["optimal_power_mw"] == 7.5
        assert sz["optimal_duration_hr"] == 4.0
        assert sz["optimal_capacity_kwh"] == 30_000.0
        assert sz["bess_npv"] == 500_000.0
        assert sz["total_project_npv"] == 3_000_000.0
        assert sz["system_lcoe_per_kwh"] == 0.055
        assert sz["total_installed_cost"] == 20_000_000.0
        assert sz["combos_evaluated"] == 12


class TestBESSEndpointValidation:
    """POST /analyses/bess with invalid inputs returns 422."""

    def test_btm_without_bill_returns_422(self, client: TestClient) -> None:
        """BTM request without bill → 422."""
        body = _minimal_request_body()
        body["bess"] = {"power_mw": 5.0, "duration_hr": 4.0}
        # No bill, no ftm → BTM mode, bill required
        resp = client.post("/analyses/bess", json=body)
        assert resp.status_code == 422

    def test_missing_bess_config_returns_422(self, client: TestClient) -> None:
        """Request without bess → 422."""
        body = _minimal_request_body()
        body["bill"] = {
            "rate_file_path": "/tmp/rates.json",
            "load_profile_path": "/tmp/load.csv",
        }
        # No "bess" key
        resp = client.post("/analyses/bess", json=body)
        assert resp.status_code == 422
