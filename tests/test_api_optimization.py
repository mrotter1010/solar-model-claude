"""Tests for the optimization API endpoint (POST /analyses/optimize).

All tests mock optimize_solar / optimize_solar_bess to avoid calling real
PySAM. Tests cover:
- buildable_acres direct input
- buildability_run_id lookup
- Validation: both/neither acres source, bess without dispatch_mode
- All four modes: production, lcoe, npv, solar_bess
- Response includes run_id (confirms persistence flow)
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from src.api.app import create_app


@pytest.fixture()
def client() -> TestClient:
    """Create a TestClient for the FastAPI app."""
    app = create_app()
    return TestClient(app)


# ---------------------------------------------------------------------------
# Mock optimizer return values
# ---------------------------------------------------------------------------

def _mock_solar_result(mode: str = "production") -> dict:
    """Build a mock optimize_solar() return value."""
    winner_point = {
        "gcr": 0.35,
        "dcac_ratio": 1.30,
        "mw_dc": 10.5,
        "mw_ac": 8.077,
        "num_modules": 28000,
        "annual_energy_kwh": 18_500_000.0,
        "annual_energy_mwh": 18500.0,
        "capacity_factor_ac": 0.254,
        "clipping_correction_pct": 1.2,
        "capex_total": 12_600_000.0 if mode in ("lcoe", "npv") else None,
        "opex_per_year": 210_000.0 if mode in ("lcoe", "npv") else None,
        "net_capex": 8_820_000.0 if mode in ("lcoe", "npv") else None,
        "lcoe_per_mwh": 35.5 if mode in ("lcoe", "npv") else None,
        "lcoe_per_kwh": 0.0355 if mode in ("lcoe", "npv") else None,
        "annual_revenue_year1": 925_000.0 if mode == "npv" else None,
        "npv": 2_500_000.0 if mode == "npv" else None,
        "simple_payback_years": 10.2 if mode == "npv" else None,
        "irr": 0.12 if mode == "npv" else None,
        "failed": False,
        "error": None,
    }

    winners = {
        "max_production": winner_point,
        "max_yield": winner_point,
    }
    if mode in ("lcoe", "npv"):
        winners["lcoe"] = winner_point
    if mode == "npv":
        winners["npv"] = winner_point

    return {
        "mode": mode,
        "winners": winners,
        "sweep_results": [winner_point],
        "sweep_metadata": {
            "mode": mode,
            "racking": "tracker",
            "buildable_acres": 100.0,
            "utilization_factor": 0.75,
            "module_power_w": 551.0,
            "module_area_m2": 2.51,
            "gcr_range": (0.30, 0.60),
            "dcac_range": (1.15, 1.60),
            "total_combinations": 70,
            "failed_combinations": 0,
            "retain_hourly": False,
        },
    }


def _mock_bess_result() -> dict:
    """Build a mock optimize_solar_bess() return value."""
    solar_point = {
        "gcr": 0.35,
        "dcac_ratio": 1.30,
        "mw_dc": 10.5,
        "mw_ac": 8.077,
        "num_modules": 28000,
        "annual_energy_kwh": 18_500_000.0,
        "annual_energy_mwh": 18500.0,
        "capacity_factor_ac": 0.254,
        "npv": 2_500_000.0,
        "capex_total": 12_600_000.0,
        "opex_per_year": 210_000.0,
        "failed": False,
        "error": None,
    }
    bess_combo = {
        "solar_gcr": 0.35,
        "solar_dcac": 1.30,
        "solar_mw_ac": 8.077,
        "bess_power_mw": 2.019,
        "bess_duration_hr": 4.0,
        "bess_capacity_mwh": 8.077,
        "solar_npv": 2_500_000.0,
        "bess_npv": 350_000.0,
        "combined_npv": 2_850_000.0,
        "bess_capex": 2_746_180.0,
        "bess_opex_per_year": 50_475.0,
        "bess_annual_revenue": 180_000.0,
        "dispatch_mode": "ftm",
        "charging_mode": "solar_and_grid",
        "failed": False,
        "error": None,
    }
    return {
        "mode": "solar_bess",
        "winners": {
            "max_production": solar_point,
            "max_yield": solar_point,
            "solar_only_npv": solar_point,
            "solar_bess_npv": bess_combo,
        },
        "bess_adds_value": True,
        "bess_winner_detail": {
            "solar": solar_point,
            "bess": {
                "power_mw": 2.019,
                "duration_hr": 4.0,
                "capacity_mwh": 8.077,
                "capex": 2_746_180.0,
                "opex_per_year": 50_475.0,
                "annual_revenue": 180_000.0,
                "npv": 350_000.0,
                "dispatch_mode": "ftm",
                "charging_mode": "solar_and_grid",
            },
            "combined": {
                "npv": 2_850_000.0,
                "solar_npv": 2_500_000.0,
                "bess_npv": 350_000.0,
                "total_capex": 15_346_180.0,
                "total_opex": 260_475.0,
            },
        },
        "solar_sweep_results": [solar_point],
        "bess_sweep_results": [bess_combo],
        "sweep_metadata": {
            "mode": "solar_bess",
            "racking": "tracker",
            "buildable_acres": 100.0,
            "utilization_factor": 0.75,
            "module_power_w": 551.0,
            "module_area_m2": 2.51,
            "gcr_range": (0.30, 0.60),
            "dcac_range": (1.15, 1.60),
            "total_combinations": 70,
            "failed_combinations": 0,
            "dispatch_mode": "ftm",
            "charging_mode": "solar_and_grid",
            "total_solar_combinations": 70,
            "total_bess_combinations": 700,
            "failed_bess_combinations": 0,
            "bess_power_fractions": [0.15, 0.25, 0.40, 0.55, 0.70],
            "bess_durations_hr": [2.0, 4.0],
            "bess_cost_per_kwh": 340.0,
            "bess_opex_per_kw_year": 25.0,
            "retain_hourly": True,
        },
    }


# ---------------------------------------------------------------------------
# Base request payloads
# ---------------------------------------------------------------------------

def _base_request(**overrides) -> dict:
    """Build a minimal valid optimization request dict."""
    req = {
        "latitude": 33.45,
        "longitude": -112.07,
        "buildable_acres": 100.0,
        "racking": "tracker",
    }
    req.update(overrides)
    return req


def _production_request(**overrides) -> dict:
    """Production mode — no economic inputs."""
    return _base_request(**overrides)


def _lcoe_request(**overrides) -> dict:
    """LCOE mode — economic inputs, no revenue."""
    req = _base_request(
        solar_cost_per_kw_dc=1200.0,
        solar_opex_per_kw_dc_year=20.0,
        discount_rate_pct=7.0,
        project_lifetime_years=25,
        degradation_pct=0.5,
        itc_pct=30.0,
    )
    req.update(overrides)
    return req


def _npv_request(**overrides) -> dict:
    """NPV mode — economic + revenue inputs."""
    req = _lcoe_request(
        energy_price_per_kwh=0.05,
        energy_cost_escalator_pct=2.0,
    )
    req.update(overrides)
    return req


def _bess_request(**overrides) -> dict:
    """Solar+BESS mode."""
    req = _npv_request(
        bess_enabled=True,
        dispatch_mode="ftm",
        lmp_prices=[50.0] * 8760,
        lmp_iso="pjm",
        lmp_zone="AEP",
    )
    req.update(overrides)
    return req


# ---------------------------------------------------------------------------
# Tests: Validation
# ---------------------------------------------------------------------------


class TestOptimizationValidation:
    """Request validation tests (no mocking needed — Pydantic rejects)."""

    def test_both_buildable_acres_and_run_id_returns_422(
        self, client: TestClient
    ) -> None:
        """Both buildable_acres AND buildability_run_id → 422."""
        payload = _base_request(
            buildable_acres=100.0,
            buildability_run_id="some-run",
        )
        resp = client.post("/analyses/optimize", json=payload)
        assert resp.status_code == 422
        assert "exactly one" in resp.json()["detail"][0]["msg"].lower()

    def test_neither_buildable_acres_nor_run_id_returns_422(
        self, client: TestClient
    ) -> None:
        """Neither buildable_acres nor buildability_run_id → 422."""
        payload = {
            "latitude": 33.45,
            "longitude": -112.07,
        }
        resp = client.post("/analyses/optimize", json=payload)
        assert resp.status_code == 422
        assert "required" in resp.json()["detail"][0]["msg"].lower()

    def test_bess_enabled_without_dispatch_mode_returns_422(
        self, client: TestClient
    ) -> None:
        """bess_enabled=True without dispatch_mode → 422."""
        payload = _base_request(bess_enabled=True)
        resp = client.post("/analyses/optimize", json=payload)
        assert resp.status_code == 422
        assert "dispatch_mode" in resp.json()["detail"][0]["msg"].lower()

    def test_invalid_racking_returns_422(self, client: TestClient) -> None:
        """Invalid racking value → 422."""
        payload = _base_request(racking="rooftop")
        resp = client.post("/analyses/optimize", json=payload)
        assert resp.status_code == 422

    def test_invalid_dispatch_mode_returns_422(
        self, client: TestClient
    ) -> None:
        """Invalid dispatch_mode → 422."""
        payload = _base_request(
            bess_enabled=True,
            dispatch_mode="invalid",
        )
        resp = client.post("/analyses/optimize", json=payload)
        assert resp.status_code == 422

    def test_gcr_range_inverted_returns_422(
        self, client: TestClient
    ) -> None:
        """gcr_range with min > max → 422."""
        payload = _base_request(gcr_range=[0.60, 0.30])
        resp = client.post("/analyses/optimize", json=payload)
        assert resp.status_code == 422

    def test_dcac_range_inverted_returns_422(
        self, client: TestClient
    ) -> None:
        """dcac_range with min > max → 422."""
        payload = _base_request(dcac_range=[1.60, 1.15])
        resp = client.post("/analyses/optimize", json=payload)
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Tests: Production mode
# ---------------------------------------------------------------------------


class TestProductionMode:
    """Production mode (no economic inputs)."""

    @patch("src.api.routes.optimization.optimize_solar")
    def test_production_mode_returns_200(
        self, mock_optimize, client: TestClient
    ) -> None:
        """POST with buildable_acres, no economics → production mode."""
        mock_optimize.return_value = _mock_solar_result("production")
        payload = _production_request()

        resp = client.post("/analyses/optimize", json=payload)

        assert resp.status_code == 200
        data = resp.json()
        assert data["mode"] == "production"
        assert "max_production" in data["winners"]
        assert "max_yield" in data["winners"]
        assert data["run_id"]  # confirms persistence flow created a run_name

    @patch("src.api.routes.optimization.optimize_solar")
    def test_production_mode_calls_optimizer_with_correct_args(
        self, mock_optimize, client: TestClient
    ) -> None:
        """Verify optimizer is called with request parameters."""
        mock_optimize.return_value = _mock_solar_result("production")
        payload = _production_request(
            gcr_range=[0.30, 0.50],
            gcr_step=0.10,
            utilization_factor=0.80,
        )

        client.post("/analyses/optimize", json=payload)

        assert mock_optimize.called
        kwargs = mock_optimize.call_args.kwargs
        assert kwargs["buildable_acres"] == 100.0
        assert kwargs["gcr_range"] == (0.30, 0.50)
        assert kwargs["gcr_step"] == 0.10
        assert kwargs["utilization_factor"] == 0.80
        assert kwargs["racking"] == "tracker"
        # No economic inputs in production mode
        assert kwargs["solar_cost_per_kw_dc"] is None


# ---------------------------------------------------------------------------
# Tests: LCOE mode
# ---------------------------------------------------------------------------


class TestLCOEMode:
    """LCOE mode (economic inputs, no revenue)."""

    @patch("src.api.routes.optimization.optimize_solar")
    def test_lcoe_mode_returns_200(
        self, mock_optimize, client: TestClient
    ) -> None:
        """POST with economics, no revenue → lcoe mode."""
        mock_optimize.return_value = _mock_solar_result("lcoe")
        payload = _lcoe_request()

        resp = client.post("/analyses/optimize", json=payload)

        assert resp.status_code == 200
        data = resp.json()
        assert data["mode"] == "lcoe"
        assert "lcoe" in data["winners"]

    @patch("src.api.routes.optimization.optimize_solar")
    def test_lcoe_passes_economic_inputs(
        self, mock_optimize, client: TestClient
    ) -> None:
        """Verify economic inputs are forwarded to optimizer."""
        mock_optimize.return_value = _mock_solar_result("lcoe")
        payload = _lcoe_request()

        client.post("/analyses/optimize", json=payload)

        kwargs = mock_optimize.call_args.kwargs
        assert kwargs["solar_cost_per_kw_dc"] == 1200.0
        assert kwargs["discount_rate_pct"] == 7.0
        assert kwargs["itc_pct"] == 30.0
        assert kwargs["project_lifetime_years"] == 25


# ---------------------------------------------------------------------------
# Tests: NPV mode
# ---------------------------------------------------------------------------


class TestNPVMode:
    """NPV mode (economic + revenue inputs)."""

    @patch("src.api.routes.optimization.optimize_solar")
    def test_npv_mode_returns_200(
        self, mock_optimize, client: TestClient
    ) -> None:
        """POST with economics + revenue → npv mode."""
        mock_optimize.return_value = _mock_solar_result("npv")
        payload = _npv_request()

        resp = client.post("/analyses/optimize", json=payload)

        assert resp.status_code == 200
        data = resp.json()
        assert data["mode"] == "npv"
        assert "npv" in data["winners"]

    @patch("src.api.routes.optimization.optimize_solar")
    def test_npv_passes_revenue_inputs(
        self, mock_optimize, client: TestClient
    ) -> None:
        """Verify revenue inputs are forwarded."""
        mock_optimize.return_value = _mock_solar_result("npv")
        payload = _npv_request()

        client.post("/analyses/optimize", json=payload)

        kwargs = mock_optimize.call_args.kwargs
        assert kwargs["energy_price_per_kwh"] == 0.05
        assert kwargs["energy_cost_escalator_pct"] == 2.0


# ---------------------------------------------------------------------------
# Tests: Solar+BESS mode
# ---------------------------------------------------------------------------


class TestBESSMode:
    """Solar+BESS joint optimization mode."""

    @patch("src.api.routes.optimization.optimize_solar_bess")
    def test_bess_mode_returns_200(
        self, mock_bess, client: TestClient
    ) -> None:
        """POST with bess_enabled=True → solar_bess mode."""
        mock_bess.return_value = _mock_bess_result()
        payload = _bess_request()

        resp = client.post("/analyses/optimize", json=payload)

        assert resp.status_code == 200
        data = resp.json()
        assert data["mode"] == "solar_bess"
        assert data["bess_adds_value"] is True
        assert data["bess_winner_detail"] is not None

    @patch("src.api.routes.optimization.optimize_solar_bess")
    def test_bess_mode_passes_dispatch_config(
        self, mock_bess, client: TestClient
    ) -> None:
        """Verify BESS dispatch configuration is forwarded."""
        mock_bess.return_value = _mock_bess_result()
        payload = _bess_request(charging_mode="solar_only")

        client.post("/analyses/optimize", json=payload)

        kwargs = mock_bess.call_args.kwargs
        assert kwargs["dispatch_mode"] == "ftm"
        assert kwargs["charging_mode"] == "solar_only"


# ---------------------------------------------------------------------------
# Tests: Buildability run_id lookup
# ---------------------------------------------------------------------------


class TestBuildabilityRunIdLookup:
    """Tests for resolving buildable_acres from a previous buildability run."""

    @patch("src.api.routes.optimization.optimize_solar")
    def test_buildability_run_id_resolves_acres(
        self, mock_optimize, client: TestClient, tmp_path: Path
    ) -> None:
        """buildability_run_id → reads results.json → extracts acres."""
        mock_optimize.return_value = _mock_solar_result("production")

        # Create a fake buildability results file
        run_id = "test-buildability-run"
        results_dir = Path("outputs/api") / run_id
        results_dir.mkdir(parents=True, exist_ok=True)
        results_file = results_dir / "results.json"
        results_file.write_text(
            json.dumps({"buildable_acres": 150.0, "status": "success"}),
            encoding="utf-8",
        )

        try:
            payload = {
                "latitude": 33.45,
                "longitude": -112.07,
                "buildability_run_id": run_id,
            }
            resp = client.post("/analyses/optimize", json=payload)

            assert resp.status_code == 200
            kwargs = mock_optimize.call_args.kwargs
            assert kwargs["buildable_acres"] == 150.0
        finally:
            # Clean up
            results_file.unlink(missing_ok=True)
            results_dir.rmdir()

    def test_missing_buildability_run_id_returns_404(
        self, client: TestClient
    ) -> None:
        """buildability_run_id with no results file → 404."""
        payload = {
            "latitude": 33.45,
            "longitude": -112.07,
            "buildability_run_id": "nonexistent-run-id",
        }
        resp = client.post("/analyses/optimize", json=payload)

        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()

    @patch("src.api.routes.optimization.optimize_solar")
    def test_buildability_result_without_acres_returns_422(
        self, mock_optimize, client: TestClient
    ) -> None:
        """buildability_run_id result missing buildable_acres → 422."""
        # Create a results file without buildable_acres
        run_id = "test-no-acres-run"
        results_dir = Path("outputs/api") / run_id
        results_dir.mkdir(parents=True, exist_ok=True)
        results_file = results_dir / "results.json"
        results_file.write_text(
            json.dumps({"status": "success", "slope": {}}),
            encoding="utf-8",
        )

        try:
            payload = {
                "latitude": 33.45,
                "longitude": -112.07,
                "buildability_run_id": run_id,
            }
            resp = client.post("/analyses/optimize", json=payload)

            assert resp.status_code == 422
            assert "buildable_acres" in resp.json()["detail"].lower()
        finally:
            results_file.unlink(missing_ok=True)
            results_dir.rmdir()


# ---------------------------------------------------------------------------
# Tests: Response structure
# ---------------------------------------------------------------------------


class TestResponseStructure:
    """Verify response shape and persistence."""

    @patch("src.api.routes.optimization.optimize_solar")
    def test_response_has_run_id(
        self, mock_optimize, client: TestClient
    ) -> None:
        """Response includes a non-empty run_id string."""
        mock_optimize.return_value = _mock_solar_result("production")
        payload = _production_request()

        resp = client.post("/analyses/optimize", json=payload)
        data = resp.json()

        assert isinstance(data["run_id"], str)
        assert len(data["run_id"]) > 0

    @patch("src.api.routes.optimization.optimize_solar")
    def test_response_has_sweep_metadata(
        self, mock_optimize, client: TestClient
    ) -> None:
        """Response includes sweep_metadata with configuration details."""
        mock_optimize.return_value = _mock_solar_result("production")
        payload = _production_request()

        resp = client.post("/analyses/optimize", json=payload)
        data = resp.json()

        assert "sweep_metadata" in data
        meta = data["sweep_metadata"]
        assert meta["racking"] == "tracker"
        assert meta["buildable_acres"] == 100.0
        assert meta["total_combinations"] == 70

    @patch("src.api.routes.optimization.optimize_solar")
    def test_results_json_persisted(
        self, mock_optimize, client: TestClient
    ) -> None:
        """Results JSON file is written to outputs/api/{run_id}/."""
        mock_optimize.return_value = _mock_solar_result("production")
        payload = _production_request()

        resp = client.post("/analyses/optimize", json=payload)
        data = resp.json()
        run_id = data["run_id"]

        results_path = Path("outputs/api") / run_id / "results.json"
        try:
            assert results_path.exists()
            persisted = json.loads(results_path.read_text(encoding="utf-8"))
            assert persisted["mode"] == "production"
            assert persisted["run_id"] == run_id
        finally:
            # Clean up
            results_path.unlink(missing_ok=True)
            results_path.parent.rmdir()

    @patch("src.api.routes.optimization.optimize_solar")
    def test_bess_fields_none_for_solar_only(
        self, mock_optimize, client: TestClient
    ) -> None:
        """Solar-only modes have bess_adds_value=None, bess_winner_detail=None."""
        mock_optimize.return_value = _mock_solar_result("production")
        payload = _production_request()

        resp = client.post("/analyses/optimize", json=payload)
        data = resp.json()

        assert data["bess_adds_value"] is None
        assert data["bess_winner_detail"] is None


# ---------------------------------------------------------------------------
# Tests: Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """Error handling for optimizer failures."""

    @patch("src.api.routes.optimization.optimize_solar")
    def test_optimizer_exception_returns_500(
        self, mock_optimize, client: TestClient
    ) -> None:
        """If optimizer raises, endpoint returns 500."""
        mock_optimize.side_effect = RuntimeError("All sweep points failed")
        payload = _production_request()

        resp = client.post("/analyses/optimize", json=payload)

        assert resp.status_code == 500
        assert "sweep points failed" in resp.json()["detail"]

    @patch("src.api.routes.optimization.CECDatabase")
    def test_invalid_module_name_returns_422(
        self, mock_cec, client: TestClient
    ) -> None:
        """Module not found in CEC database → 422."""
        mock_cec.return_value.get_module_params.side_effect = (
            KeyError("Module not found")
        )
        payload = _production_request(module_name="Nonexistent Module XYZ")

        resp = client.post("/analyses/optimize", json=payload)

        assert resp.status_code == 422
        assert "not found" in resp.json()["detail"].lower()

    @patch("src.api.routes.optimization.CECDatabase")
    def test_invalid_inverter_name_returns_422(
        self, mock_cec, client: TestClient
    ) -> None:
        """Inverter not found in CEC database → 422."""
        # Module lookup succeeds, inverter fails
        mock_module = type("MockParams", (), {"pmax": 551.0, "area": 2.51})()
        mock_cec.return_value.get_module_params.return_value = mock_module
        mock_cec.return_value.get_inverter_params.side_effect = (
            KeyError("Inverter not found")
        )
        payload = _production_request(
            module_name="Some Real Module",
            inverter_name="Nonexistent Inverter ABC",
        )

        resp = client.post("/analyses/optimize", json=payload)

        assert resp.status_code == 422
        assert "not found" in resp.json()["detail"].lower()
