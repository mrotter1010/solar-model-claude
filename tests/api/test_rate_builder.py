"""Tests for the POST /rates/build API endpoint."""

import json
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from src.api.app import create_app


@pytest.fixture()
def client() -> TestClient:
    """Create a TestClient for the FastAPI app."""
    app = create_app()
    return TestClient(app)


def _schedule_12x24(value: int = 0) -> list[list[int]]:
    """Build a uniform 12x24 schedule matrix filled with a single value."""
    return [[value] * 24 for _ in range(12)]


def _minimal_energy_rate_body(
    save_to_disk: bool = False,
    utility_name: str = "Test Utility",
    tariff_name: str = "Test Tariff",
) -> dict:
    """Build a minimal valid request body with energy-only charges.

    Single flat energy rate of $0.10/kWh, no demand charges.
    """
    return {
        "rate": {
            "utility_name": utility_name,
            "tariff_name": tariff_name,
            "energyratestructure": [[{"rate": 0.10}]],
            "energyweekdayschedule": _schedule_12x24(0),
            "energyweekendschedule": _schedule_12x24(0),
        },
        "save_to_disk": save_to_disk,
    }


def _energy_plus_demand_body() -> dict:
    """Build a request body with energy + TOU demand charges.

    Two energy periods (peak/off-peak) and matching demand structure.
    """
    # Summer months (Jun-Sep) = period 1 (peak), rest = period 0 (off-peak)
    weekday_schedule = []
    for month in range(12):
        if month in (5, 6, 7, 8):  # Jun-Sep (0-indexed)
            weekday_schedule.append([1] * 24)
        else:
            weekday_schedule.append([0] * 24)
    weekend_schedule = _schedule_12x24(0)  # All off-peak on weekends

    return {
        "rate": {
            "utility_name": "Demand Utility",
            "tariff_name": "TOU-D",
            "energyratestructure": [
                [{"rate": 0.08}],  # off-peak
                [{"rate": 0.15}],  # peak
            ],
            "energyweekdayschedule": weekday_schedule,
            "energyweekendschedule": weekend_schedule,
            "demandratestructure": [
                [{"rate": 5.00}],  # off-peak demand
                [{"rate": 12.00}],  # peak demand
            ],
            "demandweekdayschedule": weekday_schedule,
            "demandweekendschedule": weekend_schedule,
        },
        "save_to_disk": False,
    }


def _nem_body() -> dict:
    """Build a request body with net metering (flat_rate mode)."""
    body = _minimal_energy_rate_body()
    body["rate"]["net_metering"] = {
        "mode": "flat_rate",
        "export_rate": 0.04,
        "true_up_rate": 0.02,
    }
    return body


# ---------------------------------------------------------------------------
# Valid energy-only rate
# ---------------------------------------------------------------------------


class TestEnergyOnlyRate:
    """POST /rates/build with a valid energy-only rate schedule."""

    def test_returns_200(self, client: TestClient) -> None:
        """Minimal energy-only request returns 200."""
        resp = client.post("/rates/build", json=_minimal_energy_rate_body())

        assert resp.status_code == 200

    def test_rate_fields_present(self, client: TestClient) -> None:
        """Response includes the built rate schedule with correct fields."""
        resp = client.post("/rates/build", json=_minimal_energy_rate_body())
        data = resp.json()

        rate = data["rate"]
        assert rate["utility_name"] == "Test Utility"
        assert rate["tariff_name"] == "Test Tariff"
        assert len(rate["energyratestructure"]) == 1
        assert rate["energyratestructure"][0][0]["rate"] == 0.10
        assert len(rate["energyweekdayschedule"]) == 12
        assert len(rate["energyweekdayschedule"][0]) == 24

    def test_source_set_to_api(self, client: TestClient) -> None:
        """Built rate schedule has source='api'."""
        resp = client.post("/rates/build", json=_minimal_energy_rate_body())
        data = resp.json()

        assert data["rate"]["source"] == "api"

    def test_file_path_none_by_default(self, client: TestClient) -> None:
        """file_path is None when save_to_disk is false."""
        resp = client.post("/rates/build", json=_minimal_energy_rate_body())
        data = resp.json()

        assert data["file_path"] is None


# ---------------------------------------------------------------------------
# Energy + demand charges
# ---------------------------------------------------------------------------


class TestEnergyPlusDemandRate:
    """POST /rates/build with energy + TOU demand charges."""

    def test_returns_200_with_demand(self, client: TestClient) -> None:
        """Energy + demand rate returns 200."""
        resp = client.post("/rates/build", json=_energy_plus_demand_body())

        assert resp.status_code == 200

    def test_demand_structure_present(self, client: TestClient) -> None:
        """Response includes demand rate structures."""
        resp = client.post("/rates/build", json=_energy_plus_demand_body())
        rate = resp.json()["rate"]

        assert rate["demandratestructure"] is not None
        assert len(rate["demandratestructure"]) == 2
        assert rate["demandratestructure"][1][0]["rate"] == 12.00
        assert rate["demandweekdayschedule"] is not None
        assert rate["demandweekendschedule"] is not None


# ---------------------------------------------------------------------------
# Net metering
# ---------------------------------------------------------------------------


class TestNetMeteringRate:
    """POST /rates/build with net metering configuration."""

    def test_returns_200_with_nem(self, client: TestClient) -> None:
        """Rate with flat_rate NEM returns 200."""
        resp = client.post("/rates/build", json=_nem_body())

        assert resp.status_code == 200

    def test_nem_fields_present(self, client: TestClient) -> None:
        """Response includes net_metering configuration."""
        resp = client.post("/rates/build", json=_nem_body())
        nem = resp.json()["rate"]["net_metering"]

        assert nem["mode"] == "flat_rate"
        assert nem["export_rate"] == 0.04
        assert nem["true_up_rate"] == 0.02


# ---------------------------------------------------------------------------
# save_to_disk behavior
# ---------------------------------------------------------------------------


class TestSaveToDisk:
    """POST /rates/build with save_to_disk=true persists to disk."""

    def test_save_creates_file(
        self, client: TestClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """save_to_disk=True creates a JSON file and returns file_path."""
        # Redirect uploads/rates to tmp_path
        monkeypatch.chdir(tmp_path)

        body = _minimal_energy_rate_body(save_to_disk=True)
        resp = client.post("/rates/build", json=body)

        assert resp.status_code == 200
        data = resp.json()
        assert data["file_path"] is not None
        assert data["file_path"].endswith(".json")

        # Verify file exists and contains valid JSON
        saved = Path(data["file_path"])
        assert saved.exists()
        content = json.loads(saved.read_text())
        assert content["utility_name"] == "Test Utility"
        assert content["source"] == "api"

    def test_save_false_returns_none(self, client: TestClient) -> None:
        """save_to_disk=False (explicit) returns file_path=None."""
        body = _minimal_energy_rate_body(save_to_disk=False)
        resp = client.post("/rates/build", json=body)

        assert resp.status_code == 200
        assert resp.json()["file_path"] is None

    def test_save_to_disk_defaults_false(self, client: TestClient) -> None:
        """Omitting save_to_disk defaults to false."""
        body = _minimal_energy_rate_body()
        del body["save_to_disk"]
        resp = client.post("/rates/build", json=body)

        assert resp.status_code == 200
        assert resp.json()["file_path"] is None


# ---------------------------------------------------------------------------
# Filename sanitization
# ---------------------------------------------------------------------------


class TestFilenameSanitization:
    """Filenames are sanitized for spaces, slashes, and quotes."""

    def test_spaces_replaced(
        self, client: TestClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spaces in utility/tariff names become underscores."""
        monkeypatch.chdir(tmp_path)

        body = _minimal_energy_rate_body(
            save_to_disk=True,
            utility_name="Pacific Gas & Electric",
            tariff_name="B 19 S",
        )
        resp = client.post("/rates/build", json=body)

        assert resp.status_code == 200
        file_path = resp.json()["file_path"]
        filename = Path(file_path).name
        assert " " not in filename
        assert filename == "Pacific_Gas_&_Electric_B_19_S.json"

    def test_slashes_replaced(
        self, client: TestClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Slashes in names become underscores."""
        monkeypatch.chdir(tmp_path)

        body = _minimal_energy_rate_body(
            save_to_disk=True,
            utility_name="SoCal Edison",
            tariff_name="TOU-GS/2B",
        )
        resp = client.post("/rates/build", json=body)

        assert resp.status_code == 200
        file_path = resp.json()["file_path"]
        filename = Path(file_path).name
        assert "/" not in filename
        assert filename == "SoCal_Edison_TOU-GS_2B.json"


# ---------------------------------------------------------------------------
# Validation errors → 422
# ---------------------------------------------------------------------------


class TestValidationErrors:
    """Invalid rate data returns 422."""

    def test_11x24_schedule_returns_422(self, client: TestClient) -> None:
        """Energy schedule with 11 rows (not 12) returns 422."""
        body = _minimal_energy_rate_body()
        body["rate"]["energyweekdayschedule"] = [[0] * 24 for _ in range(11)]

        resp = client.post("/rates/build", json=body)

        assert resp.status_code == 422

    def test_12x23_schedule_returns_422(self, client: TestClient) -> None:
        """Energy schedule with 23 columns (not 24) returns 422."""
        body = _minimal_energy_rate_body()
        body["rate"]["energyweekdayschedule"] = [[0] * 23 for _ in range(12)]

        resp = client.post("/rates/build", json=body)

        assert resp.status_code == 422

    def test_missing_required_fields_returns_422(self, client: TestClient) -> None:
        """Missing utility_name returns 422."""
        body = _minimal_energy_rate_body()
        del body["rate"]["utility_name"]

        resp = client.post("/rates/build", json=body)

        assert resp.status_code == 422
