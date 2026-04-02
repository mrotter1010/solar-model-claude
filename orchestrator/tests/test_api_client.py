"""Tests for the analysis API HTTP client."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from orchestrator.tools.api_client import AnalysisAPIClient
from orchestrator.tools.definitions import TOOL_DEFINITIONS, TOOL_ENDPOINTS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_json_response(data: dict, status_code: int = 200) -> MagicMock:
    """Build a mock httpx.Response that returns JSON."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = data
    resp.raise_for_status = MagicMock()
    return resp


def _mock_bytes_response(content: bytes, status_code: int = 200) -> MagicMock:
    """Build a mock httpx.Response that returns raw bytes."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = content
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_httpx_client():
    """Mocked httpx.AsyncClient with get/post as AsyncMocks."""
    client = AsyncMock()
    client.get = AsyncMock()
    client.post = AsyncMock()
    client.aclose = AsyncMock()
    return client


@pytest.fixture
def api_client(mock_httpx_client):
    """AnalysisAPIClient with its internal httpx client replaced by a mock."""
    client = AnalysisAPIClient.__new__(AnalysisAPIClient)
    client._client = mock_httpx_client
    return client


@pytest.fixture
def api_client_with_key(mock_httpx_client):
    """AnalysisAPIClient constructed with an API key."""
    client = AnalysisAPIClient("http://test:8000", api_key="secret-key", timeout=30)
    client._client = mock_httpx_client
    return client


# ---------------------------------------------------------------------------
# GET endpoint tests
# ---------------------------------------------------------------------------


class TestHealthCheck:

    @pytest.mark.anyio
    async def test_health_check(self, api_client, mock_httpx_client):
        """GET /health returns parsed JSON."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"status": "healthy"}
        )

        result = await api_client.health_check()

        mock_httpx_client.get.assert_called_once_with("/health")
        assert result == {"status": "healthy"}


class TestSearchModules:

    @pytest.mark.anyio
    async def test_search_modules(self, api_client, mock_httpx_client):
        """GET /analyses/equipment/modules with search query param."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"modules": [{"name": "CS6W-550MS"}]}
        )

        result = await api_client.search_modules(search="CS6W")

        mock_httpx_client.get.assert_called_once_with(
            "/analyses/equipment/modules", params={"search": "CS6W"}
        )
        assert result == {"modules": [{"name": "CS6W-550MS"}]}

    @pytest.mark.anyio
    async def test_search_modules_empty_search(self, api_client, mock_httpx_client):
        """Empty search string sends no params."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"modules": []}
        )

        result = await api_client.search_modules(search="")

        mock_httpx_client.get.assert_called_once_with(
            "/analyses/equipment/modules", params={}
        )
        assert result == {"modules": []}


class TestSearchInverters:

    @pytest.mark.anyio
    async def test_search_inverters(self, api_client, mock_httpx_client):
        """GET /analyses/equipment/inverters with search query param."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"inverters": [{"name": "SG250HX"}]}
        )

        result = await api_client.search_inverters(search="Sungrow")

        mock_httpx_client.get.assert_called_once_with(
            "/analyses/equipment/inverters", params={"search": "Sungrow"}
        )
        assert result == {"inverters": [{"name": "SG250HX"}]}


class TestListLoadTypes:

    @pytest.mark.anyio
    async def test_list_load_types(self, api_client, mock_httpx_client):
        """GET /analyses/load-types returns parsed JSON."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"load_types": ["MediumOffice", "RetailStandalone"]}
        )

        result = await api_client.list_load_types()

        mock_httpx_client.get.assert_called_once_with("/analyses/load-types")
        assert result == {"load_types": ["MediumOffice", "RetailStandalone"]}


class TestGetLmpPrices:

    @pytest.mark.anyio
    async def test_get_lmp_prices_with_zone(self, api_client, mock_httpx_client):
        """GET /lmp/prices with iso and zone query params."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"iso": "pjm", "zone": "AEP", "summary": {"mean_price": 35.50}}
        )

        result = await api_client.get_lmp_prices(iso="pjm", zone="AEP")

        mock_httpx_client.get.assert_called_once_with(
            "/lmp/prices", params={"iso": "pjm", "zone": "AEP"}
        )
        assert result["iso"] == "pjm"
        assert result["zone"] == "AEP"

    @pytest.mark.anyio
    async def test_get_lmp_prices_with_latlon(self, api_client, mock_httpx_client):
        """GET /lmp/prices with lat/lon for zone auto-detection."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"iso": "ercot", "zone": "LZ_HOUSTON"}
        )

        result = await api_client.get_lmp_prices(
            iso="ercot", lat=29.76, lon=-95.37
        )

        mock_httpx_client.get.assert_called_once_with(
            "/lmp/prices", params={"iso": "ercot", "lat": 29.76, "lon": -95.37}
        )
        assert result["zone"] == "LZ_HOUSTON"

    @pytest.mark.anyio
    async def test_get_lmp_prices_all_params(self, api_client, mock_httpx_client):
        """GET /lmp/prices with all optional params populated."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"iso": "caiso", "zone": "NP15", "market": "REAL_TIME_HOURLY", "year": 2024}
        )

        result = await api_client.get_lmp_prices(
            iso="caiso", zone="NP15", market="REAL_TIME_HOURLY", year=2024
        )

        mock_httpx_client.get.assert_called_once_with(
            "/lmp/prices",
            params={
                "iso": "caiso",
                "zone": "NP15",
                "market": "REAL_TIME_HOURLY",
                "year": 2024,
            },
        )
        assert result["market"] == "REAL_TIME_HOURLY"
        assert result["year"] == 2024

    @pytest.mark.anyio
    async def test_get_lmp_prices_iso_only(self, api_client, mock_httpx_client):
        """GET /lmp/prices with iso only — no optional params sent."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"iso": "pjm"}
        )

        await api_client.get_lmp_prices(iso="pjm")

        mock_httpx_client.get.assert_called_once_with(
            "/lmp/prices", params={"iso": "pjm"}
        )


# ---------------------------------------------------------------------------
# Tool definition validation tests
# ---------------------------------------------------------------------------


class TestToolDefinitions:

    def test_get_lmp_prices_definition_exists(self):
        """get_lmp_prices tool definition is present in TOOL_DEFINITIONS."""
        tool_names = [t["function"]["name"] for t in TOOL_DEFINITIONS]
        assert "get_lmp_prices" in tool_names

    def test_get_lmp_prices_definition_schema(self):
        """get_lmp_prices definition has correct required params and enum."""
        defn = next(
            t for t in TOOL_DEFINITIONS
            if t["function"]["name"] == "get_lmp_prices"
        )
        func = defn["function"]

        # Top-level structure
        assert defn["type"] == "function"
        assert "description" in func
        assert func["parameters"]["type"] == "object"

        # Required params
        assert func["parameters"]["required"] == ["iso"]

        # iso enum
        props = func["parameters"]["properties"]
        assert props["iso"]["enum"] == ["pjm", "ercot", "caiso"]

        # Optional params exist
        for param in ("zone", "lat", "lon", "market", "year"):
            assert param in props, f"Missing param: {param}"

    def test_get_lmp_prices_endpoint_mapping(self):
        """TOOL_ENDPOINTS maps get_lmp_prices to GET /lmp/prices."""
        assert "get_lmp_prices" in TOOL_ENDPOINTS
        method, path = TOOL_ENDPOINTS["get_lmp_prices"]
        assert method == "GET"
        assert path == "/lmp/prices"

    def test_tool_count(self):
        """TOOL_DEFINITIONS has 13 tools and TOOL_ENDPOINTS has 13 entries."""
        assert len(TOOL_DEFINITIONS) == 13
        assert len(TOOL_ENDPOINTS) == 13

    def test_all_tools_have_endpoints(self):
        """Every tool in TOOL_DEFINITIONS has a matching TOOL_ENDPOINTS entry."""
        tool_names = {t["function"]["name"] for t in TOOL_DEFINITIONS}
        endpoint_names = set(TOOL_ENDPOINTS.keys())
        assert tool_names == endpoint_names


# ---------------------------------------------------------------------------
# POST endpoint tests
# ---------------------------------------------------------------------------


class TestBuildRate:

    @pytest.mark.anyio
    async def test_build_rate(self, api_client, mock_httpx_client):
        """POST /rates/build with correct JSON body structure."""
        mock_httpx_client.post.return_value = _mock_json_response(
            {"rate_id": "rate-001"}
        )
        rate_data = {"utility": "APS", "schedule": "E-TOU"}

        result = await api_client.build_rate(rate=rate_data, save_to_disk=True)

        mock_httpx_client.post.assert_called_once_with(
            "/rates/build",
            json={"rate": rate_data, "save_to_disk": True},
        )
        assert result == {"rate_id": "rate-001"}


class TestRunProduction:

    @pytest.mark.anyio
    async def test_run_production(self, api_client, mock_httpx_client):
        """POST /analyses/production with payload as JSON body."""
        payload = {"site": {"lat": 33.45, "lon": -112.07}, "system": {"dc_mw": 5.0}}
        mock_httpx_client.post.return_value = _mock_json_response(
            {"run_id": "run-001", "annual_kwh": 10_000_000}
        )

        result = await api_client.run_production(payload)

        mock_httpx_client.post.assert_called_once_with(
            "/analyses/production", json=payload
        )
        assert result["run_id"] == "run-001"


class TestRunBillSavings:

    @pytest.mark.anyio
    async def test_run_bill_savings(self, api_client, mock_httpx_client):
        """POST /analyses/bill-savings with payload."""
        payload = {"run_id": "run-001", "rate_id": "rate-001"}
        mock_httpx_client.post.return_value = _mock_json_response(
            {"savings": 50000}
        )

        result = await api_client.run_bill_savings(payload)

        mock_httpx_client.post.assert_called_once_with(
            "/analyses/bill-savings", json=payload
        )
        assert result == {"savings": 50000}


class TestRunBess:

    @pytest.mark.anyio
    async def test_run_bess(self, api_client, mock_httpx_client):
        """POST /analyses/bess with payload."""
        payload = {"run_id": "run-001", "bess": {"capacity_kwh": 500}}
        mock_httpx_client.post.return_value = _mock_json_response(
            {"bess_run_id": "bess-001"}
        )

        result = await api_client.run_bess(payload)

        mock_httpx_client.post.assert_called_once_with(
            "/analyses/bess", json=payload
        )
        assert result == {"bess_run_id": "bess-001"}


class TestRunBuildability:

    @pytest.mark.anyio
    async def test_run_buildability(self, api_client, mock_httpx_client):
        """POST /analyses/buildability with payload."""
        payload = {"lat": 33.45, "lon": -112.07, "area_acres": 50}
        mock_httpx_client.post.return_value = _mock_json_response(
            {"buildable_pct": 0.85}
        )

        result = await api_client.run_buildability(payload)

        mock_httpx_client.post.assert_called_once_with(
            "/analyses/buildability", json=payload
        )
        assert result == {"buildable_pct": 0.85}


# ---------------------------------------------------------------------------
# Result retrieval tests
# ---------------------------------------------------------------------------


class TestGetResults:

    @pytest.mark.anyio
    async def test_get_results(self, api_client, mock_httpx_client):
        """GET /analyses/{run_id}/results returns parsed JSON."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"annual_kwh": 10_000_000, "cf": 0.238}
        )

        result = await api_client.get_results(run_id="run-001")

        mock_httpx_client.get.assert_called_once_with("/analyses/run-001/results")
        assert result["cf"] == 0.238


class TestGetReport:

    @pytest.mark.anyio
    async def test_get_report(self, api_client, mock_httpx_client):
        """GET /analyses/{run_id}/report returns raw bytes."""
        pdf_bytes = b"%PDF-1.4 fake content"
        mock_httpx_client.get.return_value = _mock_bytes_response(pdf_bytes)

        result = await api_client.get_report(run_id="run-001")

        mock_httpx_client.get.assert_called_once_with("/analyses/run-001/report")
        assert result == pdf_bytes
        assert isinstance(result, bytes)


class TestGetTimeseries:

    @pytest.mark.anyio
    async def test_get_timeseries(self, api_client, mock_httpx_client):
        """GET /analyses/{run_id}/timeseries returns raw bytes."""
        csv_bytes = b"hour,kwh\n1,100\n2,200"
        mock_httpx_client.get.return_value = _mock_bytes_response(csv_bytes)

        result = await api_client.get_timeseries(run_id="run-001")

        mock_httpx_client.get.assert_called_once_with("/analyses/run-001/timeseries")
        assert result == csv_bytes
        assert isinstance(result, bytes)


# ---------------------------------------------------------------------------
# API key header tests
# ---------------------------------------------------------------------------


class TestApiKeyHeader:

    def test_api_key_header(self):
        """X-API-Key header is set when api_key is provided."""
        client = AnalysisAPIClient("http://test:8000", api_key="my-key", timeout=30)

        assert client._client.headers["X-API-Key"] == "my-key"

    def test_no_api_key_header(self):
        """No X-API-Key header when api_key is None."""
        client = AnalysisAPIClient("http://test:8000", api_key=None, timeout=30)

        assert "X-API-Key" not in client._client.headers


# ---------------------------------------------------------------------------
# Tool dispatch tests
# ---------------------------------------------------------------------------


class TestExecuteToolDispatch:

    @pytest.mark.anyio
    async def test_execute_tool_dispatch(self, api_client, mock_httpx_client):
        """execute_tool dispatches search_modules correctly."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"modules": [{"name": "Canadian Solar"}]}
        )

        result = await api_client.execute_tool(
            "search_modules", {"search": "Canadian"}
        )

        mock_httpx_client.get.assert_called_once_with(
            "/analyses/equipment/modules", params={"search": "Canadian"}
        )
        assert result == {"modules": [{"name": "Canadian Solar"}]}

    @pytest.mark.anyio
    async def test_execute_tool_dispatch_run(self, api_client, mock_httpx_client):
        """execute_tool dispatches run_production correctly."""
        payload = {"site": {"lat": 33.45}, "system": {"dc_mw": 5.0}}
        mock_httpx_client.post.return_value = _mock_json_response(
            {"run_id": "run-abc"}
        )

        result = await api_client.execute_tool("run_production", payload)

        mock_httpx_client.post.assert_called_once_with(
            "/analyses/production", json=payload
        )
        assert result["run_id"] == "run-abc"

    @pytest.mark.anyio
    async def test_execute_tool_dispatch_lmp(self, api_client, mock_httpx_client):
        """execute_tool dispatches get_lmp_prices correctly."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"iso": "ercot", "zone": "LZ_HOUSTON", "summary": {"mean_price": 28.0}}
        )

        result = await api_client.execute_tool(
            "get_lmp_prices",
            {"iso": "ercot", "zone": "LZ_HOUSTON", "year": 2024},
        )

        mock_httpx_client.get.assert_called_once_with(
            "/lmp/prices",
            params={"iso": "ercot", "zone": "LZ_HOUSTON", "year": 2024},
        )
        assert result["iso"] == "ercot"
        assert result["zone"] == "LZ_HOUSTON"

    @pytest.mark.anyio
    async def test_execute_tool_unknown(self, api_client):
        """Unknown tool name raises ValueError."""
        with pytest.raises(ValueError, match="Unknown tool"):
            await api_client.execute_tool("unknown_tool", {})
