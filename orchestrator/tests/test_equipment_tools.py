"""Tests for equipment search tool definitions and API client dispatch.

Validates that search_modules and search_inverters tool definitions include
the new numeric filter params (min_stc, max_stc, min_paco, max_paco) and
that the API client passes them through correctly.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

from orchestrator.tools.api_client import AnalysisAPIClient
from orchestrator.tools.definitions import TOOL_DEFINITIONS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_tool_def(name: str) -> dict:
    """Extract a tool definition by name."""
    return next(t for t in TOOL_DEFINITIONS if t["function"]["name"] == name)


def _mock_json_response(data: dict) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = data
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# Tool definition schema tests
# ---------------------------------------------------------------------------


class TestSearchModulesDefinition:

    def test_has_min_stc_param(self) -> None:
        defn = _get_tool_def("search_modules")
        props = defn["function"]["parameters"]["properties"]
        assert "min_stc" in props
        assert props["min_stc"]["type"] == "number"

    def test_has_max_stc_param(self) -> None:
        defn = _get_tool_def("search_modules")
        props = defn["function"]["parameters"]["properties"]
        assert "max_stc" in props
        assert props["max_stc"]["type"] == "number"

    def test_search_param_still_exists(self) -> None:
        defn = _get_tool_def("search_modules")
        props = defn["function"]["parameters"]["properties"]
        assert "search" in props

    def test_description_mentions_multi_token(self) -> None:
        defn = _get_tool_def("search_modules")
        desc = defn["function"]["description"]
        assert "multi-token" in desc.lower()


class TestSearchInvertersDefinition:

    def test_has_min_paco_param(self) -> None:
        defn = _get_tool_def("search_inverters")
        props = defn["function"]["parameters"]["properties"]
        assert "min_paco" in props
        assert props["min_paco"]["type"] == "number"

    def test_has_max_paco_param(self) -> None:
        defn = _get_tool_def("search_inverters")
        props = defn["function"]["parameters"]["properties"]
        assert "max_paco" in props
        assert props["max_paco"]["type"] == "number"

    def test_search_param_still_exists(self) -> None:
        defn = _get_tool_def("search_inverters")
        props = defn["function"]["parameters"]["properties"]
        assert "search" in props

    def test_description_mentions_multi_token(self) -> None:
        defn = _get_tool_def("search_inverters")
        desc = defn["function"]["description"]
        assert "multi-token" in desc.lower()


# ---------------------------------------------------------------------------
# API client dispatch tests
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_httpx_client():
    client = AsyncMock()
    client.get = AsyncMock()
    client.aclose = AsyncMock()
    return client


@pytest.fixture
def api_client(mock_httpx_client):
    client = AnalysisAPIClient.__new__(AnalysisAPIClient)
    client._client = mock_httpx_client
    return client


class TestSearchModulesDispatch:

    @pytest.mark.anyio
    async def test_passes_stc_params(self, api_client, mock_httpx_client):
        """search_modules passes min_stc and max_stc to HTTP params."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"count": 5, "modules": ["mod1"]}
        )

        await api_client.search_modules(
            search="Trina", min_stc=540, max_stc=560
        )

        mock_httpx_client.get.assert_called_once_with(
            "/analyses/equipment/modules",
            params={"search": "Trina", "min_stc": 540, "max_stc": 560},
        )

    @pytest.mark.anyio
    async def test_omits_none_params(self, api_client, mock_httpx_client):
        """None params are not sent in HTTP request."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"count": 0, "modules": []}
        )

        await api_client.search_modules(search="Trina")

        mock_httpx_client.get.assert_called_once_with(
            "/analyses/equipment/modules",
            params={"search": "Trina"},
        )

    @pytest.mark.anyio
    async def test_execute_tool_passes_stc(self, api_client, mock_httpx_client):
        """execute_tool dispatch for search_modules passes min_stc/max_stc."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"count": 1, "modules": ["mod1"]}
        )

        await api_client.execute_tool(
            "search_modules",
            {"search": "Trina", "min_stc": 540, "max_stc": 560},
        )

        mock_httpx_client.get.assert_called_once_with(
            "/analyses/equipment/modules",
            params={"search": "Trina", "min_stc": 540, "max_stc": 560},
        )


class TestSearchInvertersDispatch:

    @pytest.mark.anyio
    async def test_passes_paco_params(self, api_client, mock_httpx_client):
        """search_inverters passes min_paco and max_paco to HTTP params."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"count": 3, "inverters": ["inv1"]}
        )

        await api_client.search_inverters(
            search="Sungrow", min_paco=200000, max_paco=300000
        )

        mock_httpx_client.get.assert_called_once_with(
            "/analyses/equipment/inverters",
            params={
                "search": "Sungrow",
                "min_paco": 200000,
                "max_paco": 300000,
            },
        )

    @pytest.mark.anyio
    async def test_execute_tool_passes_paco(self, api_client, mock_httpx_client):
        """execute_tool dispatch for search_inverters passes min_paco/max_paco."""
        mock_httpx_client.get.return_value = _mock_json_response(
            {"count": 1, "inverters": ["inv1"]}
        )

        await api_client.execute_tool(
            "search_inverters",
            {"search": "Sungrow", "min_paco": 200000},
        )

        mock_httpx_client.get.assert_called_once_with(
            "/analyses/equipment/inverters",
            params={"search": "Sungrow", "min_paco": 200000},
        )
