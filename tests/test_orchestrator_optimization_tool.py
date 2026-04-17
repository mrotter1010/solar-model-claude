"""Tests for the run_optimization orchestrator tool integration.

Verifies:
- Tool definition exists in TOOL_DEFINITIONS with correct schema
- TOOL_ENDPOINTS maps to POST /analyses/optimize
- API client dispatches run_optimization correctly
- Result summarization strips sweep_results but preserves winners
"""

import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.planning.executor import (
    _LARGE_RESULT_KEYS,
    _RUN_TOOLS,
    _summarize_tool_result,
)
from orchestrator.tools.api_client import AnalysisAPIClient
from orchestrator.tools.definitions import TOOL_DEFINITIONS, TOOL_ENDPOINTS


# ---------------------------------------------------------------------------
# Helper to find a tool definition by name
# ---------------------------------------------------------------------------

def _find_tool(name: str) -> dict | None:
    """Find a tool definition by function name."""
    for tool in TOOL_DEFINITIONS:
        if tool["function"]["name"] == name:
            return tool
    return None


# ---------------------------------------------------------------------------
# Tool definition tests
# ---------------------------------------------------------------------------


class TestOptimizationToolDefinition:
    """Verify run_optimization exists in TOOL_DEFINITIONS with correct schema."""

    def test_tool_exists(self) -> None:
        """run_optimization is present in TOOL_DEFINITIONS."""
        tool = _find_tool("run_optimization")
        assert tool is not None, (
            "run_optimization not found in TOOL_DEFINITIONS"
        )

    def test_tool_type_is_function(self) -> None:
        """Top-level type is 'function' (OpenAI format)."""
        tool = _find_tool("run_optimization")
        assert tool["type"] == "function"

    def test_required_fields(self) -> None:
        """latitude and longitude are the only required fields."""
        tool = _find_tool("run_optimization")
        params = tool["function"]["parameters"]
        assert set(params["required"]) == {"latitude", "longitude"}

    def test_has_location_properties(self) -> None:
        """Schema includes latitude and longitude as numbers."""
        tool = _find_tool("run_optimization")
        props = tool["function"]["parameters"]["properties"]
        assert props["latitude"]["type"] == "number"
        assert props["longitude"]["type"] == "number"

    def test_has_buildable_acres(self) -> None:
        """Schema includes buildable_acres as a number."""
        tool = _find_tool("run_optimization")
        props = tool["function"]["parameters"]["properties"]
        assert "buildable_acres" in props
        assert props["buildable_acres"]["type"] == "number"

    def test_has_buildability_run_id(self) -> None:
        """Schema includes buildability_run_id as a string."""
        tool = _find_tool("run_optimization")
        props = tool["function"]["parameters"]["properties"]
        assert "buildability_run_id" in props
        assert props["buildability_run_id"]["type"] == "string"

    def test_has_racking_enum(self) -> None:
        """Schema includes racking with fixed/tracker enum."""
        tool = _find_tool("run_optimization")
        props = tool["function"]["parameters"]["properties"]
        assert props["racking"]["enum"] == ["fixed", "tracker"]

    def test_has_bess_parameters(self) -> None:
        """Schema includes BESS-related parameters."""
        tool = _find_tool("run_optimization")
        props = tool["function"]["parameters"]["properties"]

        assert "bess_enabled" in props
        assert props["bess_enabled"]["type"] == "boolean"

        assert "dispatch_mode" in props
        assert props["dispatch_mode"]["enum"] == ["btm", "ftm"]

        assert "bess_power_fractions" in props
        assert "bess_durations_hr" in props

    def test_has_economics_parameters(self) -> None:
        """Schema includes economic input parameters."""
        tool = _find_tool("run_optimization")
        props = tool["function"]["parameters"]["properties"]

        econ_params = [
            "solar_cost_per_kw_dc",
            "solar_opex_per_kw_dc_year",
            "discount_rate_pct",
            "project_lifetime_years",
            "degradation_pct",
            "itc_pct",
            "energy_price_per_kwh",
            "energy_cost_escalator_pct",
        ]
        for param in econ_params:
            assert param in props, f"Missing economic parameter: {param}"

    def test_has_sweep_parameters(self) -> None:
        """Schema includes GCR and DC:AC sweep parameters."""
        tool = _find_tool("run_optimization")
        props = tool["function"]["parameters"]["properties"]

        assert "gcr_range" in props
        assert "gcr_step" in props
        assert "dcac_range" in props
        assert "dcac_step" in props
        assert "utilization_factor" in props

    def test_has_bess_economics(self) -> None:
        """Schema includes BESS cost parameters."""
        tool = _find_tool("run_optimization")
        props = tool["function"]["parameters"]["properties"]

        assert "bess_cost_per_kwh" in props
        assert "bess_opex_per_kw_year" in props

    def test_has_btm_ftm_inputs(self) -> None:
        """Schema includes BTM and FTM dispatch inputs."""
        tool = _find_tool("run_optimization")
        props = tool["function"]["parameters"]["properties"]

        # BTM inputs
        assert "rate_schedule" in props
        assert "load_profile_kwh" in props

        # FTM inputs
        assert "lmp_prices" in props
        assert "lmp_iso" in props
        assert "lmp_zone" in props

    def test_description_mentions_modes(self) -> None:
        """Tool description mentions all four optimization modes."""
        tool = _find_tool("run_optimization")
        desc = tool["function"]["description"]

        assert "production" in desc.lower()
        assert "lcoe" in desc.lower() or "LCOE" in desc
        assert "npv" in desc.lower() or "NPV" in desc
        assert "bess" in desc.lower() or "BESS" in desc


# ---------------------------------------------------------------------------
# TOOL_ENDPOINTS tests
# ---------------------------------------------------------------------------


class TestOptimizationEndpointMapping:
    """Verify TOOL_ENDPOINTS maps run_optimization correctly."""

    def test_endpoint_exists(self) -> None:
        """run_optimization is in TOOL_ENDPOINTS."""
        assert "run_optimization" in TOOL_ENDPOINTS

    def test_endpoint_method_is_post(self) -> None:
        """Endpoint uses POST method."""
        method, _ = TOOL_ENDPOINTS["run_optimization"]
        assert method == "POST"

    def test_endpoint_path(self) -> None:
        """Endpoint path is /analyses/optimize."""
        _, path = TOOL_ENDPOINTS["run_optimization"]
        assert path == "/analyses/optimize"


# ---------------------------------------------------------------------------
# API client dispatch tests
# ---------------------------------------------------------------------------


class TestAPIClientDispatch:
    """Verify execute_tool dispatches run_optimization to the correct method."""

    def test_execute_tool_dispatches_to_run_optimization(self) -> None:
        """execute_tool('run_optimization', ...) calls run_optimization method."""

        async def _run() -> dict:
            client = AnalysisAPIClient(base_url="http://test:8000")
            mock_response = {
                "run_id": "optimize_test_123",
                "mode": "production",
                "winners": {"max_energy": {"gcr": 0.35, "dcac": 1.30}},
                "sweep_metadata": {"points_evaluated": 50},
            }
            try:
                with patch.object(
                    client, "run_optimization", new_callable=AsyncMock,
                    return_value=mock_response,
                ) as mock_method:
                    result = await client.execute_tool(
                        "run_optimization",
                        {"latitude": 33.45, "longitude": -111.98, "buildable_acres": 100},
                    )
                mock_method.assert_called_once_with(
                    {"latitude": 33.45, "longitude": -111.98, "buildable_acres": 100}
                )
                return result
            finally:
                await client.aclose()

        result = asyncio.run(_run())
        assert result["run_id"] == "optimize_test_123"
        assert result["mode"] == "production"

    def test_run_optimization_method_posts_to_correct_path(self) -> None:
        """run_optimization() POSTs to /analyses/optimize."""

        async def _run() -> None:
            client = AnalysisAPIClient(base_url="http://test:8000")
            mock_resp = AsyncMock()
            mock_resp.json.return_value = {"run_id": "test_123", "mode": "production"}
            mock_resp.raise_for_status = lambda: None
            try:
                with patch.object(
                    client._client, "post", new_callable=AsyncMock,
                    return_value=mock_resp,
                ) as mock_post:
                    payload = {"latitude": 33.45, "longitude": -111.98, "buildable_acres": 100}
                    await client.run_optimization(payload)
                mock_post.assert_called_once_with("/analyses/optimize", json=payload)
            finally:
                await client.aclose()

        asyncio.run(_run())


# ---------------------------------------------------------------------------
# Executor result summarization tests
# ---------------------------------------------------------------------------


class TestOptimizationResultSummarization:
    """Verify result summarization strips sweep_results but preserves winners."""

    def test_run_optimization_in_run_tools(self) -> None:
        """run_optimization is in _RUN_TOOLS for run_id caching."""
        assert "run_optimization" in _RUN_TOOLS

    def test_large_result_keys_configured(self) -> None:
        """_LARGE_RESULT_KEYS has sweep_results and bess_sweep_results."""
        assert "run_optimization" in _LARGE_RESULT_KEYS
        keys = _LARGE_RESULT_KEYS["run_optimization"]
        assert "sweep_results" in keys
        assert "bess_sweep_results" in keys

    def test_strips_sweep_results(self) -> None:
        """Summarization removes sweep_results from optimization output."""
        raw_result = {
            "run_id": "optimize_test",
            "mode": "npv",
            "winners": {
                "max_npv": {"gcr": 0.35, "dcac": 1.30, "npv": 1_500_000},
            },
            "sweep_metadata": {"points_evaluated": 90},
            "sweep_results": [
                {"gcr": 0.25, "dcac": 1.15, "energy_mwh": 1000},
                {"gcr": 0.30, "dcac": 1.20, "energy_mwh": 1100},
                # ... could be hundreds of rows
            ],
        }

        summarized = _summarize_tool_result("run_optimization", raw_result)

        assert "sweep_results" not in summarized
        assert summarized["run_id"] == "optimize_test"
        assert summarized["mode"] == "npv"
        assert summarized["winners"] == raw_result["winners"]
        assert summarized["sweep_metadata"] == raw_result["sweep_metadata"]

    def test_strips_bess_sweep_results(self) -> None:
        """Summarization removes bess_sweep_results from BESS optimization."""
        raw_result = {
            "run_id": "optimize_bess_test",
            "mode": "solar_bess",
            "winners": {
                "max_npv": {"gcr": 0.35, "dcac": 1.30, "npv": 2_000_000},
            },
            "bess_adds_value": True,
            "bess_winner_detail": {
                "solar": {"gcr": 0.35, "dcac": 1.30},
                "bess": {"power_mw": 5.0, "duration_hr": 4},
            },
            "sweep_metadata": {"points_evaluated": 120},
            "sweep_results": [{"gcr": 0.25, "energy_mwh": 900}],
            "bess_sweep_results": [
                {"power_mw": 2.5, "duration_hr": 2, "npv": 100_000},
                {"power_mw": 5.0, "duration_hr": 4, "npv": 200_000},
            ],
        }

        summarized = _summarize_tool_result("run_optimization", raw_result)

        assert "sweep_results" not in summarized
        assert "bess_sweep_results" not in summarized
        # Preserved fields
        assert summarized["bess_adds_value"] is True
        assert summarized["bess_winner_detail"] is not None
        assert summarized["winners"] == raw_result["winners"]

    def test_preserves_result_without_sweep_data(self) -> None:
        """If API response has no sweep_results (already stripped), no error."""
        clean_result = {
            "run_id": "optimize_clean",
            "mode": "production",
            "winners": {"max_energy": {"gcr": 0.35, "dcac": 1.30}},
            "sweep_metadata": {"points_evaluated": 50},
        }

        summarized = _summarize_tool_result("run_optimization", clean_result)

        # Should pass through unchanged (no keys to strip)
        assert summarized == clean_result

    def test_other_tools_unaffected(self) -> None:
        """Summarization of non-optimization tools is unchanged."""
        prod_result = {
            "run_id": "prod_123",
            "annual_energy_mwh": 12000,
            "capacity_factor_pct": 25.4,
        }

        summarized = _summarize_tool_result("run_production", prod_result)
        assert summarized == prod_result

    def test_sample_summarized_output(self) -> None:
        """Demonstrate what GPT-5 would see in context after summarization.

        This is the key test — verifying the context-window-friendly shape
        of an optimization result stored in the session.
        """
        raw_result = {
            "run_id": "optimize_phoenix_20260417",
            "mode": "npv",
            "winners": {
                "max_energy": {
                    "gcr": 0.30,
                    "dcac": 1.40,
                    "dc_capacity_mw": 14.0,
                    "ac_capacity_mw": 10.0,
                    "annual_energy_mwh": 28500,
                    "capacity_factor_pct": 23.2,
                },
                "min_lcoe": {
                    "gcr": 0.35,
                    "dcac": 1.30,
                    "dc_capacity_mw": 13.0,
                    "ac_capacity_mw": 10.0,
                    "annual_energy_mwh": 27800,
                    "lcoe_cents_per_kwh": 3.2,
                },
                "max_npv": {
                    "gcr": 0.35,
                    "dcac": 1.35,
                    "dc_capacity_mw": 13.5,
                    "ac_capacity_mw": 10.0,
                    "annual_energy_mwh": 28100,
                    "npv": 4_500_000,
                },
            },
            "sweep_metadata": {
                "points_evaluated": 90,
                "gcr_range": [0.25, 0.45],
                "dcac_range": [1.15, 1.60],
            },
            # These would be stripped:
            "sweep_results": [{"row": i} for i in range(90)],
            "bess_sweep_results": [{"row": i} for i in range(30)],
        }

        summarized = _summarize_tool_result("run_optimization", raw_result)
        serialized = json.dumps(summarized, indent=2)

        # Should be compact enough for GPT-5 context
        assert len(serialized) < 2000, (
            f"Summarized result too large for context: {len(serialized)} chars"
        )

        # Verify structure is what GPT-5 needs to synthesize a response
        parsed = json.loads(serialized)
        assert "winners" in parsed
        assert len(parsed["winners"]) == 3
        assert "sweep_metadata" in parsed
        assert "sweep_results" not in parsed
        assert "bess_sweep_results" not in parsed
