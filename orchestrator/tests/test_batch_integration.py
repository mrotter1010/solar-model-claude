"""Tests for batch processing orchestrator integration.

Covers the run_batch tool definition, API client method, executor
routing/caching, and result summarization.
"""

import json

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from orchestrator.config import OrchestratorConfig
from orchestrator.conversation.manager import ConversationManager
from orchestrator.conversation.models import ChatMessage, SessionStatus
from orchestrator.planning.executor import (
    ExecutionResult,
    Executor,
    _RUN_TOOLS,
    _summarize_tool_result,
)
from orchestrator.planning.planner import Planner
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


def _make_tool_call(tool_id: str, name: str, arguments: dict) -> MagicMock:
    """Build a mock OpenAI tool_call object."""
    tc = MagicMock()
    tc.id = tool_id
    tc.function.name = name
    tc.function.arguments = json.dumps(arguments)
    return tc


def _make_response(
    content: str | None = None, tool_calls: list | None = None
) -> MagicMock:
    """Build a mock OpenAI ChatCompletion response."""
    message = MagicMock()
    message.content = content
    message.tool_calls = tool_calls

    choice = MagicMock()
    choice.message = message

    response = MagicMock()
    response.choices = [choice]
    return response


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
def test_config(tmp_path):
    """OrchestratorConfig with a temporary system prompt."""
    prompt_file = tmp_path / "system_prompt.md"
    prompt_file.write_text("You are a solar analyst assistant.")
    return OrchestratorConfig(
        openai_api_key="test-key",
        openai_model="gpt-5",
        system_prompt_path=str(prompt_file),
        max_plan_steps=5,
    )


@pytest.fixture
def mock_openai_client():
    """Mocked AsyncOpenAI client."""
    return AsyncMock()


@pytest.fixture
def planner(test_config, mock_openai_client):
    """Planner with a mocked OpenAI client."""
    with patch(
        "orchestrator.planning.planner.AsyncOpenAI",
        return_value=mock_openai_client,
    ):
        p = Planner(test_config)
    p._client = mock_openai_client
    return p


@pytest.fixture
def conversation_manager():
    """Real ConversationManager (in-memory)."""
    return ConversationManager(session_ttl_minutes=60)


@pytest.fixture
def executor(planner, api_client_mock, conversation_manager, test_config):
    """Executor wired to mocked planner and API client."""
    return Executor(
        planner=planner,
        api_client=api_client_mock,
        conversation_manager=conversation_manager,
        max_steps=test_config.max_plan_steps,
    )


@pytest.fixture
def api_client_mock():
    """Fully mocked AnalysisAPIClient for executor tests."""
    return AsyncMock(spec=AnalysisAPIClient)


def _setup_session(
    conversation_manager: ConversationManager,
    session_id: str = "batch-session",
    plan: str = "1. Run batch analysis",
) -> str:
    """Create a session in PLAN_PENDING state."""
    conversation_manager.get_or_create_session(session_id)
    conversation_manager.add_message(
        session_id,
        ChatMessage(role="user", content="Run batch on my uploaded file"),
    )
    conversation_manager.set_pending_plan(session_id, plan)
    return session_id


# ---------------------------------------------------------------------------
# Tool definition tests
# ---------------------------------------------------------------------------


class TestRunBatchToolDefinition:
    """Verify run_batch tool schema is valid and complete."""

    def test_run_batch_definition_exists(self) -> None:
        """run_batch tool definition is present in TOOL_DEFINITIONS."""
        tool_names = [t["function"]["name"] for t in TOOL_DEFINITIONS]
        assert "run_batch" in tool_names

    def test_run_batch_definition_schema(self) -> None:
        """run_batch definition has correct structure and required params."""
        defn = next(
            t for t in TOOL_DEFINITIONS
            if t["function"]["name"] == "run_batch"
        )
        func = defn["function"]

        # Top-level structure
        assert defn["type"] == "function"
        assert "description" in func
        assert func["parameters"]["type"] == "object"

        # Required params
        assert set(func["parameters"]["required"]) == {
            "file_path", "confirm"
        }

        # Properties exist with correct types
        props = func["parameters"]["properties"]
        assert props["file_path"]["type"] == "string"
        assert props["confirm"]["type"] == "boolean"

    def test_run_batch_description_mentions_key_constraints(self) -> None:
        """Description mentions batch limits and BTM restriction."""
        defn = next(
            t for t in TOOL_DEFINITIONS
            if t["function"]["name"] == "run_batch"
        )
        desc = defn["function"]["description"]

        assert "25" in desc  # row limit
        assert "BTM" in desc  # BTM not supported
        assert "FTM" in desc  # FTM only
        assert "validate" in desc.lower()  # validates before running

    def test_run_batch_endpoint_mapping(self) -> None:
        """TOOL_ENDPOINTS maps run_batch to POST /analyses/batch."""
        assert "run_batch" in TOOL_ENDPOINTS
        method, path = TOOL_ENDPOINTS["run_batch"]
        assert method == "POST"
        assert path == "/analyses/batch/run"

    def test_tool_count_updated(self) -> None:
        """TOOL_DEFINITIONS and TOOL_ENDPOINTS both have 15 entries."""
        assert len(TOOL_DEFINITIONS) == 15
        assert len(TOOL_ENDPOINTS) == 15

    def test_all_tools_have_endpoints(self) -> None:
        """Every tool in TOOL_DEFINITIONS has a TOOL_ENDPOINTS entry."""
        tool_names = {t["function"]["name"] for t in TOOL_DEFINITIONS}
        endpoint_names = set(TOOL_ENDPOINTS.keys())
        assert tool_names == endpoint_names


# ---------------------------------------------------------------------------
# API client tests
# ---------------------------------------------------------------------------


class TestRunBatchApiClient:
    """Tests for AnalysisAPIClient.run_batch() HTTP call."""

    @pytest.mark.anyio
    async def test_run_batch_sends_json_file_path(
        self, api_client, mock_httpx_client
    ) -> None:
        """run_batch sends JSON with file_path to /analyses/batch/run."""
        mock_httpx_client.post.return_value = _mock_json_response({
            "run_id": "batch-abc",
            "total_rows": 1,
            "succeeded": 1,
            "failed": 0,
        })

        result = await api_client.run_batch(
            file_path="/uploads/batch/batch_input.csv"
        )

        # Verify POST to correct endpoint with JSON body
        mock_httpx_client.post.assert_called_once()
        call_args = mock_httpx_client.post.call_args
        assert call_args[0][0] == "/analyses/batch/run"
        assert call_args[1]["json"] == {
            "file_path": "/uploads/batch/batch_input.csv"
        }

        assert result["run_id"] == "batch-abc"

    @pytest.mark.anyio
    async def test_execute_tool_dispatches_run_batch(
        self, api_client, mock_httpx_client
    ) -> None:
        """execute_tool routes 'run_batch' to run_batch method."""
        mock_httpx_client.post.return_value = _mock_json_response({
            "run_id": "batch-dispatch",
            "total_rows": 0,
            "succeeded": 0,
            "failed": 0,
        })

        result = await api_client.execute_tool(
            "run_batch",
            {"file_path": "/uploads/batch/test.csv", "confirm": True},
        )

        assert result["run_id"] == "batch-dispatch"
        mock_httpx_client.post.assert_called_once()


# ---------------------------------------------------------------------------
# Executor integration tests
# ---------------------------------------------------------------------------


class TestRunBatchInRunTools:
    """Verify run_batch is recognized as a run tool for result caching."""

    def test_run_batch_in_run_tools(self) -> None:
        """run_batch is in the _RUN_TOOLS set."""
        assert "run_batch" in _RUN_TOOLS


class TestRunBatchExecutorRouting:
    """Verify executor routes run_batch and caches results."""

    @pytest.mark.anyio
    async def test_executor_routes_run_batch(
        self,
        executor,
        planner,
        api_client_mock,
        conversation_manager,
        mock_openai_client,
    ) -> None:
        """run_batch tool call is dispatched and result cached."""
        session_id = _setup_session(conversation_manager)

        tc = _make_tool_call(
            "call-batch",
            "run_batch",
            {"file_path": "/uploads/batch.csv", "confirm": True},
        )
        resp_tool = _make_response(tool_calls=[tc])
        resp_synth = _make_response(
            content="Batch complete: 3/3 sites succeeded."
        )

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp_tool, resp_synth]
        )

        batch_result = {
            "run_id": "batch-999",
            "total_rows": 3,
            "succeeded": 3,
            "failed": 0,
            "total_runtime_seconds": 45.0,
            "warnings": [],
            "files": {
                "workbook_url": "/analyses/batch-999/batch_results",
            },
            "summary": [
                {"name": "Site A", "analysis_type": "production",
                 "status": "success", "error": None},
                {"name": "Site B", "analysis_type": "buildability",
                 "status": "success", "error": None},
                {"name": "Site C", "analysis_type": "optimization",
                 "status": "success", "error": None},
            ],
        }
        api_client_mock.execute_tool = AsyncMock(return_value=batch_result)

        result = await executor.execute_plan(session_id)

        assert result.success is True
        assert len(result.steps) == 1
        assert result.steps[0]["tool"] == "run_batch"
        assert result.steps[0]["success"] is True
        assert result.steps[0]["result"]["run_id"] == "batch-999"
        assert result.synthesis == "Batch complete: 3/3 sites succeeded."

        # Verify run_id was cached
        session = conversation_manager.get_session(session_id)
        assert "batch-999" in session.run_ids
        assert session.cached_results["batch-999"] == batch_result


# ---------------------------------------------------------------------------
# Summarization tests
# ---------------------------------------------------------------------------


class TestSummarizeBatchResult:
    """Tests for _summarize_tool_result with run_batch results."""

    def test_short_summary_unchanged(self) -> None:
        """Batch result with <=5 summary rows is returned as-is."""
        result = {
            "run_id": "batch-short",
            "total_rows": 3,
            "succeeded": 3,
            "failed": 0,
            "summary": [
                {"name": "A", "status": "success", "error": None},
                {"name": "B", "status": "success", "error": None},
                {"name": "C", "status": "success", "error": None},
            ],
        }

        summarized = _summarize_tool_result("run_batch", result)

        assert summarized is result  # Same object — not modified
        assert len(summarized["summary"]) == 3
        assert "summary_truncated" not in summarized

    def test_long_summary_truncated(self) -> None:
        """Batch result with >5 summary rows is truncated to first 5."""
        rows = [
            {"name": f"Site {i}", "status": "success", "error": None}
            for i in range(15)
        ]
        result = {
            "run_id": "batch-long",
            "total_rows": 15,
            "succeeded": 15,
            "failed": 0,
            "summary": rows,
        }

        summarized = _summarize_tool_result("run_batch", result)

        assert len(summarized["summary"]) == 5
        assert summarized["summary_truncated"] is True
        assert summarized["summary_total"] == 15
        # Original result should not be mutated
        assert len(result["summary"]) == 15

    def test_exactly_five_rows_unchanged(self) -> None:
        """Batch result with exactly 5 summary rows is not truncated."""
        rows = [
            {"name": f"Site {i}", "status": "success", "error": None}
            for i in range(5)
        ]
        result = {
            "run_id": "batch-five",
            "total_rows": 5,
            "summary": rows,
        }

        summarized = _summarize_tool_result("run_batch", result)

        assert summarized is result
        assert "summary_truncated" not in summarized

    def test_no_summary_key_unchanged(self) -> None:
        """Batch error result (no summary key) passes through unchanged."""
        result = {
            "error": "Validation failed",
            "tool": "run_batch",
        }

        summarized = _summarize_tool_result("run_batch", result)

        assert summarized is result

    def test_other_tools_unaffected(self) -> None:
        """Non-batch tools with summary key are not modified."""
        result = {
            "run_id": "prod-001",
            "summary": list(range(100)),
        }

        summarized = _summarize_tool_result("run_production", result)

        assert summarized is result
        assert len(summarized["summary"]) == 100
