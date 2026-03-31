"""Tests for sequential plan step execution."""

import json

import httpx
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from orchestrator.config import OrchestratorConfig
from orchestrator.conversation.manager import ConversationManager
from orchestrator.conversation.models import ChatMessage, SessionStatus
from orchestrator.planning.executor import ExecutionResult, Executor
from orchestrator.planning.planner import Planner
from orchestrator.tools.api_client import AnalysisAPIClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
def api_client():
    """Mocked AnalysisAPIClient."""
    client = AsyncMock(spec=AnalysisAPIClient)
    return client


@pytest.fixture
def conversation_manager():
    """Real ConversationManager (in-memory, no mocking needed)."""
    return ConversationManager(session_ttl_minutes=60)


@pytest.fixture
def executor(planner, api_client, conversation_manager, test_config):
    """Executor wired to mocked planner and API client."""
    return Executor(
        planner=planner,
        api_client=api_client,
        conversation_manager=conversation_manager,
        max_steps=test_config.max_plan_steps,
    )


def _setup_session(
    conversation_manager: ConversationManager,
    session_id: str = "test-session",
    plan: str = "1. Run health check",
) -> str:
    """Create a session in PLAN_PENDING state with a user message."""
    conversation_manager.get_or_create_session(session_id)
    conversation_manager.add_message(
        session_id,
        ChatMessage(role="user", content="Analyze a 5 MW site in Phoenix"),
    )
    conversation_manager.set_pending_plan(session_id, plan)
    return session_id


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestExecuteSimplePlan:
    """Single-tool execution."""

    @pytest.mark.anyio
    async def test_execute_simple_plan(
        self, executor, planner, api_client, conversation_manager, mock_openai_client
    ):
        """Single tool call -> API result -> text synthesis."""
        session_id = _setup_session(conversation_manager)

        # First OpenAI call (generate_execution_calls) -> tool call
        tc = _make_tool_call("call-1", "health_check", {})
        resp_with_tool = _make_response(tool_calls=[tc])

        # Second OpenAI call (continue_execution) -> synthesis
        resp_synthesis = _make_response(content="API is healthy and operational.")

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp_with_tool, resp_synthesis]
        )

        api_client.execute_tool = AsyncMock(
            return_value={"status": "healthy"}
        )

        result = await executor.execute_plan(session_id)

        assert result.success is True
        assert len(result.steps) == 1
        assert result.steps[0]["tool"] == "health_check"
        assert result.steps[0]["success"] is True
        assert result.synthesis == "API is healthy and operational."


class TestExecuteMultiStepPlan:
    """Multi-tool execution with sequential steps."""

    @pytest.mark.anyio
    async def test_execute_multi_step_plan(
        self, executor, planner, api_client, conversation_manager, mock_openai_client
    ):
        """Two tool calls across two iterations -> synthesis."""
        session_id = _setup_session(
            conversation_manager,
            plan="1. Search modules\n2. Run production",
        )

        # Iteration 1: search_modules
        tc1 = _make_tool_call("call-1", "search_modules", {"search": "CS6W"})
        resp1 = _make_response(tool_calls=[tc1])

        # Iteration 2: run_production
        tc2 = _make_tool_call("call-2", "run_production", {
            "site": {"name": "Test", "lat": 33.45, "lon": -112.07},
            "system": {"dc_capacity_mw": 5.0},
        })
        resp2 = _make_response(tool_calls=[tc2])

        # Iteration 3: synthesis
        resp3 = _make_response(content="Site produces 10,422 MWh/yr (CF 23.8%).")

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp1, resp2, resp3]
        )

        api_client.execute_tool = AsyncMock(
            side_effect=[
                {"modules": [{"name": "CS6W-550MS"}]},
                {"run_id": "run-001", "annual_kwh": 10_422_000},
            ]
        )

        result = await executor.execute_plan(session_id)

        assert result.success is True
        assert len(result.steps) == 2
        assert result.steps[0]["tool"] == "search_modules"
        assert result.steps[1]["tool"] == "run_production"
        assert result.synthesis is not None
        assert "10,422" in result.synthesis


class TestExecutePlanWithApiError:
    """Tool call fails with an API error."""

    @pytest.mark.anyio
    async def test_execute_plan_with_api_error(
        self, executor, planner, api_client, conversation_manager, mock_openai_client
    ):
        """API 422 error is caught, recorded, and sent to GPT-5 for synthesis."""
        session_id = _setup_session(conversation_manager)

        tc = _make_tool_call("call-1", "run_production", {
            "site": {"name": "Bad", "lat": 0, "lon": 0},
        })
        resp_tool = _make_response(tool_calls=[tc])
        resp_synth = _make_response(
            content="The production run failed due to invalid coordinates."
        )

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp_tool, resp_synth]
        )

        # Simulate a 422 error
        mock_request = MagicMock(spec=httpx.Request)
        mock_request.url = "http://localhost:8000/analyses/production"
        mock_http_response = MagicMock(spec=httpx.Response)
        mock_http_response.status_code = 422
        mock_http_response.text = "Validation error"
        api_client.execute_tool = AsyncMock(
            side_effect=httpx.HTTPStatusError(
                "422 Unprocessable Entity",
                request=mock_request,
                response=mock_http_response,
            )
        )

        result = await executor.execute_plan(session_id)

        assert result.success is False
        assert len(result.steps) == 1
        assert result.steps[0]["success"] is False
        assert result.steps[0]["error"] is not None
        assert result.synthesis is not None


class TestExecutePlanCachesResults:
    """Run results are cached in the conversation manager."""

    @pytest.mark.anyio
    async def test_execute_plan_caches_results(
        self, executor, planner, api_client, conversation_manager, mock_openai_client
    ):
        """run_production response with run_id is cached."""
        session_id = _setup_session(conversation_manager)

        tc = _make_tool_call("call-1", "run_production", {
            "site": {"name": "Test", "lat": 33.45, "lon": -112.07},
            "system": {"dc_capacity_mw": 5.0},
        })
        resp_tool = _make_response(tool_calls=[tc])
        resp_synth = _make_response(content="Done.")

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp_tool, resp_synth]
        )

        api_result = {"run_id": "abc123", "annual_kwh": 10_000_000}
        api_client.execute_tool = AsyncMock(return_value=api_result)

        await executor.execute_plan(session_id)

        session = conversation_manager.get_session(session_id)
        assert "abc123" in session.run_ids
        assert session.cached_results["abc123"] == api_result


class TestExecutePlanMaxStepsCap:
    """Loop stops at max_plan_steps."""

    @pytest.mark.anyio
    async def test_execute_plan_max_steps_cap(
        self, executor, planner, api_client, conversation_manager, mock_openai_client
    ):
        """Verify loop stops at max_steps and returns partial results."""
        session_id = _setup_session(conversation_manager)

        # Create tool-call responses that never stop
        def infinite_tool_calls(*args, **kwargs):
            tc = _make_tool_call("call-inf", "health_check", {})
            return _make_response(tool_calls=[tc])

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=infinite_tool_calls
        )

        api_client.execute_tool = AsyncMock(
            return_value={"status": "healthy"}
        )

        result = await executor.execute_plan(session_id)

        # max_steps is 5 from fixture config
        assert len(result.steps) == 5
        assert "maximum step limit" in result.synthesis


class TestExecutePlanSessionNotFound:
    """Nonexistent session returns error."""

    @pytest.mark.anyio
    async def test_execute_plan_session_not_found(self, executor):
        """Error result for a session that doesn't exist."""
        result = await executor.execute_plan("nonexistent-session")

        assert result.success is False
        assert "not found" in result.error.lower()
        assert result.synthesis is None


class TestExecutePlanWrongStatus:
    """Session in wrong status returns error."""

    @pytest.mark.anyio
    async def test_execute_plan_wrong_status(
        self, executor, conversation_manager
    ):
        """IDLE session (no pending plan) -> error result."""
        session_id = "idle-session"
        conversation_manager.get_or_create_session(session_id)
        # Status defaults to IDLE, no pending plan

        result = await executor.execute_plan(session_id)

        assert result.success is False
        assert "plan_pending" in result.error.lower()


class TestExecuteMessagesAddedToHistory:
    """Verify assistant and tool messages are tracked in conversation."""

    @pytest.mark.anyio
    async def test_execute_messages_added_to_history(
        self, executor, planner, api_client, conversation_manager, mock_openai_client
    ):
        """Assistant tool-call and tool-result messages appear in history."""
        session_id = _setup_session(conversation_manager)

        tc = _make_tool_call("call-msg", "health_check", {})
        resp_tool = _make_response(tool_calls=[tc])
        resp_synth = _make_response(content="All good.")

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp_tool, resp_synth]
        )

        api_client.execute_tool = AsyncMock(
            return_value={"status": "healthy"}
        )

        await executor.execute_plan(session_id)

        session = conversation_manager.get_session(session_id)
        roles = [m.role for m in session.messages]

        # Original user message + assistant (tool_calls) + tool (result)
        assert "assistant" in roles
        assert "tool" in roles

        # Verify the tool message has correct structure
        tool_msgs = [m for m in session.messages if m.role == "tool"]
        assert len(tool_msgs) == 1
        assert tool_msgs[0].tool_call_id == "call-msg"
        assert tool_msgs[0].name == "health_check"


class TestExecuteBytesResponseHandling:
    """Bytes responses (PDF/CSV) are summarized, not serialized raw."""

    @pytest.mark.anyio
    async def test_execute_bytes_response_handling(
        self, executor, planner, api_client, conversation_manager, mock_openai_client
    ):
        """get_report returning bytes -> summary dict sent to GPT-5."""
        session_id = _setup_session(conversation_manager)

        tc = _make_tool_call("call-pdf", "get_report", {"run_id": "run-99"})
        resp_tool = _make_response(tool_calls=[tc])
        resp_synth = _make_response(content="Report downloaded successfully.")

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp_tool, resp_synth]
        )

        pdf_bytes = b"%PDF-1.4 fake pdf content here" * 100
        api_client.execute_tool = AsyncMock(return_value=pdf_bytes)

        result = await executor.execute_plan(session_id)

        assert result.success is True
        assert result.steps[0]["result"]["status"] == "downloaded"
        assert result.steps[0]["result"]["size_bytes"] == len(pdf_bytes)

        # Verify the tool message content sent to GPT-5 is JSON, not raw bytes
        session = conversation_manager.get_session(session_id)
        tool_msgs = [m for m in session.messages if m.role == "tool"]
        parsed = json.loads(tool_msgs[0].content)
        assert parsed["status"] == "downloaded"
        assert parsed["type"] == "pdf"
        assert parsed["size_bytes"] == len(pdf_bytes)
