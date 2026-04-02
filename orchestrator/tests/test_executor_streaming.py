"""Tests for the async generator execution path (_execute_plan_generator)."""

import json

import httpx
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from orchestrator.config import OrchestratorConfig
from orchestrator.conversation.manager import ConversationManager
from orchestrator.conversation.models import ChatMessage, SessionStatus
from orchestrator.planning.events import (
    DoneEvent,
    ErrorEvent,
    StepCompleteEvent,
    StepStartEvent,
    SynthesisEvent,
)
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


async def _collect_events(gen):
    """Collect all events from an async generator into a list."""
    events = []
    async for event in gen:
        events.append(event)
    return events


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
    return AsyncMock(spec=AnalysisAPIClient)


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
# Tests — Event ordering
# ---------------------------------------------------------------------------


class TestGeneratorEventOrder:
    """Verify the generator yields events in the expected sequence."""

    @pytest.mark.anyio
    async def test_single_step_event_order(
        self, executor, api_client, conversation_manager, mock_openai_client
    ):
        """Single tool: StepStart → StepComplete → Synthesis → Done."""
        session_id = _setup_session(conversation_manager)

        tc = _make_tool_call("call-1", "health_check", {})
        resp_tool = _make_response(tool_calls=[tc])
        resp_synth = _make_response(content="API is healthy.")

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp_tool, resp_synth]
        )
        api_client.execute_tool = AsyncMock(
            return_value={"status": "healthy"}
        )

        events = await _collect_events(
            executor._execute_plan_generator(session_id)
        )

        types = [type(e) for e in events]
        assert types == [
            StepStartEvent,
            StepCompleteEvent,
            SynthesisEvent,
            DoneEvent,
        ]

    @pytest.mark.anyio
    async def test_multi_step_event_order(
        self, executor, api_client, conversation_manager, mock_openai_client
    ):
        """Two tools: Start/Complete pairs then Synthesis then Done."""
        session_id = _setup_session(
            conversation_manager,
            plan="1. Search modules\n2. Run production",
        )

        tc1 = _make_tool_call("call-1", "search_modules", {"search": "CS6W"})
        resp1 = _make_response(tool_calls=[tc1])

        tc2 = _make_tool_call("call-2", "run_production", {"dc_mw": 5.0})
        resp2 = _make_response(tool_calls=[tc2])

        resp3 = _make_response(content="Site produces 10,422 MWh/yr.")

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp1, resp2, resp3]
        )
        api_client.execute_tool = AsyncMock(
            side_effect=[
                {"modules": [{"name": "CS6W-550MS"}]},
                {"run_id": "run-001", "annual_kwh": 10_422_000},
            ]
        )

        events = await _collect_events(
            executor._execute_plan_generator(session_id)
        )

        types = [type(e) for e in events]
        assert types == [
            StepStartEvent,
            StepCompleteEvent,
            StepStartEvent,
            StepCompleteEvent,
            SynthesisEvent,
            DoneEvent,
        ]

    @pytest.mark.anyio
    async def test_step_numbers_increment(
        self, executor, api_client, conversation_manager, mock_openai_client
    ):
        """Step numbers are sequential starting from 1."""
        session_id = _setup_session(
            conversation_manager,
            plan="1. Search\n2. Run",
        )

        tc1 = _make_tool_call("call-1", "search_modules", {"search": "CS6W"})
        tc2 = _make_tool_call("call-2", "run_production", {"dc_mw": 5.0})
        resp1 = _make_response(tool_calls=[tc1])
        resp2 = _make_response(tool_calls=[tc2])
        resp3 = _make_response(content="Done.")

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp1, resp2, resp3]
        )
        api_client.execute_tool = AsyncMock(
            side_effect=[{"modules": []}, {"run_id": "r1"}]
        )

        events = await _collect_events(
            executor._execute_plan_generator(session_id)
        )

        start_events = [e for e in events if isinstance(e, StepStartEvent)]
        complete_events = [
            e for e in events if isinstance(e, StepCompleteEvent)
        ]

        assert [e.step_number for e in start_events] == [1, 2]
        assert [e.step_number for e in complete_events] == [1, 2]


# ---------------------------------------------------------------------------
# Tests — Event content
# ---------------------------------------------------------------------------


class TestGeneratorEventContent:
    """Verify individual event fields are populated correctly."""

    @pytest.mark.anyio
    async def test_step_start_fields(
        self, executor, api_client, conversation_manager, mock_openai_client
    ):
        """StepStartEvent has correct tool_name and truncated tool_input."""
        session_id = _setup_session(conversation_manager)

        args = {"site": {"name": "Test", "lat": 33.45, "lon": -112.07}}
        tc = _make_tool_call("call-1", "run_production", args)
        resp_tool = _make_response(tool_calls=[tc])
        resp_synth = _make_response(content="Done.")

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp_tool, resp_synth]
        )
        api_client.execute_tool = AsyncMock(return_value={"run_id": "r1"})

        events = await _collect_events(
            executor._execute_plan_generator(session_id)
        )

        start = events[0]
        assert isinstance(start, StepStartEvent)
        assert start.step_number == 1
        assert start.tool_name == "run_production"
        assert "Test" in start.tool_input

    @pytest.mark.anyio
    async def test_step_complete_has_step_data(
        self, executor, api_client, conversation_manager, mock_openai_client
    ):
        """StepCompleteEvent.step_data matches the full step dict."""
        session_id = _setup_session(conversation_manager)

        tc = _make_tool_call("call-1", "health_check", {})
        resp_tool = _make_response(tool_calls=[tc])
        resp_synth = _make_response(content="OK.")

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp_tool, resp_synth]
        )
        api_client.execute_tool = AsyncMock(
            return_value={"status": "healthy"}
        )

        events = await _collect_events(
            executor._execute_plan_generator(session_id)
        )

        complete = [e for e in events if isinstance(e, StepCompleteEvent)][0]
        assert complete.step_data["tool"] == "health_check"
        assert complete.step_data["result"] == {"status": "healthy"}
        assert complete.step_data["success"] is True
        assert complete.step_data["error"] is None

    @pytest.mark.anyio
    async def test_synthesis_event_text(
        self, executor, api_client, conversation_manager, mock_openai_client
    ):
        """SynthesisEvent carries the full GPT-5 synthesis text."""
        session_id = _setup_session(conversation_manager)

        tc = _make_tool_call("call-1", "health_check", {})
        resp_tool = _make_response(tool_calls=[tc])
        synthesis_text = "The API is healthy and all systems are operational."
        resp_synth = _make_response(content=synthesis_text)

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp_tool, resp_synth]
        )
        api_client.execute_tool = AsyncMock(
            return_value={"status": "healthy"}
        )

        events = await _collect_events(
            executor._execute_plan_generator(session_id)
        )

        synth = [e for e in events if isinstance(e, SynthesisEvent)][0]
        assert synth.text == synthesis_text

    @pytest.mark.anyio
    async def test_done_event_fields(
        self, executor, api_client, conversation_manager, mock_openai_client
    ):
        """DoneEvent has correct success flag and total_steps count."""
        session_id = _setup_session(conversation_manager)

        tc = _make_tool_call("call-1", "health_check", {})
        resp_tool = _make_response(tool_calls=[tc])
        resp_synth = _make_response(content="OK.")

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp_tool, resp_synth]
        )
        api_client.execute_tool = AsyncMock(
            return_value={"status": "healthy"}
        )

        events = await _collect_events(
            executor._execute_plan_generator(session_id)
        )

        done = [e for e in events if isinstance(e, DoneEvent)][0]
        assert done.success is True
        assert done.total_steps == 1


# ---------------------------------------------------------------------------
# Tests — Error paths
# ---------------------------------------------------------------------------


class TestGeneratorErrorHandling:
    """Verify error paths yield ErrorEvent correctly."""

    @pytest.mark.anyio
    async def test_session_not_found_yields_error(self, executor):
        """Nonexistent session yields ErrorEvent + DoneEvent."""
        events = await _collect_events(
            executor._execute_plan_generator("nonexistent")
        )

        types = [type(e) for e in events]
        assert types == [ErrorEvent, DoneEvent]
        assert "not found" in events[0].message.lower()
        assert events[1].success is False
        assert events[1].total_steps == 0

    @pytest.mark.anyio
    async def test_wrong_status_yields_error(
        self, executor, conversation_manager
    ):
        """IDLE session yields ErrorEvent + DoneEvent."""
        conversation_manager.get_or_create_session("idle-session")

        events = await _collect_events(
            executor._execute_plan_generator("idle-session")
        )

        types = [type(e) for e in events]
        assert types == [ErrorEvent, DoneEvent]
        assert "plan_pending" in events[0].message.lower()

    @pytest.mark.anyio
    async def test_tool_failure_yields_failed_step_complete(
        self, executor, api_client, conversation_manager, mock_openai_client
    ):
        """API error on tool call yields StepComplete with success=False."""
        session_id = _setup_session(conversation_manager)

        tc = _make_tool_call("call-1", "run_production", {})
        resp_tool = _make_response(tool_calls=[tc])
        resp_synth = _make_response(content="Failed due to bad coordinates.")

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp_tool, resp_synth]
        )

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

        events = await _collect_events(
            executor._execute_plan_generator(session_id)
        )

        complete = [e for e in events if isinstance(e, StepCompleteEvent)][0]
        assert complete.success is False
        assert complete.step_data["error"] is not None

        done = [e for e in events if isinstance(e, DoneEvent)][0]
        assert done.success is False

    @pytest.mark.anyio
    async def test_openai_error_during_loop_yields_error_event(
        self, executor, api_client, conversation_manager, mock_openai_client
    ):
        """OpenAI failure mid-loop yields steps so far + ErrorEvent + Done."""
        session_id = _setup_session(conversation_manager)

        tc = _make_tool_call("call-1", "health_check", {})
        resp_tool = _make_response(tool_calls=[tc])

        # First call succeeds (tool call), second call (continue) fails
        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp_tool, Exception("OpenAI rate limit")]
        )
        api_client.execute_tool = AsyncMock(
            return_value={"status": "healthy"}
        )

        events = await _collect_events(
            executor._execute_plan_generator(session_id)
        )

        types = [type(e) for e in events]
        assert StepStartEvent in types
        assert StepCompleteEvent in types
        assert ErrorEvent in types
        assert DoneEvent in types

        # No SynthesisEvent on OpenAI error
        assert SynthesisEvent not in types

        error = [e for e in events if isinstance(e, ErrorEvent)][0]
        assert "OpenAI error" in error.message

        done = [e for e in events if isinstance(e, DoneEvent)][0]
        assert done.success is False
        assert done.total_steps == 1

    @pytest.mark.anyio
    async def test_max_steps_yields_synthesis_event(
        self, executor, api_client, conversation_manager, mock_openai_client
    ):
        """Max steps reached yields SynthesisEvent with limit message."""
        session_id = _setup_session(conversation_manager)

        def infinite_tool_calls(*args, **kwargs):
            tc = _make_tool_call("call-inf", "health_check", {})
            return _make_response(tool_calls=[tc])

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=infinite_tool_calls
        )
        api_client.execute_tool = AsyncMock(
            return_value={"status": "healthy"}
        )

        events = await _collect_events(
            executor._execute_plan_generator(session_id)
        )

        synth_events = [e for e in events if isinstance(e, SynthesisEvent)]
        assert len(synth_events) == 1
        assert "maximum step limit" in synth_events[0].text

        done = [e for e in events if isinstance(e, DoneEvent)][0]
        assert done.total_steps == 5  # max_steps from fixture


# ---------------------------------------------------------------------------
# Tests — Sync wrapper backward compatibility
# ---------------------------------------------------------------------------


class TestSyncWrapperCompatibility:
    """Verify execute_plan returns identical results to the old implementation."""

    @pytest.mark.anyio
    async def test_sync_wrapper_simple_plan(
        self, executor, api_client, conversation_manager, mock_openai_client
    ):
        """Sync wrapper returns same ExecutionResult as before."""
        session_id = _setup_session(conversation_manager)

        tc = _make_tool_call("call-1", "health_check", {})
        resp_tool = _make_response(tool_calls=[tc])
        resp_synth = _make_response(content="API is healthy.")

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp_tool, resp_synth]
        )
        api_client.execute_tool = AsyncMock(
            return_value={"status": "healthy"}
        )

        result = await executor.execute_plan(session_id)

        assert isinstance(result, ExecutionResult)
        assert result.success is True
        assert result.error is None
        assert len(result.steps) == 1
        assert result.steps[0]["tool"] == "health_check"
        assert result.steps[0]["success"] is True
        assert result.synthesis == "API is healthy."

    @pytest.mark.anyio
    async def test_sync_wrapper_error_result(self, executor):
        """Sync wrapper returns correct error for nonexistent session."""
        result = await executor.execute_plan("nonexistent")

        assert isinstance(result, ExecutionResult)
        assert result.success is False
        assert "not found" in result.error.lower()
        assert result.synthesis is None
        assert result.steps == []

    @pytest.mark.anyio
    async def test_sync_wrapper_with_tool_failure(
        self, executor, api_client, conversation_manager, mock_openai_client
    ):
        """Sync wrapper marks success=False when a tool fails."""
        session_id = _setup_session(conversation_manager)

        tc = _make_tool_call("call-1", "run_production", {})
        resp_tool = _make_response(tool_calls=[tc])
        resp_synth = _make_response(content="Run failed.")

        mock_openai_client.chat.completions.create = AsyncMock(
            side_effect=[resp_tool, resp_synth]
        )
        api_client.execute_tool = AsyncMock(
            side_effect=RuntimeError("Connection refused")
        )

        result = await executor.execute_plan(session_id)

        assert result.success is False
        assert len(result.steps) == 1
        assert result.steps[0]["success"] is False
        assert "Connection refused" in result.steps[0]["error"]
        assert result.synthesis == "Run failed."
