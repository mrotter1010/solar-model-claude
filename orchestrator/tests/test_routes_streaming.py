"""Tests for the SSE streaming endpoint POST /chat/approve/stream."""

import json

import pytest
from unittest.mock import AsyncMock

import httpx
from httpx import ASGITransport

from orchestrator.app import app
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
from orchestrator.planning.models import PlannerResponse, ResponseType


# ---------------------------------------------------------------------------
# SSE parsing helper
# ---------------------------------------------------------------------------


def _parse_sse_events(raw: str) -> list[dict]:
    """Parse raw SSE text into a list of {event, data} dicts.

    Handles the standard SSE format:
        event: <name>
        data: <json>

    Skips empty lines and comment lines (starting with ':').
    """
    events = []
    current: dict = {}

    for line in raw.splitlines():
        if line.startswith(":") or not line.strip():
            # Delimiter between events or SSE comment — flush if we have data
            if current:
                events.append(current)
                current = {}
            continue

        if line.startswith("event:"):
            current["event"] = line[len("event:"):].strip()
        elif line.startswith("data:"):
            current["data"] = line[len("data:"):].strip()

    # Flush final event if present
    if current:
        events.append(current)

    return events


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conversation_manager():
    """Fresh ConversationManager per test."""
    return ConversationManager(session_ttl_minutes=60)


@pytest.fixture
def mock_planner():
    """Mocked Planner with default generate_plan returning a RESPONSE."""
    planner = AsyncMock()
    planner.generate_plan = AsyncMock(
        return_value=PlannerResponse(
            response_type=ResponseType.RESPONSE,
            content="Hello! I can help with solar analysis.",
        )
    )
    return planner


@pytest.fixture
def mock_executor():
    """Mocked Executor with both execute_plan and _execute_plan_generator."""
    executor = AsyncMock(spec=Executor)
    return executor


@pytest.fixture
def mock_conversation_db():
    """Mocked ConversationDB with async no-op methods."""
    db = AsyncMock()
    db.create_conversation = AsyncMock(return_value={
        "id": "00000000-0000-0000-0000-000000000099",
        "created_at": "2026-04-08T00:00:00Z",
    })
    db.add_message = AsyncMock()
    db.get_next_sequence = AsyncMock(return_value=1)
    db.update_conversation_title = AsyncMock()
    db.get_conversation = AsyncMock(return_value=None)
    return db


@pytest.fixture
async def client(
    conversation_manager, mock_planner, mock_executor, mock_conversation_db
):
    """Async HTTP client with app state injected (no lifespan)."""
    app.state.conversation_manager = conversation_manager
    app.state.planner = mock_planner
    app.state.executor = mock_executor
    app.state.conversation_db = mock_conversation_db

    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"X-User-Id": "00000000-0000-0000-0000-000000000001"},
    ) as c:
        yield c


def _setup_plan_session(
    conversation_manager: ConversationManager,
    session_id: str = "sse-session",
) -> str:
    """Create a session in PLAN_PENDING state."""
    conversation_manager.get_or_create_session(session_id)
    conversation_manager.add_message(
        session_id,
        ChatMessage(role="user", content="Analyze a 5 MW site in Phoenix"),
    )
    conversation_manager.set_pending_plan(
        session_id, "ANALYSIS PLAN\n1. Run production"
    )
    return session_id


async def _fake_generator(*events):
    """Build an async generator that yields the given events."""
    for event in events:
        yield event


# ---------------------------------------------------------------------------
# Tests — Content-Type and basic streaming
# ---------------------------------------------------------------------------


class TestStreamContentType:

    @pytest.mark.anyio
    async def test_stream_returns_event_stream_content_type(
        self, client, mock_executor, conversation_manager
    ):
        """POST /chat/approve/stream returns content-type text/event-stream."""
        session_id = _setup_plan_session(conversation_manager)

        mock_executor._execute_plan_generator = lambda sid: _fake_generator(
            DoneEvent(success=True, total_steps=0),
        )

        resp = await client.post(
            "/chat/approve/stream",
            json={"conversation_id": session_id},
        )

        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers["content-type"]


# ---------------------------------------------------------------------------
# Tests — SSE event ordering
# ---------------------------------------------------------------------------


class TestStreamEventOrder:

    @pytest.mark.anyio
    async def test_single_step_event_order(
        self, client, mock_executor, conversation_manager
    ):
        """Single tool yields: step_start, step_complete, synthesis, done."""
        session_id = _setup_plan_session(conversation_manager)

        mock_executor._execute_plan_generator = lambda sid: _fake_generator(
            StepStartEvent(step_number=1, tool_name="health_check", tool_input="{}"),
            StepCompleteEvent(
                step_number=1,
                tool_name="health_check",
                success=True,
                result_summary='{"status": "healthy"}',
                step_data={"tool": "health_check", "success": True},
            ),
            SynthesisEvent(text="API is healthy."),
            DoneEvent(success=True, total_steps=1),
        )

        resp = await client.post(
            "/chat/approve/stream",
            json={"conversation_id": session_id},
        )

        events = _parse_sse_events(resp.text)
        event_types = [e["event"] for e in events]
        assert event_types == [
            "step_start",
            "step_complete",
            "synthesis",
            "done",
        ]

    @pytest.mark.anyio
    async def test_multi_step_event_order(
        self, client, mock_executor, conversation_manager
    ):
        """Two tools yield: start/complete pairs, then synthesis, then done."""
        session_id = _setup_plan_session(conversation_manager)

        mock_executor._execute_plan_generator = lambda sid: _fake_generator(
            StepStartEvent(step_number=1, tool_name="search_modules", tool_input="{}"),
            StepCompleteEvent(
                step_number=1,
                tool_name="search_modules",
                success=True,
                result_summary='{"modules": []}',
                step_data={},
            ),
            StepStartEvent(step_number=2, tool_name="run_production", tool_input="{}"),
            StepCompleteEvent(
                step_number=2,
                tool_name="run_production",
                success=True,
                result_summary='{"run_id": "r1"}',
                step_data={},
            ),
            SynthesisEvent(text="Production complete."),
            DoneEvent(success=True, total_steps=2),
        )

        resp = await client.post(
            "/chat/approve/stream",
            json={"conversation_id": session_id},
        )

        events = _parse_sse_events(resp.text)
        event_types = [e["event"] for e in events]
        assert event_types == [
            "step_start",
            "step_complete",
            "step_start",
            "step_complete",
            "synthesis",
            "done",
        ]


# ---------------------------------------------------------------------------
# Tests — SSE event data is valid JSON with correct fields
# ---------------------------------------------------------------------------


class TestStreamEventData:

    @pytest.mark.anyio
    async def test_step_start_data_fields(
        self, client, mock_executor, conversation_manager
    ):
        """step_start event data has step_number, tool_name, tool_input."""
        session_id = _setup_plan_session(conversation_manager)

        mock_executor._execute_plan_generator = lambda sid: _fake_generator(
            StepStartEvent(
                step_number=1,
                tool_name="run_production",
                tool_input='{"dc_mw": 5.0}',
            ),
            DoneEvent(success=True, total_steps=1),
        )

        resp = await client.post(
            "/chat/approve/stream",
            json={"conversation_id": session_id},
        )

        events = _parse_sse_events(resp.text)
        start_event = events[0]
        assert start_event["event"] == "step_start"
        data = json.loads(start_event["data"])
        assert data["step_number"] == 1
        assert data["tool_name"] == "run_production"
        assert data["tool_input"] == '{"dc_mw": 5.0}'

    @pytest.mark.anyio
    async def test_step_complete_data_excludes_step_data(
        self, client, mock_executor, conversation_manager
    ):
        """step_complete SSE data has summary fields but NOT step_data."""
        session_id = _setup_plan_session(conversation_manager)

        mock_executor._execute_plan_generator = lambda sid: _fake_generator(
            StepCompleteEvent(
                step_number=1,
                tool_name="health_check",
                success=True,
                result_summary='{"status": "healthy"}',
                step_data={"tool": "health_check", "result": {"status": "healthy"}},
            ),
            DoneEvent(success=True, total_steps=1),
        )

        resp = await client.post(
            "/chat/approve/stream",
            json={"conversation_id": session_id},
        )

        events = _parse_sse_events(resp.text)
        complete_event = [e for e in events if e["event"] == "step_complete"][0]
        data = json.loads(complete_event["data"])
        assert data["step_number"] == 1
        assert data["tool_name"] == "health_check"
        assert data["success"] is True
        assert data["result_summary"] == '{"status": "healthy"}'
        assert "step_data" not in data

    @pytest.mark.anyio
    async def test_synthesis_event_data(
        self, client, mock_executor, conversation_manager
    ):
        """synthesis event data has text field."""
        session_id = _setup_plan_session(conversation_manager)

        mock_executor._execute_plan_generator = lambda sid: _fake_generator(
            SynthesisEvent(text="Based on the analysis, CF is 23.8%."),
            DoneEvent(success=True, total_steps=1),
        )

        resp = await client.post(
            "/chat/approve/stream",
            json={"conversation_id": session_id},
        )

        events = _parse_sse_events(resp.text)
        synth = [e for e in events if e["event"] == "synthesis"][0]
        data = json.loads(synth["data"])
        assert data["text"] == "Based on the analysis, CF is 23.8%."

    @pytest.mark.anyio
    async def test_done_event_data(
        self, client, mock_executor, conversation_manager
    ):
        """done event data has success and total_steps."""
        session_id = _setup_plan_session(conversation_manager)

        mock_executor._execute_plan_generator = lambda sid: _fake_generator(
            DoneEvent(success=True, total_steps=3),
        )

        resp = await client.post(
            "/chat/approve/stream",
            json={"conversation_id": session_id},
        )

        events = _parse_sse_events(resp.text)
        done = [e for e in events if e["event"] == "done"][0]
        data = json.loads(done["data"])
        assert data["success"] is True
        assert data["total_steps"] == 3

    @pytest.mark.anyio
    async def test_error_event_data(
        self, client, mock_executor, conversation_manager
    ):
        """error event data has message field."""
        session_id = _setup_plan_session(conversation_manager)

        mock_executor._execute_plan_generator = lambda sid: _fake_generator(
            ErrorEvent(message="OpenAI error: rate limit"),
            DoneEvent(success=False, total_steps=0),
        )

        resp = await client.post(
            "/chat/approve/stream",
            json={"conversation_id": session_id},
        )

        events = _parse_sse_events(resp.text)
        error = [e for e in events if e["event"] == "error"][0]
        data = json.loads(error["data"])
        assert data["message"] == "OpenAI error: rate limit"

    @pytest.mark.anyio
    async def test_step_complete_includes_result_from_step_data(
        self, client, mock_executor, conversation_manager
    ):
        """step_complete SSE data includes result promoted from step_data."""
        session_id = _setup_plan_session(conversation_manager)
        full_result = {
            "run_id": "phoenix_prod_20260401",
            "status": "success",
            "production": {"annual_energy_mwh": 10422.5},
        }

        mock_executor._execute_plan_generator = lambda sid: _fake_generator(
            StepCompleteEvent(
                step_number=1,
                tool_name="run_production",
                success=True,
                result_summary='{"run_id": "phoenix_prod_20260401", "status...',
                step_data={
                    "tool": "run_production",
                    "arguments": {"site": {"name": "Phoenix"}},
                    "result": full_result,
                    "success": True,
                    "error": None,
                },
            ),
            DoneEvent(success=True, total_steps=1),
        )

        resp = await client.post(
            "/chat/approve/stream",
            json={"conversation_id": session_id},
        )

        events = _parse_sse_events(resp.text)
        complete_event = [e for e in events if e["event"] == "step_complete"][0]
        data = json.loads(complete_event["data"])

        # result is promoted from step_data for frontend download links
        assert data["result"] == full_result
        assert data["result"]["run_id"] == "phoenix_prod_20260401"
        # step_data wrapper is still excluded
        assert "step_data" not in data
        # arguments are NOT included (only result is promoted)
        assert "arguments" not in data

    @pytest.mark.anyio
    async def test_step_complete_result_null_when_no_result_in_step_data(
        self, client, mock_executor, conversation_manager
    ):
        """step_complete result is null when step_data has no result key."""
        session_id = _setup_plan_session(conversation_manager)

        mock_executor._execute_plan_generator = lambda sid: _fake_generator(
            StepCompleteEvent(
                step_number=1,
                tool_name="health_check",
                success=True,
                result_summary='{"status": "ok"}',
                step_data={"tool": "health_check", "success": True},
            ),
            DoneEvent(success=True, total_steps=1),
        )

        resp = await client.post(
            "/chat/approve/stream",
            json={"conversation_id": session_id},
        )

        events = _parse_sse_events(resp.text)
        complete_event = [e for e in events if e["event"] == "step_complete"][0]
        data = json.loads(complete_event["data"])
        assert data["result"] is None


# ---------------------------------------------------------------------------
# Tests — HTTP error cases (before streaming starts)
# ---------------------------------------------------------------------------


class TestStreamErrorCases:

    @pytest.mark.anyio
    async def test_stream_session_not_found(self, client):
        """Nonexistent session returns 404, not SSE stream."""
        resp = await client.post(
            "/chat/approve/stream",
            json={"conversation_id": "nonexistent"},
        )

        assert resp.status_code == 404
        assert "Session not found" in resp.json()["detail"]

    @pytest.mark.anyio
    async def test_stream_no_pending_plan(self, client, conversation_manager):
        """IDLE session (no plan) returns 400, not SSE stream."""
        conversation_manager.get_or_create_session("idle-session")

        resp = await client.post(
            "/chat/approve/stream",
            json={"conversation_id": "idle-session"},
        )

        assert resp.status_code == 400
        assert "No pending plan" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Tests — Synthesis added to conversation history
# ---------------------------------------------------------------------------


class TestStreamSynthesisHistory:

    @pytest.mark.anyio
    async def test_stream_adds_synthesis_to_conversation(
        self, client, mock_executor, conversation_manager
    ):
        """Streaming endpoint adds synthesis as assistant message to history."""
        session_id = _setup_plan_session(conversation_manager)

        mock_executor._execute_plan_generator = lambda sid: _fake_generator(
            SynthesisEvent(text="Production analysis complete: 10,422 MWh/yr."),
            DoneEvent(success=True, total_steps=1),
        )

        await client.post(
            "/chat/approve/stream",
            json={"conversation_id": session_id},
        )

        session = conversation_manager.get_session(session_id)
        assistant_msgs = [m for m in session.messages if m.role == "assistant"]
        assert any(
            "10,422" in (m.content or "") for m in assistant_msgs
        )

    @pytest.mark.anyio
    async def test_stream_no_synthesis_on_error(
        self, client, mock_executor, conversation_manager
    ):
        """Error-only stream does not add synthesis to conversation."""
        session_id = _setup_plan_session(conversation_manager)

        mock_executor._execute_plan_generator = lambda sid: _fake_generator(
            ErrorEvent(message="Session not found"),
            DoneEvent(success=False, total_steps=0),
        )

        await client.post(
            "/chat/approve/stream",
            json={"conversation_id": session_id},
        )

        session = conversation_manager.get_session(session_id)
        # Only the initial user message — no assistant synthesis added
        assistant_msgs = [m for m in session.messages if m.role == "assistant"]
        assert len(assistant_msgs) == 0


# ---------------------------------------------------------------------------
# Tests — Existing /chat/approve endpoint unchanged
# ---------------------------------------------------------------------------


class TestApproveEndpointUnchanged:

    @pytest.mark.anyio
    async def test_approve_still_returns_json(
        self, client, mock_executor, conversation_manager
    ):
        """POST /chat/approve still works and returns JSON, not SSE."""
        session_id = _setup_plan_session(conversation_manager)

        async def _mock_execute(sid):
            conversation_manager.set_status(sid, SessionStatus.COMPLETE)
            result = ExecutionResult()
            result.synthesis = "Production analysis complete: 10,422 MWh/yr."
            result.success = True
            result.steps = [
                {
                    "tool": "run_production",
                    "arguments": {},
                    "result": {"run_id": "abc123"},
                    "success": True,
                    "error": None,
                }
            ]
            return result

        mock_executor.execute_plan = AsyncMock(side_effect=_mock_execute)

        resp = await client.post(
            "/chat/approve",
            json={"conversation_id": session_id},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["conversation_id"] == session_id
        assert data["success"] is True
        assert "10,422" in data["content"]
        assert len(data["steps"]) == 1
        assert data["status"] == "complete"
        assert "text/event-stream" not in resp.headers.get("content-type", "")
