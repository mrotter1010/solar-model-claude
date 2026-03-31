"""Tests for orchestrator FastAPI route endpoints."""

import pytest
from unittest.mock import AsyncMock

import httpx
from httpx import ASGITransport

from orchestrator.app import app
from orchestrator.conversation.manager import ConversationManager
from orchestrator.conversation.models import SessionStatus
from orchestrator.planning.executor import ExecutionResult
from orchestrator.planning.models import PlannerResponse, ResponseType


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
    """Mocked Executor."""
    return AsyncMock()


@pytest.fixture
async def client(conversation_manager, mock_planner, mock_executor):
    """Async HTTP client with app state injected (no lifespan)."""
    app.state.conversation_manager = conversation_manager
    app.state.planner = mock_planner
    app.state.executor = mock_executor

    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test"
    ) as c:
        yield c


def _plan_response(content: str = "ANALYSIS PLAN\n1. Run production") -> PlannerResponse:
    """Helper to create a PLAN-type PlannerResponse."""
    return PlannerResponse(
        response_type=ResponseType.PLAN,
        content=content,
    )


def _make_execution_result(
    synthesis: str = "Production analysis complete: 10,422 MWh/yr.",
    success: bool = True,
    steps: list[dict] | None = None,
) -> ExecutionResult:
    """Helper to build an ExecutionResult."""
    result = ExecutionResult()
    result.synthesis = synthesis
    result.success = success
    result.steps = steps or [
        {
            "tool": "run_production",
            "arguments": {},
            "result": {"run_id": "abc123"},
            "success": True,
            "error": None,
        }
    ]
    return result


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class TestHealth:

    @pytest.mark.anyio
    async def test_health(self, client):
        """GET /health returns 200 with status ok."""
        resp = await client.get("/health")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["service"] == "orchestrator"


# ---------------------------------------------------------------------------
# POST /chat
# ---------------------------------------------------------------------------


class TestChat:

    @pytest.mark.anyio
    async def test_chat_creates_session(self, client, mock_planner):
        """POST /chat with a new session_id creates session and returns response."""
        resp = await client.post(
            "/chat",
            json={"session_id": "sess-1", "message": "Hello"},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["session_id"] == "sess-1"
        assert data["response_type"] == "response"
        assert data["content"] == "Hello! I can help with solar analysis."
        assert data["status"] == "idle"

    @pytest.mark.anyio
    async def test_chat_returns_plan(self, client, mock_planner):
        """POST /chat that triggers a plan returns response_type=plan."""
        mock_planner.generate_plan = AsyncMock(return_value=_plan_response())

        resp = await client.post(
            "/chat",
            json={"session_id": "sess-2", "message": "Analyze a 5 MW site in Phoenix"},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["response_type"] == "plan"
        assert "ANALYSIS PLAN" in data["content"]
        assert data["status"] == "plan_pending"

    @pytest.mark.anyio
    async def test_chat_plan_then_adjust(
        self, client, mock_planner, conversation_manager
    ):
        """Two /chat calls: plan then adjustment — conversation history preserved."""
        # First call → plan
        mock_planner.generate_plan = AsyncMock(return_value=_plan_response())
        resp1 = await client.post(
            "/chat",
            json={"session_id": "sess-3", "message": "Analyze 5 MW in Phoenix"},
        )
        assert resp1.json()["response_type"] == "plan"

        # Second call → adjustment (returns general response)
        mock_planner.generate_plan = AsyncMock(
            return_value=PlannerResponse(
                response_type=ResponseType.RESPONSE,
                content="Updated: using fixed-tilt instead of tracker.",
            )
        )
        resp2 = await client.post(
            "/chat",
            json={"session_id": "sess-3", "message": "Use fixed-tilt instead"},
        )

        assert resp2.status_code == 200
        data = resp2.json()
        assert data["response_type"] == "response"

        # Verify conversation history has all messages
        session = conversation_manager.get_session("sess-3")
        assert len(session.messages) == 4  # user, assistant, user, assistant

    @pytest.mark.anyio
    async def test_chat_error_handling(self, client, mock_planner):
        """Planner exception returns error response, not a 500 stack trace."""
        mock_planner.generate_plan = AsyncMock(
            side_effect=RuntimeError("OpenAI API timeout")
        )

        resp = await client.post(
            "/chat",
            json={"session_id": "sess-err", "message": "Hello"},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["response_type"] == "error"
        assert "OpenAI API timeout" in data["content"]


# ---------------------------------------------------------------------------
# POST /chat/approve
# ---------------------------------------------------------------------------


class TestApprove:

    @pytest.mark.anyio
    async def test_approve_executes_plan(
        self, client, mock_planner, mock_executor, conversation_manager
    ):
        """POST /chat (plan) then POST /chat/approve returns execution result."""
        # Step 1: get a plan
        mock_planner.generate_plan = AsyncMock(return_value=_plan_response())
        await client.post(
            "/chat",
            json={"session_id": "sess-a", "message": "Analyze 5 MW"},
        )

        # Step 2: approve — mock executor to return result and update status
        async def _mock_execute(session_id):
            conversation_manager.set_status(session_id, SessionStatus.COMPLETE)
            return _make_execution_result()

        mock_executor.execute_plan = AsyncMock(side_effect=_mock_execute)

        resp = await client.post(
            "/chat/approve",
            json={"session_id": "sess-a"},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["session_id"] == "sess-a"
        assert data["success"] is True
        assert "10,422" in data["content"]
        assert len(data["steps"]) == 1
        assert data["status"] == "complete"

    @pytest.mark.anyio
    async def test_approve_no_pending_plan(self, client, mock_planner):
        """Approve on IDLE session (no plan) returns 400."""
        # Create a session via /chat with a non-plan response
        await client.post(
            "/chat",
            json={"session_id": "sess-idle", "message": "Hello"},
        )

        resp = await client.post(
            "/chat/approve",
            json={"session_id": "sess-idle"},
        )

        assert resp.status_code == 400
        assert "No pending plan" in resp.json()["detail"]

    @pytest.mark.anyio
    async def test_approve_session_not_found(self, client):
        """Approve on nonexistent session returns 404."""
        resp = await client.post(
            "/chat/approve",
            json={"session_id": "nonexistent"},
        )

        assert resp.status_code == 404
        assert "Session not found" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# GET /sessions/{session_id}
# ---------------------------------------------------------------------------


class TestGetSession:

    @pytest.mark.anyio
    async def test_get_session(self, client, mock_planner):
        """GET /sessions/{id} returns session metadata after /chat."""
        await client.post(
            "/chat",
            json={"session_id": "sess-info", "message": "Hello"},
        )

        resp = await client.get("/sessions/sess-info")

        assert resp.status_code == 200
        data = resp.json()
        assert data["session_id"] == "sess-info"
        assert data["status"] == "idle"
        assert data["message_count"] == 2  # user + assistant
        assert data["run_ids"] == []
        assert "created_at" in data
        assert "last_activity" in data

    @pytest.mark.anyio
    async def test_get_session_not_found(self, client):
        """GET /sessions/nonexistent returns 404."""
        resp = await client.get("/sessions/nonexistent")

        assert resp.status_code == 404
        assert "Session not found" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Full-flow integration test
# ---------------------------------------------------------------------------


class TestFullChatFlowIntegration:

    @pytest.mark.anyio
    async def test_full_chat_flow_integration(
        self, client, mock_planner, mock_executor, conversation_manager
    ):
        """End-to-end: chat → adjust → approve → session check → follow-up.

        Uses mocked planner/executor but real ConversationManager to exercise
        the session state machine through HTTP.
        """
        session_id = "integration-sess"

        # --- Step 1: POST /chat → returns a plan ---
        mock_planner.generate_plan = AsyncMock(
            return_value=PlannerResponse(
                response_type=ResponseType.PLAN,
                content="ANALYSIS PLAN\n1. Run production for 5 MW Phoenix site",
            )
        )

        resp1 = await client.post(
            "/chat",
            json={"session_id": session_id, "message": "Analyze a 5 MW site in Phoenix"},
        )

        assert resp1.status_code == 200
        data1 = resp1.json()
        assert data1["response_type"] == "plan"
        assert data1["status"] == "plan_pending"
        assert "ANALYSIS PLAN" in data1["content"]

        # --- Step 2: POST /chat → adjustment, returns updated plan ---
        mock_planner.generate_plan = AsyncMock(
            return_value=PlannerResponse(
                response_type=ResponseType.PLAN,
                content="ANALYSIS PLAN\n1. Run production (fixed-tilt, 5 MW Phoenix)",
            )
        )

        resp2 = await client.post(
            "/chat",
            json={"session_id": session_id, "message": "Use fixed-tilt instead of tracker"},
        )

        assert resp2.status_code == 200
        data2 = resp2.json()
        assert data2["response_type"] == "plan"
        assert data2["status"] == "plan_pending"
        assert "fixed-tilt" in data2["content"]

        # --- Step 3: POST /chat/approve → execution with synthesis ---
        async def _mock_execute(sid):
            conversation_manager.set_status(sid, SessionStatus.COMPLETE)
            result = _make_execution_result(
                synthesis="Production analysis complete: 10,422 MWh/yr (CF 23.8%).",
                success=True,
                steps=[
                    {
                        "tool": "run_production",
                        "arguments": {"site": {"lat": 33.45}},
                        "result": {"run_id": "run-int-001", "annual_kwh": 10_422_000},
                        "success": True,
                        "error": None,
                    },
                    {
                        "tool": "get_results",
                        "arguments": {"run_id": "run-int-001"},
                        "result": {"cf": 0.238},
                        "success": True,
                        "error": None,
                    },
                ],
            )
            return result

        mock_executor.execute_plan = AsyncMock(side_effect=_mock_execute)

        # Also cache the run_id so it shows up in session metadata
        conversation_manager.cache_result(
            session_id, "run-int-001", {"annual_kwh": 10_422_000}
        )

        resp3 = await client.post(
            "/chat/approve",
            json={"session_id": session_id},
        )

        assert resp3.status_code == 200
        data3 = resp3.json()
        assert data3["success"] is True
        assert "10,422" in data3["content"]
        assert len(data3["steps"]) == 2
        assert data3["status"] == "complete"

        # --- Step 4: GET /sessions/{id} → verify state ---
        resp4 = await client.get(f"/sessions/{session_id}")

        assert resp4.status_code == 200
        data4 = resp4.json()
        assert data4["session_id"] == session_id
        assert data4["status"] == "complete"
        assert "run-int-001" in data4["run_ids"]
        # Messages: user, assistant(plan), user, assistant(plan), assistant(synthesis)
        assert data4["message_count"] == 5

        # --- Step 5: POST /chat with follow-up → works on same session ---
        mock_planner.generate_plan = AsyncMock(
            return_value=PlannerResponse(
                response_type=ResponseType.RESPONSE,
                content="The capacity factor of 23.8% is typical for Phoenix fixed-tilt.",
            )
        )

        resp5 = await client.post(
            "/chat",
            json={"session_id": session_id, "message": "Is that CF typical for Phoenix?"},
        )

        assert resp5.status_code == 200
        data5 = resp5.json()
        assert data5["response_type"] == "response"
        assert "23.8%" in data5["content"]

        # Session now has 7 messages: 5 prior + user follow-up + assistant response
        session = conversation_manager.get_session(session_id)
        assert len(session.messages) == 7
