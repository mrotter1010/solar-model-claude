"""FastAPI application for the solar orchestrator service."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from orchestrator.config import OrchestratorConfig
from orchestrator.conversation.manager import ConversationManager
from orchestrator.conversation.models import ChatMessage, SessionStatus
from orchestrator.planning.executor import Executor
from orchestrator.planning.models import ResponseType
from orchestrator.planning.planner import Planner
from orchestrator.tools.api_client import AnalysisAPIClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class ChatRequest(BaseModel):
    """Incoming chat message."""

    session_id: str
    message: str


class ChatResponse(BaseModel):
    """Response to a chat message."""

    session_id: str
    response_type: str
    content: str
    status: str


class ApproveRequest(BaseModel):
    """Request to approve and execute a pending plan."""

    session_id: str


class ApproveResponse(BaseModel):
    """Response after plan execution."""

    session_id: str
    content: str
    success: bool
    steps: list[dict]
    status: str


class SessionResponse(BaseModel):
    """Session metadata."""

    session_id: str
    status: str
    message_count: int
    run_ids: list[str]
    created_at: str
    last_activity: str


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create service dependencies on startup, tear down on shutdown."""
    config = OrchestratorConfig()
    app.state.conversation_manager = ConversationManager(
        config.session_ttl_minutes
    )
    app.state.api_client = AnalysisAPIClient(
        config.analysis_api_url, config.analysis_api_key, config.request_timeout
    )
    app.state.planner = Planner(config)
    app.state.executor = Executor(
        app.state.planner,
        app.state.api_client,
        app.state.conversation_manager,
        max_steps=config.max_plan_steps,
    )
    yield
    await app.state.api_client.aclose()


app = FastAPI(title="Solar Orchestrator", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health")
async def health() -> dict:
    """Health check endpoint."""
    return {"status": "ok", "service": "orchestrator"}


@app.post("/chat", response_model=ChatResponse)
async def chat(body: ChatRequest, request: Request) -> ChatResponse:
    """Send a user message and receive a response (possibly a plan)."""
    cm: ConversationManager = request.app.state.conversation_manager
    planner: Planner = request.app.state.planner

    try:
        cm.get_or_create_session(body.session_id)
        cm.add_message(
            body.session_id,
            ChatMessage(role="user", content=body.message),
        )

        messages = cm.get_openai_messages(body.session_id)
        response = await planner.generate_plan(messages)

        if response.response_type == ResponseType.PLAN:
            cm.set_pending_plan(body.session_id, response.content)

        cm.add_message(
            body.session_id,
            ChatMessage(role="assistant", content=response.content),
        )

        session = cm.get_session(body.session_id)
        return ChatResponse(
            session_id=body.session_id,
            response_type=response.response_type.value,
            content=response.content,
            status=session.status.value,
        )

    except Exception as exc:
        logger.error("Error in /chat: %s", exc)
        session = cm.get_session(body.session_id)
        status = session.status.value if session else "idle"
        return ChatResponse(
            session_id=body.session_id,
            response_type="error",
            content=f"An error occurred: {exc}",
            status=status,
        )


@app.post("/chat/approve", response_model=ApproveResponse)
async def approve(body: ApproveRequest, request: Request) -> ApproveResponse:
    """Approve and execute a pending analysis plan."""
    cm: ConversationManager = request.app.state.conversation_manager
    executor: Executor = request.app.state.executor

    session = cm.get_session(body.session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")

    if session.status != SessionStatus.PLAN_PENDING:
        raise HTTPException(
            status_code=400, detail="No pending plan to approve"
        )

    try:
        result = await executor.execute_plan(body.session_id)

        if result.synthesis:
            cm.add_message(
                body.session_id,
                ChatMessage(role="assistant", content=result.synthesis),
            )

        session = cm.get_session(body.session_id)
        return ApproveResponse(
            session_id=body.session_id,
            content=result.synthesis or "",
            success=result.success,
            steps=result.steps,
            status=session.status.value,
        )

    except Exception as exc:
        logger.error("Error in /chat/approve: %s", exc)
        session = cm.get_session(body.session_id)
        status = session.status.value if session else "idle"
        return ApproveResponse(
            session_id=body.session_id,
            content=f"An error occurred during execution: {exc}",
            success=False,
            steps=[],
            status=status,
        )


@app.get("/sessions/{session_id}", response_model=SessionResponse)
async def get_session(session_id: str, request: Request) -> SessionResponse:
    """Retrieve session metadata."""
    cm: ConversationManager = request.app.state.conversation_manager
    session = cm.get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")

    return SessionResponse(
        session_id=session.session_id,
        status=session.status.value,
        message_count=len(session.messages),
        run_ids=session.run_ids,
        created_at=session.created_at.isoformat(),
        last_activity=session.last_activity.isoformat(),
    )
