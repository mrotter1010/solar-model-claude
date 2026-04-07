"""FastAPI application for the solar orchestrator service."""

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette.responses import StreamingResponse

from orchestrator.config import OrchestratorConfig
from orchestrator.conversation.manager import ConversationManager
from orchestrator.conversation.models import ChatMessage, SessionStatus
from orchestrator.database import ConversationDB
from orchestrator.planning.events import (
    DoneEvent,
    ErrorEvent,
    StepCompleteEvent,
    StepStartEvent,
    SynthesisEvent,
)
from orchestrator.planning.executor import Executor
from orchestrator.planning.models import ResponseType
from orchestrator.planning.planner import Planner
from orchestrator.middleware import InviteCodeMiddleware, UserIdentityMiddleware
from orchestrator.tools.api_client import AnalysisAPIClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class ChatRequest(BaseModel):
    """Incoming chat message."""

    conversation_id: str | None = None
    message: str
    file_context: str | None = None


class ChatResponse(BaseModel):
    """Response to a chat message."""

    conversation_id: str | None = None
    response_type: str
    content: str
    status: str


class ApproveRequest(BaseModel):
    """Request to approve and execute a pending plan."""

    conversation_id: str


class ApproveResponse(BaseModel):
    """Response after plan execution."""

    conversation_id: str
    content: str
    success: bool
    steps: list[dict]
    status: str


class SessionResponse(BaseModel):
    """Session metadata."""

    conversation_id: str
    status: str
    message_count: int
    run_ids: list[str]
    created_at: str
    last_activity: str


class TitleRequest(BaseModel):
    """Request to update a conversation title."""

    title: str


def _require_user_id(request: Request) -> str:
    """Extract user_id from request state or raise 400."""
    user_id = getattr(request.state, "user_id", None)
    if not user_id:
        raise HTTPException(
            status_code=400, detail="X-User-Id header is required"
        )
    return user_id


async def _generate_and_save_title(
    user_message: str,
    assistant_response: str,
    conversation_id: str,
    anonymous_user_id: str,
    planner: Planner,
    conversation_db: ConversationDB,
) -> None:
    """Generate a conversation title in the background after the first exchange.

    Makes a lightweight GPT-5 call to summarize the conversation into a short
    title (5-10 words), then saves it to the database. Errors are logged but
    never raised — title generation must not affect the chat flow.
    """
    try:
        response = await planner._client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Generate a short descriptive title (5-10 words) for this "
                        "conversation. Return ONLY the title, no quotes, no "
                        "punctuation at the end, no explanation."
                    ),
                },
                {"role": "user", "content": user_message},
                {"role": "assistant", "content": assistant_response},
            ],
            max_tokens=30,
        )
        content = response.choices[0].message.content or ""
        title = content.strip().strip('"\'')
        if not title:
            return
        await conversation_db.update_conversation_title(
            conversation_id, anonymous_user_id, title
        )
        logger.info(
            "Generated title for conversation %s: %s", conversation_id, title
        )
    except Exception as e:
        logger.exception(
            "Failed to generate title for conversation %s", conversation_id
        )


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
    app.state.conversation_db = ConversationDB(config.database_url)
    yield
    await app.state.conversation_db.close()
    await app.state.api_client.aclose()


app = FastAPI(title="Solar Orchestrator", version="0.1.0", lifespan=lifespan)

_cors_origins = [
    origin.strip()
    for origin in os.environ.get(
        "CORS_ORIGINS", "http://localhost:5173"
    ).split(",")
    if origin.strip()
]

app.add_middleware(UserIdentityMiddleware)
app.add_middleware(InviteCodeMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
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
    conversation_db: ConversationDB = request.app.state.conversation_db
    user_id = _require_user_id(request)

    is_new_conversation = body.conversation_id is None

    # Determine the in-memory session key
    if is_new_conversation:
        session_key = f"temp-{uuid.uuid4()}"
    else:
        session_key = body.conversation_id

    conversation_id = body.conversation_id

    try:
        session = cm.get_or_create_session(session_key)

        # Hydrate session from DB when resuming a conversation with
        # no in-memory history (session expired or server restarted).
        if not is_new_conversation and not session.messages:
            conv = await conversation_db.get_conversation(
                conversation_id, user_id
            )
            if conv and conv.get("messages"):
                for msg in conv["messages"]:
                    cm.add_message(
                        session_key,
                        ChatMessage(
                            role=msg["role"], content=msg["content"]
                        ),
                    )
                logger.info(
                    "Hydrated session %s with %d messages from DB",
                    conversation_id,
                    len(conv["messages"]),
                )

        if body.file_context:
            content = (
                f"{body.file_context}\n\n{body.message}"
                if body.message
                else body.file_context
            )
        else:
            content = body.message

        cm.add_message(
            session_key,
            ChatMessage(role="user", content=content),
        )

        messages = cm.get_openai_messages(session_key)
        response = await planner.generate_plan(messages)

        if response.response_type == ResponseType.PLAN:
            cm.set_pending_plan(session_key, response.content)

        cm.add_message(
            session_key,
            ChatMessage(role="assistant", content=response.content),
        )

        # ----- DB persistence -----
        response_metadata = {"responseType": response.response_type.value}

        if is_new_conversation:
            conv = await conversation_db.create_conversation(user_id)
            conversation_id = str(conv["id"])

            await conversation_db.add_message(
                conv["id"], "user", content, None, 1
            )
            await conversation_db.add_message(
                conv["id"], "assistant", response.content, response_metadata, 2
            )

            cm.rekey_session(session_key, conversation_id)
            session_key = conversation_id
        else:
            user_seq = await conversation_db.get_next_sequence(conversation_id)
            await conversation_db.add_message(
                conversation_id, "user", content, None, user_seq
            )
            assistant_seq = await conversation_db.get_next_sequence(conversation_id)
            await conversation_db.add_message(
                conversation_id,
                "assistant",
                response.content,
                response_metadata,
                assistant_seq,
            )

        # Fire off async title generation for new conversations
        if is_new_conversation:
            asyncio.create_task(
                _generate_and_save_title(
                    user_message=content,
                    assistant_response=response.content,
                    conversation_id=conversation_id,
                    anonymous_user_id=user_id,
                    planner=planner,
                    conversation_db=conversation_db,
                )
            )

        session = cm.get_session(session_key)
        return ChatResponse(
            conversation_id=conversation_id,
            response_type=response.response_type.value,
            content=response.content,
            status=session.status.value,
        )

    except Exception as exc:
        logger.error("Error in /chat: %s", exc, exc_info=True)
        session = cm.get_session(session_key)
        status = session.status.value if session else "idle"
        return ChatResponse(
            conversation_id=conversation_id,
            response_type="error",
            content=f"An error occurred: {exc}",
            status=status,
        )


@app.post("/chat/approve", response_model=ApproveResponse)
async def approve(body: ApproveRequest, request: Request) -> ApproveResponse:
    """Approve and execute a pending analysis plan."""
    cm: ConversationManager = request.app.state.conversation_manager
    executor: Executor = request.app.state.executor
    conversation_db: ConversationDB = request.app.state.conversation_db

    session = cm.get_session(body.conversation_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")

    if session.status != SessionStatus.PLAN_PENDING:
        raise HTTPException(
            status_code=400, detail="No pending plan to approve"
        )

    try:
        result = await executor.execute_plan(body.conversation_id)

        if result.synthesis:
            cm.add_message(
                body.conversation_id,
                ChatMessage(role="assistant", content=result.synthesis),
            )

            # Persist execution result to DB
            seq = await conversation_db.get_next_sequence(body.conversation_id)
            await conversation_db.add_message(
                body.conversation_id,
                "assistant",
                result.synthesis,
                {"responseType": "response", "steps": result.steps},
                seq,
            )

        session = cm.get_session(body.conversation_id)
        return ApproveResponse(
            conversation_id=body.conversation_id,
            content=result.synthesis or "",
            success=result.success,
            steps=result.steps,
            status=session.status.value,
        )

    except Exception as exc:
        logger.error("Error in /chat/approve: %s", exc, exc_info=True)
        session = cm.get_session(body.conversation_id)
        status = session.status.value if session else "idle"
        return ApproveResponse(
            conversation_id=body.conversation_id,
            content=f"An error occurred during execution: {exc}",
            success=False,
            steps=[],
            status=status,
        )


# ---------------------------------------------------------------------------
# SSE event serialization
# ---------------------------------------------------------------------------

_EVENT_TYPE_NAMES: dict[type, str] = {
    StepStartEvent: "step_start",
    StepCompleteEvent: "step_complete",
    SynthesisEvent: "synthesis",
    ErrorEvent: "error",
    DoneEvent: "done",
}


def _serialize_event(event: object) -> tuple[str, str]:
    """Convert an ExecutionEvent to (event_name, json_data) for SSE.

    Args:
        event: An ExecutionEvent dataclass instance.

    Returns:
        Tuple of (SSE event name, JSON-serialized payload). The internal
        ``step_data`` wrapper is removed but its ``result`` field is
        promoted to the top level so the frontend can extract run_id
        and other fields for download links.
    """
    event_name = _EVENT_TYPE_NAMES[type(event)]
    payload = asdict(event)
    step_data = payload.pop("step_data", None)
    if step_data is not None:
        payload["result"] = step_data.get("result")
    return event_name, json.dumps(payload)


@app.post("/chat/approve/stream")
async def approve_stream(
    body: ApproveRequest, request: Request
) -> StreamingResponse:
    """Approve and execute a pending plan, streaming progress via SSE.

    Returns a text/event-stream response with events:
    step_start, step_complete, synthesis, error, done.
    """
    cm: ConversationManager = request.app.state.conversation_manager
    executor: Executor = request.app.state.executor
    conversation_db: ConversationDB = request.app.state.conversation_db

    session = cm.get_session(body.conversation_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")

    if session.status != SessionStatus.PLAN_PENDING:
        raise HTTPException(
            status_code=400, detail="No pending plan to approve"
        )

    async def _event_generator():
        synthesis_text = None
        collected_steps: list[dict] = []

        async for event in executor._execute_plan_generator(
            body.conversation_id
        ):
            if isinstance(event, StepCompleteEvent):
                collected_steps.append(event.step_data)
            elif isinstance(event, SynthesisEvent):
                synthesis_text = event.text

            # Inject conversation_id into the done event payload
            if isinstance(event, DoneEvent):
                payload = asdict(event)
                payload["conversation_id"] = body.conversation_id
                event_name = _EVENT_TYPE_NAMES[type(event)]
                data = json.dumps(payload)
            else:
                event_name, data = _serialize_event(event)

            yield f"event: {event_name}\ndata: {data}\n\n"

        # Persist synthesis + steps to in-memory history and DB
        if synthesis_text:
            cm.add_message(
                body.conversation_id,
                ChatMessage(role="assistant", content=synthesis_text),
            )

            seq = await conversation_db.get_next_sequence(body.conversation_id)
            await conversation_db.add_message(
                body.conversation_id,
                "assistant",
                synthesis_text,
                {"responseType": "response", "steps": collected_steps},
                seq,
            )

    return StreamingResponse(
        _event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/sessions/{conversation_id}", response_model=SessionResponse)
async def get_session(
    conversation_id: str, request: Request
) -> SessionResponse:
    """Retrieve session metadata."""
    cm: ConversationManager = request.app.state.conversation_manager
    session = cm.get_session(conversation_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")

    return SessionResponse(
        conversation_id=session.session_id,
        status=session.status.value,
        message_count=len(session.messages),
        run_ids=session.run_ids,
        created_at=session.created_at.isoformat(),
        last_activity=session.last_activity.isoformat(),
    )


# ---------------------------------------------------------------------------
# Conversation persistence endpoints
# ---------------------------------------------------------------------------


@app.get("/conversations")
async def list_conversations(request: Request) -> list[dict]:
    """List conversations for the authenticated anonymous user."""
    user_id = _require_user_id(request)
    db: ConversationDB = request.app.state.conversation_db
    rows = await db.get_conversations(user_id)
    return [
        {
            "id": str(r["id"]),
            "title": r["title"],
            "updated_at": r["updated_at"].isoformat(),
        }
        for r in rows
    ]


@app.get("/conversations/{conversation_id}")
async def get_conversation(conversation_id: str, request: Request) -> dict:
    """Get a conversation with all messages."""
    user_id = _require_user_id(request)
    db: ConversationDB = request.app.state.conversation_db
    conv = await db.get_conversation(conversation_id, user_id)
    if conv is None:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return {
        "id": str(conv["id"]),
        "title": conv["title"],
        "created_at": conv["created_at"].isoformat(),
        "updated_at": conv["updated_at"].isoformat(),
        "messages": [
            {
                "id": str(m["id"]),
                "role": m["role"],
                "content": m["content"],
                "metadata": m["metadata"],
                "sequence": m["sequence"],
                "created_at": m["created_at"].isoformat(),
            }
            for m in conv["messages"]
        ],
    }


@app.delete("/conversations/{conversation_id}", status_code=204)
async def delete_conversation(
    conversation_id: str, request: Request
) -> None:
    """Delete a conversation and all its messages."""
    user_id = _require_user_id(request)
    db: ConversationDB = request.app.state.conversation_db
    deleted = await db.delete_conversation(conversation_id, user_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Conversation not found")


@app.post("/conversations/{conversation_id}/title")
async def update_conversation_title(
    conversation_id: str, body: TitleRequest, request: Request
) -> dict:
    """Update a conversation's title."""
    user_id = _require_user_id(request)
    db: ConversationDB = request.app.state.conversation_db
    updated = await db.update_conversation_title(
        conversation_id, user_id, body.title
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return {"status": "ok"}
