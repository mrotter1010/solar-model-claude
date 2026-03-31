"""Pydantic models for conversation messages and session state."""

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class SessionStatus(str, Enum):
    """Lifecycle states for an orchestrator conversation session."""

    IDLE = "idle"
    PLAN_PENDING = "plan_pending"
    EXECUTING = "executing"
    COMPLETE = "complete"


class ChatMessage(BaseModel):
    """A single message in the conversation history.

    Covers all OpenAI message roles: user, assistant, system, and tool.
    """

    role: str
    content: str | None = None
    tool_calls: list[dict] | None = None
    tool_call_id: str | None = None
    name: str | None = None


class SessionState(BaseModel):
    """Full state for one orchestrator conversation session."""

    session_id: str
    status: SessionStatus = SessionStatus.IDLE
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_activity: datetime = Field(default_factory=datetime.utcnow)
    messages: list[ChatMessage] = Field(default_factory=list)
    pending_plan: str | None = None
    site_config: dict | None = None
    run_ids: list[str] = Field(default_factory=list)
    cached_results: dict[str, dict] = Field(default_factory=dict)
    equipment_cache: dict[str, list] = Field(default_factory=dict)
