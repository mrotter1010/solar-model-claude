"""Session state, message history, and context tracking."""

from datetime import datetime, timedelta

from orchestrator.conversation.models import ChatMessage, SessionState, SessionStatus


class ConversationManager:
    """Manages conversation sessions with lazy TTL expiration.

    Args:
        session_ttl_minutes: Minutes of inactivity before a session expires.
    """

    def __init__(self, session_ttl_minutes: int = 60) -> None:
        self._sessions: dict[str, SessionState] = {}
        self._ttl_minutes = session_ttl_minutes

    def get_or_create_session(self, session_id: str) -> SessionState:
        """Get existing session or create a new one.

        Triggers lazy cleanup of expired sessions before lookup.
        """
        self._cleanup_expired()
        if session_id not in self._sessions:
            self._sessions[session_id] = SessionState(session_id=session_id)
        self._touch(session_id)
        return self._sessions[session_id]

    def get_session(self, session_id: str) -> SessionState | None:
        """Get existing session, or None if not found or expired.

        Triggers lazy cleanup of expired sessions before lookup.
        """
        self._cleanup_expired()
        session = self._sessions.get(session_id)
        if session is not None:
            self._touch(session_id)
        return session

    def add_message(self, session_id: str, message: ChatMessage) -> None:
        """Append a message to the session history."""
        session = self._sessions[session_id]
        session.messages.append(message)
        self._touch(session_id)

    def set_pending_plan(self, session_id: str, plan_text: str) -> None:
        """Store a pending plan and set status to PLAN_PENDING."""
        session = self._sessions[session_id]
        session.pending_plan = plan_text
        session.status = SessionStatus.PLAN_PENDING
        self._touch(session_id)

    def clear_pending_plan(self, session_id: str) -> None:
        """Clear the pending plan and set status back to IDLE."""
        session = self._sessions[session_id]
        session.pending_plan = None
        session.status = SessionStatus.IDLE
        self._touch(session_id)

    def set_status(self, session_id: str, status: SessionStatus) -> None:
        """Update session status."""
        self._sessions[session_id].status = status
        self._touch(session_id)

    def cache_result(
        self, session_id: str, run_id: str, result: dict
    ) -> None:
        """Cache a full API response and add run_id to the session."""
        session = self._sessions[session_id]
        session.cached_results[run_id] = result
        if run_id not in session.run_ids:
            session.run_ids.append(run_id)
        self._touch(session_id)

    def update_site_config(self, session_id: str, config: dict) -> None:
        """Store the site/system config from the most recent execution."""
        self._sessions[session_id].site_config = config
        self._touch(session_id)

    def cache_equipment(
        self, session_id: str, search_term: str, results: list
    ) -> None:
        """Cache equipment search results to avoid re-querying."""
        self._sessions[session_id].equipment_cache[search_term] = results
        self._touch(session_id)

    def rekey_session(self, old_key: str, new_key: str) -> None:
        """Move a session from one key to another.

        Used to re-key a temporary session to its permanent conversation_id
        after the DB row is created.
        """
        session = self._sessions.pop(old_key)
        session.session_id = new_key
        self._sessions[new_key] = session

    def get_openai_messages(self, session_id: str) -> list[dict]:
        """Return message history formatted for the OpenAI API.

        Converts ChatMessage objects to dicts, omitting None-valued fields.
        """
        session = self._sessions[session_id]
        out: list[dict] = []
        for msg in session.messages:
            entry: dict = {"role": msg.role}
            if msg.content is not None:
                entry["content"] = msg.content
            if msg.tool_calls is not None:
                entry["tool_calls"] = msg.tool_calls
            if msg.tool_call_id is not None:
                entry["tool_call_id"] = msg.tool_call_id
            if msg.name is not None:
                entry["name"] = msg.name
            out.append(entry)
        return out

    def _cleanup_expired(self) -> None:
        """Remove sessions inactive longer than TTL."""
        cutoff = datetime.utcnow() - timedelta(minutes=self._ttl_minutes)
        expired = [
            sid
            for sid, session in self._sessions.items()
            if session.last_activity < cutoff
        ]
        for sid in expired:
            del self._sessions[sid]

    def _touch(self, session_id: str) -> None:
        """Update last_activity timestamp."""
        self._sessions[session_id].last_activity = datetime.utcnow()
