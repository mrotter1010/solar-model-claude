"""Tests for conversation session management and message history."""

from datetime import datetime, timedelta

import pytest

from orchestrator.conversation.manager import ConversationManager
from orchestrator.conversation.models import ChatMessage, SessionState, SessionStatus


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def cm():
    """Fresh ConversationManager with a short TTL for expiry tests."""
    return ConversationManager(session_ttl_minutes=60)


# ---------------------------------------------------------------------------
# Session lifecycle
# ---------------------------------------------------------------------------


class TestCreateSession:

    def test_create_session(self, cm):
        """New session has IDLE status, empty messages, and empty caches."""
        session = cm.get_or_create_session("sess-1")

        assert session.session_id == "sess-1"
        assert session.status == SessionStatus.IDLE
        assert session.messages == []
        assert session.cached_results == {}
        assert session.equipment_cache == {}
        assert session.run_ids == []
        assert session.pending_plan is None
        assert session.site_config is None


class TestGetExistingSession:

    def test_get_existing_session(self, cm):
        """Create then get — same object returned."""
        created = cm.get_or_create_session("sess-1")
        fetched = cm.get_session("sess-1")

        assert fetched is created


class TestGetNonexistentSession:

    def test_get_nonexistent_session(self, cm):
        """Returns None for a session that was never created."""
        result = cm.get_session("nonexistent")

        assert result is None


class TestSessionExpiry:

    def test_session_expiry(self, cm):
        """Session past TTL is removed on next get_session call."""
        session = cm.get_or_create_session("sess-expire")
        # Manually backdate last_activity beyond the 60-minute TTL
        session.last_activity = datetime.utcnow() - timedelta(minutes=61)

        result = cm.get_session("sess-expire")

        assert result is None


class TestGetOrCreateExpired:

    def test_get_or_create_expired(self, cm):
        """get_or_create_session creates a fresh session when the old one expired."""
        old_session = cm.get_or_create_session("sess-renew")
        old_session.last_activity = datetime.utcnow() - timedelta(minutes=61)

        new_session = cm.get_or_create_session("sess-renew")

        # Fresh session — empty messages, IDLE status
        assert new_session.status == SessionStatus.IDLE
        assert new_session.messages == []
        assert new_session is not old_session


# ---------------------------------------------------------------------------
# Message management
# ---------------------------------------------------------------------------


class TestAddMessage:

    def test_add_message(self, cm):
        """Add a message, verify it's in session.messages."""
        cm.get_or_create_session("sess-msg")
        msg = ChatMessage(role="user", content="Hello")

        cm.add_message("sess-msg", msg)

        session = cm.get_session("sess-msg")
        assert len(session.messages) == 1
        assert session.messages[0].role == "user"
        assert session.messages[0].content == "Hello"


class TestAddMessageUpdatesActivity:

    def test_add_message_updates_activity(self, cm):
        """Adding a message updates last_activity."""
        session = cm.get_or_create_session("sess-activity")
        before = session.last_activity

        cm.add_message(
            "sess-activity", ChatMessage(role="user", content="Ping")
        )

        session = cm.get_session("sess-activity")
        assert session.last_activity >= before


# ---------------------------------------------------------------------------
# Plan management
# ---------------------------------------------------------------------------


class TestSetPendingPlan:

    def test_set_pending_plan(self, cm):
        """Setting plan changes status to PLAN_PENDING and stores text."""
        cm.get_or_create_session("sess-plan")

        cm.set_pending_plan("sess-plan", "1. Run production\n2. Get results")

        session = cm.get_session("sess-plan")
        assert session.status == SessionStatus.PLAN_PENDING
        assert session.pending_plan == "1. Run production\n2. Get results"


class TestClearPendingPlan:

    def test_clear_pending_plan(self, cm):
        """Clearing plan resets status to IDLE and removes plan text."""
        cm.get_or_create_session("sess-clear")
        cm.set_pending_plan("sess-clear", "Some plan")

        cm.clear_pending_plan("sess-clear")

        session = cm.get_session("sess-clear")
        assert session.status == SessionStatus.IDLE
        assert session.pending_plan is None


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


class TestCacheResult:

    def test_cache_result(self, cm):
        """Cache a result, verify it's in cached_results and run_id tracked."""
        cm.get_or_create_session("sess-cache")
        result_data = {"annual_kwh": 10_000_000, "cf": 0.238}

        cm.cache_result("sess-cache", "run-001", result_data)

        session = cm.get_session("sess-cache")
        assert "run-001" in session.run_ids
        assert session.cached_results["run-001"] == result_data


class TestUpdateSiteConfig:

    def test_update_site_config(self, cm):
        """Store a config dict, verify retrieval."""
        cm.get_or_create_session("sess-config")
        config = {"lat": 33.45, "lon": -112.07, "dc_mw": 5.0}

        cm.update_site_config("sess-config", config)

        session = cm.get_session("sess-config")
        assert session.site_config == config


class TestCacheEquipment:

    def test_cache_equipment(self, cm):
        """Cache equipment search, verify retrieval."""
        cm.get_or_create_session("sess-equip")
        results = [{"name": "CS6W-550MS"}, {"name": "CS6W-545MS"}]

        cm.cache_equipment("sess-equip", "CS6W", results)

        session = cm.get_session("sess-equip")
        assert session.equipment_cache["CS6W"] == results


# ---------------------------------------------------------------------------
# OpenAI message formatting
# ---------------------------------------------------------------------------


class TestGetOpenaiMessages:

    def test_get_openai_messages(self, cm):
        """Various roles format correctly for OpenAI API."""
        cm.get_or_create_session("sess-fmt")
        cm.add_message("sess-fmt", ChatMessage(role="user", content="Hello"))
        cm.add_message(
            "sess-fmt",
            ChatMessage(
                role="assistant",
                content=None,
                tool_calls=[{"id": "tc-1", "type": "function", "function": {"name": "health_check", "arguments": "{}"}}],
            ),
        )
        cm.add_message(
            "sess-fmt",
            ChatMessage(
                role="tool",
                content='{"status": "ok"}',
                tool_call_id="tc-1",
                name="health_check",
            ),
        )

        messages = cm.get_openai_messages("sess-fmt")

        assert len(messages) == 3
        # User message
        assert messages[0] == {"role": "user", "content": "Hello"}
        # Assistant with tool_calls (no content since it's None)
        assert messages[1]["role"] == "assistant"
        assert "tool_calls" in messages[1]
        assert "content" not in messages[1]
        # Tool message
        assert messages[2]["role"] == "tool"
        assert messages[2]["tool_call_id"] == "tc-1"
        assert messages[2]["name"] == "health_check"

    def test_get_openai_messages_omits_none_fields(self, cm):
        """None-valued optional fields are excluded from output dicts."""
        cm.get_or_create_session("sess-none")
        cm.add_message(
            "sess-none",
            ChatMessage(role="user", content="Hello"),
        )

        messages = cm.get_openai_messages("sess-none")

        msg = messages[0]
        assert msg == {"role": "user", "content": "Hello"}
        assert "tool_calls" not in msg
        assert "tool_call_id" not in msg
        assert "name" not in msg


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


class TestCleanup:

    def test_cleanup_removes_expired_only(self, cm):
        """Cleanup removes expired sessions and keeps active ones."""
        active = cm.get_or_create_session("active-sess")
        expired = cm.get_or_create_session("expired-sess")

        # Backdate the expired session
        expired.last_activity = datetime.utcnow() - timedelta(minutes=61)

        # Trigger cleanup via get_or_create_session
        cm.get_or_create_session("trigger-cleanup")

        assert cm.get_session("active-sess") is not None
        assert cm.get_session("expired-sess") is None


# ---------------------------------------------------------------------------
# Status transitions
# ---------------------------------------------------------------------------


class TestSetStatus:

    def test_set_status(self, cm):
        """Verify status transitions work."""
        cm.get_or_create_session("sess-status")

        cm.set_status("sess-status", SessionStatus.EXECUTING)
        session = cm.get_session("sess-status")
        assert session.status == SessionStatus.EXECUTING

        cm.set_status("sess-status", SessionStatus.COMPLETE)
        session = cm.get_session("sess-status")
        assert session.status == SessionStatus.COMPLETE
