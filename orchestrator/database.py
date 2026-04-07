"""Async database access layer for conversation persistence."""

import json
import logging
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.ext.asyncio import async_sessionmaker

logger = logging.getLogger(__name__)


def _to_async_url(url: str) -> str:
    """Convert a postgresql:// URL to postgresql+asyncpg://.

    Accepts both ``postgresql://`` and ``postgresql+asyncpg://`` and
    always returns the asyncpg variant.
    """
    if url.startswith("postgresql+asyncpg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    raise ValueError(f"Unsupported database URL scheme: {url}")


class ConversationDB:
    """Async database access for conversations and messages.

    Uses SQLAlchemy Core (raw SQL via ``text()``) rather than ORM models
    to keep the orchestrator's DB layer lightweight.

    Args:
        database_url: PostgreSQL connection string (``postgresql://`` or
            ``postgresql+asyncpg://``).
    """

    def __init__(self, database_url: str) -> None:
        self._engine: AsyncEngine = create_async_engine(
            _to_async_url(database_url),
            pool_size=5,
            max_overflow=5,
        )
        self._session_factory = async_sessionmaker(
            self._engine, expire_on_commit=False
        )

    async def close(self) -> None:
        """Dispose of the connection pool."""
        await self._engine.dispose()

    # ------------------------------------------------------------------
    # Conversations
    # ------------------------------------------------------------------

    async def create_conversation(self, anonymous_user_id: UUID) -> dict:
        """Create a new conversation.

        Args:
            anonymous_user_id: The anonymous user who owns this conversation.

        Returns:
            Dict with ``id`` (UUID) and ``created_at`` (datetime).
        """
        async with self._session_factory() as session:
            result = await session.execute(
                text("""
                    INSERT INTO conversations (anonymous_user_id)
                    VALUES (:uid)
                    RETURNING id, created_at
                """),
                {"uid": anonymous_user_id},
            )
            row = result.mappings().one()
            await session.commit()
            return dict(row)

    async def get_conversations(self, anonymous_user_id: UUID) -> list[dict]:
        """List conversations for a user, newest first.

        Args:
            anonymous_user_id: The anonymous user whose conversations to list.

        Returns:
            List of dicts with ``id``, ``title``, ``updated_at``.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                text("""
                    SELECT id, title, updated_at
                    FROM conversations
                    WHERE anonymous_user_id = :uid
                    ORDER BY updated_at DESC
                """),
                {"uid": anonymous_user_id},
            )
            return [dict(row) for row in result.mappings().all()]

    async def get_conversation(
        self, conversation_id: UUID, anonymous_user_id: UUID
    ) -> dict | None:
        """Get a conversation with all its messages.

        Args:
            conversation_id: The conversation to retrieve.
            anonymous_user_id: Must match the conversation's owner.

        Returns:
            Dict with conversation fields and ``messages`` list, ordered
            by sequence. Returns ``None`` if not found or wrong owner.
        """
        async with self._session_factory() as session:
            # Fetch conversation
            conv_result = await session.execute(
                text("""
                    SELECT id, title, created_at, updated_at
                    FROM conversations
                    WHERE id = :cid AND anonymous_user_id = :uid
                """),
                {"cid": conversation_id, "uid": anonymous_user_id},
            )
            conv_row = conv_result.mappings().one_or_none()
            if conv_row is None:
                return None

            # Fetch messages
            msg_result = await session.execute(
                text("""
                    SELECT id, role, content, metadata, sequence, created_at
                    FROM messages
                    WHERE conversation_id = :cid
                    ORDER BY sequence
                """),
                {"cid": conversation_id},
            )
            messages = [dict(row) for row in msg_result.mappings().all()]

            return {**dict(conv_row), "messages": messages}

    async def delete_conversation(
        self, conversation_id: UUID, anonymous_user_id: UUID
    ) -> bool:
        """Delete a conversation and its messages (CASCADE).

        Args:
            conversation_id: The conversation to delete.
            anonymous_user_id: Must match the conversation's owner.

        Returns:
            True if deleted, False if not found or wrong owner.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                text("""
                    DELETE FROM conversations
                    WHERE id = :cid AND anonymous_user_id = :uid
                """),
                {"cid": conversation_id, "uid": anonymous_user_id},
            )
            await session.commit()
            return result.rowcount > 0

    async def update_conversation_title(
        self, conversation_id: UUID, anonymous_user_id: UUID, title: str
    ) -> bool:
        """Update a conversation's title.

        Args:
            conversation_id: The conversation to update.
            anonymous_user_id: Must match the conversation's owner.
            title: New title string.

        Returns:
            True if updated, False if not found or wrong owner.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                text("""
                    UPDATE conversations
                    SET title = :title, updated_at = NOW()
                    WHERE id = :cid AND anonymous_user_id = :uid
                """),
                {"cid": conversation_id, "uid": anonymous_user_id, "title": title},
            )
            await session.commit()
            return result.rowcount > 0

    # ------------------------------------------------------------------
    # Messages
    # ------------------------------------------------------------------

    async def add_message(
        self,
        conversation_id: UUID,
        role: str,
        content: str,
        metadata: dict | None,
        sequence: int,
    ) -> dict:
        """Append a message to a conversation.

        Also updates the conversation's ``updated_at`` timestamp.

        Args:
            conversation_id: The conversation to add the message to.
            role: Message role (``user``, ``assistant``).
            content: Message text content.
            metadata: Optional JSONB metadata (responseType, steps, etc.).
            sequence: Deterministic ordering integer.

        Returns:
            Dict with ``id`` (UUID) and ``created_at`` (datetime).
        """
        async with self._session_factory() as session:
            result = await session.execute(
                text("""
                    INSERT INTO messages (conversation_id, role, content, metadata, sequence)
                    VALUES (:cid, :role, :content, :metadata, :seq)
                    RETURNING id, created_at
                """),
                {
                    "cid": conversation_id,
                    "role": role,
                    "content": content,
                    "metadata": json.dumps(metadata) if metadata is not None else None,
                    "seq": sequence,
                },
            )
            row = result.mappings().one()

            # Touch the conversation's updated_at
            await session.execute(
                text("""
                    UPDATE conversations SET updated_at = NOW()
                    WHERE id = :cid
                """),
                {"cid": conversation_id},
            )
            await session.commit()
            return dict(row)

    async def get_next_sequence(self, conversation_id: UUID) -> int:
        """Get the next sequence number for a conversation.

        Args:
            conversation_id: The conversation to query.

        Returns:
            ``max(sequence) + 1``, or ``1`` if no messages exist.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                text("""
                    SELECT COALESCE(MAX(sequence), 0) + 1 AS next_seq
                    FROM messages
                    WHERE conversation_id = :cid
                """),
                {"cid": conversation_id},
            )
            return result.scalar_one()
