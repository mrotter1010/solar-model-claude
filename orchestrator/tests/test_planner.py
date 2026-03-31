"""Tests for analysis plan generation and response classification."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from orchestrator.config import OrchestratorConfig
from orchestrator.planning.models import PlannerResponse, ResponseType
from orchestrator.planning.planner import Planner
from orchestrator.tools.definitions import TOOL_DEFINITIONS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def test_config(tmp_path):
    """OrchestratorConfig pointing at a temporary system prompt file."""
    prompt_file = tmp_path / "system_prompt.md"
    prompt_file.write_text("You are a solar analyst assistant.")
    return OrchestratorConfig(
        openai_api_key="test-key-123",
        openai_model="gpt-5",
        system_prompt_path=str(prompt_file),
    )


@pytest.fixture
def mock_openai_client():
    """Mocked AsyncOpenAI client."""
    return AsyncMock()


def _make_chat_response(content: str | None = None, tool_calls=None):
    """Build a mock OpenAI ChatCompletion response."""
    message = MagicMock()
    message.content = content
    message.tool_calls = tool_calls

    choice = MagicMock()
    choice.message = message

    response = MagicMock()
    response.choices = [choice]
    response.model_dump.return_value = {
        "id": "chatcmpl-test",
        "choices": [
            {
                "message": {
                    "content": content,
                    "tool_calls": tool_calls,
                },
            },
        ],
    }
    return response


@pytest.fixture
def planner(test_config, mock_openai_client):
    """Planner instance with a mocked OpenAI client."""
    with patch(
        "orchestrator.planning.planner.AsyncOpenAI",
        return_value=mock_openai_client,
    ):
        p = Planner(test_config)
    p._client = mock_openai_client
    return p


@pytest.fixture
def sample_messages():
    """Minimal OpenAI-formatted message list."""
    return [
        {"role": "user", "content": "Run a production analysis for a 5 MW site in Phoenix."},
    ]


# ---------------------------------------------------------------------------
# Response classification tests
# ---------------------------------------------------------------------------


class TestClassifyResponse:
    """Tests for Planner._classify_response."""

    def test_classify_plan_response(self):
        """Content containing 'ANALYSIS PLAN' → ResponseType.PLAN."""
        content = "ANALYSIS PLAN\n1. Search for module\n2. Run production"
        assert Planner._classify_response(content) == ResponseType.PLAN

    def test_classify_plan_with_decorators(self):
        """Content with Unicode box-drawing separators + 'ANALYSIS PLAN' → PLAN."""
        content = (
            "═══════════════════════════════════════\n"
            "ANALYSIS PLAN\n"
            "═══════════════════════════════════════\n"
            "Step 1: Search for the module\n"
            "Step 2: Run production simulation\n"
        )
        assert Planner._classify_response(content) == ResponseType.PLAN

    def test_classify_general_response(self):
        """Content without plan header → ResponseType.RESPONSE."""
        content = "Sure, I can help with that. What module would you like?"
        assert Planner._classify_response(content) == ResponseType.RESPONSE

    def test_classify_empty_response(self):
        """Empty string → ResponseType.RESPONSE."""
        assert Planner._classify_response("") == ResponseType.RESPONSE


# ---------------------------------------------------------------------------
# Plan generation tests (mock OpenAI)
# ---------------------------------------------------------------------------


class TestGeneratePlan:
    """Tests for Planner.generate_plan."""

    @pytest.mark.anyio
    async def test_generate_plan_returns_plan(self, planner, mock_openai_client, sample_messages):
        """Mock OpenAI returns content with 'ANALYSIS PLAN' → response_type=PLAN."""
        plan_text = (
            "ANALYSIS PLAN\n"
            "═══════════════\n"
            "1. Search for CS6W-550MS module\n"
            "2. Run production for 5 MW site in Phoenix\n"
        )
        mock_openai_client.chat.completions.create = AsyncMock(
            return_value=_make_chat_response(content=plan_text),
        )

        result = await planner.generate_plan(sample_messages)

        assert isinstance(result, PlannerResponse)
        assert result.response_type == ResponseType.PLAN
        assert result.content == plan_text
        assert result.raw_response is not None

    @pytest.mark.anyio
    async def test_generate_plan_returns_clarification(self, planner, mock_openai_client, sample_messages):
        """Mock OpenAI returns a question (no plan header) → response_type=RESPONSE."""
        question = "What module and inverter would you like to use for this analysis?"
        mock_openai_client.chat.completions.create = AsyncMock(
            return_value=_make_chat_response(content=question),
        )

        result = await planner.generate_plan(sample_messages)

        assert result.response_type == ResponseType.RESPONSE
        assert result.content == question

    @pytest.mark.anyio
    async def test_generate_plan_prepends_system_prompt(self, planner, mock_openai_client, sample_messages):
        """System prompt is the first message sent to OpenAI."""
        mock_openai_client.chat.completions.create = AsyncMock(
            return_value=_make_chat_response(content="Hello"),
        )

        await planner.generate_plan(sample_messages)

        call_kwargs = mock_openai_client.chat.completions.create.call_args
        sent_messages = call_kwargs.kwargs["messages"]
        assert sent_messages[0]["role"] == "system"
        assert sent_messages[0]["content"] == "You are a solar analyst assistant."

    @pytest.mark.anyio
    async def test_generate_plan_includes_tools(self, planner, mock_openai_client, sample_messages):
        """TOOL_DEFINITIONS are passed to the OpenAI call."""
        mock_openai_client.chat.completions.create = AsyncMock(
            return_value=_make_chat_response(content="Hello"),
        )

        await planner.generate_plan(sample_messages)

        call_kwargs = mock_openai_client.chat.completions.create.call_args
        assert call_kwargs.kwargs["tools"] == TOOL_DEFINITIONS

    @pytest.mark.anyio
    async def test_generate_plan_handles_unexpected_tool_calls(self, planner, mock_openai_client, sample_messages):
        """If GPT-5 returns tool_calls instead of text, handle gracefully."""
        tool_call = MagicMock()
        tool_call.function.name = "search_modules"
        mock_openai_client.chat.completions.create = AsyncMock(
            return_value=_make_chat_response(content=None, tool_calls=[tool_call]),
        )

        result = await planner.generate_plan(sample_messages)

        # Should not crash; returns RESPONSE type with fallback content
        assert result.response_type == ResponseType.RESPONSE
        assert len(result.content) > 0
        assert result.raw_response is not None


# ---------------------------------------------------------------------------
# Execution call tests (mock OpenAI)
# ---------------------------------------------------------------------------


class TestGenerateExecutionCalls:
    """Tests for Planner.generate_execution_calls."""

    @pytest.mark.anyio
    async def test_generate_execution_calls_appends_instruction(self, planner, mock_openai_client, sample_messages):
        """Execution instruction message is appended to the conversation."""
        tool_call = MagicMock()
        tool_call.function.name = "search_modules"
        raw_response = _make_chat_response(content=None, tool_calls=[tool_call])
        mock_openai_client.chat.completions.create = AsyncMock(return_value=raw_response)

        approved_plan = "1. Search modules\n2. Run production"
        await planner.generate_execution_calls(sample_messages, approved_plan)

        call_kwargs = mock_openai_client.chat.completions.create.call_args
        sent_messages = call_kwargs.kwargs["messages"]

        # System prompt is first
        assert sent_messages[0]["role"] == "system"
        # Last message is the execution instruction
        last_msg = sent_messages[-1]
        assert last_msg["role"] == "user"
        assert "approved" in last_msg["content"].lower()
        assert "execute" in last_msg["content"].lower()

    @pytest.mark.anyio
    async def test_generate_execution_calls_returns_raw_response(self, planner, mock_openai_client, sample_messages):
        """Returns the raw OpenAI response object, not a PlannerResponse."""
        raw_response = _make_chat_response(content="Starting execution...")
        mock_openai_client.chat.completions.create = AsyncMock(return_value=raw_response)

        result = await planner.generate_execution_calls(
            sample_messages,
            approved_plan="1. Search modules",
        )

        # Should be the raw mock object, not a PlannerResponse
        assert result is raw_response
        assert not isinstance(result, PlannerResponse)


# ---------------------------------------------------------------------------
# System prompt loading tests
# ---------------------------------------------------------------------------


class TestLoadSystemPrompt:
    """Tests for Planner._load_system_prompt."""

    def test_load_system_prompt_success(self, tmp_path):
        """Reads file content successfully."""
        prompt_file = tmp_path / "prompt.md"
        prompt_file.write_text("You are a test assistant.")

        result = Planner._load_system_prompt(str(prompt_file))

        assert result == "You are a test assistant."

    def test_load_system_prompt_missing_file(self):
        """Raises FileNotFoundError for a nonexistent path."""
        with pytest.raises(FileNotFoundError):
            Planner._load_system_prompt("/nonexistent/path/prompt.md")
