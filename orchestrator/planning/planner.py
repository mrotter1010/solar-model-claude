"""Analysis plan generation via OpenAI GPT-5 with function-calling tools."""

import logging
from pathlib import Path

from openai import AsyncOpenAI

from orchestrator.config import OrchestratorConfig
from orchestrator.planning.models import PlannerResponse, ResponseType
from orchestrator.tools.definitions import TOOL_DEFINITIONS

logger = logging.getLogger(__name__)


class Planner:
    """Generates analysis plans and execution calls via GPT-5.

    The planner has two modes:

    1. ``generate_plan`` — sends conversation history to GPT-5 and classifies
       the text response as a plan, clarification, or general response.
    2. ``generate_execution_calls`` — after the user approves a plan, sends
       it back to GPT-5 with an instruction to execute via tool calls.

    Args:
        config: Orchestrator configuration with OpenAI credentials and model.
    """

    def __init__(self, config: OrchestratorConfig) -> None:
        self._client = AsyncOpenAI(api_key=config.openai_api_key)
        self._model = config.openai_model
        self._system_prompt = self._load_system_prompt(config.system_prompt_path)
        self._tools = TOOL_DEFINITIONS

    @staticmethod
    def _load_system_prompt(path: str) -> str:
        """Load system prompt from file.

        Args:
            path: Filesystem path to the system prompt markdown file.

        Returns:
            The system prompt text.

        Raises:
            FileNotFoundError: If the system prompt file does not exist.
        """
        return Path(path).read_text()

    async def generate_plan(self, messages: list[dict]) -> PlannerResponse:
        """Send conversation to GPT-5 and classify the response.

        Args:
            messages: OpenAI-formatted message history (from
                ``ConversationManager.get_openai_messages``). Does NOT include
                the system prompt — this method prepends it.

        Returns:
            PlannerResponse with classified response_type and content.
        """
        full_messages = [
            {"role": "system", "content": self._system_prompt},
            *messages,
        ]

        response = await self._client.chat.completions.create(
            model=self._model,
            messages=full_messages,
            tools=self._tools,
        )

        choice = response.choices[0]

        # Handle unexpected tool calls — GPT-5 should return text per the
        # system prompt, but if it returns tool calls instead, log a warning
        # and return whatever text content exists.
        if choice.message.tool_calls:
            logger.warning(
                "GPT-5 returned tool calls during plan generation "
                "(expected text). tool_calls=%s",
                [tc.function.name for tc in choice.message.tool_calls],
            )
            content = choice.message.content or (
                "The model attempted to call tools directly. "
                "Please rephrase your request."
            )
            return PlannerResponse(
                response_type=ResponseType.RESPONSE,
                content=content,
                raw_response=response.model_dump(),
            )

        content = choice.message.content or ""
        response_type = self._classify_response(content)

        return PlannerResponse(
            response_type=response_type,
            content=content,
            raw_response=response.model_dump(),
        )

    async def generate_execution_calls(
        self, messages: list[dict], approved_plan: str
    ) -> object:
        """Send the approved plan back to GPT-5 to get tool calls.

        Called by the executor after the user approves a plan. Appends an
        instruction message telling GPT-5 to execute the plan using tool calls.

        Args:
            messages: Full conversation history (OpenAI-formatted).
            approved_plan: The plan text that was approved.

        Returns:
            The raw OpenAI ChatCompletion response object. The executor
            manages the tool-call loop, so this returns the first response.
        """
        full_messages = [
            {"role": "system", "content": self._system_prompt},
            *messages,
            {
                "role": "user",
                "content": (
                    "The user has approved the above plan. Execute it now by "
                    "calling the appropriate tools in the order specified in "
                    "the plan. Call one tool at a time."
                ),
            },
        ]

        response = await self._client.chat.completions.create(
            model=self._model,
            messages=full_messages,
            tools=self._tools,
            tool_choice="auto",
        )

        return response

    async def continue_execution(self, messages: list[dict]) -> object:
        """Continue the tool-call loop with updated messages.

        Sends the full conversation (including tool results from the last
        iteration) back to GPT-5 for the next tool call or synthesis.

        Args:
            messages: Full conversation history including tool results.

        Returns:
            Raw OpenAI ChatCompletion response.
        """
        full_messages = [
            {"role": "system", "content": self._system_prompt},
            *messages,
        ]
        return await self._client.chat.completions.create(
            model=self._model,
            messages=full_messages,
            tools=self._tools,
        )

    @staticmethod
    def _classify_response(content: str) -> ResponseType:
        """Classify GPT-5's text response.

        Args:
            content: The text content from GPT-5's response.

        Returns:
            ``ResponseType.PLAN`` if the content contains "ANALYSIS PLAN",
            otherwise ``ResponseType.RESPONSE``.
        """
        if "ANALYSIS PLAN" in content:
            return ResponseType.PLAN
        return ResponseType.RESPONSE
