"""Analysis plan generation via OpenAI GPT-5 with function-calling tools."""

import logging
import re
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

    # Regex to find the CRITICAL ARCHITECTURAL CONSTRAINT section.
    # Matches from the ``## CRITICAL ARCHITECTURAL CONSTRAINT`` heading
    # up to (but not including) the next ``## `` heading or ``---`` separator.
    _CONSTRAINT_SECTION_RE = re.compile(
        r"## CRITICAL ARCHITECTURAL CONSTRAINT\n.*?(?=\n---\n|\n## )",
        re.DOTALL,
    )

    def __init__(self, config: OrchestratorConfig) -> None:
        self._client = AsyncOpenAI(api_key=config.openai_api_key)
        self._model = config.openai_model
        self._system_prompt = self._load_system_prompt(config.system_prompt_path)
        self._execution_prompt = self._build_execution_prompt(self._system_prompt)
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

    @classmethod
    def _build_execution_prompt(cls, system_prompt: str) -> str:
        """Create an execution-safe system prompt by stripping planning-only rules.

        Removes the CRITICAL ARCHITECTURAL CONSTRAINT section which tells
        GPT-5 "you cannot call tools directly." During execution mode GPT-5
        must call tools, so this instruction would cause it to generate plans
        instead.

        The result is cached at ``__init__`` time so the regex only runs once.

        Args:
            system_prompt: The full system prompt text.

        Returns:
            System prompt without the architectural constraint section.
            If the section is not found, the full prompt is returned unchanged.
        """
        result, count = cls._CONSTRAINT_SECTION_RE.subn("", system_prompt)
        if count == 0:
            logger.warning(
                "CRITICAL ARCHITECTURAL CONSTRAINT section not found in "
                "system prompt — execution prompt will use the full prompt"
            )
            return system_prompt
        # Clean up any leftover double blank lines from the removal
        result = re.sub(r"\n{3,}", "\n\n", result)
        return result

    async def generate_plan(self, messages: list[dict]) -> PlannerResponse:
        """Send conversation to GPT-5 and classify the response.

        Args:
            messages: OpenAI-formatted message history (from
                ``ConversationManager.get_openai_messages``). Does NOT include
                the system prompt — this method prepends it.

        Returns:
            PlannerResponse with classified response_type and content.
        """
        # Strip tool-chain messages (assistant with tool_calls, tool results).
        # These are only relevant during the execution loop. The synthesis
        # message already summarizes results in natural language. Sending
        # tool-chain messages without tools= causes OpenAI validation errors.
        planning_messages = [
            m for m in messages
            if m["role"] != "tool"
            and not (m["role"] == "assistant" and "tool_calls" in m)
        ]

        full_messages = [
            {"role": "system", "content": self._system_prompt},
            *planning_messages,
        ]

        # Omit tools so GPT-5 is forced to respond with text (the plan).
        # Tools are only provided during execution (generate_execution_calls).
        response = await self._client.chat.completions.create(
            model=self._model,
            messages=full_messages,
        )

        choice = response.choices[0]
        content = choice.message.content or ""
        content = self._clean_plan_response(content)
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
        # Use execution prompt (planning-only constraints stripped).
        full_messages = [
            {"role": "system", "content": self._execution_prompt},
            *messages,
            {
                "role": "user",
                "content": (
                    "The user has approved the above plan. You are now in "
                    "EXECUTION MODE. Call tools directly using the provided "
                    "tool definitions — do NOT generate plans, commentary, "
                    "or ANALYSIS PLAN blocks. Only emit tool_calls. "
                    "Execute the plan step by step in order. Call one tool "
                    "at a time. After all plan steps are complete, provide "
                    "a synthesis of the results as your final text response. "
                    "IMPORTANT: For equipment searches, call search_modules "
                    "once and search_inverters once. After getting results "
                    "with count > 0, stop searching and use those exact name "
                    "strings (copied verbatim) in the production call."
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
        # Use execution prompt (planning-only constraints stripped).
        full_messages = [
            {"role": "system", "content": self._execution_prompt},
            *messages,
        ]
        return await self._client.chat.completions.create(
            model=self._model,
            messages=full_messages,
            tools=self._tools,
        )

    # Patterns that signal the end of a plan block (confirmation prompts).
    # Matched case-insensitively. The plan is truncated after the first
    # line containing one of these.
    _PLAN_END_PATTERNS: list[re.Pattern[str]] = [
        re.compile(r"shall I proceed", re.IGNORECASE),
        re.compile(r"want to adjust", re.IGNORECASE),
        re.compile(r"would you like to adjust", re.IGNORECASE),
        re.compile(r"ready to execute", re.IGNORECASE),
        re.compile(r"approve & execute", re.IGNORECASE),
        re.compile(r"approve and execute", re.IGNORECASE),
    ]

    @staticmethod
    def _clean_plan_response(content: str) -> str:
        """Remove duplicate plan blocks from GPT-5's response.

        GPT-5 sometimes generates two ANALYSIS PLAN blocks in a single
        response — a user-facing summary and then a detailed executor-facing
        version.  This method keeps only the first plan block.

        Truncation rules (applied in order):
        1. If a second ``ANALYSIS PLAN`` header exists, everything from the
           second occurrence onward is discarded.
        2. If a confirmation prompt is found (e.g. "Shall I proceed?"), the
           response is truncated after that line.

        If the response contains zero or one plan block with no confirmation
        prompt, it passes through unchanged.

        Args:
            content: Raw text response from GPT-5.

        Returns:
            Cleaned response with at most one plan block.
        """
        if "ANALYSIS PLAN" not in content:
            return content

        # --- Rule 1: truncate at the second ANALYSIS PLAN header -----------
        first_idx = content.index("ANALYSIS PLAN")
        second_idx = content.find("ANALYSIS PLAN", first_idx + len("ANALYSIS PLAN"))
        if second_idx != -1:
            # Walk back to discard any blank lines between the plans
            content = content[:second_idx].rstrip()
            logger.debug(
                "Truncated duplicate ANALYSIS PLAN block at position %d",
                second_idx,
            )

        # --- Rule 2: truncate after the first confirmation prompt ----------
        lines = content.split("\n")
        for i, line in enumerate(lines):
            for pattern in Planner._PLAN_END_PATTERNS:
                if pattern.search(line):
                    content = "\n".join(lines[: i + 1]).rstrip()
                    return content

        return content

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
