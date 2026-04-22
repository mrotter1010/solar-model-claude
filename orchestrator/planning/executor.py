"""Sequential plan execution against the analysis API."""

import json
import logging
from collections.abc import AsyncGenerator

from orchestrator.conversation.manager import ConversationManager
from orchestrator.conversation.models import ChatMessage, SessionStatus
from orchestrator.planning.events import (
    DoneEvent,
    ErrorEvent,
    ExecutionEvent,
    StepCompleteEvent,
    StepStartEvent,
    SynthesisEvent,
    _truncate,
)
from orchestrator.planning.planner import Planner
from orchestrator.tools.api_client import AnalysisAPIClient

logger = logging.getLogger(__name__)

# Tool names whose responses may contain a run_id to cache.
_RUN_TOOLS = frozenset({
    "run_production",
    "run_bill_savings",
    "run_bess",
    "run_buildability",
    "run_optimization",
    "run_batch",
})

# Keys to strip from tool results before session storage, keyed by tool name.
# Tool results stored in session messages are sent to GPT-5 on every subsequent
# turn, so they must be compact. Large data (8760 hourly arrays, raw timeseries)
# should be stripped — GPT-5 only needs summary stats and monthly averages to
# synthesize a response for the user.
_LARGE_RESULT_KEYS: dict[str, frozenset[str]] = {
    "get_lmp_prices": frozenset({"prices", "timestamps"}),
    "run_optimization": frozenset({"sweep_results", "bess_sweep_results"}),
}


_MAX_BATCH_SUMMARY_ROWS = 5


def _summarize_tool_result(tool_name: str, result: dict) -> dict:
    """Strip large arrays from a tool result before session storage.

    Follows the same pattern as binary download handling (PDF/CSV) — the
    session stores a compact representation while full data remains
    available via API download endpoints.

    Args:
        tool_name: Name of the tool that produced this result.
        result: The raw API response dict.

    Returns:
        The result dict with large keys removed for known tools,
        or the original dict unchanged for all other tools.
    """
    keys_to_strip = _LARGE_RESULT_KEYS.get(tool_name)
    if keys_to_strip is not None:
        return {k: v for k, v in result.items() if k not in keys_to_strip}

    # Truncate batch summary array to keep session messages compact.
    if tool_name == "run_batch" and "summary" in result:
        summary = result["summary"]
        if isinstance(summary, list) and len(summary) > _MAX_BATCH_SUMMARY_ROWS:
            truncated = dict(result)
            truncated["summary"] = summary[:_MAX_BATCH_SUMMARY_ROWS]
            truncated["summary_truncated"] = True
            truncated["summary_total"] = len(summary)
            return truncated

    return result


class ExecutionResult:
    """Container for the full execution outcome.

    Attributes:
        steps: List of per-tool step dicts with keys: tool, arguments,
            result, success, error.
        synthesis: GPT-5's final text summary after all tools complete.
        success: Overall success flag (False if any step failed).
        error: Top-level error message if execution itself failed.
    """

    def __init__(self) -> None:
        self.steps: list[dict] = []
        self.synthesis: str | None = None
        self.success: bool = True
        self.error: str | None = None


class Executor:
    """Runs an approved analysis plan by driving the GPT-5 tool-call loop.

    Args:
        planner: Planner instance for OpenAI calls.
        api_client: HTTP client for the analysis API.
        conversation_manager: Session and message history manager.
        max_steps: Maximum tool-call iterations before forced stop.
    """

    def __init__(
        self,
        planner: Planner,
        api_client: AnalysisAPIClient,
        conversation_manager: ConversationManager,
        max_steps: int = 10,
    ) -> None:
        self._planner = planner
        self._api_client = api_client
        self._conversation_manager = conversation_manager
        self._max_steps = max_steps

    async def _execute_plan_generator(
        self, session_id: str
    ) -> AsyncGenerator[ExecutionEvent, None]:
        """Execute the approved plan, yielding events as steps complete.

        This is the core execution logic. It manages session state internally
        and yields events for each significant execution milestone.

        Args:
            session_id: The conversation session to execute.

        Yields:
            ExecutionEvent instances in order: StepStart/StepComplete pairs,
            then SynthesisEvent (or ErrorEvent), then DoneEvent.
        """
        step_count = 0
        overall_success = True

        # --- Validate session state ---
        session = self._conversation_manager.get_session(session_id)
        if session is None:
            yield ErrorEvent(message=f"Session not found: {session_id}")
            yield DoneEvent(success=False, total_steps=0)
            return

        if session.status != SessionStatus.PLAN_PENDING:
            yield ErrorEvent(
                message=(
                    f"Session status is {session.status.value}, "
                    f"expected plan_pending"
                )
            )
            yield DoneEvent(success=False, total_steps=0)
            return

        approved_plan = session.pending_plan or ""

        # --- Begin execution ---
        self._conversation_manager.set_status(
            session_id, SessionStatus.EXECUTING
        )

        try:
            messages = self._conversation_manager.get_openai_messages(
                session_id
            )
            response = await self._planner.generate_execution_calls(
                messages, approved_plan
            )
        except Exception as exc:
            logger.error(
                "OpenAI call failed during execution start: %s", exc
            )
            yield ErrorEvent(message=f"OpenAI error: {exc}")
            self._conversation_manager.set_status(
                session_id, SessionStatus.PLAN_PENDING
            )
            yield DoneEvent(success=False, total_steps=0)
            return

        # --- Tool-call loop ---
        iterations = 0
        while iterations < self._max_steps:
            choice = response.choices[0]

            # No tool calls → synthesis reached
            if not choice.message.tool_calls:
                yield SynthesisEvent(text=choice.message.content or "")
                break

            iterations += 1

            # Record assistant message with tool_calls in conversation
            tool_calls_data = []
            for tc in choice.message.tool_calls:
                tool_calls_data.append({
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                })

            self._conversation_manager.add_message(
                session_id,
                ChatMessage(
                    role="assistant",
                    tool_calls=tool_calls_data,
                ),
            )

            # Execute each tool call and build tool-result messages
            for tc in choice.message.tool_calls:
                tool_name = tc.function.name
                try:
                    arguments = json.loads(tc.function.arguments)
                except json.JSONDecodeError:
                    arguments = {}

                step_count += 1

                # Yield start event before executing
                yield StepStartEvent(
                    step_number=step_count,
                    tool_name=tool_name,
                    tool_input=_truncate(json.dumps(arguments), 200),
                )

                step: dict = {
                    "tool": tool_name,
                    "arguments": arguments,
                    "result": None,
                    "success": True,
                    "error": None,
                }

                try:
                    api_result = await self._api_client.execute_tool(
                        tool_name, arguments
                    )

                    # Handle bytes responses (PDF/CSV downloads)
                    if isinstance(api_result, bytes):
                        tool_content = json.dumps({
                            "status": "downloaded",
                            "type": (
                                "pdf"
                                if tool_name == "get_report"
                                else "csv"
                            ),
                            "size_bytes": len(api_result),
                        })
                        step["result"] = {
                            "status": "downloaded",
                            "size_bytes": len(api_result),
                        }
                    else:
                        summarized = _summarize_tool_result(
                            tool_name, api_result
                        )
                        tool_content = json.dumps(summarized)
                        step["result"] = api_result

                        # Cache run results
                        if tool_name in _RUN_TOOLS:
                            run_id = api_result.get("run_id")
                            if run_id:
                                self._conversation_manager.cache_result(
                                    session_id, run_id, api_result
                                )

                except Exception as exc:
                    logger.warning(
                        "Tool %s failed: %s", tool_name, exc
                    )
                    step["success"] = False
                    step["error"] = str(exc)
                    overall_success = False
                    error_result = {
                        "error": str(exc),
                        "tool": tool_name,
                    }
                    tool_content = json.dumps(
                        _summarize_tool_result(tool_name, error_result)
                    )

                # Add tool result to session BEFORE yielding the SSE event.
                # If the generator is cancelled after a yield (client
                # disconnect), the session would be left with an orphaned
                # assistant(tool_calls) and no matching tool result.
                self._conversation_manager.add_message(
                    session_id,
                    ChatMessage(
                        role="tool",
                        content=tool_content,
                        tool_call_id=tc.id,
                        name=tool_name,
                    ),
                )

                yield StepCompleteEvent(
                    step_number=step_count,
                    tool_name=tool_name,
                    success=step["success"],
                    result_summary=_truncate(tool_content, 500),
                    step_data=step,
                )

            # Get next response from GPT-5
            try:
                messages = self._conversation_manager.get_openai_messages(
                    session_id
                )
                response = await self._planner.continue_execution(messages)
            except Exception as exc:
                logger.error("OpenAI call failed during loop: %s", exc)
                yield ErrorEvent(message=f"OpenAI error: {exc}")
                overall_success = False
                self._conversation_manager.set_status(
                    session_id, SessionStatus.PLAN_PENDING
                )
                yield DoneEvent(success=False, total_steps=step_count)
                return
        else:
            # Loop hit max_steps without synthesis
            yield SynthesisEvent(
                text="Execution stopped: maximum step limit reached."
            )

        # --- Finalize ---
        self._conversation_manager.set_status(
            session_id, SessionStatus.COMPLETE
        )
        yield DoneEvent(success=overall_success, total_steps=step_count)

    async def execute_plan(self, session_id: str) -> ExecutionResult:
        """Execute the approved plan for a session.

        Synchronous wrapper that consumes the event generator and builds
        an ExecutionResult with the same structure as the original
        implementation. This is what the /chat/approve endpoint calls.

        Args:
            session_id: The conversation session to execute.

        Returns:
            ExecutionResult with step details, synthesis, and status.
        """
        result = ExecutionResult()

        async for event in self._execute_plan_generator(session_id):
            if isinstance(event, StepCompleteEvent):
                result.steps.append(event.step_data)
                if not event.success:
                    result.success = False
            elif isinstance(event, SynthesisEvent):
                result.synthesis = event.text
            elif isinstance(event, ErrorEvent):
                result.success = False
                result.error = event.message

        return result
