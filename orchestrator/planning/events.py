"""Event types for streaming execution progress."""

from __future__ import annotations

import json
from dataclasses import dataclass


def _truncate(text: str, max_len: int = 500) -> str:
    """Truncate text, appending '...' if it exceeds max_len characters."""
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


@dataclass
class StepStartEvent:
    """Emitted before a tool call begins."""

    step_number: int
    tool_name: str
    tool_input: str  # truncated JSON of arguments


@dataclass
class StepCompleteEvent:
    """Emitted after a tool call completes."""

    step_number: int
    tool_name: str
    success: bool
    result_summary: str  # truncated to ~500 chars for streaming
    step_data: dict  # full step dict for ExecutionResult reconstruction


@dataclass
class SynthesisEvent:
    """Emitted when GPT-5 produces the final text synthesis."""

    text: str


@dataclass
class ErrorEvent:
    """Emitted on a top-level execution error."""

    message: str


@dataclass
class DoneEvent:
    """Emitted when execution is complete."""

    success: bool
    total_steps: int


# Union type for all execution events.
ExecutionEvent = (
    StepStartEvent | StepCompleteEvent | SynthesisEvent | ErrorEvent | DoneEvent
)
