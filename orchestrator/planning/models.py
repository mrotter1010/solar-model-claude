"""Pydantic models for planner responses and response classification."""

from enum import Enum

from pydantic import BaseModel


class ResponseType(str, Enum):
    """Classification of GPT-5's response."""

    PLAN = "plan"
    CLARIFICATION = "clarification"
    RESPONSE = "response"
    ERROR = "error"


class PlannerResponse(BaseModel):
    """Structured response from the planner.

    Args:
        response_type: Classification of the response content.
        content: The text content from GPT-5.
        raw_response: Full OpenAI API response for debugging.
    """

    response_type: ResponseType
    content: str
    raw_response: dict | None = None
