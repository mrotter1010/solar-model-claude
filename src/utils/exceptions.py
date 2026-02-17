"""Custom exceptions for the solar model project."""

from typing import Any


class SolarModelError(Exception):
    """Base exception for all solar model errors."""

    def __init__(self, message: str, context: dict[str, Any] | None = None) -> None:
        self.context = context or {}
        super().__init__(message)

    def __str__(self) -> str:
        base = super().__str__()
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            return f"{base} [{context_str}]"
        return base


class ConfigValidationError(SolarModelError):
    """Raised when configuration validation fails."""
