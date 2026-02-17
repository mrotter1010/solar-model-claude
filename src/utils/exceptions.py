"""Custom exception hierarchy for the solar model project."""

from typing import Any


class SolarModelError(Exception):
    """Base exception for all solar modeling errors.

    Attributes:
        message: Human-readable error message.
        context: Optional dictionary with additional error context.
    """

    def __init__(self, message: str, context: dict[str, Any] | None = None) -> None:
        self.message = message
        self.context = context or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        """Format error message with context."""
        if not self.context:
            return self.message

        context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
        return f"{self.message} | Context: {context_str}"


class ConfigValidationError(SolarModelError):
    """Raised when CSV configuration validation fails.

    Example context:
        - row_number: CSV row that failed
        - field_name: Specific field that failed validation
        - site_name: Name of the site being validated
        - error: Underlying validation error message
    """


class ClimateDataError(SolarModelError):
    """Raised when climate data API calls or processing fails.

    Example context:
        - location: (latitude, longitude) tuple
        - api: API being called (e.g., "NSRDB")
        - status_code: HTTP status code if applicable
        - response: API response message
    """


class SAMExecutionError(SolarModelError):
    """Raised when PySAM model execution fails.

    Example context:
        - site_name: Site being modeled
        - model_type: PySAM model being used
        - parameter: Specific parameter that caused failure
        - pysam_error: Original PySAM error message
    """
