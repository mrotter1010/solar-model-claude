"""Tests for logging and error handling framework."""

import logging
from pathlib import Path

import pytest

from src.utils.exceptions import (
    ClimateDataError,
    ConfigValidationError,
    SAMExecutionError,
    SolarModelError,
)
from src.utils.logger import setup_logger

TEST_RESULTS_DIR = Path(__file__).parent.parent / "outputs" / "test_results"


@pytest.fixture(autouse=True)
def ensure_output_dir() -> None:
    """Ensure test results directory exists."""
    TEST_RESULTS_DIR.mkdir(parents=True, exist_ok=True)


@pytest.fixture()
def fresh_logger(tmp_path: Path) -> logging.Logger:
    """Create a fresh logger with no cached handlers for each test."""
    # Use a unique name to avoid handler caching between tests
    import uuid

    name = f"test_logger_{uuid.uuid4().hex[:8]}"
    return setup_logger(name, log_file=tmp_path / "test.log")


# --- Logger Tests ---


def test_logger_returns_logger_instance() -> None:
    """setup_logger should return a logging.Logger."""
    logger = setup_logger("test.basic")
    assert isinstance(logger, logging.Logger)


def test_logger_has_console_handler() -> None:
    """Logger should have at least one StreamHandler for console output."""
    import uuid

    logger = setup_logger(f"test.console_{uuid.uuid4().hex[:8]}")
    stream_handlers = [
        h for h in logger.handlers if isinstance(h, logging.StreamHandler)
    ]
    assert len(stream_handlers) >= 1


def test_logger_file_output(tmp_path: Path) -> None:
    """Logger with log_file should write messages to the file."""
    # Arrange
    log_file = tmp_path / "test_output.log"
    import uuid

    logger = setup_logger(
        f"test.file_{uuid.uuid4().hex[:8]}", log_file=log_file
    )

    # Act
    logger.info("Test info message")
    logger.warning("Test warning message")

    # Flush handlers to ensure write
    for handler in logger.handlers:
        handler.flush()

    # Assert - log file exists and contains messages
    assert log_file.exists()
    content = log_file.read_text()
    assert "Test info message" in content
    assert "Test warning message" in content

    # Write log content for manual inspection
    output_path = TEST_RESULTS_DIR / "test_logger_file_output.log"
    output_path.write_text(content)


def test_logger_file_creates_parent_dirs(tmp_path: Path) -> None:
    """Logger should create parent directories for log file."""
    import uuid

    log_file = tmp_path / "nested" / "dirs" / "test.log"
    logger = setup_logger(
        f"test.dirs_{uuid.uuid4().hex[:8]}", log_file=log_file
    )
    logger.info("Test message")

    for handler in logger.handlers:
        handler.flush()

    assert log_file.exists()


def test_logger_no_duplicate_handlers() -> None:
    """Calling setup_logger twice with same name should not add duplicate handlers."""
    import uuid

    name = f"test.dedup_{uuid.uuid4().hex[:8]}"
    logger1 = setup_logger(name)
    handler_count = len(logger1.handlers)
    logger2 = setup_logger(name)

    assert logger1 is logger2
    assert len(logger2.handlers) == handler_count


def test_logger_debug_level_captured_in_file(tmp_path: Path) -> None:
    """File handler should capture DEBUG messages even though console is INFO."""
    import uuid

    log_file = tmp_path / "debug_test.log"
    logger = setup_logger(
        f"test.debug_{uuid.uuid4().hex[:8]}", log_file=log_file
    )

    # Act
    logger.debug("Debug level message")

    for handler in logger.handlers:
        handler.flush()

    # Assert - debug message in file
    content = log_file.read_text()
    assert "Debug level message" in content


# --- Exception Tests ---


def test_solar_model_error_without_context() -> None:
    """SolarModelError without context should show just the message."""
    err = SolarModelError("Something went wrong")
    assert str(err) == "Something went wrong"
    assert err.message == "Something went wrong"
    assert err.context == {}


def test_solar_model_error_with_context() -> None:
    """SolarModelError with context should format as 'message | Context: k=v'."""
    err = SolarModelError(
        "Validation failed",
        context={"row": 5, "field": "latitude"},
    )

    # Assert - context is included in string representation
    error_str = str(err)
    assert "Validation failed" in error_str
    assert "Context:" in error_str
    assert "row=5" in error_str
    assert "field=latitude" in error_str

    # Write formatted error for manual inspection
    output_path = TEST_RESULTS_DIR / "test_exception_formatting.txt"
    output_path.write_text(f"Error: {error_str}\nMessage: {err.message}\nContext: {err.context}\n")


def test_config_validation_error_is_solar_model_error() -> None:
    """ConfigValidationError should be a subclass of SolarModelError."""
    err = ConfigValidationError("Bad config", context={"row_number": 3})
    assert isinstance(err, SolarModelError)
    assert isinstance(err, Exception)
    assert err.context["row_number"] == 3


def test_climate_data_error_with_context() -> None:
    """ClimateDataError should support API-specific context."""
    err = ClimateDataError(
        "NSRDB request failed",
        context={
            "location": (33.45, -112.07),
            "api": "NSRDB",
            "status_code": 403,
        },
    )
    assert isinstance(err, SolarModelError)
    assert err.context["status_code"] == 403
    assert "NSRDB request failed" in str(err)


def test_sam_execution_error_with_context() -> None:
    """SAMExecutionError should support PySAM-specific context."""
    err = SAMExecutionError(
        "PySAM simulation failed",
        context={
            "site_name": "Phoenix Solar Farm",
            "model_type": "Pvsamv1",
            "pysam_error": "Invalid module parameters",
        },
    )
    assert isinstance(err, SolarModelError)
    assert err.context["site_name"] == "Phoenix Solar Farm"
    assert "PySAM simulation failed" in str(err)


def test_exception_hierarchy() -> None:
    """All custom exceptions should be catchable as SolarModelError."""
    exceptions = [
        ConfigValidationError("config error"),
        ClimateDataError("climate error"),
        SAMExecutionError("sam error"),
    ]
    for exc in exceptions:
        # All should be catchable as SolarModelError
        with pytest.raises(SolarModelError):
            raise exc


def test_config_validation_error_in_loader() -> None:
    """Config loader should raise ConfigValidationError with context for missing file."""
    from src.config.loader import load_config

    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(Path("/nonexistent/path.csv"))

    # Assert - error has context with path
    assert exc_info.value.context["path"] == "/nonexistent/path.csv"
    assert "Config file not found" in exc_info.value.message
