"""Logging configuration for the solar model project."""

import logging
from pathlib import Path

try:
    import colorlog

    HAS_COLORLOG = True
except ImportError:
    HAS_COLORLOG = False


def setup_logger(
    name: str,
    log_file: Path | None = None,
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
) -> logging.Logger:
    """Set up a logger with console and optional file output.

    Args:
        name: Logger name (typically __name__ from calling module).
        log_file: Optional path to log file. If None, only logs to console.
        console_level: Logging level for console output (default: INFO).
        file_level: Logging level for file output (default: DEBUG).

    Returns:
        Configured logger instance.

    Example:
        >>> logger = setup_logger(__name__)
        >>> logger.info("Processing started")
        >>> logger = setup_logger(__name__, log_file=Path("outputs/logs/app.log"))
    """
    logger = logging.getLogger(name)

    # Prevent duplicate handlers if logger already configured
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)  # Capture all levels, handlers filter

    # Console handler with color if available
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)

    if HAS_COLORLOG:
        console_formatter = colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )
    else:
        console_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler if log_file provided
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(file_level)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        logger.debug(f"File logging enabled: {log_file}")

    return logger
