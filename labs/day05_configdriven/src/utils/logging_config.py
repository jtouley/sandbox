"""
Structured logging configuration using structlog.
Concise but informative - not verbose.
"""

import logging
import os
import sys

import structlog


def setup_logging(level: str = None) -> None:
    """
    Configure structured logging for the pipeline.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR). Uses $LOG_LEVEL if None.
    """
    if level is None:
        level = os.environ.get("LOG_LEVEL", "INFO")

    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level.upper())),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = None) -> structlog.BoundLogger:
    """
    Get a logger instance.

    Args:
        name: Logger name (optional, for module identification)

    Returns:
        Configured structlog logger
    """
    if name:
        return structlog.get_logger().bind(module=name)
    return structlog.get_logger()
