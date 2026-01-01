"""Centralized loguru configuration for PQ."""

import sys

from loguru import logger


def configure_logging() -> None:
    """Configure loguru with a clean, aligned format.

    Called automatically when pq is imported.
    """
    # Remove default handler
    logger.remove()

    # Add handler with clean format
    # Fixed-width level, simple format without variable-length source info
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="DEBUG",
        colorize=True,
    )


# Auto-configure on import
configure_logging()
