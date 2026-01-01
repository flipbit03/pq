"""External task handlers for import demonstration."""

from loguru import logger


def external_task(payload: dict) -> None:
    """Task imported from another module."""
    logger.info(f"📦 External task from examples.tasks: {payload}")
