"""External task handlers for import demonstration."""

from typing import Any

from loguru import logger


def external_task(data: Any) -> None:
    """Task imported from another module."""
    logger.info(f"📦 External task from examples.tasks: {data}")
