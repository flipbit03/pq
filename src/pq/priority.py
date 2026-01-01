"""Priority levels for task ordering."""

from enum import IntEnum


class Priority(IntEnum):
    """Task priority levels.

    Higher values = higher priority (processed first).
    """

    BATCH = 0
    LOW = 25
    NORMAL = 50
    HIGH = 75
    CRITICAL = 100
