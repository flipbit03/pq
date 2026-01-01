"""Priority levels for task ordering."""

from enum import IntEnum


class Priority(IntEnum):
    """Task priority levels.

    Lower values = higher priority (processed first).
    """

    CRITICAL = -20
    HIGH = -10
    NORMAL = 0
    LOW = 10
    BATCH = 20
