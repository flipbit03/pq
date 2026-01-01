"""PQ - Postgres-backed task queue."""

import pq.logging  # noqa: F401 - configures loguru on import

from pq.client import PQ

__version__ = "0.1.0"

__all__ = ["PQ"]
