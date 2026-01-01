"""Pytest fixtures for PQ tests."""

import os
from collections.abc import Generator

import pytest

from pq.client import PQ
from pq.registry import TaskRegistry


class TestPQ(PQ):
    """PQ client wrapper that uses _no_fork=True for testing.

    Fork isolation prevents in-memory side effects from being visible
    in the parent process. For unit tests that check in-memory state,
    we disable forking.
    """

    def run_worker(
        self,
        *,
        poll_interval: float = 1.0,
        max_runtime: float = 30 * 60,
        _no_fork: bool = True,  # Default to True for tests
    ) -> None:
        super().run_worker(
            poll_interval=poll_interval, max_runtime=max_runtime, _no_fork=_no_fork
        )

    def run_worker_once(
        self, *, max_runtime: float = 30 * 60, _no_fork: bool = True
    ) -> bool:
        return super().run_worker_once(max_runtime=max_runtime, _no_fork=_no_fork)


@pytest.fixture
def db_url() -> str:
    """Database URL for tests."""
    return os.environ.get(
        "PQ_DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/postgres"
    )


@pytest.fixture
def registry() -> TaskRegistry:
    """Fresh task registry for each test."""
    return TaskRegistry()


@pytest.fixture
def pq(db_url: str) -> Generator[TestPQ, None, None]:
    """PQ client with clean database for each test.

    Uses TestPQ which defaults to _no_fork=True for test compatibility.
    """
    client = TestPQ(db_url)
    client.create_tables()
    client.clear_all()
    yield client
    client.clear_all()
