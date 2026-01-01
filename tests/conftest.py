"""Pytest fixtures for PQ tests."""

import os
from collections.abc import Generator

import pytest

from pq.client import PQ
from pq.registry import TaskRegistry


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
def pq(db_url: str) -> Generator[PQ, None, None]:
    """PQ client with clean database for each test."""
    client = PQ(db_url)
    client.create_tables()
    client.clear_all()
    yield client
    client.clear_all()
