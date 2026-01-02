"""Tests for worker logic with fork isolation.

All tests use multiprocessing shared state since tasks run in forked processes.
"""

import asyncio
import multiprocessing
import multiprocessing.managers
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from pq.client import PQ
from pq.models import Periodic
from pq.priority import Priority

# Global shared state for fork-isolated tests
_shared_results: Any = None
_shared_calls: Any = None


def _set_shared_results(results: Any) -> None:
    global _shared_results
    _shared_results = results


def _set_shared_calls(calls: Any) -> None:
    global _shared_calls
    _shared_calls = calls


# Module-level handlers for testing (must be importable)
def capture_handler(value: int) -> None:
    """Capture a value to shared state."""
    _shared_results.append(value)


def noop_handler() -> None:
    """Do nothing."""
    pass


def failing_handler() -> None:
    """Always fails."""
    raise ValueError("boom")


def tracked_handler(key: str) -> None:
    """Track calls for testing."""
    _shared_calls.append((key,))


def ordered_handler(n: int) -> None:
    """Append n to results for ordering tests."""
    _shared_results.append(n)


def periodic_handler(n: int) -> None:
    """Periodic task handler."""
    _shared_results.append(n)


def periodic_noop_handler() -> None:
    """Periodic noop handler."""
    pass


def counter_handler() -> None:
    """Counter handler for periodic tests."""
    _shared_results.append(1)


async def async_capture_handler(value: str) -> None:
    """Async handler that captures a value."""
    await asyncio.sleep(0.01)
    _shared_results.append(value)


async def async_periodic_handler(n: int) -> None:
    """Async periodic handler."""
    await asyncio.sleep(0.01)
    _shared_results.append(n)


async def async_failing_handler() -> None:
    """Async handler that fails."""
    await asyncio.sleep(0.01)
    raise ValueError("async boom")


async def slow_async_handler() -> None:
    """Async handler that takes too long."""
    await asyncio.sleep(10)


def slow_sync_handler() -> None:
    """Sync handler that takes too long."""
    time.sleep(10)


class TestRunWorkerOnce:
    """Tests for run_worker_once method."""

    def test_processes_pending_task(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker processes a pending task."""
        results = manager.list()
        _set_shared_results(results)

        pq.enqueue(capture_handler, value=42)
        processed = pq.run_worker_once()

        assert processed is True
        assert list(results) == [42]

    def test_deletes_task_after_processing(self, pq: PQ) -> None:
        """Worker marks task completed after processing."""
        pq.enqueue(noop_handler)
        assert pq.pending_count() == 1

        pq.run_worker_once()

        assert pq.pending_count() == 0

    def test_skips_future_task(self, pq: PQ) -> None:
        """Worker skips tasks scheduled for the future."""
        future = datetime.now(UTC) + timedelta(hours=1)
        pq.enqueue(noop_handler, run_at=future)

        processed = pq.run_worker_once()

        assert processed is False
        assert pq.pending_count() == 1

    def test_returns_false_when_empty(self, pq: PQ) -> None:
        """Worker returns False when no tasks available."""
        processed = pq.run_worker_once()
        assert processed is False

    def test_deletes_task_on_failure(self, pq: PQ) -> None:
        """Worker marks task failed when handler fails."""
        pq.enqueue(failing_handler)
        assert pq.pending_count() == 1

        pq.run_worker_once()

        assert pq.pending_count() == 0

    def test_processes_direct_function(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker can process task with direct function path."""
        calls = manager.list()
        _set_shared_calls(calls)

        pq.enqueue(tracked_handler, key="value")
        pq.run_worker_once()

        assert list(calls) == [("value",)]

    def test_processes_higher_priority_first(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker processes higher priority tasks first."""
        results = manager.list()
        _set_shared_results(results)

        # Enqueue in reverse priority order
        pq.enqueue(ordered_handler, n=3, priority=Priority.LOW)
        pq.enqueue(ordered_handler, n=1, priority=Priority.HIGH)
        pq.enqueue(ordered_handler, n=2, priority=Priority.NORMAL)

        pq.run_worker_once()
        pq.run_worker_once()
        pq.run_worker_once()

        # Should process in priority order: HIGH, NORMAL, LOW
        assert list(results) == [1, 2, 3]

    def test_filters_by_priority(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker only processes tasks matching priority filter."""
        results = manager.list()
        _set_shared_results(results)

        # Enqueue tasks with different priorities
        pq.enqueue(ordered_handler, n=1, priority=Priority.HIGH)
        pq.enqueue(ordered_handler, n=2, priority=Priority.NORMAL)
        pq.enqueue(ordered_handler, n=3, priority=Priority.LOW)

        # Worker with HIGH-only filter
        pq.run_worker_once(priorities={Priority.HIGH})
        pq.run_worker_once(priorities={Priority.HIGH})  # Should find nothing

        # Only HIGH priority task should be processed
        assert list(results) == [1]
        assert pq.pending_count() == 2  # NORMAL and LOW still pending

        # Process remaining with no filter
        pq.run_worker_once()
        pq.run_worker_once()

        assert list(results) == [1, 2, 3]


class TestPeriodicTasks:
    """Tests for periodic task processing."""

    def test_processes_periodic_task(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker processes a periodic task."""
        results = manager.list()
        _set_shared_results(results)

        pq.schedule(periodic_handler, run_every=timedelta(hours=1), n=1)

        processed = pq.run_worker_once()

        assert processed is True
        assert list(results) == [1]

    def test_advances_next_run(self, pq: PQ) -> None:
        """Worker advances next_run after processing."""
        from sqlalchemy import select

        pq.schedule(periodic_noop_handler, run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_worker:periodic_noop_handler"
                )
            ).scalar_one()
            original_next_run = periodic.next_run

        pq.run_worker_once()

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_worker:periodic_noop_handler"
                )
            ).scalar_one()
            assert periodic.next_run > original_next_run
            # Should be ~1 hour in the future
            expected = original_next_run + timedelta(hours=1)
            assert abs((periodic.next_run - expected).total_seconds()) < 1

    def test_sets_last_run(self, pq: PQ) -> None:
        """Worker sets last_run after processing."""
        from sqlalchemy import select

        pq.schedule(periodic_noop_handler, run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_worker:periodic_noop_handler"
                )
            ).scalar_one()
            assert periodic.last_run is None

        pq.run_worker_once()

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_worker:periodic_noop_handler"
                )
            ).scalar_one()
            assert periodic.last_run is not None

    def test_skips_future_periodic(self, pq: PQ) -> None:
        """Worker skips periodic tasks not yet due."""
        from sqlalchemy import update

        pq.schedule(periodic_noop_handler, run_every=timedelta(hours=1))

        # Move next_run to the future
        with pq.session() as session:
            session.execute(
                update(Periodic)
                .where(Periodic.name == "tests.test_worker:periodic_noop_handler")
                .values(next_run=datetime.now(UTC) + timedelta(hours=1))
            )

        processed = pq.run_worker_once()

        assert processed is False

    def test_periodic_keeps_running(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Periodic task can be run multiple times."""
        results = manager.list()
        _set_shared_results(results)

        # Schedule with 0 interval so it's always ready
        pq.schedule(counter_handler, run_every=timedelta(seconds=0))

        pq.run_worker_once()
        pq.run_worker_once()
        pq.run_worker_once()

        assert len(results) == 3
        assert pq.periodic_count() == 1  # Still exists

    def test_periodic_failure_advances_schedule(self, pq: PQ) -> None:
        """Periodic task advances schedule even on failure."""
        from sqlalchemy import select

        pq.schedule(failing_handler, run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_worker:failing_handler"
                )
            ).scalar_one()
            original_next_run = periodic.next_run

        pq.run_worker_once()

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_worker:failing_handler"
                )
            ).scalar_one()
            assert periodic.next_run > original_next_run

    def test_overdue_periodic_runs_once(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Overdue periodic task runs once, not historically."""
        from sqlalchemy import update

        count = manager.list()
        _set_shared_results(count)

        # Schedule with 1 hour interval
        pq.schedule(counter_handler, run_every=timedelta(hours=1))

        # Manually set next_run to 3 hours ago (simulating missed runs)
        with pq.session() as session:
            session.execute(
                update(Periodic)
                .where(Periodic.name == "tests.test_worker:counter_handler")
                .values(next_run=datetime.now(UTC) - timedelta(hours=3))
            )

        # Run worker multiple times
        pq.run_worker_once()
        pq.run_worker_once()
        pq.run_worker_once()

        # Should only run ONCE (not 3+ times to catch up)
        assert len(count) == 1


class TestAsyncTasks:
    """Tests for async task handler support."""

    def test_processes_async_one_off_task(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker processes an async one-off task."""
        results = manager.list()
        _set_shared_results(results)

        pq.enqueue(async_capture_handler, value="async_test")
        processed = pq.run_worker_once()

        assert processed is True
        assert list(results) == ["async_test"]

    def test_processes_async_periodic_task(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker processes an async periodic task."""
        results = manager.list()
        _set_shared_results(results)

        pq.schedule(async_periodic_handler, run_every=timedelta(hours=1), n=1)

        processed = pq.run_worker_once()

        assert processed is True
        assert list(results) == [1]

    def test_async_task_failure_handled(self, pq: PQ) -> None:
        """Worker handles async task failures correctly."""
        pq.enqueue(async_failing_handler)
        assert pq.pending_count() == 1

        pq.run_worker_once()

        # Task should be marked as failed
        assert pq.pending_count() == 0


class TestTaskTimeout:
    """Tests for task timeout functionality."""

    def test_async_task_timeout(self, pq: PQ) -> None:
        """Async task that exceeds timeout is terminated."""
        pq.enqueue(slow_async_handler)
        # Use 0.1 second timeout - task should timeout
        pq.run_worker_once(max_runtime=0.1)

        # Task should be removed (marked failed)
        assert pq.pending_count() == 0

    def test_sync_task_timeout(self, pq: PQ) -> None:
        """Sync task that exceeds timeout is terminated."""
        pq.enqueue(slow_sync_handler)
        # Use 1 second timeout (SIGALRM has 1-second granularity)
        pq.run_worker_once(max_runtime=1)

        # Task should be removed (marked failed)
        assert pq.pending_count() == 0
