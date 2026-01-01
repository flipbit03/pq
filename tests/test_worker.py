"""Tests for worker logic with fork isolation.

All tests use multiprocessing shared state since tasks run in forked processes.
"""

import asyncio
import multiprocessing
import multiprocessing.managers
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


# Global handler for testing direct function import - must be defined before use
def tracked_handler(key: str) -> None:
    """Handler that tracks calls for testing."""
    _shared_calls.append((key,))


class TestRunWorkerOnce:
    """Tests for run_worker_once method."""

    def test_processes_pending_task(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker processes a pending task."""
        results = manager.list()
        _set_shared_results(results)

        @pq.task("capture")
        def capture(value: int) -> None:
            _shared_results.append(value)

        pq.enqueue("capture", value=42)
        processed = pq.run_worker_once()

        assert processed is True
        assert list(results) == [42]

    def test_deletes_task_after_processing(self, pq: PQ) -> None:
        """Worker marks task completed after processing."""

        @pq.task("my_task")
        def handler() -> None:
            pass

        pq.enqueue("my_task")
        assert pq.pending_count() == 1

        pq.run_worker_once()

        assert pq.pending_count() == 0

    def test_skips_future_task(self, pq: PQ) -> None:
        """Worker skips tasks scheduled for the future."""

        @pq.task("future_task")
        def handler() -> None:
            pass

        future = datetime.now(UTC) + timedelta(hours=1)
        pq.enqueue("future_task", run_at=future)

        processed = pq.run_worker_once()

        assert processed is False
        assert pq.pending_count() == 1

    def test_returns_false_when_empty(self, pq: PQ) -> None:
        """Worker returns False when no tasks available."""
        processed = pq.run_worker_once()
        assert processed is False

    def test_deletes_task_on_failure(self, pq: PQ) -> None:
        """Worker marks task failed when handler fails."""

        @pq.task("failing_task")
        def handler() -> None:
            raise ValueError("boom")

        pq.enqueue("failing_task")
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

        @pq.task("ordered")
        def handler(n: int) -> None:
            _shared_results.append(n)

        # Enqueue in reverse priority order
        pq.enqueue("ordered", n=3, priority=Priority.LOW)
        pq.enqueue("ordered", n=1, priority=Priority.HIGH)
        pq.enqueue("ordered", n=2, priority=Priority.NORMAL)

        pq.run_worker_once()
        pq.run_worker_once()
        pq.run_worker_once()

        # Should process in priority order: HIGH, NORMAL, LOW
        assert list(results) == [1, 2, 3]


class TestPeriodicTasks:
    """Tests for periodic task processing."""

    def test_processes_periodic_task(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker processes a periodic task."""
        results = manager.list()
        _set_shared_results(results)

        @pq.task("periodic")
        def handler(n: int) -> None:
            _shared_results.append(n)

        pq.schedule("periodic", run_every=timedelta(hours=1), n=1)

        processed = pq.run_worker_once()

        assert processed is True
        assert list(results) == [1]

    def test_advances_next_run(self, pq: PQ) -> None:
        """Worker advances next_run after processing."""
        from sqlalchemy import select

        @pq.task("periodic")
        def handler() -> None:
            pass

        pq.schedule("periodic", run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(Periodic.name == "periodic")
            ).scalar_one()
            original_next_run = periodic.next_run

        pq.run_worker_once()

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(Periodic.name == "periodic")
            ).scalar_one()
            assert periodic.next_run > original_next_run
            # Should be ~1 hour in the future
            expected = original_next_run + timedelta(hours=1)
            assert abs((periodic.next_run - expected).total_seconds()) < 1

    def test_sets_last_run(self, pq: PQ) -> None:
        """Worker sets last_run after processing."""
        from sqlalchemy import select

        @pq.task("periodic")
        def handler() -> None:
            pass

        pq.schedule("periodic", run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(Periodic.name == "periodic")
            ).scalar_one()
            assert periodic.last_run is None

        pq.run_worker_once()

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(Periodic.name == "periodic")
            ).scalar_one()
            assert periodic.last_run is not None

    def test_skips_future_periodic(self, pq: PQ) -> None:
        """Worker skips periodic tasks not yet due."""
        from sqlalchemy import update

        @pq.task("periodic")
        def handler() -> None:
            pass

        pq.schedule("periodic", run_every=timedelta(hours=1))

        # Move next_run to the future
        with pq.session() as session:
            session.execute(
                update(Periodic)
                .where(Periodic.name == "periodic")
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

        @pq.task("counter")
        def counter() -> None:
            _shared_results.append(1)

        # Schedule with 0 interval so it's always ready
        pq.schedule("counter", run_every=timedelta(seconds=0))

        pq.run_worker_once()
        pq.run_worker_once()
        pq.run_worker_once()

        assert len(results) == 3
        assert pq.periodic_count() == 1  # Still exists

    def test_periodic_failure_advances_schedule(self, pq: PQ) -> None:
        """Periodic task advances schedule even on failure."""
        from sqlalchemy import select

        @pq.task("failing")
        def handler() -> None:
            raise ValueError("boom")

        pq.schedule("failing", run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(Periodic.name == "failing")
            ).scalar_one()
            original_next_run = periodic.next_run

        pq.run_worker_once()

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(Periodic.name == "failing")
            ).scalar_one()
            assert periodic.next_run > original_next_run

    def test_overdue_periodic_runs_once(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Overdue periodic task runs once, not historically."""
        from sqlalchemy import update

        count = manager.list()
        _set_shared_results(count)

        @pq.task("overdue")
        def handler() -> None:
            _shared_results.append(1)

        # Schedule with 1 hour interval
        pq.schedule("overdue", run_every=timedelta(hours=1))

        # Manually set next_run to 3 hours ago (simulating missed runs)
        with pq.session() as session:
            session.execute(
                update(Periodic)
                .where(Periodic.name == "overdue")
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

        @pq.task("async_task")
        async def async_handler(value: str) -> None:
            await asyncio.sleep(0.01)  # Simulate async work
            _shared_results.append(value)

        pq.enqueue("async_task", value="async_test")
        processed = pq.run_worker_once()

        assert processed is True
        assert list(results) == ["async_test"]

    def test_processes_async_periodic_task(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker processes an async periodic task."""
        results = manager.list()
        _set_shared_results(results)

        @pq.task("async_periodic")
        async def async_handler(n: int) -> None:
            await asyncio.sleep(0.01)
            _shared_results.append(n)

        pq.schedule("async_periodic", run_every=timedelta(hours=1), n=1)

        processed = pq.run_worker_once()

        assert processed is True
        assert list(results) == [1]

    def test_async_task_failure_handled(self, pq: PQ) -> None:
        """Worker handles async task failures correctly."""

        @pq.task("async_failing")
        async def async_handler() -> None:
            await asyncio.sleep(0.01)
            raise ValueError("async boom")

        pq.enqueue("async_failing")
        assert pq.pending_count() == 1

        pq.run_worker_once()

        # Task should be marked as failed
        assert pq.pending_count() == 0


class TestTaskTimeout:
    """Tests for task timeout functionality."""

    def test_async_task_timeout(self, pq: PQ) -> None:
        """Async task that exceeds timeout is terminated."""

        @pq.task("slow_async")
        async def slow_handler() -> None:
            await asyncio.sleep(10)  # Would take 10 seconds

        pq.enqueue("slow_async")
        # Use 0.1 second timeout - task should timeout
        pq.run_worker_once(max_runtime=0.1)

        # Task should be removed (marked failed)
        assert pq.pending_count() == 0

    def test_sync_task_timeout(self, pq: PQ) -> None:
        """Sync task that exceeds timeout is terminated."""
        import time

        @pq.task("slow_sync")
        def slow_handler() -> None:
            time.sleep(10)  # Would take 10 seconds

        pq.enqueue("slow_sync")
        # Use 1 second timeout (SIGALRM has 1-second granularity)
        pq.run_worker_once(max_runtime=1)

        # Task should be removed (marked failed)
        assert pq.pending_count() == 0
