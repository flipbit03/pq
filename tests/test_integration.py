"""Integration tests for PQ - end-to-end scenarios.

These tests verify behavior with fork isolation enabled. Since tasks run
in forked child processes, we use multiprocessing shared state to track
side effects across process boundaries.
"""

import multiprocessing
import multiprocessing.managers
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import BaseModel

from pq.client import PQ

# Global shared state for fork-isolated tests
# These are set up by fixtures and accessed by task handlers in child processes
_shared_results: Any = None
_shared_count: Any = None


def _set_shared_results(results: Any) -> None:
    global _shared_results
    _shared_results = results


def _set_shared_count(count: Any) -> None:
    global _shared_count
    _shared_count = count


class TestEndToEnd:
    """End-to-end integration tests."""

    def test_enqueue_and_process(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Full flow: enqueue -> worker processes -> task completed."""
        results = manager.list()
        _set_shared_results(results)

        @pq.task("capture")
        def capture(value: int) -> None:
            _shared_results.append(value)

        pq.enqueue("capture", value=42)
        pq.run_worker_once()

        assert list(results) == [42]
        assert pq.pending_count() == 0

    def test_scheduled_task_waits(self, pq: PQ) -> None:
        """Future task not processed until run_at."""
        # This test doesn't need shared state - just checks task stays pending

        @pq.task("delayed")
        def delayed(v: int) -> None:
            pass

        future = datetime.now(UTC) + timedelta(hours=1)
        pq.enqueue("delayed", v=1, run_at=future)
        processed = pq.run_worker_once()

        assert processed is False
        assert pq.pending_count() == 1

    def test_periodic_executes_repeatedly(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Periodic task runs multiple times."""
        count = manager.list()
        _set_shared_count(count)

        @pq.task("counter")
        def counter() -> None:
            _shared_count.append(1)

        pq.schedule("counter", run_every=timedelta(seconds=0))  # Immediate

        pq.run_worker_once()
        pq.run_worker_once()
        pq.run_worker_once()

        assert len(count) == 3

    def test_concurrent_workers_no_duplicate(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Multiple workers don't process same task twice."""
        results = manager.list()
        _set_shared_results(results)

        @pq.task("once")
        def once(task_id: int) -> None:
            _shared_results.append(task_id)
            time.sleep(0.1)  # Simulate work

        # Enqueue single task
        pq.enqueue("once", task_id=1)

        # Two workers race
        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = [ex.submit(pq.run_worker_once) for _ in range(2)]
            for f in futures:
                f.result()

        assert list(results) == [1]  # Exactly once

    def test_concurrent_workers_process_different_tasks(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Multiple workers process different tasks concurrently."""
        results = manager.list()
        _set_shared_results(results)

        @pq.task("work")
        def work(task_id: int) -> None:
            time.sleep(0.05)  # Simulate work
            _shared_results.append(task_id)

        # Enqueue multiple tasks
        for i in range(5):
            pq.enqueue("work", task_id=i)

        # Process with multiple workers
        with ThreadPoolExecutor(max_workers=3) as ex:
            # Submit more workers than tasks to ensure racing
            futures = [ex.submit(pq.run_worker_once) for _ in range(10)]
            for f in futures:
                f.result()

        # All tasks should be processed exactly once
        assert sorted(results) == [0, 1, 2, 3, 4]

    def test_failed_task_logged_and_deleted(self, pq: PQ) -> None:
        """Failed task is logged and marked failed."""
        from io import StringIO

        from loguru import logger

        # Capture loguru output
        log_output = StringIO()
        handler_id = logger.add(log_output, format="{message}")

        try:

            @pq.task("fail")
            def fail() -> None:
                raise ValueError("boom")

            pq.enqueue("fail")
            pq.run_worker_once()

            assert "boom" in log_output.getvalue()
            assert pq.pending_count() == 0
        finally:
            logger.remove(handler_id)

    def test_mixed_one_off_and_periodic(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker handles both one-off and periodic tasks."""
        one_off_results = manager.list()
        periodic_results = manager.list()

        # Store in globals for child process access
        global _one_off_results, _periodic_results
        _one_off_results = one_off_results
        _periodic_results = periodic_results

        @pq.task("one_off")
        def one_off(n: int) -> None:
            _one_off_results.append(n)

        @pq.task("periodic")
        def periodic(n: int) -> None:
            _periodic_results.append(n)

        # Schedule both
        pq.enqueue("one_off", n=1)
        pq.enqueue("one_off", n=2)
        pq.schedule("periodic", run_every=timedelta(seconds=0), n=100)

        # Process all
        for _ in range(5):
            pq.run_worker_once()

        # One-off tasks processed once each
        assert sorted(one_off_results) == [1, 2]
        # Periodic task processed multiple times
        assert len(periodic_results) >= 3
        assert all(n == 100 for n in periodic_results)

    def test_cancel_prevents_processing(self, pq: PQ) -> None:
        """Cancelled task is not processed."""
        # Just verify task count - no side effects to check

        @pq.task("work")
        def work(n: int) -> None:
            pass

        task_id = pq.enqueue("work", n=1)
        pq.cancel(task_id)

        processed = pq.run_worker_once()

        assert processed is False

    def test_unschedule_stops_periodic(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Unscheduled periodic task stops running."""
        count = manager.list()
        _set_shared_count(count)

        @pq.task("counter")
        def counter() -> None:
            _shared_count.append(1)

        pq.schedule("counter", run_every=timedelta(seconds=0))

        pq.run_worker_once()
        pq.run_worker_once()

        pq.unschedule("counter")

        pq.run_worker_once()
        pq.run_worker_once()

        # Only 2 runs before unschedule
        assert len(count) == 2


# Additional globals for mixed test
_one_off_results: Any = None
_periodic_results: Any = None


class TestPayloadTypes:
    """Tests for various payload types."""

    def test_empty_payload(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Empty payload works."""
        called = manager.list()
        _set_shared_results(called)

        @pq.task("empty")
        def handler() -> None:
            _shared_results.append(True)

        pq.enqueue("empty")
        pq.run_worker_once()

        assert list(called) == [True]

    def test_complex_kwargs(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Complex nested kwargs are preserved."""
        results = manager.list()
        _set_shared_results(results)

        @pq.task("complex")
        def handler(**kwargs: Any) -> None:
            _shared_results.append(dict(kwargs))

        pq.enqueue(
            "complex",
            string="value",
            number=42,
            float_val=3.14,
            bool_val=True,
            null=None,
            items=[1, 2, 3],
            nested={"a": {"b": {"c": "deep"}}},
        )
        pq.run_worker_once()

        assert len(results) == 1
        assert results[0]["string"] == "value"
        assert results[0]["number"] == 42
        assert results[0]["nested"]["a"]["b"]["c"] == "deep"

    def test_positional_args(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Positional arguments work correctly."""
        results = manager.list()
        _set_shared_results(results)

        @pq.task("positional")
        def handler(a: int, b: str, c: list[int]) -> None:
            _shared_results.append((a, b, c))

        pq.enqueue("positional", 1, "hello", [1, 2, 3])
        pq.run_worker_once()

        assert list(results) == [(1, "hello", [1, 2, 3])]

    def test_mixed_args_kwargs(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Mixed positional and keyword arguments work."""
        results = manager.list()
        _set_shared_results(results)

        @pq.task("mixed")
        def handler(a: int, b: str, c: int = 0) -> None:
            _shared_results.append((a, b, c))

        pq.enqueue("mixed", 1, "hello", c=42)
        pq.run_worker_once()

        assert list(results) == [(1, "hello", 42)]

    def test_pydantic_as_arg(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Pydantic model as positional arg is serialized correctly."""

        class UserPayload(BaseModel):
            user_id: int
            email: str

        results = manager.list()
        _set_shared_results(results)

        @pq.task("pydantic")
        def handler(user: dict[str, Any]) -> None:
            _shared_results.append(dict(user))

        payload = UserPayload(user_id=123, email="test@example.com")
        pq.enqueue("pydantic", payload)
        pq.run_worker_once()

        assert len(results) == 1
        assert results[0] == {"user_id": 123, "email": "test@example.com"}

    def test_pydantic_periodic(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Pydantic model works with periodic tasks."""

        class ReportConfig(BaseModel):
            report_type: str
            recipients: list[str]

        results = manager.list()
        _set_shared_results(results)

        @pq.task("pydantic_periodic")
        def handler(config: dict[str, Any]) -> None:
            _shared_results.append(dict(config))

        config = ReportConfig(report_type="daily", recipients=["a@b.com", "c@d.com"])
        pq.schedule("pydantic_periodic", config, run_every=timedelta(seconds=0))
        pq.run_worker_once()

        assert len(results) == 1
        assert results[0] == {
            "report_type": "daily",
            "recipients": ["a@b.com", "c@d.com"],
        }

    def test_pickle_fallback_for_custom_object(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Non-JSON-serializable objects are pickled."""

        class CustomData:
            def __init__(self, value: int) -> None:
                self.value = value

        results = manager.list()
        _set_shared_results(results)

        @pq.task("pickle")
        def handler(data: CustomData) -> None:
            _shared_results.append(data.value)

        pq.enqueue("pickle", CustomData(42))
        pq.run_worker_once()

        assert list(results) == [42]

    def test_pickle_fallback_for_function(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Functions are pickled as fallback."""
        results = manager.list()
        _set_shared_results(results)

        def callback(x: int) -> int:
            return x * 2

        @pq.task("func")
        def handler(cb: Any) -> None:
            _shared_results.append(cb(21))

        pq.enqueue("func", callback)
        pq.run_worker_once()

        assert list(results) == [42]
