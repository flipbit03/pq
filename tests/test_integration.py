"""Integration tests for PQ - end-to-end scenarios."""

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from pq.client import PQ


class TestEndToEnd:
    """End-to-end integration tests."""

    def test_enqueue_and_process(self, pq: PQ) -> None:
        """Full flow: enqueue -> worker processes -> task deleted."""
        results: list[dict[str, Any]] = []

        @pq.task("capture")
        def capture(payload: dict[str, Any]) -> None:
            results.append(payload)

        pq.enqueue("capture", {"value": 42})
        pq.run_worker_once()

        assert results == [{"value": 42}]
        assert pq.pending_count() == 0

    def test_scheduled_task_waits(self, pq: PQ) -> None:
        """Future task not processed until run_at."""
        results: list[dict[str, Any]] = []

        @pq.task("delayed")
        def delayed(payload: dict[str, Any]) -> None:
            results.append(payload)

        future = datetime.now(UTC) + timedelta(hours=1)
        pq.enqueue("delayed", {"v": 1}, run_at=future)
        pq.run_worker_once()

        assert results == []  # Not yet
        assert pq.pending_count() == 1

    def test_periodic_executes_repeatedly(self, pq: PQ) -> None:
        """Periodic task runs multiple times."""
        count: list[int] = []

        @pq.task("counter")
        def counter(payload: dict[str, Any]) -> None:
            count.append(1)

        pq.schedule("counter", run_every=timedelta(seconds=0))  # Immediate

        pq.run_worker_once()
        pq.run_worker_once()
        pq.run_worker_once()

        assert len(count) == 3

    def test_concurrent_workers_no_duplicate(self, pq: PQ) -> None:
        """Multiple workers don't process same task twice."""
        results: list[int] = []

        @pq.task("once")
        def once(payload: dict[str, Any]) -> None:
            results.append(payload["id"])
            time.sleep(0.1)  # Simulate work

        # Enqueue single task
        pq.enqueue("once", {"id": 1})

        # Two workers race
        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = [ex.submit(pq.run_worker_once) for _ in range(2)]
            for f in futures:
                f.result()

        assert results == [1]  # Exactly once

    def test_concurrent_workers_process_different_tasks(self, pq: PQ) -> None:
        """Multiple workers process different tasks concurrently."""
        results: list[int] = []

        @pq.task("work")
        def work(payload: dict[str, Any]) -> None:
            time.sleep(0.05)  # Simulate work
            results.append(payload["id"])

        # Enqueue multiple tasks
        for i in range(5):
            pq.enqueue("work", {"id": i})

        # Process with multiple workers
        with ThreadPoolExecutor(max_workers=3) as ex:
            # Submit more workers than tasks to ensure racing
            futures = [ex.submit(pq.run_worker_once) for _ in range(10)]
            for f in futures:
                f.result()

        # All tasks should be processed exactly once
        assert sorted(results) == [0, 1, 2, 3, 4]

    def test_failed_task_logged_and_deleted(
        self, pq: PQ, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Failed task is logged and removed."""

        @pq.task("fail")
        def fail(payload: dict[str, Any]) -> None:
            raise ValueError("boom")

        pq.enqueue("fail", {})

        with caplog.at_level(logging.ERROR):
            pq.run_worker_once()

        assert "boom" in caplog.text
        assert pq.pending_count() == 0

    def test_mixed_one_off_and_periodic(self, pq: PQ) -> None:
        """Worker handles both one-off and periodic tasks."""
        one_off_results: list[int] = []
        periodic_results: list[int] = []

        @pq.task("one_off")
        def one_off(payload: dict[str, Any]) -> None:
            one_off_results.append(payload["n"])

        @pq.task("periodic")
        def periodic(payload: dict[str, Any]) -> None:
            periodic_results.append(payload["n"])

        # Schedule both
        pq.enqueue("one_off", {"n": 1})
        pq.enqueue("one_off", {"n": 2})
        pq.schedule("periodic", run_every=timedelta(seconds=0), payload={"n": 100})

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
        results: list[int] = []

        @pq.task("work")
        def work(payload: dict[str, Any]) -> None:
            results.append(payload["n"])

        task_id = pq.enqueue("work", {"n": 1})
        pq.cancel(task_id)

        pq.run_worker_once()

        assert results == []

    def test_unschedule_stops_periodic(self, pq: PQ) -> None:
        """Unscheduled periodic task stops running."""
        count: list[int] = []

        @pq.task("counter")
        def counter(payload: dict[str, Any]) -> None:
            count.append(1)

        pq.schedule("counter", run_every=timedelta(seconds=0))

        pq.run_worker_once()
        pq.run_worker_once()

        pq.unschedule("counter")

        pq.run_worker_once()
        pq.run_worker_once()

        # Only 2 runs before unschedule
        assert len(count) == 2


class TestPayloadTypes:
    """Tests for various payload types."""

    def test_empty_payload(self, pq: PQ) -> None:
        """Empty payload works."""
        results: list[dict[str, Any]] = []

        @pq.task("empty")
        def handler(payload: dict[str, Any]) -> None:
            results.append(payload)

        pq.enqueue("empty", {})
        pq.run_worker_once()

        assert results == [{}]

    def test_complex_payload(self, pq: PQ) -> None:
        """Complex nested payload is preserved."""
        results: list[dict[str, Any]] = []

        @pq.task("complex")
        def handler(payload: dict[str, Any]) -> None:
            results.append(payload)

        complex_payload = {
            "string": "value",
            "number": 42,
            "float": 3.14,
            "bool": True,
            "null": None,
            "list": [1, 2, 3],
            "nested": {"a": {"b": {"c": "deep"}}},
        }

        pq.enqueue("complex", complex_payload)
        pq.run_worker_once()

        assert results == [complex_payload]
