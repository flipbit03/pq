"""Integration tests for PQ - end-to-end scenarios."""

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import BaseModel

from pq.client import PQ


class TestEndToEnd:
    """End-to-end integration tests."""

    def test_enqueue_and_process(self, pq: PQ) -> None:
        """Full flow: enqueue -> worker processes -> task completed."""
        results: list[int] = []

        @pq.task("capture")
        def capture(value: int) -> None:
            results.append(value)

        pq.enqueue("capture", value=42)
        pq.run_worker_once()

        assert results == [42]
        assert pq.pending_count() == 0

    def test_scheduled_task_waits(self, pq: PQ) -> None:
        """Future task not processed until run_at."""
        results: list[int] = []

        @pq.task("delayed")
        def delayed(v: int) -> None:
            results.append(v)

        future = datetime.now(UTC) + timedelta(hours=1)
        pq.enqueue("delayed", v=1, run_at=future)
        pq.run_worker_once()

        assert results == []  # Not yet
        assert pq.pending_count() == 1

    def test_periodic_executes_repeatedly(self, pq: PQ) -> None:
        """Periodic task runs multiple times."""
        count: list[int] = []

        @pq.task("counter")
        def counter() -> None:
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
        def once(task_id: int) -> None:
            results.append(task_id)
            time.sleep(0.1)  # Simulate work

        # Enqueue single task
        pq.enqueue("once", task_id=1)

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
        def work(task_id: int) -> None:
            time.sleep(0.05)  # Simulate work
            results.append(task_id)

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

    def test_mixed_one_off_and_periodic(self, pq: PQ) -> None:
        """Worker handles both one-off and periodic tasks."""
        one_off_results: list[int] = []
        periodic_results: list[int] = []

        @pq.task("one_off")
        def one_off(n: int) -> None:
            one_off_results.append(n)

        @pq.task("periodic")
        def periodic(n: int) -> None:
            periodic_results.append(n)

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
        results: list[int] = []

        @pq.task("work")
        def work(n: int) -> None:
            results.append(n)

        task_id = pq.enqueue("work", n=1)
        pq.cancel(task_id)

        pq.run_worker_once()

        assert results == []

    def test_unschedule_stops_periodic(self, pq: PQ) -> None:
        """Unscheduled periodic task stops running."""
        count: list[int] = []

        @pq.task("counter")
        def counter() -> None:
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
        called: list[bool] = []

        @pq.task("empty")
        def handler() -> None:
            called.append(True)

        pq.enqueue("empty")
        pq.run_worker_once()

        assert called == [True]

    def test_complex_kwargs(self, pq: PQ) -> None:
        """Complex nested kwargs are preserved."""
        results: list[dict[str, Any]] = []

        @pq.task("complex")
        def handler(**kwargs: Any) -> None:
            results.append(kwargs)

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

    def test_positional_args(self, pq: PQ) -> None:
        """Positional arguments work correctly."""
        results: list[tuple[Any, ...]] = []

        @pq.task("positional")
        def handler(a: int, b: str, c: list[int]) -> None:
            results.append((a, b, c))

        pq.enqueue("positional", 1, "hello", [1, 2, 3])
        pq.run_worker_once()

        assert results == [(1, "hello", [1, 2, 3])]

    def test_mixed_args_kwargs(self, pq: PQ) -> None:
        """Mixed positional and keyword arguments work."""
        results: list[tuple[Any, ...]] = []

        @pq.task("mixed")
        def handler(a: int, b: str, c: int = 0) -> None:
            results.append((a, b, c))

        pq.enqueue("mixed", 1, "hello", c=42)
        pq.run_worker_once()

        assert results == [(1, "hello", 42)]

    def test_pydantic_as_arg(self, pq: PQ) -> None:
        """Pydantic model as positional arg is serialized correctly."""

        class UserPayload(BaseModel):
            user_id: int
            email: str

        results: list[dict[str, Any]] = []

        @pq.task("pydantic")
        def handler(user: dict[str, Any]) -> None:
            results.append(user)

        payload = UserPayload(user_id=123, email="test@example.com")
        pq.enqueue("pydantic", payload)
        pq.run_worker_once()

        assert len(results) == 1
        assert results[0] == {"user_id": 123, "email": "test@example.com"}

    def test_pydantic_periodic(self, pq: PQ) -> None:
        """Pydantic model works with periodic tasks."""

        class ReportConfig(BaseModel):
            report_type: str
            recipients: list[str]

        results: list[dict[str, Any]] = []

        @pq.task("pydantic_periodic")
        def handler(config: dict[str, Any]) -> None:
            results.append(config)

        config = ReportConfig(report_type="daily", recipients=["a@b.com", "c@d.com"])
        pq.schedule("pydantic_periodic", config, run_every=timedelta(seconds=0))
        pq.run_worker_once()

        assert len(results) == 1
        assert results[0] == {
            "report_type": "daily",
            "recipients": ["a@b.com", "c@d.com"],
        }

    def test_pickle_fallback_for_custom_object(self, pq: PQ) -> None:
        """Non-JSON-serializable objects are pickled."""

        class CustomData:
            def __init__(self, value: int) -> None:
                self.value = value

        results: list[int] = []

        @pq.task("pickle")
        def handler(data: CustomData) -> None:
            results.append(data.value)

        pq.enqueue("pickle", CustomData(42))
        pq.run_worker_once()

        assert results == [42]

    def test_pickle_fallback_for_function(self, pq: PQ) -> None:
        """Functions are pickled as fallback."""
        results: list[int] = []

        def callback(x: int) -> int:
            return x * 2

        @pq.task("func")
        def handler(cb: Any) -> None:
            results.append(cb(21))

        pq.enqueue("func", callback)
        pq.run_worker_once()

        assert results == [42]
