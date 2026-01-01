"""Tests for PQ client."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID


from pq.client import PQ
from pq.models import Periodic, Task


def dummy_handler(payload: dict[str, Any]) -> None:
    """Dummy handler for testing."""
    pass


class TestEnqueue:
    """Tests for enqueue method."""

    def test_enqueue_creates_task(self, pq: PQ) -> None:
        """Enqueue creates a task in the database."""
        pq.register("my_task", dummy_handler)
        task_id = pq.enqueue("my_task", {"key": "value"})

        assert task_id is not None
        assert pq.pending_count() == 1

    def test_enqueue_stores_correct_data(self, pq: PQ) -> None:
        """Enqueue stores name, payload, and run_at correctly."""
        pq.register("my_task", dummy_handler)
        task_id = pq.enqueue("my_task", {"key": "value"})

        with pq.session() as session:
            from sqlalchemy import select

            task = session.execute(select(Task).where(Task.id == task_id)).scalar_one()
            assert task.name == "my_task"
            assert task.payload == {"key": "value"}
            assert task.run_at <= datetime.now(UTC)

    def test_enqueue_with_run_at(self, pq: PQ) -> None:
        """Enqueue respects custom run_at time."""
        future = datetime.now(UTC) + timedelta(hours=1)
        task_id = pq.enqueue("my_task", {}, run_at=future)

        with pq.session() as session:
            from sqlalchemy import select

            task = session.execute(select(Task).where(Task.id == task_id)).scalar_one()
            # Allow small time drift
            assert abs((task.run_at - future).total_seconds()) < 1

    def test_enqueue_direct_function(self, pq: PQ) -> None:
        """Enqueue with callable stores function path."""
        task_id = pq.enqueue(dummy_handler, {"key": "value"})

        with pq.session() as session:
            from sqlalchemy import select

            task = session.execute(select(Task).where(Task.id == task_id)).scalar_one()
            assert task.name == "tests.test_client:dummy_handler"

    def test_enqueue_returns_valid_uuid(self, pq: PQ) -> None:
        """Enqueue returns a valid UUID string."""
        task_id = pq.enqueue("my_task", {})
        # Should not raise
        UUID(task_id)


class TestSchedule:
    """Tests for schedule method."""

    def test_schedule_creates_periodic(self, pq: PQ) -> None:
        """Schedule creates a periodic task."""
        pq.register("cleanup", dummy_handler)
        periodic_id = pq.schedule("cleanup", run_every=timedelta(hours=1))

        assert periodic_id is not None
        assert pq.periodic_count() == 1

    def test_schedule_stores_correct_data(self, pq: PQ) -> None:
        """Schedule stores name, payload, and run_every correctly."""
        interval = timedelta(hours=2)
        pq.schedule("cleanup", run_every=interval, payload={"full": True})

        with pq.session() as session:
            from sqlalchemy import select

            periodic = session.execute(
                select(Periodic).where(Periodic.name == "cleanup")
            ).scalar_one()
            assert periodic.name == "cleanup"
            assert periodic.payload == {"full": True}
            assert periodic.run_every == interval
            assert periodic.next_run <= datetime.now(UTC)
            assert periodic.last_run is None

    def test_schedule_upserts_existing(self, pq: PQ) -> None:
        """Scheduling same name updates existing record."""
        pq.schedule("cleanup", run_every=timedelta(hours=1))
        pq.schedule("cleanup", run_every=timedelta(hours=2), payload={"new": True})

        assert pq.periodic_count() == 1

        with pq.session() as session:
            from sqlalchemy import select

            periodic = session.execute(
                select(Periodic).where(Periodic.name == "cleanup")
            ).scalar_one()
            assert periodic.run_every == timedelta(hours=2)
            assert periodic.payload == {"new": True}


class TestCancel:
    """Tests for cancel method."""

    def test_cancel_removes_task(self, pq: PQ) -> None:
        """Cancel removes task from database."""
        task_id = pq.enqueue("my_task", {})
        assert pq.pending_count() == 1

        result = pq.cancel(task_id)

        assert result is True
        assert pq.pending_count() == 0

    def test_cancel_nonexistent_returns_false(self, pq: PQ) -> None:
        """Cancel returns False for nonexistent task."""
        result = pq.cancel("00000000-0000-0000-0000-000000000000")
        assert result is False

    def test_cancel_accepts_uuid_object(self, pq: PQ) -> None:
        """Cancel accepts UUID object."""
        task_id = pq.enqueue("my_task", {})
        result = pq.cancel(UUID(task_id))
        assert result is True


class TestUnschedule:
    """Tests for unschedule method."""

    def test_unschedule_removes_periodic(self, pq: PQ) -> None:
        """Unschedule removes periodic task."""
        pq.schedule("cleanup", run_every=timedelta(hours=1))
        assert pq.periodic_count() == 1

        result = pq.unschedule("cleanup")

        assert result is True
        assert pq.periodic_count() == 0

    def test_unschedule_nonexistent_returns_false(self, pq: PQ) -> None:
        """Unschedule returns False for nonexistent task."""
        result = pq.unschedule("nonexistent")
        assert result is False


class TestTaskDecorator:
    """Tests for @pq.task decorator."""

    def test_decorator_registers_handler(self, pq: PQ) -> None:
        """Decorator registers handler in registry."""

        @pq.task("decorated_task")
        def my_handler(payload: dict[str, Any]) -> None:
            pass

        handler = pq._registry.get("decorated_task")
        assert handler is my_handler

    def test_decorator_preserves_function(self, pq: PQ) -> None:
        """Decorator returns original function unchanged."""

        @pq.task("decorated_task")
        def my_handler(payload: dict[str, Any]) -> None:
            pass

        assert my_handler.__name__ == "my_handler"
