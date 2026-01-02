"""Tests for PQ client."""

import pytest
from croniter import croniter
from datetime import UTC, datetime, timedelta

from pq.client import PQ
from pq.models import Periodic, Task


def dummy_handler(key: str = "") -> None:
    """Dummy handler for testing."""
    pass


def cleanup_handler(full: bool = False) -> None:
    """Cleanup handler for testing periodic tasks."""
    pass


def cron_handler() -> None:
    """Handler for cron tests."""
    pass


class TestEnqueue:
    """Tests for enqueue method."""

    def test_enqueue_creates_task(self, pq: PQ) -> None:
        """Enqueue creates a task in the database."""
        task_id = pq.enqueue(dummy_handler, key="value")

        assert task_id is not None
        assert pq.pending_count() == 1

    def test_enqueue_stores_correct_data(self, pq: PQ) -> None:
        """Enqueue stores name, payload, and run_at correctly."""
        task_id = pq.enqueue(dummy_handler, key="value")

        with pq.session() as session:
            from sqlalchemy import select

            task = session.execute(select(Task).where(Task.id == task_id)).scalar_one()
            assert task.name == "tests.test_client:dummy_handler"
            assert task.payload["args"] == []
            assert task.payload["kwargs"] == {"key": "value"}
            assert task.run_at <= datetime.now(UTC)

    def test_enqueue_with_run_at(self, pq: PQ) -> None:
        """Enqueue respects custom run_at time."""
        future = datetime.now(UTC) + timedelta(hours=1)
        task_id = pq.enqueue(dummy_handler, run_at=future)

        with pq.session() as session:
            from sqlalchemy import select

            task = session.execute(select(Task).where(Task.id == task_id)).scalar_one()
            # Allow small time drift
            assert abs((task.run_at - future).total_seconds()) < 1

    def test_enqueue_stores_function_path(self, pq: PQ) -> None:
        """Enqueue stores function path as name."""
        task_id = pq.enqueue(dummy_handler, key="value")

        with pq.session() as session:
            from sqlalchemy import select

            task = session.execute(select(Task).where(Task.id == task_id)).scalar_one()
            assert task.name == "tests.test_client:dummy_handler"

    def test_enqueue_returns_int_id(self, pq: PQ) -> None:
        """Enqueue returns an integer ID."""
        task_id = pq.enqueue(dummy_handler)
        assert isinstance(task_id, int)
        assert task_id > 0


class TestSchedule:
    """Tests for schedule method."""

    def test_schedule_creates_periodic(self, pq: PQ) -> None:
        """Schedule creates a periodic task."""
        periodic_id = pq.schedule(cleanup_handler, run_every=timedelta(hours=1))

        assert periodic_id is not None
        assert pq.periodic_count() == 1

    def test_schedule_stores_correct_data(self, pq: PQ) -> None:
        """Schedule stores name, payload, and run_every correctly."""
        interval = timedelta(hours=2)
        pq.schedule(cleanup_handler, run_every=interval, full=True)

        with pq.session() as session:
            from sqlalchemy import select

            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.name == "tests.test_client:cleanup_handler"
            assert periodic.payload["kwargs"] == {"full": True}
            assert periodic.run_every == interval
            assert periodic.next_run <= datetime.now(UTC)
            assert periodic.last_run is None

    def test_schedule_upserts_existing(self, pq: PQ) -> None:
        """Scheduling same function updates existing record."""
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))
        pq.schedule(cleanup_handler, run_every=timedelta(hours=2), full=True)

        assert pq.periodic_count() == 1

        with pq.session() as session:
            from sqlalchemy import select

            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.run_every == timedelta(hours=2)
            assert periodic.payload["kwargs"] == {"full": True}

    def test_schedule_with_cron_string(self, pq: PQ) -> None:
        """Schedule with valid cron string works."""
        pq.schedule(cron_handler, cron="0 9 * * 1")  # Monday 9am

        with pq.session() as session:
            from sqlalchemy import select

            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cron_handler"
                )
            ).scalar_one()
            assert periodic.cron == "0 9 * * 1"
            assert periodic.run_every is None

    def test_schedule_with_invalid_cron_raises(self, pq: PQ) -> None:
        """Schedule with invalid cron string raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            pq.schedule(cron_handler, cron="invalid cron")

        assert "Invalid cron expression" in str(exc_info.value)
        assert "invalid cron" in str(exc_info.value)

    def test_schedule_with_croniter_object(self, pq: PQ) -> None:
        """Schedule with croniter object works."""
        cron_obj = croniter("30 14 * * 5")  # Friday 2:30pm
        pq.schedule(cron_handler, cron=cron_obj)

        with pq.session() as session:
            from sqlalchemy import select

            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cron_handler"
                )
            ).scalar_one()
            # Expression should be extracted and stored
            assert periodic.cron == "30 14 * * 5"
            assert periodic.run_every is None


class TestCancel:
    """Tests for cancel method."""

    def test_cancel_removes_task(self, pq: PQ) -> None:
        """Cancel removes task from database."""
        task_id = pq.enqueue(dummy_handler)
        assert pq.pending_count() == 1

        result = pq.cancel(task_id)

        assert result is True
        assert pq.pending_count() == 0

    def test_cancel_nonexistent_returns_false(self, pq: PQ) -> None:
        """Cancel returns False for nonexistent task."""
        result = pq.cancel(999999)
        assert result is False


class TestUnschedule:
    """Tests for unschedule method."""

    def test_unschedule_removes_periodic(self, pq: PQ) -> None:
        """Unschedule removes periodic task."""
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))
        assert pq.periodic_count() == 1

        result = pq.unschedule(cleanup_handler)

        assert result is True
        assert pq.periodic_count() == 0

    def test_unschedule_nonexistent_returns_false(self, pq: PQ) -> None:
        """Unschedule returns False for nonexistent function."""
        result = pq.unschedule(dummy_handler)
        assert result is False
