"""Tests for PQ client."""

import pytest
from croniter import croniter
from datetime import UTC, datetime, timedelta
from sqlalchemy.exc import IntegrityError

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


class TestScheduleMaxConcurrent:
    """Tests for max_concurrent parameter in schedule."""

    def test_schedule_with_max_concurrent(self, pq: PQ) -> None:
        """Schedule stores max_concurrent in DB."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), max_concurrent=1)

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.max_concurrent == 1

    def test_schedule_with_max_concurrent_none(self, pq: PQ) -> None:
        """Schedule stores max_concurrent=None for unlimited concurrency."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), max_concurrent=None)

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.max_concurrent is None

    def test_schedule_upserts_max_concurrent(self, pq: PQ) -> None:
        """Schedule upsert updates max_concurrent value."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), max_concurrent=1)
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), max_concurrent=None)

        assert pq.periodic_count() == 1

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.max_concurrent is None

    def test_schedule_max_concurrent_invalid_raises(self, pq: PQ) -> None:
        """Schedule with max_concurrent > 1 raises ValueError."""
        with pytest.raises(ValueError, match="max_concurrent must be 1 or None"):
            pq.schedule(cleanup_handler, run_every=timedelta(hours=1), max_concurrent=2)

    def test_schedule_max_concurrent_default(self, pq: PQ) -> None:
        """Schedule without max_concurrent defaults to 1."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.max_concurrent == 1


class TestScheduleActive:
    """Tests for active parameter in schedule."""

    def test_schedule_active_defaults_true(self, pq: PQ) -> None:
        """Schedule stores active=True by default."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.active is True

    def test_schedule_active_false(self, pq: PQ) -> None:
        """Schedule stores active=False when explicitly set."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), active=False)

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.active is False

    def test_schedule_upserts_active(self, pq: PQ) -> None:
        """Schedule upsert updates active flag."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), active=True)
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), active=False)

        assert pq.periodic_count() == 1

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.active is False

    def test_schedule_upserts_active_reactivate(self, pq: PQ) -> None:
        """Schedule upsert can re-enable an inactive task."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), active=False)
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), active=True)

        assert pq.periodic_count() == 1

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.active is True


class TestPeriodicKey:
    """Tests for periodic task key discriminator."""

    def test_different_keys_create_separate_entries(self, pq: PQ) -> None:
        """Same function with different keys creates separate periodic entries."""
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), key="us")
        pq.schedule(cleanup_handler, run_every=timedelta(hours=2), key="eu")

        assert pq.periodic_count() == 2

    def test_same_key_upserts(self, pq: PQ) -> None:
        """Same function + same key upserts (updates) the existing entry."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), key="region")
        pq.schedule(cleanup_handler, run_every=timedelta(hours=2), key="region")

        assert pq.periodic_count() == 1

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.run_every == timedelta(hours=2)

    def test_unschedule_with_key_removes_only_that_entry(self, pq: PQ) -> None:
        """Unschedule with key only removes the matching entry."""
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), key="us")
        pq.schedule(cleanup_handler, run_every=timedelta(hours=2), key="eu")
        assert pq.periodic_count() == 2

        result = pq.unschedule(cleanup_handler, key="us")

        assert result is True
        assert pq.periodic_count() == 1

    def test_unschedule_without_key_removes_default_entry(self, pq: PQ) -> None:
        """Unschedule without key only removes the default-key entry."""
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))
        pq.schedule(cleanup_handler, run_every=timedelta(hours=2), key="eu")
        assert pq.periodic_count() == 2

        result = pq.unschedule(cleanup_handler)

        assert result is True
        assert pq.periodic_count() == 1

    def test_default_key_is_empty_string(self, pq: PQ) -> None:
        """Omitting key stores empty string in DB."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.key == ""


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

    def test_unschedule_by_name_string(self, pq: PQ) -> None:
        """Unschedule accepts a 'module:name' string for tasks whose module no longer exists."""
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))
        assert pq.periodic_count() == 1

        result = pq.unschedule("tests.test_client:cleanup_handler")

        assert result is True
        assert pq.periodic_count() == 0

    def test_unschedule_by_name_string_nonexistent_returns_false(self, pq: PQ) -> None:
        """Unschedule with a string returns False when no matching task exists."""
        result = pq.unschedule("no.such.module:gone_function")
        assert result is False

    def test_unschedule_nonexistent_returns_false(self, pq: PQ) -> None:
        """Unschedule returns False for nonexistent function."""
        result = pq.unschedule(dummy_handler)
        assert result is False


class TestClientId:
    """Tests for client_id functionality."""

    def test_enqueue_with_client_id(self, pq: PQ) -> None:
        """Enqueue stores client_id correctly."""
        task_id = pq.enqueue(dummy_handler, client_id="my-task-1")

        task = pq.get_task(task_id)
        assert task is not None
        assert task.client_id == "my-task-1"

    def test_enqueue_duplicate_client_id_raises(self, pq: PQ) -> None:
        """Enqueue with duplicate client_id raises IntegrityError."""
        pq.enqueue(dummy_handler, client_id="unique-id")

        with pytest.raises(IntegrityError):
            pq.enqueue(dummy_handler, client_id="unique-id")

    def test_enqueue_without_client_id(self, pq: PQ) -> None:
        """Enqueue without client_id sets it to None."""
        task_id = pq.enqueue(dummy_handler)

        task = pq.get_task(task_id)
        assert task is not None
        assert task.client_id is None

    def test_schedule_with_client_id(self, pq: PQ) -> None:
        """Schedule stores client_id correctly."""
        pq.schedule(
            cleanup_handler, run_every=timedelta(hours=1), client_id="periodic-1"
        )

        periodic = pq.get_periodic_by_client_id("periodic-1")
        assert periodic is not None
        assert periodic.client_id == "periodic-1"

    def test_schedule_upsert_preserves_client_id(self, pq: PQ) -> None:
        """Schedule upsert does not overwrite client_id."""
        pq.schedule(
            cleanup_handler, run_every=timedelta(hours=1), client_id="original-id"
        )
        pq.schedule(cleanup_handler, run_every=timedelta(hours=2))

        periodic = pq.get_periodic_by_client_id("original-id")
        assert periodic is not None
        assert periodic.run_every == timedelta(hours=2)

    def test_get_task_by_client_id(self, pq: PQ) -> None:
        """get_task_by_client_id returns correct task."""
        task_id = pq.enqueue(dummy_handler, client_id="lookup-test")

        task = pq.get_task_by_client_id("lookup-test")
        assert task is not None
        assert task.id == task_id

    def test_get_task_by_client_id_not_found(self, pq: PQ) -> None:
        """get_task_by_client_id returns None for non-existent client_id."""
        task = pq.get_task_by_client_id("does-not-exist")
        assert task is None

    def test_get_periodic_by_client_id(self, pq: PQ) -> None:
        """get_periodic_by_client_id returns correct periodic."""
        periodic_id = pq.schedule(
            cleanup_handler, run_every=timedelta(hours=1), client_id="periodic-lookup"
        )

        periodic = pq.get_periodic_by_client_id("periodic-lookup")
        assert periodic is not None
        assert periodic.id == periodic_id

    def test_get_periodic_by_client_id_not_found(self, pq: PQ) -> None:
        """get_periodic_by_client_id returns None for non-existent client_id."""
        periodic = pq.get_periodic_by_client_id("does-not-exist")
        assert periodic is None

    def test_multiple_tasks_null_client_id(self, pq: PQ) -> None:
        """Multiple tasks with null client_id are allowed."""
        task_id_1 = pq.enqueue(dummy_handler)
        task_id_2 = pq.enqueue(dummy_handler)

        assert task_id_1 != task_id_2
        assert pq.pending_count() == 2


def upsert_handler(value: int = 0) -> None:
    """Handler for upsert tests."""
    pass


def failing_upsert_handler() -> None:
    """Failing handler for upsert tests."""
    raise ValueError("boom")


class TestUpsert:
    """Tests for upsert method."""

    def test_upsert_creates_new_task(self, pq: PQ) -> None:
        """Upsert creates a new task when client_id doesn't exist."""
        task_id = pq.upsert(upsert_handler, value=42, client_id="new-task")

        assert task_id is not None
        assert pq.pending_count() == 1

        task = pq.get_task_by_client_id("new-task")
        assert task is not None
        assert task.id == task_id
        assert task.payload["kwargs"] == {"value": 42}

    def test_upsert_updates_existing_task(self, pq: PQ) -> None:
        """Upsert updates task when client_id already exists."""
        # Create initial task
        task_id_1 = pq.upsert(upsert_handler, value=1, client_id="my-task")

        # Upsert with same client_id
        task_id_2 = pq.upsert(upsert_handler, value=2, client_id="my-task")

        # Should still have only 1 task
        assert pq.pending_count() == 1
        # Should return the same task ID
        assert task_id_1 == task_id_2

        task = pq.get_task_by_client_id("my-task")
        assert task is not None
        # Should have updated payload
        assert task.payload["kwargs"] == {"value": 2}

    def test_upsert_resets_status_to_pending(self, pq: PQ) -> None:
        """Upsert resets status to PENDING on conflict."""
        from pq.models import TaskStatus

        # Create and process task
        pq.upsert(dummy_handler, client_id="reset-test")
        pq.run_worker_once()

        # Task should be completed
        task = pq.get_task_by_client_id("reset-test")
        assert task is not None
        assert task.status == TaskStatus.COMPLETED

        # Upsert same client_id
        pq.upsert(dummy_handler, client_id="reset-test")

        # Status should be reset to PENDING
        task = pq.get_task_by_client_id("reset-test")
        assert task is not None
        assert task.status == TaskStatus.PENDING

    def test_upsert_resets_attempts_to_zero(self, pq: PQ) -> None:
        """Upsert resets attempts to 0 on conflict."""
        # Create and process task
        pq.upsert(dummy_handler, client_id="attempts-test")
        pq.run_worker_once()

        task = pq.get_task_by_client_id("attempts-test")
        assert task is not None
        assert task.attempts == 1

        # Upsert same client_id
        pq.upsert(dummy_handler, client_id="attempts-test")

        task = pq.get_task_by_client_id("attempts-test")
        assert task is not None
        assert task.attempts == 0

    def test_upsert_clears_timestamps(self, pq: PQ) -> None:
        """Upsert clears started_at and completed_at on conflict."""
        # Create and process task
        pq.upsert(dummy_handler, client_id="timestamps-test")
        pq.run_worker_once()

        task = pq.get_task_by_client_id("timestamps-test")
        assert task is not None
        assert task.started_at is not None
        assert task.completed_at is not None

        # Upsert same client_id
        pq.upsert(dummy_handler, client_id="timestamps-test")

        task = pq.get_task_by_client_id("timestamps-test")
        assert task is not None
        assert task.started_at is None
        assert task.completed_at is None

    def test_upsert_clears_error(self, pq: PQ) -> None:
        """Upsert clears error field on conflict."""
        from pq.models import TaskStatus

        pq.upsert(failing_upsert_handler, client_id="error-test")
        pq.run_worker_once()

        task = pq.get_task_by_client_id("error-test")
        assert task is not None
        assert task.status == TaskStatus.FAILED
        assert task.error is not None

        # Upsert same client_id with different handler
        pq.upsert(dummy_handler, client_id="error-test")

        task = pq.get_task_by_client_id("error-test")
        assert task is not None
        assert task.error is None

    def test_upsert_updates_priority(self, pq: PQ) -> None:
        """Upsert updates priority on conflict."""
        from pq.priority import Priority

        pq.upsert(dummy_handler, client_id="priority-test", priority=Priority.LOW)

        task = pq.get_task_by_client_id("priority-test")
        assert task is not None
        assert task.priority == Priority.LOW.value

        pq.upsert(dummy_handler, client_id="priority-test", priority=Priority.HIGH)

        task = pq.get_task_by_client_id("priority-test")
        assert task is not None
        assert task.priority == Priority.HIGH.value

    def test_upsert_updates_run_at(self, pq: PQ) -> None:
        """Upsert updates run_at on conflict."""
        now = datetime.now(UTC)
        future = now + timedelta(hours=2)

        pq.upsert(dummy_handler, client_id="run-at-test", run_at=now)

        task = pq.get_task_by_client_id("run-at-test")
        assert task is not None
        assert abs((task.run_at - now).total_seconds()) < 1

        pq.upsert(dummy_handler, client_id="run-at-test", run_at=future)

        task = pq.get_task_by_client_id("run-at-test")
        assert task is not None
        assert abs((task.run_at - future).total_seconds()) < 1

    def test_upsert_returns_int_id(self, pq: PQ) -> None:
        """Upsert returns an integer ID."""
        task_id = pq.upsert(dummy_handler, client_id="int-test")
        assert isinstance(task_id, int)
        assert task_id > 0


class TestReapStaleTasks:
    """Tests for reap_stale_tasks method."""

    def test_reaps_stale_running_task(self, pq: PQ) -> None:
        """RUNNING task older than threshold is reaped to FAILED."""
        from sqlalchemy import update

        from pq.models import TaskStatus

        task_id = pq.enqueue(dummy_handler, client_id="stale-1")

        # Simulate: worker claimed it and then died
        with pq.session() as session:
            session.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(
                    status=TaskStatus.RUNNING,
                    started_at=datetime.now(UTC) - timedelta(hours=2),
                )
            )

        reaped = pq.reap_stale_tasks(timedelta(hours=1))

        assert reaped == 1
        task = pq.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED
        assert task.completed_at is not None
        assert "Reaped" in (task.error or "")
        assert "Worker likely died" in (task.error or "")

    def test_does_not_reap_recent_running_task(self, pq: PQ) -> None:
        """RUNNING task within threshold is not reaped."""
        from sqlalchemy import update

        from pq.models import TaskStatus

        task_id = pq.enqueue(dummy_handler, client_id="recent-1")

        # Simulate: worker claimed it 5 min ago (still within 1h threshold)
        with pq.session() as session:
            session.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(
                    status=TaskStatus.RUNNING,
                    started_at=datetime.now(UTC) - timedelta(minutes=5),
                )
            )

        reaped = pq.reap_stale_tasks(timedelta(hours=1))

        assert reaped == 0
        task = pq.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.RUNNING

    def test_does_not_reap_non_running_tasks(self, pq: PQ) -> None:
        """Only RUNNING tasks are reaped — PENDING, COMPLETED, and FAILED are untouched."""
        from sqlalchemy import update as sa_update

        from pq.models import TaskStatus

        # PENDING
        pq.enqueue(dummy_handler, client_id="task-pending")

        # COMPLETED (enqueue + process)
        pq.enqueue(dummy_handler, client_id="task-completed")
        pq.run_worker_once()

        # FAILED (manually set — simulates a previously failed task)
        failed_id = pq.enqueue(dummy_handler, client_id="task-failed")
        with pq.session() as session:
            session.execute(
                sa_update(Task)
                .where(Task.id == failed_id)
                .values(
                    status=TaskStatus.FAILED,
                    started_at=datetime.now(UTC) - timedelta(hours=2),
                    completed_at=datetime.now(UTC) - timedelta(hours=2),
                    error="original failure",
                )
            )

        reaped = pq.reap_stale_tasks(timedelta(hours=1))

        assert reaped == 0

    def test_reaps_multiple_stale_tasks(self, pq: PQ) -> None:
        """Multiple stale tasks are reaped in a single call."""
        from sqlalchemy import update

        from pq.models import TaskStatus

        stale_started = datetime.now(UTC) - timedelta(hours=3)
        for i in range(5):
            task_id = pq.enqueue(dummy_handler, client_id=f"multi-stale-{i}")
            with pq.session() as session:
                session.execute(
                    update(Task)
                    .where(Task.id == task_id)
                    .values(
                        status=TaskStatus.RUNNING,
                        started_at=stale_started,
                    )
                )

        reaped = pq.reap_stale_tasks(timedelta(hours=1))

        assert reaped == 5
        failed = pq.list_failed()
        assert len(failed) == 5

    def test_returns_zero_when_no_stale_tasks(self, pq: PQ) -> None:
        """Returns 0 when there are no stale tasks."""
        pq.enqueue(dummy_handler)  # PENDING, not RUNNING

        reaped = pq.reap_stale_tasks(timedelta(hours=1))

        assert reaped == 0


class TestMaxRuntimeOverrideValidation:
    """``max_runtime <= 0`` is rejected at the call site.

    Why fail fast: ``signal.alarm(int(max_runtime) + 1)`` would fire at
    1 s for ``0`` and at 0 s (disabling the alarm) for negative integer
    ≥ -1; ``max_runtime * 2`` in the reaper would make the per-row
    threshold a no-op silently; periodic ``lock_duration``'s ``≤ 0``
    fallback to 3600 s is for the worker-level default, not a caller
    contract. None of these are useful behaviours to expose, so the
    client rejects them up front.
    """

    @pytest.mark.parametrize("bad_value", [0, 0.0, -1, -0.001, -1e9])
    def test_enqueue_rejects_non_positive_max_runtime(
        self, pq: PQ, bad_value: float
    ) -> None:
        with pytest.raises(ValueError, match=r"max_runtime must be > 0"):
            pq.enqueue(dummy_handler, max_runtime=bad_value)

    @pytest.mark.parametrize("bad_value", [0, -1])
    def test_upsert_rejects_non_positive_max_runtime(
        self, pq: PQ, bad_value: float
    ) -> None:
        with pytest.raises(ValueError, match=r"max_runtime must be > 0"):
            pq.upsert(dummy_handler, max_runtime=bad_value, client_id="reject-ups")

    @pytest.mark.parametrize("bad_value", [0, -1])
    def test_schedule_rejects_non_positive_max_runtime(
        self, pq: PQ, bad_value: float
    ) -> None:
        with pytest.raises(ValueError, match=r"max_runtime must be > 0"):
            pq.schedule(
                cleanup_handler,
                run_every=timedelta(hours=1),
                max_runtime=bad_value,
            )

    def test_enqueue_accepts_tiny_positive_max_runtime(self, pq: PQ) -> None:
        """``0.001`` (1 ms) is a valid value — at the boundary of the
        validator. Acceptance is the contract; the worker's
        ``signal.alarm`` resolution floor is a separate concern
        (documented on the worker)."""
        task_id = pq.enqueue(dummy_handler, max_runtime=0.001)
        with pq.session() as session:
            task = session.get(Task, task_id)
            assert task is not None
            assert task.max_runtime_seconds == 0.001

    def test_enqueue_accepts_none_explicitly(self, pq: PQ) -> None:
        """``None`` is the contract for 'use worker default' and must
        NOT raise — verifies the validator's early-return path."""
        task_id = pq.enqueue(dummy_handler, max_runtime=None)
        with pq.session() as session:
            task = session.get(Task, task_id)
            assert task is not None
            assert task.max_runtime_seconds is None


class TestMaxRuntimeOverridePersistence:
    """Tests for per-task ``max_runtime`` override storage.

    Verifies that ``Client.enqueue``, ``Client.upsert`` and ``Client.schedule``
    correctly persist the new ``max_runtime_seconds`` column on the
    ``Task`` and ``Periodic`` rows. NULL when not provided (the common
    case, which preserves pre-feature behaviour for every existing call
    site); the supplied float when provided.
    """

    def test_enqueue_persists_max_runtime_seconds(self, pq: PQ) -> None:
        task_id = pq.enqueue(
            dummy_handler, max_runtime=172_800.0, client_id="override-enq-1"
        )
        with pq.session() as session:
            task = session.get(Task, task_id)
            assert task is not None
            assert task.max_runtime_seconds == 172_800.0

    def test_enqueue_without_max_runtime_stores_null(self, pq: PQ) -> None:
        """Backward-compat: every existing enqueue call site (which never
        passes the new kwarg) keeps producing rows with NULL in the new
        column. That NULL is what the worker treats as 'use my default'."""
        task_id = pq.enqueue(dummy_handler, client_id="override-enq-null")
        with pq.session() as session:
            task = session.get(Task, task_id)
            assert task is not None
            assert task.max_runtime_seconds is None

    def test_upsert_persists_max_runtime_seconds(self, pq: PQ) -> None:
        task_id = pq.upsert(
            dummy_handler, max_runtime=3600.0, client_id="override-ups-1"
        )
        with pq.session() as session:
            task = session.get(Task, task_id)
            assert task is not None
            assert task.max_runtime_seconds == 3600.0

    def test_upsert_overwrites_max_runtime_on_conflict(self, pq: PQ) -> None:
        """On client_id conflict, ``upsert`` rewrites every field
        including ``max_runtime_seconds``. Verifies an existing value
        can be replaced AND that re-upserting with ``None`` clears it
        back to NULL (so the same call site can opt-out later)."""
        first_id = pq.upsert(
            dummy_handler, max_runtime=10.0, client_id="override-ups-conflict"
        )
        second_id = pq.upsert(
            dummy_handler, max_runtime=999.0, client_id="override-ups-conflict"
        )
        assert first_id == second_id
        with pq.session() as session:
            task = session.get(Task, second_id)
            assert task is not None
            assert task.max_runtime_seconds == 999.0

        # Now clear it back to NULL
        third_id = pq.upsert(dummy_handler, client_id="override-ups-conflict")
        assert third_id == second_id
        with pq.session() as session:
            task = session.get(Task, third_id)
            assert task is not None
            assert task.max_runtime_seconds is None

    def test_schedule_persists_max_runtime_seconds(self, pq: PQ) -> None:
        """The periodic side of the API supports the same override."""
        periodic_id = pq.schedule(
            cleanup_handler,
            run_every=timedelta(hours=1),
            max_runtime=7200.0,
            client_id="override-sched-1",
        )
        with pq.session() as session:
            periodic = session.get(Periodic, periodic_id)
            assert periodic is not None
            assert periodic.max_runtime_seconds == 7200.0

    def test_schedule_without_max_runtime_stores_null(self, pq: PQ) -> None:
        periodic_id = pq.schedule(
            cleanup_handler,
            run_every=timedelta(hours=1),
            client_id="override-sched-null",
        )
        with pq.session() as session:
            periodic = session.get(Periodic, periodic_id)
            assert periodic is not None
            assert periodic.max_runtime_seconds is None

    def test_schedule_overwrites_max_runtime_on_conflict(self, pq: PQ) -> None:
        """Re-scheduling the same (name, key) rewrites the override —
        analogous to the upsert behaviour."""
        pq.schedule(
            cleanup_handler,
            run_every=timedelta(hours=1),
            max_runtime=10.0,
            key="override-sched-conflict",
        )
        pq.schedule(
            cleanup_handler,
            run_every=timedelta(hours=1),
            max_runtime=999.0,
            key="override-sched-conflict",
        )
        from sqlalchemy import select

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(Periodic.key == "override-sched-conflict")
            ).scalar_one()
            assert periodic.max_runtime_seconds == 999.0

        # And clears back to NULL when the kwarg is omitted on re-schedule
        pq.schedule(
            cleanup_handler,
            run_every=timedelta(hours=1),
            key="override-sched-conflict",
        )
        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(Periodic.key == "override-sched-conflict")
            ).scalar_one()
            assert periodic.max_runtime_seconds is None


class TestReapStaleTasksRespectsPerTaskOverride:
    """Tests that the reaper SQL honours per-task ``max_runtime_seconds``.

    The reaper takes a default ``threshold`` (used for tasks without
    an override) and switches to ``max(default_threshold, 2 *
    max_runtime_seconds)`` per row. This means a legitimately
    long-running task that declared a big budget is NOT reaped before
    the budget * 2 elapses — even if the worker default would have
    reaped it sooner. Tasks without an override keep the previous
    behaviour exactly.
    """

    def test_does_not_reap_long_task_within_per_task_threshold(self, pq: PQ) -> None:
        """Task with ``max_runtime=3600`` (1h) started 30 min ago is NOT
        reaped, even when the reaper's default threshold is 10 min.
        Pre-feature, the row would have been reaped (started_at + 10min
        already past)."""
        from sqlalchemy import update

        from pq.models import TaskStatus

        task_id = pq.enqueue(dummy_handler, max_runtime=3600.0, client_id="long-task-1")
        with pq.session() as session:
            session.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(
                    status=TaskStatus.RUNNING,
                    started_at=datetime.now(UTC) - timedelta(minutes=30),
                )
            )

        # Default threshold = 10 min. The per-task budget * 2 = 2h, so
        # the row's effective threshold is 2h, and 30 min < 2h → not stale.
        reaped = pq.reap_stale_tasks(timedelta(minutes=10))

        assert reaped == 0
        task = pq.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.RUNNING

    def test_reaps_long_task_once_per_task_threshold_elapses(self, pq: PQ) -> None:
        """Same task as above, but started long enough ago to exceed
        ``2 * max_runtime_seconds``: gets reaped."""
        from sqlalchemy import update

        from pq.models import TaskStatus

        task_id = pq.enqueue(dummy_handler, max_runtime=60.0, client_id="long-task-2")
        # 60s budget × 2 = 120s threshold; started_at was 10 min ago.
        with pq.session() as session:
            session.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(
                    status=TaskStatus.RUNNING,
                    started_at=datetime.now(UTC) - timedelta(minutes=10),
                )
            )

        # Default threshold = 1 min. Per-task threshold = 2 min.
        # Effective per-row = max(1, 2) = 2 min. 10 min > 2 min → reaped.
        reaped = pq.reap_stale_tasks(timedelta(minutes=1))

        assert reaped == 1
        task = pq.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED

    def test_reaps_unoverridden_task_with_default_threshold(self, pq: PQ) -> None:
        """Task without ``max_runtime_seconds`` (NULL): the reaper falls
        back to the supplied default threshold. This is the
        backward-compat path — covers every existing call site that
        doesn't pass the new kwarg."""
        from sqlalchemy import update

        from pq.models import TaskStatus

        task_id = pq.enqueue(dummy_handler, client_id="null-override-stale")
        with pq.session() as session:
            session.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(
                    status=TaskStatus.RUNNING,
                    started_at=datetime.now(UTC) - timedelta(hours=2),
                )
            )

        reaped = pq.reap_stale_tasks(timedelta(hours=1))

        assert reaped == 1
        task = pq.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED

    def test_default_threshold_acts_as_floor_when_larger_than_per_task(
        self, pq: PQ
    ) -> None:
        """When the supplied default threshold is LARGER than ``2 *
        per_task``, the default still applies. This prevents a tiny
        per-task budget from accidentally tightening the reaper window
        below what the worker fleet expects."""
        from sqlalchemy import update

        from pq.models import TaskStatus

        # Tiny per-task budget (10s, so 2× = 20s) but default threshold
        # is 1h. Effective per-row = max(1h, 20s) = 1h.
        task_id = pq.enqueue(dummy_handler, max_runtime=10.0, client_id="tiny-override")
        with pq.session() as session:
            session.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(
                    status=TaskStatus.RUNNING,
                    started_at=datetime.now(UTC) - timedelta(minutes=30),
                )
            )

        # 30 min < 1h default → not reaped (despite per-task * 2 = 20s
        # being exceeded), because the default is the floor.
        reaped = pq.reap_stale_tasks(timedelta(hours=1))

        assert reaped == 0
        task = pq.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.RUNNING

    def test_mixed_tasks_reaped_independently(self, pq: PQ) -> None:
        """A batch with mixed overrides — verifies the reaper applies
        the per-row condition correctly, not a single threshold across
        all rows."""
        from sqlalchemy import update

        from pq.models import TaskStatus

        # All started 30 min ago.
        started = datetime.now(UTC) - timedelta(minutes=30)
        long_task = pq.enqueue(
            dummy_handler, max_runtime=3600.0, client_id="mixed-long"
        )
        null_task = pq.enqueue(dummy_handler, client_id="mixed-null")
        short_task = pq.enqueue(
            dummy_handler, max_runtime=60.0, client_id="mixed-short"
        )
        with pq.session() as session:
            for task_id in (long_task, null_task, short_task):
                session.execute(
                    update(Task)
                    .where(Task.id == task_id)
                    .values(status=TaskStatus.RUNNING, started_at=started)
                )

        # Default threshold = 10 min.
        # long_task: per-row = max(10min, 2h) = 2h → 30min < 2h, NOT reaped
        # null_task: per-row = 10min default → 30min > 10min, REAPED
        # short_task: per-row = max(10min, 2min) = 10min → 30min > 10min, REAPED
        reaped = pq.reap_stale_tasks(timedelta(minutes=10))

        assert reaped == 2
        assert pq.get_task(long_task).status == TaskStatus.RUNNING  # type: ignore[union-attr]
        assert pq.get_task(null_task).status == TaskStatus.FAILED  # type: ignore[union-attr]
        assert pq.get_task(short_task).status == TaskStatus.FAILED  # type: ignore[union-attr]

    def test_reaper_error_message_includes_per_row_max_runtime(self, pq: PQ) -> None:
        """The ``error`` written by the reaper names this row's own
        ``max_runtime_seconds`` AND the effective threshold that
        actually fired — so debugging "why did MY long-budget task
        get reaped?" doesn't need a separate row lookup.

        Two reaped rows in one call: one with a per-task override,
        one without. The error column on each should reflect that
        row's specific values, not a single shared message.
        """
        from sqlalchemy import update

        from pq.models import TaskStatus

        started = datetime.now(UTC) - timedelta(hours=4)
        with_override = pq.enqueue(
            dummy_handler, max_runtime=300.0, client_id="msg-with"
        )
        without_override = pq.enqueue(dummy_handler, client_id="msg-without")
        with pq.session() as session:
            for task_id in (with_override, without_override):
                session.execute(
                    update(Task)
                    .where(Task.id == task_id)
                    .values(status=TaskStatus.RUNNING, started_at=started)
                )

        reaped = pq.reap_stale_tasks(timedelta(hours=1))
        assert reaped == 2

        with_msg = pq.get_task(with_override).error or ""  # type: ignore[union-attr]
        without_msg = pq.get_task(without_override).error or ""  # type: ignore[union-attr]

        # Both messages start with the common prefix.
        assert "Reaped: task still RUNNING" in with_msg
        assert "Reaped: task still RUNNING" in without_msg

        # Per-row max_runtime_seconds value appears in each.
        assert "max_runtime_seconds=300" in with_msg, f"expected '300' in {with_msg!r}"
        assert "max_runtime_seconds=NULL" in without_msg, (
            f"expected 'NULL' in {without_msg!r}"
        )

        # Effective threshold reflects the reaper math: with-override
        # got max(3600, 600)=3600 s; without-override got 3600 s default.
        assert "effective threshold=3600" in with_msg
        assert "effective threshold=3600" in without_msg


class TestMigrationAppliesOnPopulatedTables:
    """Verifies the ``aee3e8e7e647`` migration applies cleanly to
    already-populated ``pq_tasks`` and ``pq_periodic`` tables and
    leaves all existing rows intact with NULL in the new column.

    This is the deployment-time scenario: production pq_tasks already
    has thousands of rows (the worker fleet is running, customers are
    enqueueing) when the migration runs. The migration must not lose
    or rewrite data.
    """

    def test_migration_downgrade_then_upgrade_preserves_rows(
        self, pq: PQ, db_url: str
    ) -> None:
        from alembic import command
        from alembic.config import Config
        import importlib.resources

        from pq.models import TaskStatus

        migrations_pkg = importlib.resources.files("pq.migrations")
        cfg = Config()
        cfg.set_main_option("script_location", str(migrations_pkg))
        cfg.set_main_option("sqlalchemy.url", db_url)

        # The ``pq`` fixture uses ``create_tables()`` (which bypasses
        # Alembic), so the ``pq_schema_version`` tracking table doesn't
        # exist yet. Stamp at head so the migration history matches the
        # schema before we exercise the downgrade → upgrade cycle.
        command.stamp(cfg, "head")

        # The schema gets downgraded mid-test. Wrap the whole flow in
        # try/finally so a failed assertion below doesn't leave the
        # shared test database (per-session, not per-test) without the
        # ``max_runtime_seconds`` column — that would cascade-fail every
        # subsequent test in the suite that touches pq_tasks / pq_periodic.
        try:
            # Downgrade past our migration — simulates a production database
            # at the previous head before this PR is deployed.
            command.downgrade(cfg, "-1")

            # Insert rows under the OLD schema (no max_runtime_seconds
            # column). Real-world deployment shape — production already
            # has thousands of rows when the migration is applied. Use
            # raw SQL with the enum NAMES (``'PENDING'``) because the
            # Postgres enum stores names not values (see
            # ``initial_schema`` migration: ``sa.Enum("PENDING", ...)``).
            from sqlalchemy import text

            with pq.session() as session:
                session.execute(
                    text(
                        "INSERT INTO pq_tasks (name, payload, priority, status,"
                        " run_at, client_id, attempts) VALUES (:name,"
                        " '{}'::jsonb, 50, 'PENDING', now(), :client_id, 0)"
                    ),
                    {"name": "tests.dummy", "client_id": "during-old-schema"},
                )
                session.execute(
                    text(
                        "INSERT INTO pq_periodic (name, key, payload, priority,"
                        " run_every, next_run, client_id, active) VALUES"
                        " (:name, '', '{}'::jsonb, 50, '1 hour'::interval,"
                        " now(), :client_id, true)"
                    ),
                    {"name": "tests.dummy", "client_id": "during-old-schema-p"},
                )

            # Upgrade again — the real production migration path.
            command.upgrade(cfg, "head")

            # Rows seeded under the old schema must survive intact AND
            # now have a NULL value in the newly-added column. NULL is
            # exactly what the worker treats as "use my configured
            # default", so the migration is backwards-compatible by
            # construction.
            from sqlalchemy import select

            with pq.session() as session:
                old_task = session.execute(
                    select(Task).where(Task.client_id == "during-old-schema")
                ).scalar_one()
                assert old_task.max_runtime_seconds is None
                assert old_task.status == TaskStatus.PENDING

                old_periodic = session.execute(
                    select(Periodic).where(Periodic.client_id == "during-old-schema-p")
                ).scalar_one()
                assert old_periodic.max_runtime_seconds is None

            # And the override mechanic works post-upgrade — proves
            # the column wasn't just added but is actually wired into
            # enqueue.
            new_id = pq.enqueue(
                dummy_handler, max_runtime=999.0, client_id="post-migration"
            )
            with pq.session() as session:
                new_task = session.get(Task, new_id)
                assert new_task is not None
                assert new_task.max_runtime_seconds == 999.0
        finally:
            # Belt-and-braces: even if any assertion above raised, push
            # the schema back to head before this test releases its
            # session. Subsequent tests assume head schema.
            command.upgrade(cfg, "head")
