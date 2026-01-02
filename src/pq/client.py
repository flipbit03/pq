"""PQ client - main interface for task queue."""

from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from croniter import croniter
from sqlalchemy import create_engine, delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from pq.models import Base, Periodic, Task, TaskStatus
from pq.priority import Priority
from pq.registry import get_function_path
from pq.serialization import serialize


class PQ:
    """Postgres-backed task queue client."""

    def __init__(self, database_url: str) -> None:
        """Initialize PQ with database connection.

        Args:
            database_url: PostgreSQL connection string.
        """
        self._engine: Engine = create_engine(database_url)
        self._session_factory = sessionmaker(bind=self._engine)

    @contextmanager
    def session(self) -> Any:
        """Get a database session context manager."""
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def create_tables(self) -> None:
        """Create all tables (for testing)."""
        Base.metadata.create_all(self._engine)

    def drop_tables(self) -> None:
        """Drop all tables (for testing)."""
        Base.metadata.drop_all(self._engine)

    def clear_all(self) -> None:
        """Clear all tasks and periodic schedules."""
        with self.session() as session:
            session.execute(delete(Task))
            session.execute(delete(Periodic))

    def enqueue(
        self,
        task: Callable[..., Any],
        *args: Any,
        run_at: datetime | None = None,
        priority: Priority = Priority.NORMAL,
        **kwargs: Any,
    ) -> int:
        """Enqueue a one-off task.

        Args:
            task: Callable function to execute.
            *args: Positional arguments to pass to the handler.
            run_at: When to run the task. Defaults to now.
            priority: Task priority. Higher = higher priority. Defaults to NORMAL.
            **kwargs: Keyword arguments to pass to the handler.

        Returns:
            Task ID.

        Raises:
            ValueError: If task is a lambda, closure, or cannot be imported.
        """
        name = get_function_path(task)
        payload = serialize(args, kwargs)

        if run_at is None:
            run_at = datetime.now(UTC)

        task_obj = Task(name=name, payload=payload, run_at=run_at, priority=priority)

        with self.session() as session:
            session.add(task_obj)
            session.flush()
            return task_obj.id

    def schedule(
        self,
        task: Callable[..., Any],
        *args: Any,
        run_every: timedelta | None = None,
        cron: str | None = None,
        priority: Priority = Priority.NORMAL,
        **kwargs: Any,
    ) -> int:
        """Schedule a periodic task.

        If a periodic task with this function already exists, it will be updated.
        Either run_every or cron must be provided, but not both.

        Args:
            task: Callable function to execute.
            *args: Positional arguments to pass to the handler.
            run_every: Interval between executions (e.g., timedelta(hours=1)).
            cron: Cron expression (e.g., "0 9 * * 1" for Monday 9am).
            priority: Task priority. Higher = higher priority. Defaults to NORMAL.
            **kwargs: Keyword arguments to pass to the handler.

        Returns:
            Periodic task ID.

        Raises:
            ValueError: If neither run_every nor cron is provided, or if both are.
            ValueError: If task is a lambda, closure, or cannot be imported.
        """
        if run_every is None and cron is None:
            raise ValueError("Either run_every or cron must be provided")
        if run_every is not None and cron is not None:
            raise ValueError("Only one of run_every or cron can be provided")

        name = get_function_path(task)
        payload = serialize(args, kwargs)

        # Calculate next_run based on cron or interval
        now = datetime.now(UTC)
        if cron:
            cron_iter = croniter(cron, now)
            next_run = cron_iter.get_next(datetime)
        else:
            next_run = now

        with self.session() as session:
            stmt = (
                insert(Periodic)
                .values(
                    name=name,
                    payload=payload,
                    priority=priority,
                    run_every=run_every,
                    cron=cron,
                    next_run=next_run,
                )
                .on_conflict_do_update(
                    index_elements=["name"],
                    set_={
                        "payload": payload,
                        "priority": priority,
                        "run_every": run_every,
                        "cron": cron,
                        "next_run": next_run,
                    },
                )
                .returning(Periodic.id)
            )
            result = session.execute(stmt)
            return result.scalar_one()

    def cancel(self, task_id: int) -> bool:
        """Cancel a one-off task by ID.

        Args:
            task_id: Task ID.

        Returns:
            True if task was found and deleted, False otherwise.
        """
        with self.session() as session:
            stmt = delete(Task).where(Task.id == task_id)
            result = session.execute(stmt)
            return result.rowcount > 0

    def unschedule(self, task: Callable[..., Any]) -> bool:
        """Remove a periodic task.

        Args:
            task: The scheduled function to remove.

        Returns:
            True if task was found and deleted, False otherwise.
        """
        name = get_function_path(task)
        with self.session() as session:
            stmt = delete(Periodic).where(Periodic.name == name)
            result = session.execute(stmt)
            return result.rowcount > 0

    def pending_count(self) -> int:
        """Count pending one-off tasks."""
        with self.session() as session:
            result = session.execute(
                select(func.count())
                .select_from(Task)
                .where(Task.status == TaskStatus.PENDING)
            )
            return result.scalar_one()

    def periodic_count(self) -> int:
        """Count periodic task schedules."""
        with self.session() as session:
            result = session.execute(select(func.count()).select_from(Periodic))
            return result.scalar_one()

    def get_task(self, task_id: int) -> Task | None:
        """Get a task by ID.

        Args:
            task_id: Task ID.

        Returns:
            Task object or None if not found.
        """
        with self.session() as session:
            return session.get(Task, task_id)

    def list_failed(self, limit: int = 100) -> list[Task]:
        """List failed tasks.

        Args:
            limit: Maximum number of tasks to return.

        Returns:
            List of failed tasks, most recent first.
        """
        with self.session() as session:
            stmt = (
                select(Task)
                .where(Task.status == TaskStatus.FAILED)
                .order_by(Task.completed_at.desc())
                .limit(limit)
            )
            return list(session.execute(stmt).scalars().all())

    def list_completed(self, limit: int = 100) -> list[Task]:
        """List completed tasks.

        Args:
            limit: Maximum number of tasks to return.

        Returns:
            List of completed tasks, most recent first.
        """
        with self.session() as session:
            stmt = (
                select(Task)
                .where(Task.status == TaskStatus.COMPLETED)
                .order_by(Task.completed_at.desc())
                .limit(limit)
            )
            return list(session.execute(stmt).scalars().all())

    def clear_completed(self, before: datetime | None = None) -> int:
        """Clear completed tasks.

        Args:
            before: Only clear tasks completed before this time. If None, clears all.

        Returns:
            Number of tasks deleted.
        """
        with self.session() as session:
            stmt = delete(Task).where(Task.status == TaskStatus.COMPLETED)
            if before is not None:
                stmt = stmt.where(Task.completed_at < before)
            result = session.execute(stmt)
            return result.rowcount

    def clear_failed(self, before: datetime | None = None) -> int:
        """Clear failed tasks.

        Args:
            before: Only clear tasks failed before this time. If None, clears all.

        Returns:
            Number of tasks deleted.
        """
        with self.session() as session:
            stmt = delete(Task).where(Task.status == TaskStatus.FAILED)
            if before is not None:
                stmt = stmt.where(Task.completed_at < before)
            result = session.execute(stmt)
            return result.rowcount

    def run_worker(
        self, *, poll_interval: float = 1.0, max_runtime: float = 30 * 60
    ) -> None:
        """Run the worker loop (blocking).

        Each task executes in a forked child process for memory isolation.

        Args:
            poll_interval: Seconds to sleep between polls when idle.
            max_runtime: Maximum execution time per task in seconds. Default: 30 min.
        """
        from pq.worker import run_worker

        run_worker(self, poll_interval=poll_interval, max_runtime=max_runtime)

    def run_worker_once(self, *, max_runtime: float = 30 * 60) -> bool:
        """Process a single task if available.

        Each task executes in a forked child process for memory isolation.

        Args:
            max_runtime: Maximum execution time per task in seconds. Default: 30 min.

        Returns:
            True if a task was processed, False if queue was empty.
        """
        from pq.worker import run_worker_once

        return run_worker_once(self, max_runtime=max_runtime)
