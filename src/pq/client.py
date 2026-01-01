"""PQ client - main interface for task queue."""

from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import create_engine, delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from pq.models import Base, Periodic, Task
from pq.priority import Priority
from pq.registry import TaskRegistry, get_function_path


class PQ:
    """Postgres-backed task queue client."""

    def __init__(self, database_url: str) -> None:
        """Initialize PQ with database connection.

        Args:
            database_url: PostgreSQL connection string.
        """
        self._engine: Engine = create_engine(database_url)
        self._session_factory = sessionmaker(bind=self._engine)
        self._registry = TaskRegistry()

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

    def task(self, name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator to register a task handler.

        Args:
            name: Task name to register.

        Returns:
            Decorator function.
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self._registry.register(name, func)
            return func

        return decorator

    def register(self, name: str, handler: Callable[[dict[str, Any]], None]) -> None:
        """Register a task handler explicitly.

        Args:
            name: Task name.
            handler: Function to call with payload.
        """
        self._registry.register(name, handler)

    def enqueue(
        self,
        task: str | Callable[..., Any],
        payload: dict[str, Any] | None = None,
        *,
        run_at: datetime | None = None,
        priority: Priority = Priority.NORMAL,
    ) -> int:
        """Enqueue a one-off task.

        Args:
            task: Task name (string) or callable function.
            payload: Data to pass to the task handler.
            run_at: When to run the task. Defaults to now.
            priority: Task priority. Lower = higher priority. Defaults to NORMAL.

        Returns:
            Task ID.
        """
        if callable(task):
            name = get_function_path(task)
        else:
            name = task

        if payload is None:
            payload = {}

        if run_at is None:
            run_at = datetime.now(UTC)

        task_obj = Task(name=name, payload=payload, run_at=run_at, priority=priority)

        with self.session() as session:
            session.add(task_obj)
            session.flush()
            return task_obj.id

    def schedule(
        self,
        name: str,
        *,
        run_every: timedelta,
        payload: dict[str, Any] | None = None,
        priority: Priority = Priority.NORMAL,
    ) -> int:
        """Schedule a periodic task.

        If a periodic task with this name already exists, it will be updated.

        Args:
            name: Task name.
            run_every: Interval between executions.
            payload: Data to pass to the task handler.
            priority: Task priority. Lower = higher priority. Defaults to NORMAL.

        Returns:
            Periodic task ID.
        """
        if payload is None:
            payload = {}

        next_run = datetime.now(UTC)

        with self.session() as session:
            stmt = (
                insert(Periodic)
                .values(
                    name=name,
                    payload=payload,
                    priority=priority,
                    run_every=run_every,
                    next_run=next_run,
                )
                .on_conflict_do_update(
                    index_elements=["name"],
                    set_={
                        "payload": payload,
                        "priority": priority,
                        "run_every": run_every,
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

    def unschedule(self, name: str) -> bool:
        """Remove a periodic task by name.

        Args:
            name: Task name.

        Returns:
            True if task was found and deleted, False otherwise.
        """
        with self.session() as session:
            stmt = delete(Periodic).where(Periodic.name == name)
            result = session.execute(stmt)
            return result.rowcount > 0

    def pending_count(self) -> int:
        """Count pending one-off tasks."""
        with self.session() as session:
            result = session.execute(select(func.count()).select_from(Task))
            return result.scalar_one()

    def periodic_count(self) -> int:
        """Count periodic task schedules."""
        with self.session() as session:
            result = session.execute(select(func.count()).select_from(Periodic))
            return result.scalar_one()

    def run_worker(
        self, *, poll_interval: float = 1.0, max_runtime: float = 30 * 60
    ) -> None:
        """Run the worker loop (blocking).

        Args:
            poll_interval: Seconds to sleep between polls when idle.
            max_runtime: Maximum execution time per task in seconds. Default: 30 min.
        """
        from pq.worker import run_worker

        run_worker(self, poll_interval=poll_interval, max_runtime=max_runtime)

    def run_worker_once(self, *, max_runtime: float = 30 * 60) -> bool:
        """Process a single task if available.

        Args:
            max_runtime: Maximum execution time per task in seconds. Default: 30 min.

        Returns:
            True if a task was processed, False if queue was empty.
        """
        from pq.worker import run_worker_once

        return run_worker_once(self, max_runtime=max_runtime)
