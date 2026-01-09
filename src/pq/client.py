"""PQ client - main interface for task queue."""

import importlib.resources
from collections.abc import Callable, Set
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Any, Self

from croniter import croniter
from croniter.croniter import CroniterBadCronError
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

    def close(self) -> None:
        """Close all database connections and dispose the engine.

        This closes all connections in the connection pool. After calling
        this method, the PQ instance should not be used.
        """
        self._engine.dispose()

    def __enter__(self) -> Self:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit context manager and close connections."""
        self.close()

    def run_db_migrations(self) -> None:
        """Run database migrations to latest version.

        Call this once at application startup before using the queue.
        Uses Alembic to apply any pending migrations. Safe to call
        multiple times - only pending migrations are applied.

        Example:
            pq = PQ("postgresql://localhost/mydb")
            pq.run_db_migrations()
        """
        # Lazy import to avoid fork issues on macOS
        from alembic import command
        from alembic.config import Config

        # Get migrations directory from within the installed package
        migrations_pkg = importlib.resources.files("pq.migrations")
        migrations_dir = str(migrations_pkg)

        alembic_cfg = Config()
        alembic_cfg.set_main_option("script_location", migrations_dir)
        alembic_cfg.set_main_option(
            "sqlalchemy.url", self._engine.url.render_as_string(hide_password=False)
        )
        command.upgrade(alembic_cfg, "head")

    def create_tables(self) -> None:
        """Create all tables directly (for testing only).

        For production, use run_db_migrations() instead. This method
        bypasses Alembic and creates tables directly via SQLAlchemy,
        which doesn't track schema versions.
        """
        Base.metadata.create_all(self._engine)

    def drop_tables(self) -> None:
        """Drop all tables (for testing only)."""
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
        client_id: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Enqueue a one-off task.

        Args:
            task: Callable function to execute.
            *args: Positional arguments to pass to the handler.
            run_at: When to run the task. Defaults to now.
            priority: Task priority. Higher = higher priority. Defaults to NORMAL.
            client_id: Optional client-provided identifier. Must be unique if provided.
            **kwargs: Keyword arguments to pass to the handler.

        Returns:
            Task ID.

        Raises:
            ValueError: If task is a lambda, closure, or cannot be imported.
            IntegrityError: If client_id already exists.
        """
        name = get_function_path(task)
        payload = serialize(args, kwargs)

        if run_at is None:
            run_at = datetime.now(UTC)

        task_obj = Task(
            name=name,
            payload=payload,
            run_at=run_at,
            priority=priority,
            client_id=client_id,
        )

        with self.session() as session:
            session.add(task_obj)
            session.flush()
            return task_obj.id

    def schedule(
        self,
        task: Callable[..., Any],
        *args: Any,
        run_every: timedelta | None = None,
        cron: str | croniter | None = None,
        priority: Priority = Priority.NORMAL,
        client_id: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Schedule a periodic task.

        If a periodic task with this function already exists, it will be updated.
        Either run_every or cron must be provided, but not both.

        Args:
            task: Callable function to execute.
            *args: Positional arguments to pass to the handler.
            run_every: Interval between executions (e.g., timedelta(hours=1)).
            cron: Cron expression string (e.g., "0 9 * * 1") or croniter object.
            priority: Task priority. Higher = higher priority. Defaults to NORMAL.
            client_id: Optional client-provided identifier. Must be unique if provided.
            **kwargs: Keyword arguments to pass to the handler.

        Returns:
            Periodic task ID.

        Raises:
            ValueError: If neither run_every nor cron is provided, or if both are.
            ValueError: If cron expression is invalid.
            ValueError: If task is a lambda, closure, or cannot be imported.
            IntegrityError: If client_id already exists.
        """
        if run_every is None and cron is None:
            raise ValueError("Either run_every or cron must be provided")
        if run_every is not None and cron is not None:
            raise ValueError("Only one of run_every or cron can be provided")

        # Validate and normalize cron expression
        cron_expr: str | None = None
        if cron is not None:
            if isinstance(cron, croniter):
                # Extract expression from croniter object
                cron_expr = " ".join(str(f) for f in cron.expressions)
            else:
                # Validate string expression
                try:
                    croniter(cron)
                except (KeyError, ValueError, CroniterBadCronError) as e:
                    raise ValueError(f"Invalid cron expression '{cron}': {e}") from e
                cron_expr = cron

        name = get_function_path(task)
        payload = serialize(args, kwargs)

        # Calculate next_run based on cron or interval
        now = datetime.now(UTC)
        if cron_expr:
            cron_iter = croniter(cron_expr, now)
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
                    cron=cron_expr,
                    next_run=next_run,
                    client_id=client_id,
                )
                .on_conflict_do_update(
                    index_elements=["name"],
                    set_={
                        "payload": payload,
                        "priority": priority,
                        "run_every": run_every,
                        "cron": cron_expr,
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
            task = session.get(Task, task_id)
            if task:
                session.expunge(task)
            return task

    def get_task_by_client_id(self, client_id: str) -> Task | None:
        """Get a task by client_id.

        Args:
            client_id: Client-provided identifier.

        Returns:
            Task object or None if not found.
        """
        with self.session() as session:
            stmt = select(Task).where(Task.client_id == client_id)
            task = session.execute(stmt).scalar_one_or_none()
            if task:
                session.expunge(task)
            return task

    def get_periodic_by_client_id(self, client_id: str) -> Periodic | None:
        """Get a periodic task by client_id.

        Args:
            client_id: Client-provided identifier.

        Returns:
            Periodic object or None if not found.
        """
        with self.session() as session:
            stmt = select(Periodic).where(Periodic.client_id == client_id)
            periodic = session.execute(stmt).scalar_one_or_none()
            if periodic:
                session.expunge(periodic)
            return periodic

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
            tasks = list(session.execute(stmt).scalars().all())
            for task in tasks:
                session.expunge(task)
            return tasks

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
            tasks = list(session.execute(stmt).scalars().all())
            for task in tasks:
                session.expunge(task)
            return tasks

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
        self,
        *,
        poll_interval: float = 1.0,
        max_runtime: float = 30 * 60,
        priorities: Set[Priority] | None = None,
    ) -> None:
        """Run the worker loop (blocking).

        Each task executes in a forked child process for memory isolation.

        Args:
            poll_interval: Seconds to sleep between polls when idle.
            max_runtime: Maximum execution time per task in seconds. Default: 30 min.
            priorities: If set, only process tasks with these priority levels.
                Use this to dedicate workers to specific priority tiers.
        """
        from pq.worker import run_worker

        run_worker(
            self,
            poll_interval=poll_interval,
            max_runtime=max_runtime,
            priorities=priorities,
        )

    def run_worker_once(
        self,
        *,
        max_runtime: float = 30 * 60,
        priorities: Set[Priority] | None = None,
    ) -> bool:
        """Process a single task if available.

        Each task executes in a forked child process for memory isolation.

        Args:
            max_runtime: Maximum execution time per task in seconds. Default: 30 min.
            priorities: If set, only process tasks with these priority levels.

        Returns:
            True if a task was processed, False if queue was empty.
        """
        from pq.worker import run_worker_once

        return run_worker_once(self, max_runtime=max_runtime, priorities=priorities)
