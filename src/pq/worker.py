"""Worker loop for processing tasks."""

from __future__ import annotations

import asyncio
import inspect
import signal
import threading
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from croniter import croniter
from loguru import logger
from sqlalchemy import func, select

from pq.models import Periodic, Task, TaskStatus
from pq.serialization import deserialize

if TYPE_CHECKING:
    from collections.abc import Callable

    from pq.client import PQ

# Default max runtime: 30 minutes
DEFAULT_MAX_RUNTIME: float = 30 * 60


class TaskTimeoutError(Exception):
    """Raised when a task exceeds its max runtime."""

    pass


def _timeout_handler(signum: int, frame: Any) -> None:
    """Signal handler for task timeout."""
    raise TaskTimeoutError("Task exceeded max runtime")


def _execute_handler(
    handler: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    max_runtime: float,
) -> None:
    """Execute a handler, supporting both sync and async functions with timeout.

    Args:
        handler: Task handler function.
        args: Positional arguments for handler.
        kwargs: Keyword arguments for handler.
        max_runtime: Maximum execution time in seconds.
    """
    if inspect.iscoroutinefunction(handler):
        asyncio.run(asyncio.wait_for(handler(*args, **kwargs), timeout=max_runtime))
    elif threading.current_thread() is threading.main_thread():
        # Use SIGALRM for sync timeout (Unix only, main thread only)
        old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(int(max_runtime))
        try:
            handler(*args, **kwargs)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
    else:
        # In non-main threads, run without timeout (signals not available)
        handler(*args, **kwargs)


def run_worker(
    pq: PQ, *, poll_interval: float = 1.0, max_runtime: float = DEFAULT_MAX_RUNTIME
) -> None:
    """Run the worker loop indefinitely.

    Args:
        pq: PQ client instance.
        poll_interval: Seconds to sleep between polls when idle.
        max_runtime: Maximum execution time per task in seconds. Default: 30 min.
    """
    logger.info("Starting PQ worker...")
    try:
        while True:
            if not run_worker_once(pq, max_runtime=max_runtime):
                time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Worker stopped.")


def run_worker_once(pq: PQ, *, max_runtime: float = DEFAULT_MAX_RUNTIME) -> bool:
    """Process a single task if available.

    Checks one-off tasks first, then periodic tasks.

    Args:
        pq: PQ client instance.
        max_runtime: Maximum execution time per task in seconds. Default: 30 min.

    Returns:
        True if a task was processed, False if queue was empty.
    """
    # Try one-off task first
    if _process_one_off_task(pq, max_runtime=max_runtime):
        return True

    # Try periodic task
    if _process_periodic_task(pq, max_runtime=max_runtime):
        return True

    return False


def _process_one_off_task(pq: PQ, *, max_runtime: float) -> bool:
    """Claim and process a one-off task.

    Args:
        pq: PQ client instance.
        max_runtime: Maximum execution time in seconds.

    Returns:
        True if a task was processed.
    """
    session = pq._session_factory()
    task = None
    try:
        # Claim highest priority pending task with FOR UPDATE SKIP LOCKED
        stmt = (
            select(Task)
            .where(Task.status == TaskStatus.PENDING)
            .where(Task.run_at <= func.now())
            .order_by(Task.priority.desc(), Task.run_at)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        task = session.execute(stmt).scalar_one_or_none()

        if task is None:
            return False

        # Mark as running
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now(UTC)
        task.attempts += 1
        session.commit()

        # Get task data for execution
        name = task.name
        payload = task.payload
        task_id = task.id

    except Exception as e:
        session.rollback()
        logger.error(f"Error claiming task: {e}")
        return False
    finally:
        session.close()

    # Execute handler outside transaction
    session = pq._session_factory()
    start = time.perf_counter()
    try:
        handler = pq._registry.resolve(name)
        args, kwargs = deserialize(payload)
        _execute_handler(handler, args, kwargs, max_runtime=max_runtime)
        elapsed = time.perf_counter() - start

        # Mark as completed
        task = session.get(Task, task_id)
        if task:
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now(UTC)
            session.commit()

        logger.debug(f"Task '{name}' completed in {elapsed:.3f} s")

    except asyncio.TimeoutError:
        elapsed = time.perf_counter() - start
        task = session.get(Task, task_id)
        if task:
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now(UTC)
            task.error = f"Timed out after {elapsed:.3f} s"
            session.commit()
        logger.error(f"Task '{name}' timed out after {elapsed:.3f} s")

    except TaskTimeoutError:
        elapsed = time.perf_counter() - start
        task = session.get(Task, task_id)
        if task:
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now(UTC)
            task.error = f"Timed out after {elapsed:.3f} s"
            session.commit()
        logger.error(f"Task '{name}' timed out after {elapsed:.3f} s")

    except Exception as e:
        elapsed = time.perf_counter() - start
        task = session.get(Task, task_id)
        if task:
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now(UTC)
            task.error = str(e)
            session.commit()
        logger.error(f"Task '{name}' failed after {elapsed:.3f} s: {e}")

    finally:
        session.close()

    return True


def _calculate_next_run_cron(cron_expr: str) -> datetime:
    """Calculate the next run time using a cron expression.

    Args:
        cron_expr: Cron expression string.

    Returns:
        The next run datetime.
    """
    now = datetime.now(UTC)
    cron = croniter(cron_expr, now)
    return cron.get_next(datetime)


def _process_periodic_task(pq: PQ, *, max_runtime: float) -> bool:
    """Claim and process a periodic task.

    Args:
        pq: PQ client instance.
        max_runtime: Maximum execution time in seconds.

    Returns:
        True if a task was processed.
    """
    session = pq._session_factory()
    name = None
    payload = None

    try:
        # Claim highest priority due periodic task with FOR UPDATE SKIP LOCKED
        stmt = (
            select(Periodic)
            .where(Periodic.next_run <= func.now())
            .order_by(Periodic.priority.desc(), Periodic.next_run)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        periodic = session.execute(stmt).scalar_one_or_none()

        if periodic is None:
            return False

        # Get task data
        name = periodic.name
        payload = periodic.payload

        # Advance schedule BEFORE execution
        periodic.last_run = func.now()
        if periodic.cron:
            periodic.next_run = _calculate_next_run_cron(periodic.cron)
        else:
            periodic.next_run = func.now() + periodic.run_every
        session.commit()

    except Exception as e:
        session.rollback()
        logger.error(f"Error claiming periodic task: {e}")
        return False
    finally:
        session.close()

    # Execute handler outside transaction
    if name is not None:
        start = time.perf_counter()
        try:
            handler = pq._registry.resolve(name)
            args, kwargs = deserialize(payload)
            _execute_handler(handler, args, kwargs, max_runtime=max_runtime)
            elapsed = time.perf_counter() - start
            logger.debug(f"Periodic task '{name}' completed in {elapsed:.3f} s")
        except asyncio.TimeoutError:
            elapsed = time.perf_counter() - start
            logger.error(f"Periodic task '{name}' timed out after {elapsed:.3f} s")
        except TaskTimeoutError:
            elapsed = time.perf_counter() - start
            logger.error(f"Periodic task '{name}' timed out after {elapsed:.3f} s")
        except Exception as e:
            elapsed = time.perf_counter() - start
            logger.error(f"Periodic task '{name}' failed after {elapsed:.3f} s: {e}")
        return True

    return False
