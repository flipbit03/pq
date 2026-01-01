"""Worker loop for processing tasks."""

from __future__ import annotations

import asyncio
import inspect
import signal
import threading
import time
from typing import TYPE_CHECKING, Any

from loguru import logger
from sqlalchemy import func, select

from pq.models import Periodic, Task

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
    handler: Callable[..., Any], payload: dict[str, Any], *, max_runtime: float
) -> None:
    """Execute a handler, supporting both sync and async functions with timeout.

    Args:
        handler: Task handler function.
        payload: Data to pass to handler.
        max_runtime: Maximum execution time in seconds.
    """
    if inspect.iscoroutinefunction(handler):
        asyncio.run(asyncio.wait_for(handler(payload), timeout=max_runtime))
    elif threading.current_thread() is threading.main_thread():
        # Use SIGALRM for sync timeout (Unix only, main thread only)
        old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(int(max_runtime))
        try:
            handler(payload)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
    else:
        # In non-main threads, run without timeout (signals not available)
        handler(payload)


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
    try:
        # Claim highest priority due task with FOR UPDATE SKIP LOCKED
        stmt = (
            select(Task)
            .where(Task.run_at <= func.now())
            .order_by(Task.priority, Task.run_at)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        task = session.execute(stmt).scalar_one_or_none()

        if task is None:
            return False

        # Get task data before deleting
        name = task.name
        payload = task.payload

        # Delete task (always, even on failure)
        session.delete(task)
        session.commit()

        # Execute handler outside transaction
        start = time.perf_counter()
        try:
            handler = pq._registry.resolve(name)
            _execute_handler(handler, payload, max_runtime=max_runtime)
            elapsed = time.perf_counter() - start
            logger.debug(f"Task '{name}' completed in {elapsed:.3f} s")
        except asyncio.TimeoutError:
            elapsed = time.perf_counter() - start
            logger.error(f"Task '{name}' timed out after {elapsed:.3f} s")
        except TaskTimeoutError:
            elapsed = time.perf_counter() - start
            logger.error(f"Task '{name}' timed out after {elapsed:.3f} s")
        except Exception as e:
            elapsed = time.perf_counter() - start
            logger.error(f"Task '{name}' failed after {elapsed:.3f} s: {e}")

        return True
    except Exception as e:
        session.rollback()
        logger.error(f"Error processing task: {e}")
        return False
    finally:
        session.close()


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
            .order_by(Periodic.priority, Periodic.next_run)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        periodic = session.execute(stmt).scalar_one_or_none()

        if periodic is None:
            return False

        # Get task data
        name = periodic.name
        payload = periodic.payload

        # Advance schedule BEFORE execution (base on now, not old next_run)
        periodic.last_run = func.now()
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
            _execute_handler(handler, payload, max_runtime=max_runtime)
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
