"""Worker loop for processing tasks with fork isolation.

Each task runs in a forked child process for memory isolation.
If a task OOMs or crashes, only the child is affected - the worker continues.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import signal
import sys
import time
import traceback
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from croniter import croniter
from loguru import logger
from sqlalchemy import func, select

from pq.models import Periodic, Task, TaskStatus
from pq.registry import resolve_function_path
from pq.serialization import deserialize

if TYPE_CHECKING:
    from collections.abc import Callable, Set

    from pq.client import PQ
    from pq.priority import Priority

# Default max runtime: 30 minutes
DEFAULT_MAX_RUNTIME: float = 30 * 60

# Default retention: 7 days
DEFAULT_RETENTION_DAYS: int = 7

# Default cleanup interval: 1 hour
DEFAULT_CLEANUP_INTERVAL: float = 3600

# Exit codes for child process
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_TIMEOUT = 124  # Like GNU timeout


class WorkerError(Exception):
    """Base class for worker errors."""

    pass


class TaskTimeoutError(WorkerError):
    """Raised when a task exceeds its max runtime."""

    pass


class TaskOOMError(WorkerError):
    """Raised when a task is killed by OOM killer."""

    pass


class TaskKilledError(WorkerError):
    """Raised when a task is killed by a signal."""

    pass


def _child_timeout_handler(signum: int, frame: Any) -> None:
    """Signal handler for timeout in child process."""
    os._exit(EXIT_TIMEOUT)


def _run_in_child(
    handler: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    max_runtime: float,
    error_write_fd: int,
) -> None:
    """Execute handler in child process.

    This function never returns - it always calls os._exit().
    """
    # Create new process group so we don't get parent's signals
    os.setpgrp()

    # Set up timeout
    signal.signal(signal.SIGALRM, _child_timeout_handler)
    signal.alarm(int(max_runtime) + 1)  # +1 buffer for async timeout

    try:
        if inspect.iscoroutinefunction(handler):
            asyncio.run(asyncio.wait_for(handler(*args, **kwargs), timeout=max_runtime))
        else:
            handler(*args, **kwargs)
        os._exit(EXIT_SUCCESS)

    except asyncio.TimeoutError:
        os._exit(EXIT_TIMEOUT)

    except Exception as e:
        # Send error message to parent via pipe
        try:
            error_msg = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            os.write(error_write_fd, error_msg.encode("utf-8", errors="replace"))
        except Exception:
            pass  # Best effort
        os._exit(EXIT_FAILURE)


def _execute_in_fork(
    handler: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    max_runtime: float,
) -> None:
    """Execute handler in a forked child process for isolation.

    The child process has isolated memory, so OOM or crashes only affect
    the child. Parent monitors via os.wait4() and handles various exit
    scenarios.

    Args:
        handler: Task handler function.
        args: Positional arguments for handler.
        kwargs: Keyword arguments for handler.
        max_runtime: Maximum execution time in seconds.

    Raises:
        TaskTimeoutError: If task exceeds max runtime.
        TaskOOMError: If task is killed by OOM killer (SIGKILL).
        TaskKilledError: If task is killed by another signal.
        Exception: If task raises an exception.
    """
    # Create pipe for error message communication
    read_fd, write_fd = os.pipe()

    child_pid = os.fork()

    if child_pid == 0:
        # === CHILD PROCESS ===
        os.close(read_fd)
        _run_in_child(handler, args, kwargs, max_runtime, write_fd)
        # _run_in_child never returns, but just in case:
        os._exit(EXIT_FAILURE)

    else:
        # === PARENT PROCESS ===
        os.close(write_fd)

        # Wait for child to finish
        _, status, rusage = os.wait4(child_pid, 0)

        # Read any error message from child
        error_bytes = b""
        try:
            while True:
                chunk = os.read(read_fd, 4096)
                if not chunk:
                    break
                error_bytes += chunk
        except Exception:
            pass
        finally:
            os.close(read_fd)

        error_msg = error_bytes.decode("utf-8", errors="replace") if error_bytes else ""

        # Check how child exited
        if os.WIFSIGNALED(status):
            signal_num = os.WTERMSIG(status)
            if signal_num == signal.SIGKILL:
                # SIGKILL (9) often means OOM killer
                # ru_maxrss is in KB on Linux, bytes on macOS
                max_rss_kb = rusage.ru_maxrss
                if sys.platform == "darwin":
                    max_rss_kb = max_rss_kb // 1024
                raise TaskOOMError(
                    f"Task killed (likely OOM). Max RSS: {max_rss_kb} KB"
                )
            else:
                raise TaskKilledError(f"Task killed by signal {signal_num}")

        elif os.WIFEXITED(status):
            exit_code = os.WEXITSTATUS(status)
            if exit_code == EXIT_SUCCESS:
                return  # Success!
            elif exit_code == EXIT_TIMEOUT:
                raise TaskTimeoutError("Task exceeded max runtime")
            else:
                # Task raised an exception
                if error_msg:
                    raise Exception(error_msg.split("\n")[0])  # First line
                else:
                    raise Exception(f"Task failed with exit code {exit_code}")


def _maybe_run_cleanup(
    pq: PQ,
    retention_days: int,
    cleanup_interval: float,
    last_cleanup: list[float],
) -> None:
    """Run cleanup if retention is enabled and interval has passed.

    Args:
        pq: PQ client instance.
        retention_days: Days to keep completed/failed tasks. 0 to disable.
        cleanup_interval: Seconds between cleanup runs.
        last_cleanup: Mutable list containing last cleanup timestamp.
    """
    if retention_days <= 0:
        return

    now = time.time()
    if now - last_cleanup[0] < cleanup_interval:
        return

    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    completed = pq.clear_completed(before=cutoff)
    failed = pq.clear_failed(before=cutoff)

    if completed or failed:
        logger.info(f"Cleanup: removed {completed} completed, {failed} failed tasks")

    last_cleanup[0] = now


def run_worker(
    pq: PQ,
    *,
    poll_interval: float = 1.0,
    max_runtime: float = DEFAULT_MAX_RUNTIME,
    priorities: Set[Priority] | None = None,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    cleanup_interval: float = DEFAULT_CLEANUP_INTERVAL,
) -> None:
    """Run the worker loop indefinitely.

    Each task executes in a forked child process for memory isolation.

    Args:
        pq: PQ client instance.
        poll_interval: Seconds to sleep between polls when idle.
        max_runtime: Maximum execution time per task in seconds. Default: 30 min.
        priorities: If set, only process tasks with these priority levels.
            Use this to dedicate workers to specific priority tiers.
        retention_days: Days to keep completed/failed tasks. Default: 7.
            Set to 0 to disable automatic cleanup.
        cleanup_interval: Seconds between cleanup runs. Default: 3600 (1 hour).
    """
    if priorities:
        priority_names = ", ".join(p.name for p in sorted(priorities, reverse=True))
        logger.info(f"Starting PQ worker (priorities: {priority_names})...")
    else:
        logger.info("Starting PQ worker (fork isolation enabled)...")

    last_cleanup: list[float] = [0.0]  # Mutable container for tracking

    try:
        while True:
            if not run_worker_once(pq, max_runtime=max_runtime, priorities=priorities):
                _maybe_run_cleanup(pq, retention_days, cleanup_interval, last_cleanup)
                time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Worker stopped.")


def run_worker_once(
    pq: PQ,
    *,
    max_runtime: float = DEFAULT_MAX_RUNTIME,
    priorities: Set[Priority] | None = None,
) -> bool:
    """Process a single task if available.

    Checks one-off tasks first, then periodic tasks.

    Args:
        pq: PQ client instance.
        max_runtime: Maximum execution time per task in seconds. Default: 30 min.
        priorities: If set, only process tasks with these priority levels.

    Returns:
        True if a task was processed, False if queue was empty.
    """
    # Try one-off task first
    if _process_one_off_task(pq, max_runtime=max_runtime, priorities=priorities):
        return True

    # Try periodic task
    if _process_periodic_task(pq, max_runtime=max_runtime, priorities=priorities):
        return True

    return False


def _process_one_off_task(
    pq: PQ,
    *,
    max_runtime: float,
    priorities: Set[Priority] | None = None,
) -> bool:
    """Claim and process a one-off task.

    Args:
        pq: PQ client instance.
        max_runtime: Maximum execution time in seconds.
        priorities: If set, only process tasks with these priority levels.

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
        )
        if priorities:
            stmt = stmt.where(Task.priority.in_([p.value for p in priorities]))
        stmt = (
            stmt.order_by(Task.priority.desc(), Task.run_at)
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

    # Execute handler in forked process
    session = pq._session_factory()
    start = time.perf_counter()
    try:
        handler = resolve_function_path(name)
        args, kwargs = deserialize(payload)
        _execute_in_fork(handler, args, kwargs, max_runtime=max_runtime)
        elapsed = time.perf_counter() - start

        # Mark as completed
        task = session.get(Task, task_id)
        if task:
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now(UTC)
            session.commit()

        logger.debug(f"Task '{name}' completed in {elapsed:.3f} s")

    except TaskTimeoutError:
        elapsed = time.perf_counter() - start
        task = session.get(Task, task_id)
        if task:
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now(UTC)
            task.error = f"Timed out after {elapsed:.3f} s"
            session.commit()
        logger.error(f"Task '{name}' timed out after {elapsed:.3f} s")

    except TaskOOMError as e:
        elapsed = time.perf_counter() - start
        task = session.get(Task, task_id)
        if task:
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now(UTC)
            task.error = str(e)
            session.commit()
        logger.error(f"Task '{name}' OOM after {elapsed:.3f} s: {e}")

    except TaskKilledError as e:
        elapsed = time.perf_counter() - start
        task = session.get(Task, task_id)
        if task:
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now(UTC)
            task.error = str(e)
            session.commit()
        logger.error(f"Task '{name}' killed after {elapsed:.3f} s: {e}")

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


def _process_periodic_task(
    pq: PQ,
    *,
    max_runtime: float,
    priorities: Set[Priority] | None = None,
) -> bool:
    """Claim and process a periodic task.

    Args:
        pq: PQ client instance.
        max_runtime: Maximum execution time in seconds.
        priorities: If set, only process tasks with these priority levels.

    Returns:
        True if a task was processed.
    """
    session = pq._session_factory()
    name = None
    payload = None

    try:
        # Claim highest priority due periodic task with FOR UPDATE SKIP LOCKED
        stmt = select(Periodic).where(Periodic.next_run <= func.now())
        if priorities:
            stmt = stmt.where(Periodic.priority.in_([p.value for p in priorities]))
        stmt = (
            stmt.order_by(Periodic.priority.desc(), Periodic.next_run)
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

    # Execute handler in forked process
    if name is not None:
        start = time.perf_counter()
        try:
            handler = resolve_function_path(name)
            args, kwargs = deserialize(payload)
            _execute_in_fork(handler, args, kwargs, max_runtime=max_runtime)
            elapsed = time.perf_counter() - start
            logger.debug(f"Periodic task '{name}' completed in {elapsed:.3f} s")

        except TaskTimeoutError:
            elapsed = time.perf_counter() - start
            logger.error(f"Periodic task '{name}' timed out after {elapsed:.3f} s")

        except TaskOOMError as e:
            elapsed = time.perf_counter() - start
            logger.error(f"Periodic task '{name}' OOM after {elapsed:.3f} s: {e}")

        except TaskKilledError as e:
            elapsed = time.perf_counter() - start
            logger.error(f"Periodic task '{name}' killed after {elapsed:.3f} s: {e}")

        except Exception as e:
            elapsed = time.perf_counter() - start
            logger.error(f"Periodic task '{name}' failed after {elapsed:.3f} s: {e}")

        return True

    return False
