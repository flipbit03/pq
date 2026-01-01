"""Worker loop for processing tasks."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from loguru import logger
from sqlalchemy import func, select

from pq.models import Periodic, Task

if TYPE_CHECKING:
    from pq.client import PQ


def run_worker(pq: PQ, *, poll_interval: float = 1.0) -> None:
    """Run the worker loop indefinitely.

    Args:
        pq: PQ client instance.
        poll_interval: Seconds to sleep between polls when idle.
    """
    logger.info("Starting PQ worker...")
    try:
        while True:
            if not run_worker_once(pq):
                time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Worker stopped.")


def run_worker_once(pq: PQ) -> bool:
    """Process a single task if available.

    Checks one-off tasks first, then periodic tasks.

    Args:
        pq: PQ client instance.

    Returns:
        True if a task was processed, False if queue was empty.
    """
    # Try one-off task first
    if _process_one_off_task(pq):
        return True

    # Try periodic task
    if _process_periodic_task(pq):
        return True

    return False


def _process_one_off_task(pq: PQ) -> bool:
    """Claim and process a one-off task.

    Args:
        pq: PQ client instance.

    Returns:
        True if a task was processed.
    """
    session = pq._session_factory()
    try:
        # Claim task with FOR UPDATE SKIP LOCKED
        stmt = (
            select(Task)
            .where(Task.run_at <= func.now())
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
        try:
            handler = pq._registry.resolve(name)
            handler(payload)
            logger.debug(f"Task {name} completed successfully")
        except Exception as e:
            logger.error(f"Task {name} failed: {e}")

        return True
    except Exception as e:
        session.rollback()
        logger.error(f"Error processing task: {e}")
        return False
    finally:
        session.close()


def _process_periodic_task(pq: PQ) -> bool:
    """Claim and process a periodic task.

    Args:
        pq: PQ client instance.

    Returns:
        True if a task was processed.
    """
    session = pq._session_factory()
    name = None
    payload = None

    try:
        # Claim periodic task with FOR UPDATE SKIP LOCKED
        stmt = (
            select(Periodic)
            .where(Periodic.next_run <= func.now())
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
        try:
            handler = pq._registry.resolve(name)
            handler(payload)
            logger.debug(f"Periodic task {name} completed successfully")
        except Exception as e:
            logger.error(f"Periodic task {name} failed: {e}")
        return True

    return False
