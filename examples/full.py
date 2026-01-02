#!/usr/bin/env python
"""
PQ Example - Comprehensive showcase of all features (sync and async).

Usage:
    uv run examples/full.py full            # Run FULL showcase of all features
    uv run examples/full.py demo            # Run demo (enqueue + process)
    uv run examples/full.py enqueue         # Queue one-off tasks (various methods)
    uv run examples/full.py schedule        # Schedule periodic tasks
    uv run examples/full.py delayed         # Queue a task for the future
    uv run examples/full.py cancel          # Demo cancelling a task
    uv run examples/full.py unschedule      # Demo removing a periodic task
    uv run examples/full.py work            # Run worker (Ctrl+C to stop)
    uv run examples/full.py status          # Show queue status
    uv run examples/full.py clear           # Clear all tasks

=== ENQUEUEING TASKS ===

Pass the function directly:
    pq.enqueue(my_handler, arg1, arg2, kwarg=value)

With positional and keyword args:
    pq.enqueue(greet, "World")
    pq.enqueue(greet, name="World")

Run at specific time:
    pq.enqueue(greet, "Future", run_at=datetime(...))

From imported module:
    from examples.tasks import external_task
    pq.enqueue(external_task, data)

=== PERIODIC TASKS ===

Schedule recurring task:
    pq.schedule(tick, run_every=timedelta(hours=1))

With arguments:
    pq.schedule(report, run_every=timedelta(minutes=5), report_type="daily")

Remove schedule:
    pq.unschedule(tick)

=== PRIORITY ===

Priority levels (higher value = higher priority):
    from pq import Priority
    pq.enqueue(task, priority=Priority.CRITICAL)  # 100
    pq.enqueue(task, priority=Priority.HIGH)      # 75
    pq.enqueue(task, priority=Priority.NORMAL)    # 50 (default)
    pq.enqueue(task, priority=Priority.LOW)       # 25
    pq.enqueue(task, priority=Priority.BATCH)     # 0
"""

import asyncio
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

from loguru import logger

from pq import PQ

# Import from sibling module - demonstrates cross-module function reference
# When run as script, we need to handle the import specially
try:
    from examples.tasks import external_task
except ModuleNotFoundError:
    from tasks import external_task  # type: ignore[import-not-found]

# =============================================================================
# SETUP
# =============================================================================

pq = PQ("postgresql://postgres:postgres@localhost:5433/postgres")
pq.create_tables()


# =============================================================================
# TASK HANDLERS
# =============================================================================


def greet(name: str) -> None:
    """Simple greeting task."""
    logger.info(f"Hello, {name}!")


def add(a: int, b: int) -> None:
    """Math task."""
    result = a + b
    logger.info(f"{a} + {b} = {result}")


def tick() -> None:
    """Periodic heartbeat task."""
    logger.info("Tick!")


def report(report_type: str = "default") -> None:
    """Periodic report task with payload."""
    logger.info(f"Generating {report_type} report...")


def flaky_task(reason: str = "unknown") -> None:
    """Task that fails - demonstrates error handling."""
    logger.info("About to fail...")
    raise ValueError(f"Simulated error: {reason}")


async def async_greet(name: str) -> None:
    """Async greeting task - demonstrates async handler support."""
    await asyncio.sleep(0.1)  # Simulate async I/O
    logger.info(f"[async] Hello, {name}!")


async def async_fetch(url: str = "unknown") -> None:
    """Simulates async data fetching."""
    await asyncio.sleep(0.2)  # Simulate network delay
    logger.info(f"[async] Fetched data for: {url}")


def send_email(to: str, subject: str) -> None:
    """Email task."""
    logger.info(f"Sending email to {to}: {subject}")


def standalone_task(data: Any) -> None:
    """Task that demonstrates direct function reference."""
    logger.info(f"Standalone task executed with: {data}")


def process_with_callback(data: Any, transformer: Any) -> None:
    """Task that receives a custom object and callback (both pickled)."""
    result = transformer(data.total)
    logger.info(f"Processed: DataBundle.total={data.total} -> transformed={result}")


# =============================================================================
# COMMANDS
# =============================================================================


def cmd_full() -> None:
    """Complete showcase of ALL PQ features."""
    import time

    logger.info("=" * 60)
    logger.info("PQ - Full Feature Showcase")
    logger.info("=" * 60)

    # Start fresh
    pq.clear_all()
    logger.info("[1/10] CLEARED - Starting fresh")

    # === One-off Tasks ===
    logger.info("[2/10] ENQUEUEING ONE-OFF TASKS")
    logger.info("-" * 40)

    # Direct function reference
    id1 = pq.enqueue(greet, name="World")
    logger.info(f"Enqueued greet -> id={id1}")

    id2 = pq.enqueue(send_email, to="alice@test.com", subject="Hello!")
    logger.info(f"Enqueued send_email -> id={id2}")

    id3 = pq.enqueue(standalone_task, {"data": "direct_call"})
    logger.info(f"Enqueued standalone_task -> id={id3}")

    # Imported function reference (cross-module)
    id4 = pq.enqueue(external_task, {"source": "imported"})
    logger.info(f"Enqueued external_task (imported) -> id={id4}")

    # Math task
    id5 = pq.enqueue(add, a=100, b=200)
    logger.info(f"Enqueued add -> id={id5}")

    logger.info(f"Total pending: {pq.pending_count()}\n")

    # === Delayed Task ===
    logger.info("[3/10] DELAYED TASK (run_at)")
    logger.info("-" * 40)
    run_at = datetime.now(UTC) + timedelta(seconds=3)
    id6 = pq.enqueue(greet, name="Future", run_at=run_at)
    logger.info(f"Scheduled for {run_at.strftime('%H:%M:%S')} -> id={id6}")
    logger.info(f"Total pending: {pq.pending_count()}\n")

    # === Cancellation ===
    logger.info("[4/10] TASK CANCELLATION")
    logger.info("-" * 40)
    cancel_id = pq.enqueue(greet, name="WillBeCancelled")
    logger.info(f"Enqueued task -> id={cancel_id}")
    logger.info(f"Pending before cancel: {pq.pending_count()}")
    pq.cancel(cancel_id)
    logger.info(f"Cancelled task -> id={cancel_id}")
    logger.info(f"Pending after cancel: {pq.pending_count()}\n")

    # === Error Handling ===
    logger.info("[5/10] ERROR HANDLING")
    logger.info("-" * 40)
    pq.enqueue(flaky_task, reason="demo failure")
    logger.info("Enqueued flaky task (will fail)")
    pq.run_worker_once()  # This will log the error but continue
    logger.info("Worker continued after error (task marked failed)\n")

    # === Async Tasks ===
    logger.info("[6/10] ASYNC TASKS")
    logger.info("-" * 40)
    id_async1 = pq.enqueue(async_greet, name="Async World")
    logger.info(f"Enqueued async_greet -> id={id_async1}")
    id_async2 = pq.enqueue(async_fetch, url="https://api.example.com")
    logger.info(f"Enqueued async_fetch -> id={id_async2}")
    logger.info("Processing async tasks...")
    pq.run_worker_once()
    pq.run_worker_once()
    logger.info("Async tasks completed\n")

    # === Pickle Serialization (non-JSON types) ===
    logger.info("[7/10] PICKLE SERIALIZATION (lambdas, custom objects)")
    logger.info("-" * 40)

    # Custom class instance as positional arg (pickled)
    class DataBundle:
        def __init__(self, items: list[int]) -> None:
            self.items = items
            self.total = sum(items)

    bundle = DataBundle([10, 20, 30])
    logger.info(f"Created DataBundle with total={bundle.total}")

    # Lambda as keyword arg (pickled)
    doubler = lambda x: x * 2  # noqa: E731

    id_pickle = pq.enqueue(process_with_callback, bundle, transformer=doubler)
    logger.info(f"Enqueued task with pickled arg + kwarg -> id={id_pickle}")

    pq.run_worker_once()
    logger.info("Pickle serialization demo complete\n")

    # === Periodic Tasks ===
    logger.info("[8/10] PERIODIC TASKS")
    logger.info("-" * 40)
    pq.schedule(tick, run_every=timedelta(seconds=2))
    logger.info("Scheduled tick every 2 seconds")
    pq.schedule(report, run_every=timedelta(seconds=4), report_type="status")
    logger.info("Scheduled report every 4 seconds with payload")
    logger.info(f"Periodic count: {pq.periodic_count()}\n")

    # === Process Everything ===
    logger.info("[9/10] PROCESSING")
    logger.info("-" * 40)
    logger.info("Processing one-off tasks...")

    # Process immediate one-off tasks
    while pq.run_worker_once():
        pass

    # Wait for delayed task
    logger.info("Waiting for delayed task...")
    time.sleep(3.5)
    pq.run_worker_once()

    # Process a few periodic ticks
    logger.info("Processing periodic tasks (5 seconds)...")
    end_time = time.time() + 5
    while time.time() < end_time:
        pq.run_worker_once()
        time.sleep(0.5)

    # Unschedule periodic
    logger.info("[10/10] CLEANUP")
    logger.info("-" * 40)
    logger.info("Unscheduling periodic tasks...")
    pq.unschedule(tick)
    pq.unschedule(report)

    # Final status
    logger.info("" + "=" * 60)
    logger.info("FINAL STATUS")
    logger.info("=" * 60)
    logger.info(f"Pending one-off: {pq.pending_count()}")
    logger.info(f"Periodic schedules: {pq.periodic_count()}")
    logger.info("Full showcase complete!")


def cmd_demo() -> None:
    """Run a full demo: enqueue tasks and process them."""
    logger.info("=== PQ Demo ===")

    # Clear any existing tasks
    pq.clear_all()

    # Enqueue various tasks
    logger.info("Enqueueing tasks...")

    pq.enqueue(greet, name="World")
    pq.enqueue(add, a=10, b=20)
    pq.enqueue(send_email, to="user@example.com", subject="Hello!")
    pq.enqueue(standalone_task, {"data": "test"})

    logger.info(f"Queued {pq.pending_count()} tasks")

    # Process all tasks
    logger.info("Processing tasks...")
    while pq.run_worker_once():
        pass

    logger.info("Demo complete!")


def cmd_enqueue() -> None:
    """Enqueue one-off tasks using various methods."""
    logger.info("=== Enqueueing One-Off Tasks ===")

    task_id = pq.enqueue(greet, name="Alice")
    logger.info(f"Enqueued greet -> id={task_id}")

    task_id = pq.enqueue(send_email, to="bob@test.com", subject="Hi")
    logger.info(f"Enqueued send_email -> id={task_id}")

    task_id = pq.enqueue(standalone_task, {"key": "value"})
    logger.info(f"Enqueued standalone_task -> id={task_id}")

    logger.info(f"Total pending: {pq.pending_count()}")


def cmd_delayed() -> None:
    """Enqueue a task scheduled for the future."""
    logger.info("=== Scheduling Delayed Task ===")

    # Schedule task for 10 seconds from now
    run_at = datetime.now(UTC) + timedelta(seconds=10)
    task_id = pq.enqueue(greet, name="Future", run_at=run_at)

    logger.info(f"Scheduled task for {run_at.strftime('%H:%M:%S')} -> id={task_id}")
    logger.info("Run 'work' command to process when ready")


def cmd_schedule() -> None:
    """Schedule periodic tasks."""
    logger.info("=== Scheduling Periodic Tasks ===")

    pq.schedule(tick, run_every=timedelta(seconds=5))
    logger.info("Scheduled tick every 5 seconds")

    pq.schedule(report, run_every=timedelta(seconds=10), report_type="hourly")
    logger.info("Scheduled report every 10 seconds with payload")

    logger.info(f"Total periodic schedules: {pq.periodic_count()}")
    logger.info("Run 'work' command to start processing")


def cmd_cancel() -> None:
    """Demo cancelling a task before it runs."""
    logger.info("=== Cancel Demo ===")

    task_id = pq.enqueue(greet, name="NeverRuns")
    logger.info(f"Enqueued task -> id={task_id}")
    logger.info(f"Pending count: {pq.pending_count()}")

    result = pq.cancel(task_id)
    logger.info(f"Cancelled: {result}")
    logger.info(f"Pending count: {pq.pending_count()}")


def cmd_unschedule() -> None:
    """Demo removing a periodic task."""
    logger.info("=== Unschedule Demo ===")

    pq.schedule(tick, run_every=timedelta(seconds=5))
    logger.info(f"Scheduled tick. Periodic count: {pq.periodic_count()}")

    result = pq.unschedule(tick)
    logger.info(f"Unscheduled: {result}")
    logger.info(f"Periodic count: {pq.periodic_count()}")


def cmd_work() -> None:
    """Run the worker loop."""
    logger.info("=== Starting Worker ===")
    logger.info(f"Pending: {pq.pending_count()}, Periodic: {pq.periodic_count()}")
    logger.info("Press Ctrl+C to stop")
    pq.run_worker(poll_interval=1.0)


def cmd_status() -> None:
    """Show queue status."""
    logger.info("=== Queue Status ===")
    logger.info(f"Pending one-off tasks: {pq.pending_count()}")
    logger.info(f"Periodic schedules:    {pq.periodic_count()}")


def cmd_clear() -> None:
    """Clear all tasks and schedules."""
    pq.clear_all()
    logger.info("Cleared all tasks and schedules")


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:
    commands = {
        "full": cmd_full,
        "demo": cmd_demo,
        "enqueue": cmd_enqueue,
        "delayed": cmd_delayed,
        "schedule": cmd_schedule,
        "cancel": cmd_cancel,
        "unschedule": cmd_unschedule,
        "work": cmd_work,
        "status": cmd_status,
        "clear": cmd_clear,
    }

    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print(__doc__)
        print(f"Commands: {', '.join(commands.keys())}")
        sys.exit(1)

    commands[sys.argv[1]]()


if __name__ == "__main__":
    main()
