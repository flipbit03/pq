#!/usr/bin/env python
"""
PQ Example - Comprehensive showcase of all features.

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

=== REGISTRATION METHODS ===

1. Decorator (recommended):
    @pq.task("task_name")
    def my_handler(payload): ...

2. Explicit registration:
    def my_handler(payload): ...
    pq.register("task_name", my_handler)

3. Direct function (no registration needed):
    pq.enqueue(my_handler, payload)  # Uses module:function path

4. Imported function (cross-module):
    from examples.tasks import external_task
    pq.enqueue(external_task, payload)  # Stores "examples.tasks:external_task"

=== ENQUEUEING METHODS ===

1. By registered name:
    pq.enqueue("task_name", {"key": "value"})

2. Run at specific time:
    pq.enqueue("task_name", payload, run_at=datetime(...))

3. Direct function reference:
    pq.enqueue(some_function, {"key": "value"})

=== PERIODIC TASKS ===

1. Schedule recurring task:
    pq.schedule("task_name", run_every=timedelta(hours=1))

2. With payload:
    pq.schedule("task_name", run_every=timedelta(minutes=5), payload={...})

3. Remove schedule:
    pq.unschedule("task_name")
"""

import sys
from datetime import UTC, datetime, timedelta

from loguru import logger

from pq import PQ

# Import from sibling module - demonstrates cross-module function reference
# When run as script, we need to handle the import specially
try:
    from examples.tasks import external_task
except ModuleNotFoundError:
    from tasks import external_task

# =============================================================================
# SETUP
# =============================================================================

pq = PQ("postgresql://postgres:postgres@localhost:5433/postgres")
pq.create_tables()


# =============================================================================
# TASK HANDLERS - Multiple registration methods
# =============================================================================


# Method 1: Decorator (recommended)
@pq.task("greet")
def greet(payload: dict) -> None:
    """Simple greeting task."""
    logger.info(f"👋 Hello, {payload['name']}!")


@pq.task("add")
def add(payload: dict) -> None:
    """Math task."""
    result = payload["a"] + payload["b"]
    logger.info(f"🔢 {payload['a']} + {payload['b']} = {result}")


@pq.task("tick")
def tick(payload: dict) -> None:
    """Periodic heartbeat task."""
    logger.info("⏰ Tick!")


@pq.task("report")
def report(payload: dict) -> None:
    """Periodic report task with payload."""
    logger.info(f"📊 Generating {payload.get('type', 'default')} report...")


# Method 2: Explicit registration
def send_email(payload: dict) -> None:
    """Email task registered explicitly."""
    logger.info(f"📧 Sending email to {payload['to']}: {payload['subject']}")


pq.register("send_email", send_email)


# Method 3: Direct function (no registration needed - uses import path)
def standalone_task(payload: dict) -> None:
    """Task that doesn't need prior registration."""
    logger.info(f"🚀 Standalone task executed with: {payload}")


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
    logger.info("[1/7] CLEARED - Starting fresh")

    # === Registration Methods ===
    logger.info("[2/7] REGISTRATION METHODS")
    logger.info("-" * 40)
    logger.info("✓ @pq.task('greet') - Decorator registration")
    logger.info("✓ pq.register('send_email', fn) - Explicit registration")
    logger.info("✓ standalone_task - Direct function (same module)")
    logger.info("✓ external_task - Imported function (cross-module)")
    logger.info("")

    # === One-off Tasks ===
    logger.info("[3/7] ENQUEUEING ONE-OFF TASKS")
    logger.info("-" * 40)

    # By name (decorator-registered)
    id1 = pq.enqueue("greet", {"name": "World"})
    logger.info(f"Enqueued 'greet' by name -> id={id1}")

    # By name (explicitly registered)
    id2 = pq.enqueue("send_email", {"to": "alice@test.com", "subject": "Hello!"})
    logger.info(f"Enqueued 'send_email' by name -> id={id2}")

    # Direct function reference (same module)
    id3 = pq.enqueue(standalone_task, {"data": "direct_call"})
    logger.info(f"Enqueued standalone_task directly -> id={id3}")

    # Imported function reference (cross-module)
    id4 = pq.enqueue(external_task, {"source": "imported"})
    logger.info(f"Enqueued external_task (imported) -> id={id4}")

    # Math task
    id5 = pq.enqueue("add", {"a": 100, "b": 200})
    logger.info(f"Enqueued 'add' by name -> id={id5}")

    logger.info(f"Total pending: {pq.pending_count()}\n")

    # === Delayed Task ===
    logger.info("[4/7] DELAYED TASK (run_at)")
    logger.info("-" * 40)
    run_at = datetime.now(UTC) + timedelta(seconds=3)
    id6 = pq.enqueue("greet", {"name": "Future"}, run_at=run_at)
    logger.info(f"Scheduled for {run_at.strftime('%H:%M:%S')} -> id={id6}")
    logger.info(f"Total pending: {pq.pending_count()}\n")

    # === Cancellation ===
    logger.info("[5/7] TASK CANCELLATION")
    logger.info("-" * 40)
    cancel_id = pq.enqueue("greet", {"name": "WillBeCancelled"})
    logger.info(f"Enqueued task -> id={cancel_id}")
    logger.info(f"Pending before cancel: {pq.pending_count()}")
    pq.cancel(cancel_id)
    logger.info(f"Cancelled task -> id={cancel_id}")
    logger.info(f"Pending after cancel: {pq.pending_count()}\n")

    # === Periodic Tasks ===
    logger.info("[6/7] PERIODIC TASKS")
    logger.info("-" * 40)
    pq.schedule("tick", run_every=timedelta(seconds=2))
    logger.info("Scheduled 'tick' every 2 seconds")
    pq.schedule("report", run_every=timedelta(seconds=4), payload={"type": "status"})
    logger.info("Scheduled 'report' every 4 seconds with payload")
    logger.info(f"Periodic count: {pq.periodic_count()}\n")

    # === Process Everything ===
    logger.info("[7/7] PROCESSING")
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
    logger.info("Unscheduling periodic tasks...")
    pq.unschedule("tick")
    pq.unschedule("report")

    # Final status
    logger.info("" + "=" * 60)
    logger.info("FINAL STATUS")
    logger.info("=" * 60)
    logger.info(f"Pending one-off: {pq.pending_count()}")
    logger.info(f"Periodic schedules: {pq.periodic_count()}")
    logger.info("✅ Full showcase complete!")


def cmd_demo() -> None:
    """Run a full demo: enqueue tasks and process them."""
    logger.info("=== PQ Demo ===")

    # Clear any existing tasks
    pq.clear_all()

    # Enqueue various tasks
    logger.info("Enqueueing tasks...")

    # By name (decorator-registered)
    pq.enqueue("greet", {"name": "World"})
    pq.enqueue("add", {"a": 10, "b": 20})

    # By name (explicitly registered)
    pq.enqueue("send_email", {"to": "user@example.com", "subject": "Hello!"})

    # Direct function reference (no prior registration)
    pq.enqueue(standalone_task, {"data": "test"})

    logger.info(f"Queued {pq.pending_count()} tasks")

    # Process all tasks
    logger.info("Processing tasks...")
    while pq.run_worker_once():
        pass

    logger.info("✅ Demo complete!")


def cmd_enqueue() -> None:
    """Enqueue one-off tasks using various methods."""
    logger.info("=== Enqueueing One-Off Tasks ===")

    # Method 1: By registered name
    task_id = pq.enqueue("greet", {"name": "Alice"})
    logger.info(f"Enqueued 'greet' by name -> id={task_id}")

    # Method 2: By explicitly registered name
    task_id = pq.enqueue("send_email", {"to": "bob@test.com", "subject": "Hi"})
    logger.info(f"Enqueued 'send_email' by name -> id={task_id}")

    # Method 3: Direct function reference
    task_id = pq.enqueue(standalone_task, {"key": "value"})
    logger.info(f"Enqueued standalone_task directly -> id={task_id}")

    logger.info(f"Total pending: {pq.pending_count()}")


def cmd_delayed() -> None:
    """Enqueue a task scheduled for the future."""
    logger.info("=== Scheduling Delayed Task ===")

    # Schedule task for 10 seconds from now
    run_at = datetime.now(UTC) + timedelta(seconds=10)
    task_id = pq.enqueue("greet", {"name": "Future"}, run_at=run_at)

    logger.info(f"Scheduled task for {run_at.strftime('%H:%M:%S')} -> id={task_id}")
    logger.info("Run 'work' command to process when ready")


def cmd_schedule() -> None:
    """Schedule periodic tasks."""
    logger.info("=== Scheduling Periodic Tasks ===")

    # Simple periodic task
    pq.schedule("tick", run_every=timedelta(seconds=5))
    logger.info("Scheduled 'tick' every 5 seconds")

    # Periodic task with payload
    pq.schedule("report", run_every=timedelta(seconds=10), payload={"type": "hourly"})
    logger.info("Scheduled 'report' every 10 seconds with payload")

    logger.info(f"Total periodic schedules: {pq.periodic_count()}")
    logger.info("Run 'work' command to start processing")


def cmd_cancel() -> None:
    """Demo cancelling a task before it runs."""
    logger.info("=== Cancel Demo ===")

    # Enqueue a task
    task_id = pq.enqueue("greet", {"name": "NeverRuns"})
    logger.info(f"Enqueued task -> id={task_id}")
    logger.info(f"Pending count: {pq.pending_count()}")

    # Cancel it
    result = pq.cancel(task_id)
    logger.info(f"Cancelled: {result}")
    logger.info(f"Pending count: {pq.pending_count()}")


def cmd_unschedule() -> None:
    """Demo removing a periodic task."""
    logger.info("=== Unschedule Demo ===")

    # Schedule a task
    pq.schedule("tick", run_every=timedelta(seconds=5))
    logger.info(f"Scheduled 'tick'. Periodic count: {pq.periodic_count()}")

    # Remove it
    result = pq.unschedule("tick")
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
