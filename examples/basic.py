#!/usr/bin/env python
"""
Basic PQ example.

Usage:
    uv run examples/basic.py enqueue      # Queue some one-off tasks
    uv run examples/basic.py periodic     # Schedule a periodic task
    uv run examples/basic.py work         # Run worker to process tasks
    uv run examples/basic.py status       # Show queue status
"""

import sys
from datetime import timedelta

from loguru import logger

from pq import PQ

# Connect to postgres
pq = PQ("postgresql://postgres:postgres@localhost:5433/postgres")
pq.create_tables()


# --- Task handlers ---


@pq.task("greet")
def greet(payload: dict) -> None:
    logger.info(f"Hello, {payload['name']}!")


@pq.task("add")
def add(payload: dict) -> None:
    result = payload["a"] + payload["b"]
    logger.info(f"{payload['a']} + {payload['b']} = {result}")


@pq.task("tick")
def tick(payload: dict) -> None:
    logger.info("Tick!")


# --- Commands ---


def cmd_enqueue() -> None:
    """Enqueue some one-off tasks."""
    pq.enqueue("greet", {"name": "World"})
    pq.enqueue("greet", {"name": "PQ"})
    pq.enqueue("add", {"a": 2, "b": 3})
    logger.info(f"Queued 3 tasks. Total pending: {pq.pending_count()}")


def cmd_periodic() -> None:
    """Schedule a periodic task."""
    pq.schedule("tick", run_every=timedelta(seconds=5))
    logger.info("Scheduled 'tick' to run every 5 seconds")


def cmd_work() -> None:
    """Run the worker loop."""
    logger.info("Starting worker... (Ctrl+C to stop)")
    pq.run_worker(poll_interval=1.0)


def cmd_status() -> None:
    """Show queue status."""
    logger.info(f"Pending one-off tasks: {pq.pending_count()}")
    logger.info(f"Periodic schedules: {pq.periodic_count()}")


def main() -> None:
    commands = {
        "enqueue": cmd_enqueue,
        "periodic": cmd_periodic,
        "work": cmd_work,
        "status": cmd_status,
    }

    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print(__doc__)
        print(f"Commands: {', '.join(commands.keys())}")
        sys.exit(1)

    commands[sys.argv[1]]()


if __name__ == "__main__":
    main()
