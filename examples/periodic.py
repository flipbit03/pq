#!/usr/bin/env python
"""Periodic tasks example - scheduled and cron-based tasks.

Run:
    uv run examples/periodic.py
"""

import time
from datetime import timedelta

from pq import PQ

pq = PQ("postgresql://postgres:postgres@localhost:5433/postgres")
pq.create_tables()
pq.clear_all()


# Task handlers
def heartbeat() -> None:
    print("💓 Heartbeat")


def cleanup(full: bool = False) -> None:
    mode = "full" if full else "quick"
    print(f"🧹 Running {mode} cleanup")


# Schedule with interval
pq.schedule(heartbeat, run_every=timedelta(seconds=2))
print("Scheduled heartbeat every 2 seconds")

# Schedule with cron (every minute at :00)
pq.schedule(cleanup, cron="* * * * *", full=True)
print("Scheduled cleanup every minute")

print(f"Periodic tasks: {pq.periodic_count()}")

# Run for 5 seconds
print("\nRunning for 5 seconds...")
end = time.time() + 5
while time.time() < end:
    pq.run_worker_once()
    time.sleep(0.5)

# Unschedule
pq.unschedule(heartbeat)
pq.unschedule(cleanup)
print("\nUnscheduled all tasks")
