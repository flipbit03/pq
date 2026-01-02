#!/usr/bin/env python
"""Error handling example - failures and cleanup.

Run:
    uv run examples/error_handling.py
"""

from datetime import UTC, datetime, timedelta

from pq import PQ

pq = PQ("postgresql://postgres:postgres@localhost:5433/postgres")
pq.create_tables()
pq.clear_all()


def succeed() -> None:
    print("✓ Task succeeded")


def fail() -> None:
    print("✗ Task about to fail...")
    raise ValueError("Something went wrong!")


# Enqueue mix of tasks
pq.enqueue(succeed)
pq.enqueue(fail)
pq.enqueue(succeed)

print(f"Enqueued {pq.pending_count()} tasks\n")

# Process all
while pq.run_worker_once():
    pass

# Check results
print(f"\nCompleted: {len(pq.list_completed())}")
print(f"Failed: {len(pq.list_failed())}")

# Inspect failures
for task in pq.list_failed():
    print(f"  - {task.name}: {task.error}")

# Cleanup old tasks
week_ago = datetime.now(UTC) - timedelta(days=7)
pq.clear_completed(before=week_ago)
pq.clear_failed(before=week_ago)
print("\nCleaned up tasks older than 7 days")
