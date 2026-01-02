#!/usr/bin/env python
"""Priority queues example - task prioritization.

Run:
    uv run examples/priority.py
"""

from pq import PQ, Priority

pq = PQ("postgresql://postgres:postgres@localhost:5433/postgres")
pq.create_tables()
pq.clear_all()


def process(name: str) -> None:
    print(f"Processing: {name}")


# Enqueue in reverse priority order
pq.enqueue(process, "batch job", priority=Priority.BATCH)      # 0
pq.enqueue(process, "low priority", priority=Priority.LOW)     # 25
pq.enqueue(process, "normal task", priority=Priority.NORMAL)   # 50
pq.enqueue(process, "high priority", priority=Priority.HIGH)   # 75
pq.enqueue(process, "CRITICAL!", priority=Priority.CRITICAL)   # 100

print(f"Enqueued {pq.pending_count()} tasks\n")

# Process all - they run in priority order (highest first)
print("Processing in priority order:")
while pq.run_worker_once():
    pass
