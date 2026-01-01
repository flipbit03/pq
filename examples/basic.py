#!/usr/bin/env python
"""Basic PQ example - run with: uv run examples/basic.py"""

from datetime import timedelta

from pq import PQ

# Connect to postgres
pq = PQ("postgresql://postgres:postgres@localhost:5433/postgres")
pq.create_tables()


# Register task handlers
@pq.task("greet")
def greet(payload: dict) -> None:
    print(f"Hello, {payload['name']}!")


@pq.task("add")
def add(payload: dict) -> None:
    result = payload["a"] + payload["b"]
    print(f"{payload['a']} + {payload['b']} = {result}")


# Enqueue some one-off tasks
pq.enqueue("greet", {"name": "World"})
pq.enqueue("greet", {"name": "PQ"})
pq.enqueue("add", {"a": 2, "b": 3})

print(f"Queued {pq.pending_count()} tasks")

# Process all tasks
while pq.run_worker_once():
    pass

print("Done!")
