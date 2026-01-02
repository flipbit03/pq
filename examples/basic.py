#!/usr/bin/env python
"""Basic pq example - enqueue and process a task.

Run:
    uv run examples/basic.py
"""

from pq import PQ

# Connect to Postgres
pq = PQ("postgresql://postgres:postgres@localhost:5433/postgres")
pq.create_tables()


# Define a task handler
def send_email(to: str, subject: str) -> None:
    print(f"Sending email to {to}: {subject}")


# Enqueue a task
task_id = pq.enqueue(send_email, to="user@example.com", subject="Hello!")
print(f"Enqueued task {task_id}")

# Process it
pq.run_worker_once()
