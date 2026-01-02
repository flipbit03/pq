#!/usr/bin/env python
"""Async tasks example - async handler support.

Run:
    uv run examples/async_tasks.py
"""

import asyncio

from pq import PQ

pq = PQ("postgresql://postgres:postgres@localhost:5433/postgres")
pq.create_tables()
pq.clear_all()


async def fetch_data(url: str) -> None:
    """Simulate async HTTP request."""
    print(f"Fetching {url}...")
    await asyncio.sleep(0.5)  # Simulate network delay
    print(f"Got response from {url}")


async def process_batch(items: list[str]) -> None:
    """Process items concurrently."""
    print(f"Processing {len(items)} items...")
    await asyncio.gather(*[asyncio.sleep(0.1) for _ in items])
    print(f"Done: {items}")


# Enqueue async tasks
pq.enqueue(fetch_data, url="https://api.example.com/users")
pq.enqueue(fetch_data, url="https://api.example.com/orders")
pq.enqueue(process_batch, items=["a", "b", "c"])

print(f"Enqueued {pq.pending_count()} async tasks\n")

# Process all
while pq.run_worker_once():
    pass
