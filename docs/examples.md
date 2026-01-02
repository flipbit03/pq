# Examples

Runnable examples in the [`examples/`](https://github.com/ricwo/pq/tree/main/examples) directory.

## Basic Usage

[`examples/basic.py`](https://github.com/ricwo/pq/blob/main/examples/basic.py) - Enqueue and process a task.

```python
from pq import PQ

pq = PQ("postgresql://localhost/mydb")
pq.create_tables()

def send_email(to: str, subject: str) -> None:
    print(f"Sending email to {to}: {subject}")

pq.enqueue(send_email, to="user@example.com", subject="Hello!")
pq.run_worker_once()
```

## Periodic Tasks

[`examples/periodic.py`](https://github.com/ricwo/pq/blob/main/examples/periodic.py) - Scheduled and cron-based tasks.

```python
from datetime import timedelta
from pq import PQ

pq = PQ("postgresql://localhost/mydb")

def heartbeat() -> None:
    print("alive")

# Run every 5 minutes
pq.schedule(heartbeat, run_every=timedelta(minutes=5))

# Run at 9am on Mondays
pq.schedule(heartbeat, cron="0 9 * * 1")

# Remove schedule
pq.unschedule(heartbeat)
```

## Priority Queues

[`examples/priority.py`](https://github.com/ricwo/pq/blob/main/examples/priority.py) - Task prioritization.

```python
from pq import PQ, Priority

pq = PQ("postgresql://localhost/mydb")

def process(name: str) -> None:
    print(f"Processing: {name}")

# Higher priority tasks run first
pq.enqueue(process, "batch", priority=Priority.BATCH)      # 0
pq.enqueue(process, "normal", priority=Priority.NORMAL)    # 50
pq.enqueue(process, "urgent", priority=Priority.CRITICAL)  # 100
```

## Async Tasks

[`examples/async_tasks.py`](https://github.com/ricwo/pq/blob/main/examples/async_tasks.py) - Async handler support.

```python
import asyncio
from pq import PQ

pq = PQ("postgresql://localhost/mydb")

async def fetch_data(url: str) -> None:
    print(f"Fetching {url}...")
    await asyncio.sleep(1)
    print("Done")

pq.enqueue(fetch_data, url="https://api.example.com")
pq.run_worker_once()
```

## Error Handling

[`examples/error_handling.py`](https://github.com/ricwo/pq/blob/main/examples/error_handling.py) - Failures and cleanup.

```python
from datetime import UTC, datetime, timedelta
from pq import PQ

pq = PQ("postgresql://localhost/mydb")

# Check failed tasks
for task in pq.list_failed():
    print(f"{task.name}: {task.error}")

# Cleanup old tasks
week_ago = datetime.now(UTC) - timedelta(days=7)
pq.clear_failed(before=week_ago)
pq.clear_completed(before=week_ago)
```

## Running Examples

```bash
# Start Postgres
make dev

# Run any example
uv run examples/basic.py
uv run examples/periodic.py
uv run examples/priority.py
uv run examples/async_tasks.py
uv run examples/error_handling.py
```
