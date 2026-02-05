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

# Multiple schedules for the same function using key
def sync_data(region: str) -> None:
    print(f"Syncing {region}")

pq.schedule(sync_data, run_every=timedelta(hours=1), key="us", region="us")
pq.schedule(sync_data, run_every=timedelta(hours=2), key="eu", region="eu")

# Remove only one schedule
pq.unschedule(sync_data, key="us")

# Remove default (no key) schedule
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

## Client IDs

Use `client_id` for custom identifiers to link tasks to your domain objects.

```python
from pq import PQ

pq = PQ("postgresql://localhost/mydb")

def process_order(order_id: int) -> None:
    print(f"Processing order {order_id}")

# Enqueue with a client-provided ID
pq.enqueue(process_order, order_id=123, client_id="order-123")

# Look up by client_id
task = pq.get_task_by_client_id("order-123")
print(f"Task status: {task.status}")

# Cancel by client_id
if task:
    pq.cancel(task.id)

# Duplicate client_id raises IntegrityError
# pq.enqueue(process_order, order_id=456, client_id="order-123")  # Error!
```

## Upsert

Use `upsert()` to insert or update a task by `client_id`. Unlike `enqueue()`, it won't raise on duplicates - it updates the existing task instead.

```python
from pq import PQ

pq = PQ("postgresql://localhost/mydb")

def send_notification(user_id: int, message: str) -> None:
    print(f"Notifying user {user_id}: {message}")

# First call creates the task
pq.upsert(send_notification, user_id=42, message="Hello", client_id="notify-42")

# Second call updates the existing task (resets status to PENDING)
pq.upsert(send_notification, user_id=42, message="Updated!", client_id="notify-42")

# Useful for debouncing - only the latest version runs
for i in range(100):
    pq.upsert(send_notification, user_id=42, message=f"Message {i}", client_id="notify-42")
# Only the last message will be processed
```

## Worker Lifecycle Hooks

Use `pre_execute` and `post_execute` hooks to run code before/after task execution. Hooks run in the forked child process, making them ideal for fork-unsafe resources like OpenTelemetry.

```python
from pq import PQ, Task, Periodic

pq = PQ("postgresql://localhost/mydb")

def setup_otel(task: Task | Periodic) -> None:
    """Called before task execution in forked child."""
    # Initialize tracer, create span, etc.
    print(f"[OTel] Starting span for {task.name}")

def flush_otel(task: Task | Periodic, error: Exception | None) -> None:
    """Called after task execution (success or failure)."""
    if error:
        print(f"[OTel] Recording error: {error}")
    print(f"[OTel] Flushing traces for {task.name}")

# Run worker with hooks
pq.run_worker(pre_execute=setup_otel, post_execute=flush_otel)
```

Hooks receive the full task object with metadata:

```python
def pre_hook(task: Task | Periodic) -> None:
    print(f"Task ID: {task.id}")
    print(f"Task name: {task.name}")
    print(f"Client ID: {task.client_id}")
    print(f"Priority: {task.priority}")
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
