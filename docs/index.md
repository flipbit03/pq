# pq

Postgres-backed job queue for Python with fork-based worker isolation.

## Features

- **Fork isolation** - Each task runs in a forked process. OOM or crashes don't affect the worker.
- **Natural Python API** - Pass `*args, **kwargs` directly. Pydantic models and custom objects work.
- **Periodic tasks** - Schedule with intervals or cron expressions.
- **Priority queues** - Five priority levels, higher priority tasks run first.
- **Async support** - Async handlers work seamlessly.
- **Concurrent workers** - Multiple workers with `FOR UPDATE SKIP LOCKED` prevents duplicate processing.

## Installation

```bash
uv add pq
```

Requires PostgreSQL and Python 3.14+.

## Quick Start

```python
from pq import PQ

pq = PQ("postgresql://localhost/mydb")
pq.create_tables()

def send_email(to: str, subject: str, body: str) -> None:
    print(f"Sending email to {to}: {subject}")

pq.enqueue(send_email, to="user@example.com", subject="Hello", body="...")
pq.run_worker()
```

## Enqueueing Tasks

```python
def greet(name: str) -> None:
    print(f"Hello, {name}!")

pq.enqueue(greet, name="World")
pq.enqueue(greet, "World")  # Positional args work too

# Delayed execution
from datetime import datetime, timedelta, UTC
pq.enqueue(greet, "World", run_at=datetime.now(UTC) + timedelta(hours=1))

# Priority
from pq import Priority
pq.enqueue(greet, "World", priority=Priority.CRITICAL)  # 100
pq.enqueue(greet, "World", priority=Priority.HIGH)      # 75
pq.enqueue(greet, "World", priority=Priority.NORMAL)    # 50 (default)
pq.enqueue(greet, "World", priority=Priority.LOW)       # 25
pq.enqueue(greet, "World", priority=Priority.BATCH)     # 0
```

## Periodic Tasks

```python
from datetime import timedelta

def heartbeat() -> None:
    print("alive")

def weekly_report() -> None:
    print("generating report...")

# Fixed interval
pq.schedule(heartbeat, run_every=timedelta(minutes=5))

# Cron expression (Monday 9am)
pq.schedule(weekly_report, cron="0 9 * * 1")

# With arguments
def report(report_type: str) -> None:
    print(f"generating {report_type} report...")

pq.schedule(report, run_every=timedelta(hours=1), report_type="hourly")

# Remove schedule
pq.unschedule(heartbeat)
```

## Serialization

Arguments are serialized automatically:

| Type | Method |
|------|--------|
| JSON-serializable (str, int, list, dict) | JSON |
| Pydantic models | `model_dump()` → JSON |
| Custom objects, functions | dill (pickle) |

```python
from pydantic import BaseModel

class User(BaseModel):
    id: int
    email: str

def process(user: dict, transform: callable) -> None:
    print(transform(user))

# Pydantic model → dict, function → pickled
pq.enqueue(process, User(id=1, email="a@b.com"), transform=lambda x: x["id"] * 2)
```

## Async Tasks

```python
import httpx

async def fetch(url: str) -> None:
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        print(response.status_code)

pq.enqueue(fetch, "https://example.com")
```

## Worker

```python
# Run forever (poll every second when idle)
pq.run_worker(poll_interval=1.0)

# Process single task
if pq.run_worker_once():
    print("Processed a task")

# Timeout (kill tasks running longer than 5 minutes)
pq.run_worker(max_runtime=300)

# Dedicated worker for specific priorities
from pq import Priority
pq.run_worker(priorities={Priority.CRITICAL, Priority.HIGH})
```

## Dedicated Priority Workers

Run separate workers for different priority tiers to ensure high-priority tasks aren't blocked:

```bash
# Terminal 1: High-priority worker (CRITICAL + HIGH only)
python -c "from myapp import pq; from pq import Priority; pq.run_worker(priorities={Priority.CRITICAL, Priority.HIGH})"

# Terminal 2-3: General workers (all priorities)
python -c "from myapp import pq; pq.run_worker()"
```

This ensures critical tasks get processed immediately even when the queue is busy.

## Task Management

```python
def my_task() -> None:
    pass

# Cancel a pending task
task_id = pq.enqueue(my_task)
pq.cancel(task_id)

# Counts
pq.pending_count()
pq.periodic_count()

# List failed/completed
pq.list_failed(limit=10)
pq.list_completed(limit=10)

# Clear old tasks
pq.clear_failed(before=datetime.now(UTC) - timedelta(days=7))
pq.clear_completed(before=datetime.now(UTC) - timedelta(days=1))
pq.clear_all()
```

## Fork Isolation

Every task runs in a forked child process:

```
Worker (parent)
    │
    ├── fork() → Child executes task → exits
    │           (OOM/crash only affects child)
    │
    └── Continues processing next task
```

The parent monitors via `os.wait4()` and detects:

- **Timeout** - Task exceeded `max_runtime`
- **OOM** - Killed by SIGKILL (OOM killer)
- **Signals** - Killed by other signals

## Multiple Workers

Run multiple workers for parallel processing:

```bash
# Terminal 1
python -c "from myapp import pq; pq.run_worker()"

# Terminal 2
python -c "from myapp import pq; pq.run_worker()"
```

Tasks are claimed with `FOR UPDATE SKIP LOCKED` - each task runs exactly once.

## Error Handling

Failed tasks are marked with status `FAILED`:

```python
for task in pq.list_failed():
    print(f"{task.name}: {task.error}")
```
