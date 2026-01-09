# pq

Postgres-backed job queue for Python with fork-based worker isolation.

## Features

- **Fork isolation** - Each task runs in a forked process. OOM or crashes don't affect the worker.
- **Natural Python API** - Pass functions directly with `*args, **kwargs`.
- **Periodic tasks** - Schedule with intervals or cron expressions.
- **Priority queues** - Five levels, higher priority tasks run first.
- **Async support** - Async handlers work seamlessly.
- **Concurrent workers** - `FOR UPDATE SKIP LOCKED` prevents duplicate processing.

## Installation

```bash
uv add pq
```

Requires PostgreSQL and Python 3.13+.

## Quick Start

```python
from pq import PQ

pq = PQ("postgresql://localhost/mydb")
pq.run_db_migrations()  # Creates/updates tables

def send_email(to: str, subject: str) -> None:
    print(f"Sending to {to}: {subject}")

pq.enqueue(send_email, to="user@example.com", subject="Hello")
pq.run_worker()
```

## Tasks

### Enqueueing

```python
def greet(name: str) -> None:
    print(f"Hello, {name}!")

pq.enqueue(greet, name="World")
pq.enqueue(greet, "World")  # Positional args work too
```

### Delayed Execution

```python
from datetime import datetime, timedelta, UTC

pq.enqueue(greet, "World", run_at=datetime.now(UTC) + timedelta(hours=1))
```

### Priority

```python
from pq import Priority

pq.enqueue(task, priority=Priority.CRITICAL)  # 100 - highest
pq.enqueue(task, priority=Priority.HIGH)      # 75
pq.enqueue(task, priority=Priority.NORMAL)    # 50 (default)
pq.enqueue(task, priority=Priority.LOW)       # 25
pq.enqueue(task, priority=Priority.BATCH)     # 0 - lowest
```

### Cancellation

```python
task_id = pq.enqueue(my_task)
pq.cancel(task_id)  # Returns True if found and cancelled
```

## Periodic Tasks

### Intervals

```python
from datetime import timedelta

def heartbeat() -> None:
    print("alive")

pq.schedule(heartbeat, run_every=timedelta(minutes=5))
```

### Cron Expressions

```python
def weekly_report() -> None:
    print("generating report...")

pq.schedule(weekly_report, cron="0 9 * * 1")  # Monday 9am
```

### With Arguments

```python
pq.schedule(report, run_every=timedelta(hours=1), report_type="hourly")
```

### Unscheduling

```python
pq.unschedule(heartbeat)  # Returns True if found
```

## Workers

### Running

```python
# Run forever, poll every second when idle
pq.run_worker(poll_interval=1.0)

# Process single task (for testing)
processed = pq.run_worker_once()
```

### Timeout

Kill tasks that run too long:

```python
pq.run_worker(max_runtime=300)  # 5 minute timeout
```

### Priority-Dedicated Workers

Reserve workers for high-priority tasks:

```python
from pq import Priority

# This worker only processes CRITICAL and HIGH
pq.run_worker(priorities={Priority.CRITICAL, Priority.HIGH})
```

Run multiple workers in separate terminals:

```bash
# Terminal 1: High-priority only
python -c "from myapp import pq; from pq import Priority; pq.run_worker(priorities={Priority.CRITICAL, Priority.HIGH})"

# Terminal 2-3: All priorities
python -c "from myapp import pq; pq.run_worker()"
```

## Serialization

Arguments are serialized automatically:

| Type | Method |
|------|--------|
| JSON-serializable (str, int, list, dict) | JSON |
| Pydantic models | `model_dump()` → JSON |
| Custom objects, lambdas | dill (pickle) |

```python
from typing import Callable
from pydantic import BaseModel

class User(BaseModel):
    id: int
    email: str

def process(user: User, transform: Callable[[int], int]) -> None:
    print(transform(user.id))

pq.enqueue(process, User(id=1, email="a@b.com"), transform=lambda x: x * 2)
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

## Error Handling

Failed tasks are marked with status `FAILED`:

```python
for task in pq.list_failed():
    print(f"{task.name}: {task.error}")

# Cleanup
pq.clear_failed(before=datetime.now(UTC) - timedelta(days=7))
pq.clear_completed(before=datetime.now(UTC) - timedelta(days=1))
```

## Architecture

### Fork Isolation

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

### Concurrent Workers

Multiple workers can run in parallel. Tasks are claimed with `FOR UPDATE SKIP LOCKED`, ensuring each task runs exactly once.

## Development

```bash
make dev       # Start Postgres
uv run pytest  # Run tests
```

See [CLAUDE.md](CLAUDE.md) for full development instructions.

## License

MIT
