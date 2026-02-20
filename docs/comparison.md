# Comparison

How pq compares to other Python task queues.

## Overview

| | Broker | Worker model | Periodic tasks | Process isolation |
|---|---|---|---|---|
| **pq** | Postgres | Fork per task | Interval + cron | Yes (fork) |
| **Celery** | Redis / RabbitMQ | Process pool | Beat scheduler | Per worker |
| **RQ** | Redis | Process per worker | rq-scheduler | Per worker |
| **Dramatiq** | Redis / RabbitMQ | Thread / process pool | APScheduler | Per worker |
| **ARQ** | Redis | Async event loop | Built-in cron | No |
| **Procrastinate** | Postgres | Async event loop | Built-in | No |

## Why Postgres as a broker?

Most task queues require a separate message broker (Redis, RabbitMQ). If your application already uses Postgres, adding another stateful service means:

- Another thing to deploy, monitor, back up, and keep running
- Another failure mode (broker goes down, queue is lost)
- No transactional guarantees between your writes and your enqueues

With a Postgres-backed queue, you can enqueue a task in the same transaction as your database write. If the transaction rolls back, the task is never created. No two-phase commit, no outbox pattern.

The tradeoff is throughput. Postgres is not optimized for high-frequency polling. If you need 10,000+ jobs per second, use a dedicated broker. For most applications processing hundreds or low thousands of jobs per second, Postgres handles it well.

## pq vs Celery

Celery is the most widely used Python task queue. It's battle-tested and feature-rich.

**Choose Celery when:**

- You need very high throughput
- You need cross-language workers (Celery has Node.js, Go clients)
- You need complex workflows (chains, chords, groups)
- Your team already knows Celery

**Choose pq when:**

- You don't want to run Redis or RabbitMQ
- You want true process isolation per task (not per worker)
- You want transactional enqueueing
- You prefer a smaller API surface

## pq vs RQ (Redis Queue)

RQ is a simple Redis-based queue. It's easy to get started with.

**Choose RQ when:**

- You already run Redis
- You want a minimal, well-known library

**Choose pq when:**

- You don't want to add Redis to your stack
- You need periodic tasks with overlap control
- You want fork isolation per task (RQ isolates per worker, not per task)

## pq vs Procrastinate

Procrastinate is the closest alternative -- it's also Postgres-backed and well-designed.

**Choose Procrastinate when:**

- You want async-first workers (Procrastinate runs tasks in an async event loop)
- You use Django (Procrastinate has first-class Django support)
- You want a more mature, established library

**Choose pq when:**

- You want fork-based process isolation (one process per task)
- You want to pass functions directly without decorating them
- You don't need Django integration

## pq vs ARQ

ARQ is an async-first Redis-based queue.

**Choose ARQ when:**

- Your application is fully async
- You already run Redis

**Choose pq when:**

- You don't want Redis
- You need process isolation (ARQ runs everything in one event loop)
- You need priority queues with dedicated workers

## Fork isolation

This is pq's key differentiator. Most task queues run tasks in long-lived worker processes. If a task leaks memory, the worker gradually degrades. If a task segfaults or triggers the OOM killer, the worker dies and all in-flight tasks are lost.

pq forks a new child process for every task. The child executes the task and exits. Memory leaks are cleaned up by the OS. OOM kills only affect the child. The parent worker continues processing.

This matters most when:

- Tasks run untrusted or unpredictable code
- Tasks have variable memory usage
- You need high reliability without complex monitoring
- You use fork-unsafe libraries (OpenTelemetry, some database drivers) that need per-task initialization via lifecycle hooks
