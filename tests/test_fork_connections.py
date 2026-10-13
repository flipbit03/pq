"""A task child must not use the parent worker's database connections.

The worker forks a child per task, and the child inherits the parent's
SQLAlchemy engine: its pool holds the sockets the parent claims on. A handler
that reaches the application's ``PQ`` instance (to enqueue a follow-up task,
say) checks out one of those connections, and two processes then speak on one
Postgres session.

Over TLS one query from the child is enough: it advances the session's record
sequence numbers in the child's copy of the SSL state only, and the parent's
next record is rejected (``bad record mac`` / ``decryption failed``). Over a
plain socket the damage needs overlap: the parent's query queues behind the
child's, and whichever process reads first gets the other's result.
"""

import gc
import multiprocessing
import multiprocessing.managers
import os
import sys
import time
import weakref
from collections.abc import Generator
from multiprocessing.managers import ListProxy
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest
from loguru import logger
from sqlalchemy import event, text

from pq import client
from pq.client import PQ, _live_instances
from pq.models import TaskStatus

if TYPE_CHECKING:
    from loguru import Record

_shared_results: ListProxy[int] | None = None
_app_pq: PQ | None = None
"""The application's PQ instance, as a handler reaches it: a module global
captured before the fork, the way applications hold a PQ singleton."""


def _set_shared_results(results: ListProxy[int]) -> None:
    global _shared_results
    _shared_results = results


def _set_app_pq(pq: PQ) -> None:
    global _app_pq
    _app_pq = pq


def _backend_pid(pq: PQ) -> int:
    with pq.session() as session:
        return session.execute(text("SELECT pg_backend_pid()")).scalar_one()


def _backend_pids_in_fork(*instances: PQ) -> list[int]:
    """Fork; the child reports the backend pid each instance gives it.

    The child fails instead if a fork hook raised: CPython reports those
    through ``sys.unraisablehook``.
    """
    hook_errors: list[str] = []
    read_fd, write_fd = os.pipe()
    previous_hook = sys.unraisablehook
    sys.unraisablehook = lambda u: hook_errors.append(repr(u.exc_value))
    try:
        child = os.fork()
    finally:
        sys.unraisablehook = previous_hook
    if child == 0:
        status = 1
        try:
            if hook_errors:
                os.write(write_fd, "\n".join(hook_errors).encode())
            else:
                pids = " ".join(str(_backend_pid(i)) for i in instances)
                os.write(write_fd, pids.encode())
                status = 0
        finally:
            os._exit(status)
    os.close(write_fd)
    with os.fdopen(read_fd, "rb") as reader:
        output = reader.read().decode()
    _, wait_status = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(wait_status) == 0, f"the child failed: {output}"
    return [int(pid) for pid in output.split()]


def backend_pid_handler() -> None:
    assert _app_pq is not None
    assert _shared_results is not None
    _shared_results.append(_backend_pid(_app_pq))


def hold_connection_handler(seconds: float) -> None:
    """Keep a query open on a connection checked out of the inherited pool."""
    assert _app_pq is not None
    with _app_pq.session() as session:
        session.execute(text("SELECT pg_sleep(:s)"), {"s": seconds})


def noop_handler() -> None:
    pass


def _run_worker_capturing_errors(db_url: str, errors: ListProxy[str]) -> None:
    """Worker process: run the real loop until SIGTERM, recording ERROR logs."""
    from pq.worker import run_worker

    pq = PQ(db_url)
    _set_app_pq(pq)
    logger.add(
        lambda message: errors.append(message.record["message"]),
        level="ERROR",
    )
    run_worker(pq, concurrency=2, poll_interval=0.02, drain_timeout=5)


def _assert_worker_loop_runs_clean(
    pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
) -> None:
    """Run the real worker loop while one task child holds a query open
    through the application's PQ instance and the other slot claims and
    reaps around it. The loop must log no errors and finish every task."""
    errors = manager.list()
    hold_id = pq.enqueue(hold_connection_handler, seconds=2.0)
    for _ in range(5):
        pq.enqueue(noop_handler)

    worker = multiprocessing.get_context("fork").Process(
        target=_run_worker_capturing_errors, args=(db_url, errors)
    )
    worker.start()
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        hold = pq.get_task(hold_id)
        if hold is not None and hold.status in (
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
        ):
            break
        time.sleep(0.05)
    worker.terminate()  # SIGTERM: the worker drains and returns
    worker.join(timeout=10)
    if worker.is_alive():
        worker.kill()
        worker.join()
        pytest.fail(f"worker hung; errors logged: {list(errors)}")

    assert list(errors) == []
    assert pq.pending_count() == 0


class TestTaskChildConnections:
    def test_child_gets_its_own_database_session(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """The child must not reuse the session the parent's pool holds."""
        results = manager.list()
        _set_shared_results(results)
        _set_app_pq(pq)
        parent_backend = _backend_pid(pq)

        pq.enqueue(backend_pid_handler)
        assert pq.run_worker_once() is True

        assert len(results) == 1
        assert results[0] != parent_backend, (
            "the task child queried on the parent's pooled Postgres session "
            f"(backend pid {parent_backend})"
        )

    def test_parent_keeps_working_while_a_child_uses_the_pool(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """The real worker loop must claim cleanly while a child holds a query
        open through the application's PQ instance."""
        _assert_worker_loop_runs_clean(pq, db_url, manager)


class TestTaskChildConnectionsOverTls:
    def test_parent_session_survives_a_child_query(self, tls_pq: PQ) -> None:
        """One query from the child must not break the parent's open session.

        Fork, let the parent check a connection out (its pre-ping passes), let
        the child query through its inherited pool, then query on the parent's
        still-open session. This is the order the worker loop produces: a
        child forked earlier enqueues while the parent claims or reaps.
        """
        _backend_pid(tls_pq)  # one idle pooled connection, inherited by the child
        go_read, go_write = os.pipe()
        done_read, done_write = os.pipe()
        child = os.fork()
        if child == 0:
            os.read(go_read, 1)
            try:
                _backend_pid(tls_pq)
            finally:
                os.write(done_write, b"x")
                os._exit(0)
        try:
            with tls_pq.session() as session:
                session.execute(text("SELECT 1"))
                os.write(go_write, b"x")
                os.read(done_read, 1)
                session.execute(text("SELECT 1"))
        finally:
            os.waitpid(child, 0)

    def test_worker_loop_claims_cleanly_over_tls(
        self,
        tls_pq: PQ,
        tls_db_url: str,
        manager: multiprocessing.managers.SyncManager,
    ) -> None:
        """Over TLS one child query is enough to break the parent's session."""
        _assert_worker_loop_runs_clean(tls_pq, tls_db_url, manager)


class TestForkHookInstanceTracking:
    def test_collected_instance_does_not_break_the_fork_hook(
        self, pq: PQ, db_url: str
    ) -> None:
        """A PQ instance garbage-collected before a fork drops out of the
        hook's WeakSet; the hook raises nothing and still resets the
        instances that remain."""
        collected = PQ(db_url)
        collected_ref = weakref.ref(collected)
        del collected
        gc.collect()
        assert collected_ref() is None
        assert pq in _live_instances
        parent_backend = _backend_pid(pq)

        assert _backend_pids_in_fork(pq) != [parent_backend]
        assert _backend_pid(pq) == parent_backend

    def test_closed_instance_is_still_reset_after_fork(self, db_url: str) -> None:
        """close() disposes the pool, but the engine reconnects if the
        instance is used again, so the hook must still cover it."""
        pq = PQ(db_url)
        try:
            pq.close()
            parent_backend = _backend_pid(pq)

            assert _backend_pids_in_fork(pq) != [parent_backend]
        finally:
            pq.close()

    def test_failing_instance_does_not_stop_the_fork_hook(self, db_url: str) -> None:
        """An error while resetting one instance must not leave the other
        instances on their inherited pools."""
        instances = [PQ(db_url), PQ(db_url)]

        def fail(_: object) -> None:
            raise RuntimeError("engine_disposed listener failed")

        try:
            # engine_disposed fires after the pool is replaced, so every
            # instance is reset if the hook reaches it, despite the error.
            for instance in instances:
                event.listen(instance._engine, "engine_disposed", fail)
            parent_backends = [_backend_pid(instance) for instance in instances]

            child_backends = _backend_pids_in_fork(*instances)

            for parent_backend, child_backend in zip(
                parent_backends, child_backends, strict=True
            ):
                assert child_backend != parent_backend
        finally:
            for instance in instances:
                event.remove(instance._engine, "engine_disposed", fail)
                instance.close()


class TestResetPoolsAfterFork:
    """The fork hook on its own, with mocked engines: no fork, no database."""

    @pytest.fixture
    def engines(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> Generator[list[Mock], None, None]:
        """Two tracked PQ instances whose engines are mocks."""
        monkeypatch.setattr(client, "_live_instances", weakref.WeakSet())
        engines = [Mock(), Mock()]
        # Held until teardown: the hook's set only references them weakly.
        instances = [PQ("postgresql://localhost/unused") for _ in engines]
        for instance, engine in zip(instances, engines, strict=True):
            monkeypatch.setattr(instance, "_engine", engine)
        yield engines

    def test_drops_every_pool_without_closing_its_connections(
        self, engines: list[Mock]
    ) -> None:
        client._reset_pools_after_fork()

        for engine in engines:
            engine.dispose.assert_called_once_with(close=False)

    def test_logs_a_failure_and_resets_the_other_instances(
        self, engines: list[Mock], log_records: list["Record"]
    ) -> None:
        for engine in engines:
            engine.dispose.side_effect = RuntimeError("listener failed")

        client._reset_pools_after_fork()

        for engine in engines:
            engine.dispose.assert_called_once_with(close=False)
        assert [r["message"] for r in log_records] == [
            "Failed to reset a PQ connection pool after fork"
        ] * 2
        for record in log_records:
            assert record["level"].name == "ERROR"
            assert record["exception"] is not None
            assert str(record["exception"].value) == "listener failed"
