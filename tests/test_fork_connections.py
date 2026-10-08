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

import multiprocessing
import multiprocessing.managers
import os
import time
from typing import Any

import pytest
from loguru import logger
from sqlalchemy import text

from pq.client import PQ

_shared_results: Any = None
_app_pq: PQ | None = None
"""The application's PQ instance, as a handler reaches it: a module global
captured before the fork, the way applications hold a PQ singleton."""


def _set_shared_results(results: Any) -> None:
    global _shared_results
    _shared_results = results


def _set_app_pq(pq: PQ) -> None:
    global _app_pq
    _app_pq = pq


def _backend_pid(pq: PQ) -> int:
    with pq.session() as session:
        return session.execute(text("SELECT pg_backend_pid()")).scalar_one()


def backend_pid_handler() -> None:
    assert _app_pq is not None
    _shared_results.append(_backend_pid(_app_pq))


def hold_connection_handler(seconds: float) -> None:
    """Keep a query open on a connection checked out of the inherited pool."""
    assert _app_pq is not None
    with _app_pq.session() as session:
        session.execute(text("SELECT pg_sleep(:s)"), {"s": seconds})


def noop_handler() -> None:
    pass


def _run_worker_capturing_errors(
    db_url: str, errors: Any, app_pq_holder: Any, seconds: float
) -> None:
    """Worker process: run the real loop for ``seconds``, recording ERROR logs."""
    from pq.worker import run_worker

    pq = PQ(db_url)
    _set_app_pq(pq)
    logger.add(
        lambda message: errors.append(message.record["message"]),
        level="ERROR",
    )
    app_pq_holder.append(os.getpid())
    deadline = time.monotonic() + seconds

    # Stop the loop from a signal once the window is over.
    import signal

    signal.signal(signal.SIGALRM, lambda *_: os.kill(os.getpid(), signal.SIGTERM))
    signal.setitimer(signal.ITIMER_REAL, max(deadline - time.monotonic(), 0.1))
    run_worker(pq, concurrency=2, poll_interval=0.02, drain_timeout=5)


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
        errors = manager.list()
        started = manager.list()
        pq.enqueue(hold_connection_handler, seconds=2.0)
        for _ in range(5):
            pq.enqueue(noop_handler)

        worker = multiprocessing.get_context("fork").Process(
            target=_run_worker_capturing_errors,
            args=(db_url, errors, started, 6.0),
        )
        worker.start()
        worker.join(timeout=30)
        if worker.is_alive():
            worker.kill()
            worker.join()
            pytest.fail(f"worker hung; errors logged: {list(errors)}")

        assert list(errors) == []
        assert pq.pending_count() == 0


def _child_queries_while_parent_holds_session(pq: PQ) -> None:
    """Fork, let the parent check a connection out (its pre-ping passes), let
    the child query through its inherited pool, then query on the parent's
    still-open session. This is the order the worker loop produces: a child
    forked earlier enqueues while the parent claims or reaps."""
    _backend_pid(pq)  # one idle pooled connection, inherited by the child
    go_read, go_write = os.pipe()
    done_read, done_write = os.pipe()
    child = os.fork()
    if child == 0:
        os.read(go_read, 1)
        try:
            _backend_pid(pq)
        finally:
            os.write(done_write, b"x")
            os._exit(0)
    try:
        with pq.session() as session:
            session.execute(text("SELECT 1"))
            os.write(go_write, b"x")
            os.read(done_read, 1)
            session.execute(text("SELECT 1"))
    finally:
        os.waitpid(child, 0)


@pytest.mark.skipif(
    "PQ_TLS_DATABASE_URL" not in os.environ,
    reason="needs a Postgres reachable over TLS (PQ_TLS_DATABASE_URL)",
)
class TestTaskChildConnectionsOverTls:
    @pytest.fixture
    def tls_pq(self) -> Any:
        pq = PQ(os.environ["PQ_TLS_DATABASE_URL"])
        pq.create_tables()
        pq.clear_all()
        yield pq
        pq.clear_all()
        pq.close()

    def test_parent_session_survives_a_child_query(self, tls_pq: PQ) -> None:
        """One query from the child must not break the parent's open session."""
        _child_queries_while_parent_holds_session(tls_pq)

    def test_worker_loop_claims_cleanly_over_tls(
        self, tls_pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        errors = manager.list()
        started = manager.list()
        tls_pq.enqueue(hold_connection_handler, seconds=2.0)
        for _ in range(5):
            tls_pq.enqueue(noop_handler)

        worker = multiprocessing.get_context("fork").Process(
            target=_run_worker_capturing_errors,
            args=(os.environ["PQ_TLS_DATABASE_URL"], errors, started, 6.0),
        )
        worker.start()
        worker.join(timeout=30)
        if worker.is_alive():
            worker.kill()
            worker.join()
            pytest.fail(f"worker hung; errors logged: {list(errors)}")

        assert list(errors) == []
        assert tls_pq.pending_count() == 0
