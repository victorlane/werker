"""Proves the thread_sensitive=False fix works, deterministically
(thread-identity checks, not timing). Before this fix, every one of these
calls funneled through asgiref's single process-wide 1-worker executor
and every assertion here would fail with exactly one thread id observed.
"""

import asyncio
import threading
from unittest.mock import patch

import pytest
from asgiref.sync import sync_to_async
from django.utils import timezone

from example.demoapp.tasks import say_hello
from werker.broker.postgres import PostgresBroker
from werker.concurrency import DEFAULT_BOOKKEEPING_CONCURRENCY
from werker.models import DBTaskResult, TaskStatus
from werker.results.db import DBResultStore

CONCURRENT_CALLS = 8


def _seed_ready_rows(n: int) -> None:
    now = timezone.now()
    DBTaskResult.objects.bulk_create(
        [
            DBTaskResult(
                task_path="tests.dummy_task",
                queue_name="default",
                status=TaskStatus.READY,
                run_after=now,
                enqueued_at=now,
            )
            for _ in range(n)
        ]
    )


@pytest.mark.django_db(transaction=True)
async def test_broker_aclaim_runs_concurrently_across_multiple_threads():
    await sync_to_async(_seed_ready_rows, thread_sensitive=True)(CONCURRENT_CALLS)
    broker = PostgresBroker(options={"BOOKKEEPING_CONCURRENCY": 4})

    seen_thread_ids: set[int] = set()
    lock = threading.Lock()
    real_claim = broker.claim

    def instrumented_claim(*args, **kwargs):
        with lock:
            seen_thread_ids.add(threading.get_ident())
        return real_claim(*args, **kwargs)

    broker.claim = instrumented_claim  # instance-level shadow, see aclaim's self.claim lookup

    results = await asyncio.gather(
        *[
            broker.aclaim(queue_names=["default"], limit=1, worker_id=f"worker-{i}")
            for i in range(CONCURRENT_CALLS)
        ]
    )

    assert sum(len(r) for r in results) == CONCURRENT_CALLS
    assert len(seen_thread_ids) > 1, (
        "all claims ran on a single thread, the thread_sensitive=False fix regressed"
    )


@pytest.mark.django_db(transaction=True)
async def test_result_store_amark_successful_runs_concurrently_across_multiple_threads():
    def _seed_running_rows():
        now = timezone.now()
        return DBTaskResult.objects.bulk_create(
            [
                DBTaskResult(
                    task_path="tests.dummy_task",
                    queue_name="default",
                    status=TaskStatus.RUNNING,
                    run_after=now,
                    enqueued_at=now,
                )
                for _ in range(CONCURRENT_CALLS)
            ]
        )

    rows = await sync_to_async(_seed_running_rows, thread_sensitive=True)()

    store = DBResultStore(options={"BOOKKEEPING_CONCURRENCY": 4})
    seen_thread_ids: set[int] = set()
    lock = threading.Lock()
    real_mark_successful = store.mark_successful

    def instrumented_mark_successful(*args, **kwargs):
        with lock:
            seen_thread_ids.add(threading.get_ident())
        return real_mark_successful(*args, **kwargs)

    store.mark_successful = instrumented_mark_successful

    await asyncio.gather(
        *[store.amark_successful(str(row.id), return_value="ok") for row in rows]
    )

    assert len(seen_thread_ids) > 1, (
        "all mark_successful calls ran on a single thread, the fix regressed"
    )
    successful_count = await sync_to_async(
        DBTaskResult.objects.filter(status=TaskStatus.SUCCESSFUL).count, thread_sensitive=True
    )()
    assert successful_count == CONCURRENT_CALLS


@pytest.mark.django_db(transaction=True)
async def test_aenqueue_end_to_end_does_not_serialize_onto_asgirefs_shared_thread():
    """task.aenqueue(), the actual public API, no longer funnels through
    PostgresTaskBackend's inherited default. PostgresTaskBackend overrides
    aenqueue itself now."""
    seen_thread_ids: set[int] = set()
    lock = threading.Lock()
    original_create = DBResultStore.create

    def instrumented_create(self, **kwargs):
        with lock:
            seen_thread_ids.add(threading.get_ident())
        return original_create(self, **kwargs)

    with patch.object(DBResultStore, "create", instrumented_create):
        results = await asyncio.gather(
            *[say_hello.aenqueue(f"user-{i}") for i in range(CONCURRENT_CALLS)]
        )

    assert len(results) == CONCURRENT_CALLS
    assert len(seen_thread_ids) > 1, (
        "aenqueue() serialized onto a single thread, PostgresTaskBackend.aenqueue "
        "regressed to Django's default thread_sensitive=True wrapper"
    )


def test_bookkeeping_concurrency_is_configurable_and_defaults_sensibly():
    custom = PostgresBroker(options={"BOOKKEEPING_CONCURRENCY": 2})
    default = PostgresBroker(options=None)

    assert custom._executor._max_workers == 2
    assert default._executor._max_workers == DEFAULT_BOOKKEEPING_CONCURRENCY
