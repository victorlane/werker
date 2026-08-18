"""Phase 4 checkpoint: `taskworker --once` (via Worker.run() directly, not
the management command, for a tighter test loop) against real Postgres,
covering both a sync and a native async @task, plus the retry/failure path.

Uses the `a`-prefixed async entrypoints throughout (.aenqueue/.arefresh) —
these are async test functions, and Django's async-safety check correctly
refuses a raw sync ORM call (like plain .enqueue()) from a thread with a
running event loop.
"""

import pytest
from django.tasks.base import TaskResultStatus
from django.test import override_settings

from example.demoapp.tasks import always_fails, say_hello, say_hello_async
from werker.worker.core import Worker


@pytest.mark.django_db(transaction=True)
async def test_sync_and_async_tasks_execute_and_succeed():
    sync_result = await say_hello.aenqueue("world")
    async_result = await say_hello_async.aenqueue("world")

    worker = Worker(once=True)
    await worker.run()

    await sync_result.arefresh()
    await async_result.arefresh()

    assert sync_result.status == TaskResultStatus.SUCCESSFUL
    assert sync_result.return_value == "hello, world"

    assert async_result.status == TaskResultStatus.SUCCESSFUL
    assert async_result.return_value == "hello (async), world"


@pytest.mark.django_db(transaction=True)
async def test_failing_task_retries_with_backoff_by_default():
    result = await always_fails.aenqueue()

    worker = Worker(once=True)
    await worker.run()

    await result.arefresh()

    # One attempt made, default MAX_RETRIES=3 means it's still eligible for
    # retry — back to READY with a future run_after, not terminally FAILED yet.
    assert result.status == TaskResultStatus.READY
    assert result.attempts == 1
    assert len(result.errors) == 1
    assert result.errors[0].exception_class is ValueError


@pytest.mark.django_db(transaction=True)
async def test_failing_task_marked_failed_once_retries_exhausted():
    with override_settings(
        TASKS={
            "default": {
                "BACKEND": "werker.backend.PostgresTaskBackend",
                "OPTIONS": {"MAX_RETRIES": 0},
            }
        }
    ):
        result = await always_fails.aenqueue()

        worker = Worker(once=True)
        await worker.run()

        await result.arefresh()

    assert result.status == TaskResultStatus.FAILED
    assert result.attempts == 1
