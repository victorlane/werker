"""@timeout and TASK_TIMEOUT for async and sync tasks."""

import asyncio
import threading
import time

import pytest
from django.tasks import task
from django.tasks.base import TaskResultStatus
from django.test import override_settings

from werker.decorators import at_most_once, retry, timeout
from werker.worker.core import Worker

release_sync = threading.Event()


@retry(max_retries=0)
@timeout(0.2)
@task
async def slow_async() -> None:
    await asyncio.sleep(30)


@retry(max_retries=0)
@timeout(0.2)
@task
def slow_sync() -> None:
    release_sync.wait(10)


@at_most_once
@timeout(0.2)
@task
def slow_sync_at_most_once() -> None:
    release_sync.wait(10)


@task
async def raises_its_own_timeout() -> None:
    raise TimeoutError("from the task itself")


@pytest.mark.django_db(transaction=True)
async def test_async_task_past_timeout_is_cancelled_and_failed():
    result = await slow_async.aenqueue()
    started = time.monotonic()
    await asyncio.wait_for(Worker(once=True).run(), timeout=10)

    await result.arefresh()
    assert time.monotonic() - started < 5
    assert result.status == TaskResultStatus.FAILED
    assert result.errors[0].exception_class_path == "werker.exceptions.TaskTimeout"


@pytest.mark.django_db(transaction=True)
async def test_sync_task_past_timeout_is_failed_and_its_thread_abandoned():
    release_sync.clear()
    result = await slow_sync.aenqueue()
    worker = Worker(once=True)
    capacity = worker._available_capacity()

    await asyncio.wait_for(worker.run(), timeout=10)

    await result.arefresh()
    assert result.status == TaskResultStatus.FAILED
    assert result.errors[0].exception_class_path == "werker.exceptions.TaskTimeout"
    # The thread is still blocked, so it still holds a slot.
    assert worker._abandoned == 1
    assert worker._available_capacity() == capacity - 1
    release_sync.set()


@pytest.mark.django_db(transaction=True)
async def test_abandoned_slot_is_released_when_the_thread_returns():
    release_sync.clear()
    await slow_sync.aenqueue()
    worker = Worker(once=False)
    worker.poll_interval = 0.05
    run = asyncio.ensure_future(worker.run())
    try:
        for _ in range(100):
            if worker._abandoned == 1:
                break
            await asyncio.sleep(0.05)
        assert worker._abandoned == 1

        release_sync.set()
        for _ in range(100):
            if worker._abandoned == 0:
                break
            await asyncio.sleep(0.05)
        assert worker._abandoned == 0
    finally:
        release_sync.set()
        worker.request_shutdown()
        await asyncio.wait_for(run, timeout=10)


@pytest.mark.django_db(transaction=True)
async def test_at_most_once_task_is_not_retried_after_timeout():
    release_sync.clear()
    result = await slow_sync_at_most_once.aenqueue()
    await asyncio.wait_for(Worker(once=True).run(), timeout=10)
    release_sync.set()

    await result.arefresh()
    assert result.status == TaskResultStatus.FAILED
    assert result.attempts == 1


@pytest.mark.django_db(transaction=True)
async def test_backend_task_timeout_applies_without_decorator():
    with override_settings(
        TASKS={
            "default": {
                "BACKEND": "werker.backend.PostgresTaskBackend",
                "OPTIONS": {"TASK_TIMEOUT": 0.2, "MAX_RETRIES": 0},
            }
        }
    ):
        from example.demoapp.tasks import say_hello_async

        result = await say_hello_async.aenqueue("fast")
        worker = Worker(once=True)
        assert worker.task_timeout == 0.2
        await asyncio.wait_for(worker.run(), timeout=10)
        await result.arefresh()
    assert result.status == TaskResultStatus.SUCCESSFUL


@pytest.mark.django_db(transaction=True)
async def test_task_raising_timeout_error_itself_is_not_a_task_timeout():
    result = await raises_its_own_timeout.aenqueue()
    await asyncio.wait_for(Worker(once=True).run(), timeout=10)
    await result.arefresh()
    assert result.errors[0].exception_class is TimeoutError
