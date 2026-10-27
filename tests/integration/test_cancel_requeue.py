"""werker.api.cancel / requeue, from code and from the admin."""

import asyncio
import threading

import pytest
from django.tasks import task
from django.tasks.base import TaskResultStatus
from django.tasks.exceptions import TaskResultDoesNotExist

from example.demoapp.tasks import always_fails, say_hello
from werker import api
from werker.decorators import retry
from werker.models import DBTaskResult, TaskStatus
from werker.worker.core import Worker

sync_started = threading.Event()
release_sync = threading.Event()


@task
async def waits_forever() -> None:
    await asyncio.sleep(60)


@retry(max_retries=5)
@task
def blocks_then_fails() -> None:
    sync_started.set()
    release_sync.wait(10)
    raise RuntimeError("failed after cancel")


async def _run_until(worker, predicate, timeout=10):
    run = asyncio.ensure_future(worker.run())
    try:
        for _ in range(int(timeout / 0.05)):
            if await predicate():
                return
            await asyncio.sleep(0.05)
        raise AssertionError("condition not reached")
    finally:
        worker.request_shutdown()
        await asyncio.wait_for(run, timeout=10)


def _fast_worker():
    worker = Worker()
    worker.poll_interval = 0.05
    worker.heartbeat_interval = 0.05
    return worker


@pytest.mark.django_db(transaction=True)
def test_cancel_ready_task_fails_it_without_running():
    result = say_hello.enqueue("never")
    assert api.cancel(result.id) is True

    result.refresh()
    assert result.status == TaskResultStatus.FAILED
    assert result.errors[0].exception_class_path == "werker.exceptions.TaskCancelled"

    asyncio.run(Worker(once=True).run())
    result.refresh()
    assert result.status == TaskResultStatus.FAILED
    assert result.worker_ids == []


@pytest.mark.django_db(transaction=True)
def test_cancel_finished_task_returns_false():
    result = say_hello.enqueue("done")
    asyncio.run(Worker(once=True).run())
    assert api.cancel(result.id) is False


@pytest.mark.django_db
def test_cancel_unknown_id_raises():
    with pytest.raises(TaskResultDoesNotExist):
        api.cancel("not-a-uuid")


@pytest.mark.django_db(transaction=True)
async def test_cancel_running_async_task_interrupts_it():
    result = await waits_forever.aenqueue()
    worker = _fast_worker()
    requested = False

    async def cancelled():
        nonlocal requested
        row = await DBTaskResult.objects.aget(id=result.id)
        if row.status == TaskStatus.RUNNING and not requested:
            requested = await api.acancel(result.id)
        return row.status == TaskStatus.FAILED

    await _run_until(worker, cancelled)
    await result.arefresh()
    assert result.attempts == 1
    assert result.errors[-1].exception_class_path == "werker.exceptions.TaskCancelled"


@pytest.mark.django_db(transaction=True)
async def test_cancel_running_sync_task_stops_its_retries():
    sync_started.clear()
    release_sync.clear()
    result = await blocks_then_fails.aenqueue()
    worker = _fast_worker()

    async def finished():
        if sync_started.is_set() and not release_sync.is_set():
            await api.acancel(result.id)
            await asyncio.sleep(0.2)  # a few heartbeats
            release_sync.set()
        row = await DBTaskResult.objects.aget(id=result.id)
        return row.status == TaskStatus.FAILED

    try:
        await _run_until(worker, finished)
    finally:
        release_sync.set()
    await result.arefresh()
    assert result.attempts == 1  # despite max_retries=5
    assert result.errors[-1].exception_class is RuntimeError


@pytest.mark.django_db(transaction=True)
def test_requeue_failed_task_runs_it_again_with_fresh_budget():
    from django.test import override_settings

    with override_settings(
        TASKS={
            "default": {
                "BACKEND": "werker.backend.PostgresTaskBackend",
                "OPTIONS": {"MAX_RETRIES": 0},
            }
        }
    ):
        result = always_fails.enqueue()
        asyncio.run(Worker(once=True).run())
        result.refresh()
        assert result.status == TaskResultStatus.FAILED

        assert api.requeue(result.id) is True
        row = DBTaskResult.objects.get(id=result.id)
        assert row.status == TaskStatus.READY
        assert row.attempts == 0
        assert row.finished_at is None

        asyncio.run(Worker(once=True).run())
        result.refresh()
    assert result.status == TaskResultStatus.FAILED
    # TaskResult.attempts counts worker_ids, which keep their history.
    assert DBTaskResult.objects.get(id=result.id).attempts == 1
    assert len(result.errors) == 2  # history kept


@pytest.mark.django_db(transaction=True)
def test_requeue_refuses_non_failed_task():
    result = say_hello.enqueue("ok")
    assert api.requeue(result.id) is False


@pytest.mark.django_db(transaction=True)
def test_admin_actions_cancel_and_requeue(admin_client):
    ready = say_hello.enqueue("admin")
    url = "/admin/werker/dbtaskresult/"

    response = admin_client.post(
        url, {"action": "cancel_tasks", "_selected_action": [str(ready.id)]}, follow=True
    )
    assert response.status_code == 200
    assert "1 task(s) cancelled." in response.content.decode()
    assert DBTaskResult.objects.get(id=ready.id).status == TaskStatus.FAILED

    response = admin_client.post(
        url, {"action": "requeue_tasks", "_selected_action": [str(ready.id)]}, follow=True
    )
    assert "1 task(s) requeued." in response.content.decode()
    assert DBTaskResult.objects.get(id=ready.id).status == TaskStatus.READY
