"""The worker must survive transient broker errors, record errors raised
outside task code, and honour each row's own retry budget."""

import asyncio

import pytest
from django.db import OperationalError
from django.tasks import task
from django.tasks.base import TaskResultStatus
from django.test import override_settings
from django.utils import timezone

from example.demoapp.tasks import always_fails, say_hello
from werker.models import DBTaskResult, TaskStatus
from werker.worker.core import Worker
from werker.worker.reaper import reaper_loop


@task
def exits() -> None:
    raise SystemExit(3)


def _backend_options(**options):
    return override_settings(
        TASKS={"default": {"BACKEND": "werker.backend.PostgresTaskBackend", "OPTIONS": options}}
    )


@pytest.mark.django_db(transaction=True)
async def test_claim_loop_survives_transient_broker_error(monkeypatch):
    result = await say_hello.aenqueue("again")
    worker = Worker(once=True)
    worker.poll_interval = 0.01

    real_aclaim = worker.backend.broker.aclaim
    calls = 0

    async def flaky_aclaim(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OperationalError("server closed the connection unexpectedly")
        return await real_aclaim(**kwargs)

    monkeypatch.setattr(worker.backend.broker, "aclaim", flaky_aclaim)
    await asyncio.wait_for(worker.run(), timeout=10)

    await result.arefresh()
    assert calls >= 2
    assert result.status == TaskResultStatus.SUCCESSFUL


@pytest.mark.django_db(transaction=True)
async def test_reaper_loop_survives_transient_broker_error(monkeypatch):
    worker = Worker()
    worker.reaper_poll_interval = 0.01
    calls = 0

    async def failing_reclaim(**kwargs):
        nonlocal calls
        calls += 1
        if calls >= 3:
            worker.request_shutdown()
        raise OperationalError("server closed the connection unexpectedly")

    monkeypatch.setattr(worker.backend.broker, "areclaim_stale", failing_reclaim)
    await asyncio.wait_for(reaper_loop(worker), timeout=10)

    assert calls == 3


@pytest.mark.django_db(transaction=True)
async def test_unloadable_task_is_marked_failed_instead_of_left_running():
    with _backend_options(MAX_RETRIES=0):
        result = await say_hello.aenqueue("gone")
        # Simulates a deploy that removed the task while it sat in the queue.
        await DBTaskResult.objects.filter(id=result.id).aupdate(
            task_path="example.demoapp.tasks.removed_task"
        )

        await asyncio.wait_for(Worker(once=True).run(), timeout=10)

    row = await DBTaskResult.objects.aget(id=result.id)
    assert row.status == TaskStatus.FAILED
    assert row.claimed_by == ""
    assert len(row.errors) == 1
    assert "ImportError" in row.errors[0]["exception_class_path"]


@pytest.mark.django_db(transaction=True)
async def test_system_exit_from_task_is_a_failed_attempt():
    with _backend_options(MAX_RETRIES=0):
        result = await exits.aenqueue()
        await asyncio.wait_for(Worker(once=True).run(), timeout=10)

    await result.arefresh()
    assert result.status == TaskResultStatus.FAILED
    assert result.errors[0].exception_class is SystemExit


@pytest.mark.django_db(transaction=True)
async def test_max_retries_counts_retries_not_attempts():
    with _backend_options(MAX_RETRIES=1):
        result = await always_fails.aenqueue()

    # The retry budget is stored on the row, so a later settings change
    # must not alter it.
    with _backend_options(MAX_RETRIES=0):
        await asyncio.wait_for(Worker(once=True).run(), timeout=10)
        await result.arefresh()
        assert result.status == TaskResultStatus.READY
        assert result.attempts == 1

        await DBTaskResult.objects.filter(id=result.id).aupdate(run_after=timezone.now())
        await asyncio.wait_for(Worker(once=True).run(), timeout=10)

    await result.arefresh()
    assert result.status == TaskResultStatus.FAILED
    assert result.attempts == 2
