"""A worker only holds as many sync tasks as it has threads for, so it
can't hoard work that other workers could be running.
"""

import asyncio
import time

import pytest
from django.tasks import task, task_backends
from django.tasks.base import TaskResultStatus
from django.test import override_settings
from django.utils import timezone

from example.demoapp.tasks import say_hello
from werker.models import DBTaskResult, RateLimitBucket, TaskStatus
from werker.worker.core import Worker

TWO_THREADS = {
    "default": {
        "BACKEND": "werker.backend.PostgresTaskBackend",
        "OPTIONS": {"CONCURRENCY": 2, "POLL_INTERVAL": 0.01},
    }
}


@task
def blocks_a_thread() -> None:
    time.sleep(0.1)


@pytest.mark.django_db(transaction=True)
async def test_worker_holds_no_more_sync_tasks_than_threads():
    with override_settings(TASKS=TWO_THREADS):
        results = [await blocks_a_thread.aenqueue() for _ in range(6)]
        # Started, not just claimed: a claim the worker can't run is released
        # within milliseconds, but a started task holds its row until done.
        started = DBTaskResult.objects.filter(status=TaskStatus.RUNNING).exclude(worker_ids=[])
        worker_run = asyncio.ensure_future(Worker(once=True).run())
        max_started = 0
        while not worker_run.done():
            max_started = max(max_started, await started.acount())
            await asyncio.sleep(0.005)
        await worker_run

    assert max_started <= 2
    for result in results:
        await result.arefresh()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert result.attempts == 1


@pytest.mark.django_db(transaction=True)
def test_release_returns_a_claim_without_counting_the_attempt():
    result = say_hello.enqueue("world")
    broker = task_backends["default"].broker
    [item] = broker.claim(queue_names=["default"], limit=1, worker_id="worker-a")

    assert broker.release(item, worker_id="worker-a") is True
    assert broker.release(item, worker_id="worker-a") is False

    row = DBTaskResult.objects.get(id=result.id)
    assert row.status == TaskStatus.READY
    assert row.attempts == 0
    assert row.started_at is None
    assert row.claimed_by == ""


@pytest.mark.django_db(transaction=True)
def test_release_refunds_the_rate_limit_token_its_claim_took():
    say_hello.enqueue("world")
    RateLimitBucket.objects.create(key=say_hello.module_path, tokens=0.5, updated_at=timezone.now())
    broker = task_backends["default"].broker
    [item] = broker.claim(queue_names=["default"], limit=1, worker_id="worker-a")
    tokens_after_claim = RateLimitBucket.objects.get(key=say_hello.module_path).tokens

    broker.release(item, worker_id="worker-a")

    assert RateLimitBucket.objects.get(key=say_hello.module_path).tokens == tokens_after_claim + 1
