"""@rate_limit: token-bucket rate and global concurrency, at claim time."""

import asyncio
import time

import pytest
from django.tasks import task
from django.tasks.base import TaskResultStatus

from example.demoapp.tasks import say_hello
from werker import registry
from werker.broker.postgres import PostgresBroker
from werker.decorators import rate_limit
from werker.models import DBTaskResult, TaskStatus
from werker.worker.core import Worker


@rate_limit(rate="2/m")
@task
def two_per_minute(n: int) -> int:
    return n


@rate_limit(rate="20/s", burst=1)
@task
def twenty_per_second(n: int) -> int:
    return n


@rate_limit(concurrency=2)
@task
def two_at_a_time(n: int) -> int:
    return n


def _claim(worker_id="w-1", limit=10):
    return PostgresBroker(options=None).claim(
        queue_names=["default"],
        limit=limit,
        worker_id=worker_id,
        limits=registry.limited_policies(),
    )


def _paths(batch):
    return sorted(item.task_path.rsplit(".", 1)[-1] for item in batch)


@pytest.mark.django_db(transaction=True)
def test_rate_allows_a_burst_then_holds_back_with_a_wait_hint():
    for n in range(5):
        two_per_minute.enqueue(n)

    first = _claim()
    assert len(first) == 2  # burst = 2
    second = _claim()
    assert len(second) == 0
    # One token per 30 seconds.
    assert second.wait_hint is not None
    assert 20 < second.wait_hint <= 30
    assert DBTaskResult.objects.filter(status=TaskStatus.READY).count() == 3
    # Held-back tasks don't use up attempts.
    assert set(DBTaskResult.objects.filter(status=TaskStatus.READY).values_list(
        "attempts", flat=True
    )) == {0}


@pytest.mark.django_db(transaction=True)
def test_concurrency_counts_running_tasks_across_workers():
    for n in range(5):
        two_at_a_time.enqueue(n)

    assert len(_claim("w-1")) == 2
    assert len(_claim("w-2")) == 0

    finished = DBTaskResult.objects.filter(status=TaskStatus.RUNNING).first()
    DBTaskResult.objects.filter(id=finished.id).update(status=TaskStatus.SUCCESSFUL)
    assert len(_claim("w-2")) == 1


@pytest.mark.django_db(transaction=True)
def test_unlimited_tasks_use_the_remaining_capacity():
    for n in range(3):
        two_per_minute.enqueue(n)
    for name in ("a", "b", "c"):
        say_hello.enqueue(name)

    batch = _claim(limit=10)
    assert _paths(batch) == ["say_hello"] * 3 + ["two_per_minute"] * 2


@pytest.mark.django_db(transaction=True)
async def test_worker_paces_tasks_to_the_rate():
    results = [await twenty_per_second.aenqueue(n) for n in range(10)]
    started = time.monotonic()
    await asyncio.wait_for(Worker(once=True).run(), timeout=20)
    elapsed = time.monotonic() - started

    for result in results:
        await result.arefresh()
        assert result.status == TaskResultStatus.SUCCESSFUL
    # burst 1, then one every 50ms: about 0.45s for 10 tasks.
    assert 0.35 < elapsed < 5


@pytest.mark.django_db(transaction=True)
async def test_concurrency_limit_holds_with_two_workers():
    for n in range(8):
        await two_at_a_time.aenqueue(n)
    workers = [Worker(), Worker()]
    for worker in workers:
        worker.poll_interval = 0.02
    runs = [asyncio.ensure_future(w.run()) for w in workers]
    peak = 0
    try:
        for _ in range(500):
            running = await DBTaskResult.objects.filter(status=TaskStatus.RUNNING).acount()
            peak = max(peak, running)
            if not await DBTaskResult.objects.exclude(status=TaskStatus.SUCCESSFUL).aexists():
                break
            await asyncio.sleep(0.01)
        assert not await DBTaskResult.objects.exclude(status=TaskStatus.SUCCESSFUL).aexists()
    finally:
        for worker in workers:
            worker.request_shutdown()
        await asyncio.wait_for(asyncio.gather(*runs), timeout=10)
    assert 1 <= peak <= 2
