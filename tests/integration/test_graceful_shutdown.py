"""Phase 6 checkpoint: SHUTDOWN_GRACE_PERIOD draining behavior.
Exercises Worker._drain_inflight directly with synthetic in-flight tasks
for determinism, rather than racing real OS signals against real @task
executions.
"""

import asyncio

import pytest

from werker.worker.core import Worker


@pytest.mark.django_db(transaction=True)
async def test_drain_waits_for_in_flight_work_within_grace_period():
    worker = Worker(once=False)
    worker.shutdown_grace_period = 5

    finished = asyncio.Event()

    async def quick_task():
        await asyncio.sleep(0.05)
        finished.set()

    task = asyncio.ensure_future(quick_task())
    worker._inflight.add(task)
    task.add_done_callback(worker._inflight.discard)

    await worker._drain_inflight()

    assert finished.is_set()


@pytest.mark.django_db(transaction=True)
async def test_drain_abandons_work_past_grace_period():
    worker = Worker(once=False)
    worker.shutdown_grace_period = 0.1

    started = asyncio.Event()
    was_cancelled = asyncio.Event()

    async def slow_task():
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            was_cancelled.set()
            raise

    task = asyncio.ensure_future(slow_task())
    worker._inflight.add(task)
    task.add_done_callback(worker._inflight.discard)

    await started.wait()
    await worker._drain_inflight()

    assert was_cancelled.is_set()


@pytest.mark.django_db(transaction=True)
async def test_wait_or_shutdown_returns_immediately_once_shutdown_is_set():
    worker = Worker(once=False)
    worker._shutdown.set()

    # If this actually slept the full timeout, the test would take 10s.
    await asyncio.wait_for(worker._wait_or_shutdown(timeout=10), timeout=1)
