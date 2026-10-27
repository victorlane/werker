"""LISTEN/NOTIFY: enqueue wakes an idle worker at once; rollback doesn't;
the listener reconnects after losing its connection."""

import asyncio
import time

import pytest
from asgiref.sync import sync_to_async
from django.db import connection, transaction
from django.tasks.base import TaskResultStatus

from example.demoapp.tasks import say_hello
from werker.worker.core import Worker


async def _wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not reached")


async def _listening(worker):
    async def check():
        return worker._listening

    await _wait_for(check)


def _slow_polling_worker():
    worker = Worker()
    worker.poll_interval = 10  # so only a NOTIFY can explain a fast pickup
    return worker


@pytest.mark.django_db(transaction=True)
async def test_enqueue_wakes_an_idle_worker_immediately():
    worker = _slow_polling_worker()
    run = asyncio.ensure_future(worker.run())
    try:
        await _listening(worker)
        await asyncio.sleep(0.2)  # let the claim loop go idle

        started = time.monotonic()
        result = await say_hello.aenqueue("notify")

        async def done():
            await result.arefresh()
            return result.status == TaskResultStatus.SUCCESSFUL

        await _wait_for(done, timeout=5)
        assert time.monotonic() - started < 1.0
    finally:
        worker.request_shutdown()
        await asyncio.wait_for(run, timeout=10)


@pytest.mark.django_db(transaction=True)
async def test_rolled_back_enqueue_sends_no_notification():
    worker = _slow_polling_worker()
    run = asyncio.ensure_future(worker.run())
    try:
        await _listening(worker)

        def enqueue_and_roll_back():
            with transaction.atomic():
                say_hello.enqueue("rolled back")
                transaction.set_rollback(True)

        await sync_to_async(enqueue_and_roll_back)()
        await asyncio.sleep(0.5)
        assert worker._notifications == 0

        await say_hello.aenqueue("committed")

        async def notified():
            return worker._notifications == 1

        await _wait_for(notified, timeout=5)
    finally:
        worker.request_shutdown()
        await asyncio.wait_for(run, timeout=10)


@pytest.mark.django_db(transaction=True)
async def test_listener_reconnects_after_its_connection_is_killed(monkeypatch):
    monkeypatch.setattr("werker.worker.listener.MIN_RECONNECT_DELAY", 0.1)
    worker = _slow_polling_worker()
    run = asyncio.ensure_future(worker.run())
    try:
        await _listening(worker)
        old_pid = worker._listener_pid

        def kill(pid):
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_terminate_backend(%s)", [pid])

        await sync_to_async(kill)(old_pid)

        async def reconnected():
            return worker._listening and worker._listener_pid != old_pid

        await _wait_for(reconnected, timeout=10)

        result = await say_hello.aenqueue("after reconnect")

        async def done():
            await result.arefresh()
            return result.status == TaskResultStatus.SUCCESSFUL

        await _wait_for(done, timeout=3)
    finally:
        worker.request_shutdown()
        await asyncio.wait_for(run, timeout=10)


@pytest.mark.django_db(transaction=True)
async def test_listen_notify_can_be_disabled():
    from django.test import override_settings

    with override_settings(
        TASKS={
            "default": {
                "BACKEND": "werker.backend.PostgresTaskBackend",
                "OPTIONS": {"LISTEN_NOTIFY": False, "POLL_INTERVAL": 0.05},
            }
        }
    ):
        worker = Worker()
        run = asyncio.ensure_future(worker.run())
        try:
            await asyncio.sleep(0.3)
            assert worker._listening is False
            assert worker.idle_poll_interval == 0.05
        finally:
            worker.request_shutdown()
            await asyncio.wait_for(run, timeout=10)
