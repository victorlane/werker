"""Atomicity of the enqueue write path.

enqueue()/aenqueue()/enqueue_schedule() write the DBTaskResult row and the
broker's queue markers in one transaction. If the broker write fails, the
row must not linger as an orphan READY row.
"""

import pytest
from django.tasks import task_backends

from example.demoapp.tasks import say_hello
from werker.models import DBTaskResult, ScheduleKind
from werker.schedules.registry import ScheduleDeclaration


@pytest.mark.django_db(transaction=True)
def test_enqueue_rolls_back_row_when_broker_enqueue_fails(monkeypatch):
    backend = task_backends["default"]

    def boom(item):
        raise RuntimeError("broker down")

    monkeypatch.setattr(backend.broker, "enqueue", boom)

    with pytest.raises(RuntimeError, match="broker down"):
        backend.enqueue(say_hello, ["x"], {})

    # The create() that already ran inside the atomic block must be rolled
    # back: no orphan READY row.
    assert DBTaskResult.objects.count() == 0


@pytest.mark.django_db(transaction=True)
async def test_aenqueue_rolls_back_row_when_broker_enqueue_fails(monkeypatch):
    from asgiref.sync import sync_to_async

    backend = task_backends["default"]

    def boom(item):
        raise RuntimeError("broker down")

    # aenqueue routes through _write_enqueue, which calls the sync broker.enqueue.
    monkeypatch.setattr(backend.broker, "enqueue", boom)

    with pytest.raises(RuntimeError, match="broker down"):
        await backend.aenqueue(say_hello, ["x"], {})

    count = await sync_to_async(DBTaskResult.objects.count)()
    assert count == 0


@pytest.mark.django_db(transaction=True)
def test_enqueue_schedule_rolls_back_row_when_broker_enqueue_fails(monkeypatch):
    backend = task_backends["default"]

    declaration = ScheduleDeclaration(
        name="atomic-schedule",
        task_path=say_hello.module_path,
        kind=ScheduleKind.INTERVAL,
        args=(),
        kwargs={},
        queue_name="default",
        priority=0,
        interval_seconds=60,
        timezone="UTC",
        catchup=False,
    )

    def boom(item):
        raise RuntimeError("broker down")

    monkeypatch.setattr(backend.broker, "enqueue", boom)

    with pytest.raises(RuntimeError, match="broker down"):
        backend.enqueue_schedule(declaration)

    assert DBTaskResult.objects.count() == 0
