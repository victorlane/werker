"""Worker behavior that django.tasks callers rely on: claim order, queue
selection, deferred runs, TaskContext, heartbeats, signals, and the
`taskworker` command.
"""

import asyncio
from datetime import timedelta

import pytest
from django.core.management import call_command
from django.tasks import task, task_backends
from django.tasks.base import TaskContext, TaskResultStatus
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.test import override_settings
from django.utils import timezone

from example.demoapp.tasks import say_hello
from werker.models import DBTaskResult
from werker.worker.core import Worker

TWO_QUEUES = {
    "default": {
        "BACKEND": "werker.backend.PostgresTaskBackend",
        "QUEUES": ["default", "other"],
    }
}
FAST_HEARTBEAT = {
    "default": {
        "BACKEND": "werker.backend.PostgresTaskBackend",
        "OPTIONS": {"HEARTBEAT_INTERVAL": 0.05},
    }
}


@task(takes_context=True)
async def reports_own_heartbeat(context: TaskContext) -> dict[str, object]:
    await asyncio.sleep(0.2)
    row = await DBTaskResult.objects.aget(id=context.task_result.id)
    return {
        "attempt": context.attempt,
        "heartbeat_advanced": row.last_heartbeat_at > row.claimed_at,
    }


def _claim_ids(limit, queue_names=("default",)):
    broker = task_backends["default"].broker
    return [
        item.id
        for item in broker.claim(queue_names=list(queue_names), limit=limit, worker_id="test")
    ]


@pytest.mark.django_db(transaction=True)
def test_claim_takes_the_highest_priority_first_one_at_a_time():
    now = timezone.now()
    late_low = say_hello.using(priority=0, run_after=now - timedelta(seconds=1)).enqueue("a")
    early_low = say_hello.using(priority=0, run_after=now - timedelta(seconds=2)).enqueue("b")
    high = say_hello.using(priority=10).enqueue("c")

    claimed_in_order = [_claim_ids(limit=1)[0] for _ in range(3)]

    assert claimed_in_order == [high.id, early_low.id, late_low.id]


@pytest.mark.django_db(transaction=True)
def test_deferred_task_is_not_claimed_before_run_after():
    say_hello.using(run_after=timezone.now() + timedelta(hours=1)).enqueue("later")

    assert _claim_ids(limit=10) == []


@pytest.mark.django_db(transaction=True)
@override_settings(TASKS=TWO_QUEUES)
def test_claim_only_takes_the_requested_queues():
    on_default = say_hello.enqueue("default")
    on_other = say_hello.using(queue_name="other").enqueue("other")

    assert _claim_ids(limit=10, queue_names=["other"]) == [on_other.id]
    assert _claim_ids(limit=10, queue_names=["default"]) == [on_default.id]


@pytest.mark.django_db(transaction=True)
async def test_context_task_gets_its_attempt_and_heartbeats_while_running():
    with override_settings(TASKS=FAST_HEARTBEAT):
        result = await reports_own_heartbeat.aenqueue()

        await Worker(once=True).run()

        await result.arefresh()
    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == {"attempt": 1, "heartbeat_advanced": True}


@pytest.mark.django_db(transaction=True)
async def test_each_task_signal_fires_once_per_run():
    received = []

    def receiver(signal, **kwargs):
        received.append(signal)

    for signal in (task_enqueued, task_started, task_finished):
        signal.connect(receiver, weak=False)
    try:
        await say_hello.aenqueue("world")
        await Worker(once=True).run()
    finally:
        for signal in (task_enqueued, task_started, task_finished):
            signal.disconnect(receiver)

    assert received == [task_enqueued, task_started, task_finished]


@pytest.mark.django_db(transaction=True)
@override_settings(TASKS=TWO_QUEUES)
def test_taskworker_command_drains_the_requested_queue():
    on_default = say_hello.enqueue("world")
    on_other = say_hello.using(queue_name="other").enqueue("other")

    call_command("taskworker", once=True, queues="other")

    assert DBTaskResult.objects.get(id=on_other.id).status == TaskResultStatus.SUCCESSFUL
    assert DBTaskResult.objects.get(id=on_default.id).status == TaskResultStatus.READY
