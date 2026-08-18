"""Phase 3 checkpoint: PostgresTaskBackend wired to real django.tasks,
exercised end-to-end with a manual one-shot claim (no worker loop yet —
that's phase 4). Proves @task / .enqueue() / .get_result() / .refresh()
all work against a real Postgres, and that the Broker/ResultStore split
composes correctly through the outer backend.
"""

import pytest
from django.tasks import task_backends
from django.tasks.base import TaskResultStatus

from example.demoapp.tasks import say_hello
from werker.models import DeliveryGuarantee


@pytest.mark.django_db(transaction=True)
def test_enqueue_claim_execute_refresh_round_trip():
    result = say_hello.enqueue("world")

    assert result.status == TaskResultStatus.READY
    assert result.task.func is say_hello.func
    assert result.args == ["world"]

    backend = task_backends["default"]
    claimed = backend.broker.claim(queue_names=["default"], limit=1, worker_id="test-worker")
    assert [item.id for item in claimed] == [result.id]

    backend.result_store.mark_running(result.id, worker_id="test-worker", attempt=1)
    return_value = say_hello.func("world")
    backend.result_store.mark_successful(result.id, return_value=return_value)
    backend.broker.ack(result.id)

    result.refresh()
    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == "hello, world"
    assert result.attempts == 1
    assert result.worker_ids == ["test-worker"]

    # fetched independently via get_result, not just the mutated local object
    refetched = say_hello.get_result(result.id)
    assert refetched.status == TaskResultStatus.SUCCESSFUL
    assert refetched.return_value == "hello, world"


@pytest.mark.django_db(transaction=True)
def test_default_delivery_guarantee_is_at_least_once():
    result = say_hello.enqueue("victor")
    from werker.models import DBTaskResult

    row = DBTaskResult.objects.get(id=result.id)
    assert row.delivery_guarantee == DeliveryGuarantee.AT_LEAST_ONCE
