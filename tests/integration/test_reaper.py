"""PostgresBroker.reclaim_stale, exercised directly against manually-staled
rows (not the timed reaper_loop, for determinism). Proves the
AT_LEAST_ONCE-retries vs AT_MOST_ONCE/exhausted-retries-fails branch.
"""

from datetime import timedelta

import pytest
from django.utils import timezone

from werker.broker.postgres import PostgresBroker
from werker.models import DBTaskResult, DeliveryGuarantee, TaskStatus


def _stale_running_row(**overrides) -> DBTaskResult:
    now = timezone.now()
    defaults = {
        "task_path": "tests.dummy_task",
        "queue_name": "default",
        "status": TaskStatus.RUNNING,
        "run_after": now,
        "enqueued_at": now,
        "claimed_by": "dead-worker",
        "claimed_at": now - timedelta(minutes=10),
        "last_heartbeat_at": now - timedelta(minutes=10),
        "attempts": 1,
        "max_retries": 3,
    }
    defaults.update(overrides)
    return DBTaskResult.objects.create(**defaults)


@pytest.mark.django_db(transaction=True)
def test_at_least_once_with_retries_remaining_is_retried():
    row = _stale_running_row(
        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE, attempts=1, max_retries=3
    )

    broker = PostgresBroker(options=None)
    reclaimed = broker.reclaim_stale(
        stale_before=timezone.now() - timedelta(minutes=5), limit=10, retry_backoff_seconds=1.0
    )

    assert [item.action for item in reclaimed] == ["retried"]
    row.refresh_from_db()
    assert row.status == TaskStatus.READY
    assert row.run_after > timezone.now()
    assert row.claimed_by == ""


@pytest.mark.django_db(transaction=True)
def test_at_least_once_with_retries_exhausted_is_failed():
    row = _stale_running_row(
        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE, attempts=3, max_retries=3
    )

    broker = PostgresBroker(options=None)
    reclaimed = broker.reclaim_stale(
        stale_before=timezone.now() - timedelta(minutes=5), limit=10, retry_backoff_seconds=1.0
    )

    assert [item.action for item in reclaimed] == ["failed"]
    row.refresh_from_db()
    assert row.status == TaskStatus.FAILED
    assert row.errors[-1]["exception_class_path"] == "werker.exceptions.StaleClaimReclaimed"


@pytest.mark.django_db(transaction=True)
def test_at_most_once_is_never_retried_even_with_retries_remaining():
    row = _stale_running_row(
        delivery_guarantee=DeliveryGuarantee.AT_MOST_ONCE, attempts=1, max_retries=3
    )

    broker = PostgresBroker(options=None)
    reclaimed = broker.reclaim_stale(
        stale_before=timezone.now() - timedelta(minutes=5), limit=10, retry_backoff_seconds=1.0
    )

    assert [item.action for item in reclaimed] == ["failed"]
    row.refresh_from_db()
    assert row.status == TaskStatus.FAILED


@pytest.mark.django_db(transaction=True)
def test_fresh_heartbeat_is_not_reclaimed():
    row = _stale_running_row(
        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
        last_heartbeat_at=timezone.now(),
    )

    broker = PostgresBroker(options=None)
    reclaimed = broker.reclaim_stale(
        stale_before=timezone.now() - timedelta(minutes=5), limit=10, retry_backoff_seconds=1.0
    )

    assert reclaimed == []
    row.refresh_from_db()
    assert row.status == TaskStatus.RUNNING
