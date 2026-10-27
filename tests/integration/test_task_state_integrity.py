"""Ownership and attempt tokens prevent stale workers from mutating newer work."""

import pytest
from django.utils import timezone

from werker.broker.postgres import PostgresBroker
from werker.models import DBTaskResult, TaskStatus
from werker.results.db import DBResultStore


def _running_row(**overrides):
    now = timezone.now()
    defaults = {
        "task_path": "tests.dummy_task",
        "queue_name": "default",
        "status": TaskStatus.RUNNING,
        "run_after": now,
        "enqueued_at": now,
        "claimed_by": "current-worker",
        "claimed_at": now,
        "last_heartbeat_at": now,
        "attempts": 2,
    }
    defaults.update(overrides)
    return DBTaskResult.objects.create(**defaults)


@pytest.mark.django_db(transaction=True)
def test_stale_worker_cannot_complete_fail_or_release_a_reclaimed_attempt():
    row = _running_row()
    store = DBResultStore(options=None)
    broker = PostgresBroker(options=None)

    assert not store.mark_successful(
        str(row.id), worker_id="old-worker", attempt=1, return_value="stale"
    )
    assert not store.mark_failed(
        str(row.id), worker_id="old-worker", attempt=1, error={"x": "y"}, will_retry=False
    )
    broker.ack(str(row.id), worker_id="old-worker", attempt=1)
    broker.nack(
        str(row.id), worker_id="old-worker", attempt=1, retry_after=timezone.now()
    )
    broker.heartbeat(str(row.id), worker_id="old-worker", attempt=1)

    row.refresh_from_db()
    assert row.status == TaskStatus.RUNNING
    assert row.claimed_by == "current-worker"
    assert row.attempts == 2
    assert row.errors == []


@pytest.mark.django_db(transaction=True)
def test_old_heartbeat_cannot_extend_a_new_attempt_by_the_same_worker():
    row = _running_row(claimed_by="reused-worker", attempts=2)
    broker = PostgresBroker(options=None)
    original_heartbeat = row.last_heartbeat_at

    broker.heartbeat(str(row.id), worker_id="reused-worker", attempt=1)

    row.refresh_from_db()
    assert row.last_heartbeat_at == original_heartbeat


@pytest.mark.django_db(transaction=True)
def test_worker_ids_and_errors_are_appended_without_lost_updates():
    row = _running_row(attempts=1)
    store = DBResultStore(options=None)

    assert store.mark_running(str(row.id), worker_id="current-worker", attempt=1)
    assert store.mark_failed(
        str(row.id),
        worker_id="current-worker",
        attempt=1,
        error={"exception_class_path": "first", "traceback": "first"},
        will_retry=True,
    )
    # The next claimed attempt contributes its own worker and error history.
    row.refresh_from_db()
    row.status = TaskStatus.RUNNING
    row.claimed_by = "next-worker"
    row.attempts = 2
    row.save(update_fields=["status", "claimed_by", "attempts"])
    assert store.mark_running(str(row.id), worker_id="next-worker", attempt=2)
    assert store.mark_failed(
        str(row.id),
        worker_id="next-worker",
        attempt=2,
        error={"exception_class_path": "second", "traceback": "second"},
        will_retry=False,
    )

    row.refresh_from_db()
    assert row.worker_ids == ["current-worker", "next-worker"]
    assert [error["exception_class_path"] for error in row.errors] == ["first", "second"]