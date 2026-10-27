"""RESULT_TTL: automatic pruning by the worker, one worker at a time."""

import asyncio
from datetime import timedelta

import pytest
from asgiref.sync import sync_to_async
from django.core.exceptions import ImproperlyConfigured
from django.db import connections
from django.test import override_settings
from django.utils import timezone

from werker.maintenance import parse_result_ttl, run_cleanup
from werker.models import DBTaskResult, TaskStatus, WorkerRecord
from werker.worker.core import Worker


def _row(status, finished_days_ago):
    now = timezone.now()
    finished = now - timedelta(days=finished_days_ago) if finished_days_ago is not None else None
    return DBTaskResult.objects.create(
        task_path="tests.x",
        status=status,
        run_after=now,
        enqueued_at=now,
        finished_at=finished,
    )


def _options(**options):
    return override_settings(
        TASKS={"default": {"BACKEND": "werker.backend.PostgresTaskBackend", "OPTIONS": options}}
    )


@pytest.mark.django_db(transaction=True)
def test_cleanup_prunes_per_status_ttl_and_keeps_active_rows():
    old_ok = _row(TaskStatus.SUCCESSFUL, 10)
    new_ok = _row(TaskStatus.SUCCESSFUL, 1)
    old_failed = _row(TaskStatus.FAILED, 10)
    ancient_failed = _row(TaskStatus.FAILED, 40)
    ready = _row(TaskStatus.READY, None)
    running = _row(TaskStatus.RUNNING, None)
    now = timezone.now()
    WorkerRecord.objects.create(
        worker_id="old", hostname="h", pid=1, started_at=now, last_seen_at=now - timedelta(days=8)
    )
    WorkerRecord.objects.create(
        worker_id="recent", hostname="h", pid=2, started_at=now, last_seen_at=now
    )

    deleted = run_cleanup(parse_result_ttl({"SUCCESSFUL": "7d", "FAILED": "30d"}))

    assert deleted == 2
    remaining = set(DBTaskResult.objects.values_list("id", flat=True))
    assert remaining == {new_ok.id, old_failed.id, ready.id, running.id}
    assert old_ok.id not in remaining and ancient_failed.id not in remaining
    assert list(WorkerRecord.objects.values_list("worker_id", flat=True)) == ["recent"]


@pytest.mark.django_db(transaction=True)
def test_only_one_process_cleans_up_at_a_time():
    _row(TaskStatus.SUCCESSFUL, 10)
    other = connections.create_connection("default")
    try:
        with other.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_lock(hashtext('werker:cleanup'))")
        assert run_cleanup(parse_result_ttl({"SUCCESSFUL": "7d"})) is None
        assert DBTaskResult.objects.count() == 1
    finally:
        other.close()
    assert run_cleanup(parse_result_ttl({"SUCCESSFUL": "7d"})) == 1


@pytest.mark.django_db(transaction=True)
async def test_worker_cleans_up_when_result_ttl_is_set():
    await sync_to_async(_row)(TaskStatus.SUCCESSFUL, 10)
    with _options(RESULT_TTL={"SUCCESSFUL": "7d"}, POLL_INTERVAL=0.05):
        worker = Worker()
        run = asyncio.ensure_future(worker.run())
        try:
            for _ in range(100):
                if not await DBTaskResult.objects.aexists():
                    break
                await asyncio.sleep(0.05)
            assert not await DBTaskResult.objects.aexists()
        finally:
            worker.request_shutdown()
            await asyncio.wait_for(run, timeout=10)


@pytest.mark.django_db(transaction=True)
def test_default_config_never_deletes():
    _row(TaskStatus.SUCCESSFUL, 1000)
    worker = Worker()
    assert worker.result_ttl == {}
    assert DBTaskResult.objects.count() == 1


@pytest.mark.parametrize(
    "ttl", [{"READY": "1d"}, {"SUCCESSFUL": "soon"}, ["SUCCESSFUL"], {"BOGUS": "1d"}]
)
def test_bad_result_ttl_is_rejected(ttl):
    with _options(RESULT_TTL=ttl), pytest.raises(ImproperlyConfigured):
        Worker()
