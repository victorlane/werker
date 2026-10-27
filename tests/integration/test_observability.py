"""Structured worker logs and the werker_stats probe."""

import io
import json
import logging
from datetime import timedelta

import pytest
from django.core.management import call_command
from django.utils import timezone

from example.demoapp.tasks import say_hello
from werker.models import DBTaskResult, TaskStatus
from werker.worker.core import Worker


@pytest.mark.django_db(transaction=True)
async def test_finished_attempt_logs_structured_fields(caplog):
    result = await say_hello.aenqueue("world")

    with caplog.at_level(logging.INFO, logger="werker.worker"):
        await Worker(once=True).run()

    [record] = [r for r in caplog.records if getattr(r, "task_id", None) == result.id]
    assert record.task_path == say_hello.module_path
    assert record.attempt == 1
    assert record.status == TaskStatus.SUCCESSFUL
    assert record.duration_ms >= 0
    assert record.worker_id


def _row(**fields):
    now = timezone.now()
    defaults = {"task_path": "tests.dummy_task", "run_after": now, "enqueued_at": now}
    return DBTaskResult.objects.create(**{**defaults, **fields})


@pytest.mark.django_db(transaction=True)
def test_werker_stats_reports_queue_health_as_json():
    now = timezone.now()
    _row(run_after=now - timedelta(minutes=10))
    _row()
    _row(run_after=now + timedelta(hours=1))
    _row(status=TaskStatus.RUNNING, last_heartbeat_at=now)
    _row(status=TaskStatus.RUNNING, last_heartbeat_at=now - timedelta(hours=1))
    _row(status=TaskStatus.SUCCESSFUL)
    out = io.StringIO()

    call_command("werker_stats", json=True, stdout=out)

    [default_queue] = json.loads(out.getvalue())["queues"]
    assert default_queue["queue_name"] == "default"
    assert default_queue["ready"] == 2
    assert default_queue["deferred"] == 1
    assert default_queue["running"] == 2
    assert default_queue["stale_running"] == 1
    assert 590 <= default_queue["oldest_ready_age_seconds"] <= 700


@pytest.mark.django_db(transaction=True)
def test_werker_stats_prints_a_table_by_default():
    _row()
    out = io.StringIO()

    call_command("werker_stats", stdout=out)

    assert "default" in out.getvalue()
