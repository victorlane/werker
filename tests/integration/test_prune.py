"""Tests for prune_task_results and the runserver autostart gate."""

from datetime import timedelta

import pytest
from django.core.management import call_command
from django.utils import timezone

from werker.management.commands.prune_task_results import parse_duration
from werker.models import DBTaskResult, TaskStatus
from werker.worker.autostart import _is_runserver


def test_parse_duration_formats():
    assert parse_duration("30d") == timedelta(days=30)
    assert parse_duration("12h") == timedelta(hours=12)
    assert parse_duration("90m") == timedelta(minutes=90)
    assert parse_duration("60s") == timedelta(seconds=60)
    assert parse_duration("5") == timedelta(hours=5)


def test_is_runserver_false_by_default(monkeypatch):
    monkeypatch.setattr("sys.argv", ["manage.py", "migrate"])
    assert _is_runserver() is False


def test_is_runserver_true_for_runserver(monkeypatch):
    monkeypatch.setattr("sys.argv", ["manage.py", "runserver"])
    assert _is_runserver() is True


@pytest.mark.django_db(transaction=True)
def test_prune_deletes_only_old_terminal_rows():
    now = timezone.now()
    old_successful = DBTaskResult.objects.create(
        task_path="example.demoapp.tasks.say_hello",
        args_json=[],
        kwargs_json={},
        status=TaskStatus.SUCCESSFUL,
        run_after=now - timedelta(days=60),
        enqueued_at=now - timedelta(days=60),
        finished_at=now - timedelta(days=60),
    )
    old_failed = DBTaskResult.objects.create(
        task_path="example.demoapp.tasks.say_hello",
        args_json=[],
        kwargs_json={},
        status=TaskStatus.FAILED,
        run_after=now - timedelta(days=60),
        enqueued_at=now - timedelta(days=60),
        finished_at=now - timedelta(days=60),
    )
    recent_successful = DBTaskResult.objects.create(
        task_path="example.demoapp.tasks.say_hello",
        args_json=[],
        kwargs_json={},
        status=TaskStatus.SUCCESSFUL,
        run_after=now,
        enqueued_at=now,
        finished_at=now,
    )
    running_row = DBTaskResult.objects.create(
        task_path="example.demoapp.tasks.say_hello",
        args_json=[],
        kwargs_json={},
        status=TaskStatus.RUNNING,
        run_after=now - timedelta(days=60),
        enqueued_at=now - timedelta(days=60),
        finished_at=None,
    )

    call_command("prune_task_results", older_than="30d", verbosity=0)

    assert not DBTaskResult.objects.filter(id=old_successful.id).exists()
    assert not DBTaskResult.objects.filter(id=old_failed.id).exists()
    assert DBTaskResult.objects.filter(id=recent_successful.id).exists()
    assert DBTaskResult.objects.filter(id=running_row.id).exists()


@pytest.mark.django_db(transaction=True)
def test_prune_dry_run_deletes_nothing():
    now = timezone.now()
    DBTaskResult.objects.create(
        task_path="example.demoapp.tasks.say_hello",
        args_json=[],
        kwargs_json={},
        status=TaskStatus.SUCCESSFUL,
        run_after=now - timedelta(days=60),
        enqueued_at=now - timedelta(days=60),
        finished_at=now - timedelta(days=60),
    )

    call_command("prune_task_results", older_than="30d", dry_run=True, verbosity=0)

    assert DBTaskResult.objects.count() == 1


@pytest.mark.django_db(transaction=True)
def test_prune_status_filter_only_deletes_requested_status():
    now = timezone.now()
    old_successful = DBTaskResult.objects.create(
        task_path="example.demoapp.tasks.say_hello",
        args_json=[],
        kwargs_json={},
        status=TaskStatus.SUCCESSFUL,
        run_after=now - timedelta(days=60),
        enqueued_at=now - timedelta(days=60),
        finished_at=now - timedelta(days=60),
    )
    old_failed = DBTaskResult.objects.create(
        task_path="example.demoapp.tasks.say_hello",
        args_json=[],
        kwargs_json={},
        status=TaskStatus.FAILED,
        run_after=now - timedelta(days=60),
        enqueued_at=now - timedelta(days=60),
        finished_at=now - timedelta(days=60),
    )

    call_command(
        "prune_task_results", older_than="30d", status="successful", verbosity=0
    )

    assert not DBTaskResult.objects.filter(id=old_successful.id).exists()
    assert DBTaskResult.objects.filter(id=old_failed.id).exists()


@pytest.mark.django_db(transaction=True)
def test_prune_rejects_unknown_status():
    with pytest.raises(ValueError, match="Unknown status"):
        call_command("prune_task_results", older_than="30d", status="bogus", verbosity=0)


@pytest.mark.django_db(transaction=True)
def test_prune_batch_size_deletes_all_rows_in_multiple_batches():
    now = timezone.now()
    for _ in range(5):
        DBTaskResult.objects.create(
            task_path="example.demoapp.tasks.say_hello",
            args_json=[],
            kwargs_json={},
            status=TaskStatus.SUCCESSFUL,
            run_after=now - timedelta(days=60),
            enqueued_at=now - timedelta(days=60),
            finished_at=now - timedelta(days=60),
        )

    # batch-size 2 forces the batched delete loop to run more than once.
    call_command("prune_task_results", older_than="30d", batch_size=2, verbosity=0)

    assert DBTaskResult.objects.count() == 0
