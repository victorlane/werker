"""syncschedules + scheduler dispatch against real Postgres.

Covers: sync a declaration into PeriodicTask, dispatch a due schedule into
the normal queue (which the worker then executes), skip-not-catchup when
behind, and prune/disable of removed declarations.

The scheduled tasks are module-level (django.tasks refuses nested
functions), so the declarations below register at import time.
"""

from datetime import timedelta

import pytest
from django.core.management import call_command
from django.tasks import task
from django.utils import timezone

from werker.models import DBTaskResult, PeriodicTask, ScheduleKind, TaskStatus
from werker.schedules.decorators import schedule
from werker.schedules.registry import clear
from werker.worker.core import Worker
from werker.worker.scheduler import _dispatch_sync


@schedule(every=3600, name="integration-every-hour")
@task
def periodic_thing() -> str:
    return "periodic-done"


@schedule(every=60, name="will-be-removed")
@task
def doomed() -> None:
    return None


@schedule(every=60, name="behind-no-catchup")
@task
def behind_task() -> None:
    return None


@pytest.fixture(autouse=True)
def _reset_registry():
    # Re-register before every test: other test modules clear the shared
    # registry, so import-time registration cannot be relied upon.
    schedule(every=3600, name="integration-every-hour")(periodic_thing)
    schedule(every=60, name="will-be-removed")(doomed)
    schedule(every=60, name="behind-no-catchup")(behind_task)
    yield


@pytest.mark.django_db(transaction=True)
def test_syncschedules_creates_row_and_scheduler_fires_it():
    call_command("syncschedules", verbosity=0)
    pt = PeriodicTask.objects.get(name="integration-every-hour")
    assert pt.enabled is True
    assert pt.schedule_kind == ScheduleKind.INTERVAL
    assert pt.interval_seconds == 3600

    PeriodicTask.objects.filter(id=pt.id).update(
        next_run_at=timezone.now() - timedelta(seconds=1)
    )

    worker = Worker()
    _dispatch_sync(worker, str(pt.id))

    queued = DBTaskResult.objects.filter(task_path=periodic_thing.module_path)
    assert queued.count() == 1
    assert queued.get().status == TaskStatus.READY

    pt.refresh_from_db()
    assert pt.last_run_at is not None
    assert pt.next_run_at > timezone.now()


@pytest.mark.django_db(transaction=True)
def test_syncschedules_disables_removed_declarations():
    call_command("syncschedules", verbosity=0)
    assert PeriodicTask.objects.get(name="will-be-removed").enabled is True

    clear()  # simulate the declaration disappearing from code
    call_command("syncschedules", verbosity=0)
    assert PeriodicTask.objects.get(name="will-be-removed").enabled is False


@pytest.mark.django_db(transaction=True)
def test_dispatch_skips_not_catches_up_when_behind():
    call_command("syncschedules", verbosity=0)
    pt = PeriodicTask.objects.get(name="behind-no-catchup")
    # 10 minutes overdue, catchup=False -> nothing fires, next_run_at jumps
    # to a future occurrence.
    PeriodicTask.objects.filter(id=pt.id).update(
        next_run_at=timezone.now() - timedelta(minutes=10)
    )

    worker = Worker()
    _dispatch_sync(worker, str(pt.id))

    assert not DBTaskResult.objects.filter(task_path=behind_task.module_path).exists()
    pt.refresh_from_db()
    assert pt.next_run_at > timezone.now()
