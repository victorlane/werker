"""syncschedules + scheduler dispatch against real Postgres.

Covers: sync a declaration into PeriodicTask, dispatch a due schedule into
the normal queue (which the worker then executes), skip-not-catchup when
behind, and prune/disable of removed declarations.

The scheduled tasks are module-level (django.tasks refuses nested
functions), so the declarations below register at import time.
"""

import asyncio
import threading
from datetime import timedelta

import pytest
from asgiref.sync import sync_to_async
from django.core.management import call_command
from django.core.management.base import CommandError
from django.tasks import task
from django.test import override_settings
from django.utils import timezone

from werker.broker.postgres import PostgresBroker
from werker.models import DBTaskResult, PeriodicTask, ScheduleKind, TaskStatus
from werker.schedules.decorators import schedule
from werker.schedules.registry import ScheduleDeclaration, clear, register
from werker.worker.core import Worker
from werker.worker.scheduler import _dispatch, _dispatch_sync, scheduler_loop


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
    clear()


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
    PeriodicTask.objects.filter(id=pt.id).update(
        claimed_by=worker.worker_id, last_heartbeat_at=timezone.now()
    )
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
    PeriodicTask.objects.filter(id=pt.id).update(
        claimed_by=worker.worker_id, last_heartbeat_at=timezone.now()
    )
    _dispatch_sync(worker, str(pt.id))

    assert not DBTaskResult.objects.filter(task_path=behind_task.module_path).exists()
    pt.refresh_from_db()
    assert pt.next_run_at > timezone.now()


@pytest.mark.django_db(transaction=True)
def test_schedule_claims_are_exclusive_until_the_claim_is_stale():
    call_command("syncschedules", verbosity=0)
    schedule_row = PeriodicTask.objects.get(name="integration-every-hour")
    PeriodicTask.objects.filter(id=schedule_row.id).update(next_run_at=timezone.now())
    broker = PostgresBroker({"STALE_SCHEDULE_CLAIM_TIMEOUT": 60})

    assert broker.claim_due_schedules(limit=1, worker_id="worker-one") == [str(schedule_row.id)]
    # It is still due, but a live lease belongs exclusively to worker one.
    assert broker.claim_due_schedules(limit=1, worker_id="worker-two") == []

    PeriodicTask.objects.filter(id=schedule_row.id).update(
        last_heartbeat_at=timezone.now() - timedelta(seconds=61)
    )
    assert broker.claim_due_schedules(limit=1, worker_id="worker-two") == [str(schedule_row.id)]
    schedule_row.refresh_from_db()
    assert schedule_row.claimed_by == "worker-two"


@pytest.mark.django_db(transaction=True)
def test_dispatch_requires_current_claim_owner_and_a_due_schedule():
    call_command("syncschedules", verbosity=0)
    schedule_row = PeriodicTask.objects.get(name="integration-every-hour")
    worker = Worker()
    PeriodicTask.objects.filter(id=schedule_row.id).update(
        next_run_at=timezone.now() - timedelta(seconds=1),
        claimed_by="another-worker",
        last_heartbeat_at=timezone.now(),
    )

    _dispatch_sync(worker, str(schedule_row.id))

    assert not DBTaskResult.objects.filter(task_path=periodic_thing.module_path).exists()
    schedule_row.refresh_from_db()
    assert schedule_row.claimed_by == "another-worker"


@pytest.mark.django_db(transaction=True)
def test_dispatch_does_not_fire_a_schedule_that_is_no_longer_due():
    call_command("syncschedules", verbosity=0)
    schedule_row = PeriodicTask.objects.get(name="integration-every-hour")
    worker = Worker()
    PeriodicTask.objects.filter(id=schedule_row.id).update(
        next_run_at=timezone.now() + timedelta(hours=1),
        claimed_by=worker.worker_id,
        last_heartbeat_at=timezone.now(),
    )

    _dispatch_sync(worker, str(schedule_row.id))

    assert not DBTaskResult.objects.filter(task_path=periodic_thing.module_path).exists()
    schedule_row.refresh_from_db()
    assert schedule_row.claimed_by == worker.worker_id


@pytest.mark.django_db(transaction=True)
def test_syncschedules_reports_unknown_timezone():
    clear()
    register(
        ScheduleDeclaration(
            name="invalid-timezone",
            task_path=periodic_thing.module_path,
            kind=ScheduleKind.INTERVAL,
            interval_seconds=60,
            timezone="Mars/Olympus_Mons",
        )
    )

    with pytest.raises(CommandError, match="unknown timezone"):
        call_command("syncschedules", verbosity=0)

    assert not PeriodicTask.objects.filter(name="invalid-timezone").exists()


@pytest.mark.django_db(transaction=True)
def test_syncschedules_validates_all_declarations_before_writing_any_rows():
    clear()
    register(
        ScheduleDeclaration(
            name="would-have-been-created",
            task_path=periodic_thing.module_path,
            kind=ScheduleKind.INTERVAL,
            interval_seconds=60,
        )
    )
    register(
        ScheduleDeclaration(
            name="invalid-timezone-after-valid-declaration",
            task_path=periodic_thing.module_path,
            kind=ScheduleKind.INTERVAL,
            interval_seconds=60,
            timezone="Mars/Olympus_Mons",
        )
    )

    with pytest.raises(CommandError, match="unknown timezone"):
        call_command("syncschedules", verbosity=0)

    assert not PeriodicTask.objects.filter(
        name__in=["would-have-been-created", "invalid-timezone-after-valid-declaration"]
    ).exists()


async def test_dispatch_does_not_compete_with_sync_tasks_for_threads(monkeypatch):
    thread_names = []
    monkeypatch.setattr(
        "werker.worker.scheduler._dispatch_sync",
        lambda worker, schedule_id: thread_names.append(threading.current_thread().name),
    )

    await _dispatch(Worker(), "any-id")

    assert thread_names[0].startswith("werker-broker")


@pytest.mark.django_db(transaction=True)
def test_syncschedules_updates_a_changed_declaration_and_reschedules_it():
    call_command("syncschedules", verbosity=0)
    schedule(every=7200, name="integration-every-hour")(periodic_thing)

    call_command("syncschedules", verbosity=0)

    pt = PeriodicTask.objects.get(name="integration-every-hour")
    assert pt.interval_seconds == 7200
    assert pt.next_run_at > timezone.now() + timedelta(seconds=3600)


@pytest.mark.django_db(transaction=True)
async def test_scheduler_loop_fires_a_due_schedule():
    fast_scheduler = {
        "default": {
            "BACKEND": "werker.backend.PostgresTaskBackend",
            "OPTIONS": {"SCHEDULER_POLL_INTERVAL": 0.01},
        }
    }
    await sync_to_async(call_command)("syncschedules", verbosity=0)
    await PeriodicTask.objects.filter(name="integration-every-hour").aupdate(
        next_run_at=timezone.now() - timedelta(seconds=1)
    )
    fired = DBTaskResult.objects.filter(task_path=periodic_thing.module_path)

    with override_settings(TASKS=fast_scheduler):
        worker = Worker()
        loop_task = asyncio.ensure_future(scheduler_loop(worker))
        async with asyncio.timeout(5):
            while not await fired.aexists():
                await asyncio.sleep(0.01)
        worker._shutdown.set()
        await loop_task

    assert await fired.acount() == 1
