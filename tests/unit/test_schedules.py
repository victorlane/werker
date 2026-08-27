"""Unit tests for @schedule, the schedule registry, and next-fire math.

No DB is needed; the decorator + registry are pure, and next-fire math is
deterministic given a fixed "now" anchor. Registration tests reuse the
example app's module-level @task functions (django.tasks refuses nested
functions).
"""

from datetime import UTC, datetime, timedelta

import pytest

from example.demoapp.tasks import say_hello
from werker.models import PeriodicTask, ScheduleKind
from werker.schedules.decorators import schedule
from werker.schedules.registry import clear, get_declarations
from werker.schedules.scheduler import next_run_after_fire


@pytest.fixture(autouse=True)
def _reset_registry():
    clear()
    yield
    clear()


def test_schedule_interval_registers_declaration():
    schedule(every=60, name="unit-every-minute")(say_hello)

    declarations = get_declarations()
    assert len(declarations) == 1
    d = declarations[0]
    assert d.name == "unit-every-minute"
    assert d.task_path == say_hello.module_path
    assert d.kind == ScheduleKind.INTERVAL
    assert d.interval_seconds == 60
    assert d.cron_expression == ""
    assert d.timezone == "UTC"
    assert d.catchup is False


def test_schedule_cron_registers_declaration():
    schedule(cron="*/5 * * * *", timezone="Europe/Amsterdam", name="unit-cron")(say_hello)

    d = get_declarations()[0]
    assert d.name == "unit-cron"
    assert d.kind == ScheduleKind.CRON
    assert d.cron_expression == "*/5 * * * *"
    assert d.timezone == "Europe/Amsterdam"
    assert d.interval_seconds is None


def test_schedule_accepts_timedelta_only_whole_seconds():
    with pytest.raises(ValueError):
        schedule(every=timedelta(milliseconds=1500))


def test_schedule_rejects_zero_interval():
    with pytest.raises(ValueError):
        schedule(every=0)


def test_schedule_rejects_both_and_neither():
    with pytest.raises(TypeError):
        schedule(every=1, cron="* * * * *")

    with pytest.raises(TypeError):
        schedule()


def test_schedule_rejects_invalid_cron():
    with pytest.raises(ValueError):
        schedule(cron="not a cron expression")


def test_interval_next_fire_is_strictly_after_fire():
    task_row = PeriodicTask(
        schedule_kind=ScheduleKind.INTERVAL,
        interval_seconds=60,
        timezone="UTC",
    )
    fired = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    assert next_run_after_fire(task_row, fired) == fired + timedelta(seconds=60)


def test_cron_next_fire_uses_expression():
    task_row = PeriodicTask(
        schedule_kind=ScheduleKind.CRON,
        cron_expression="0 0 * * *",  # midnight daily
        interval_seconds=None,
        timezone="UTC",
    )
    fired = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    nxt = next_run_after_fire(task_row, fired)
    assert nxt == datetime(2026, 1, 2, 0, 0, 0, tzinfo=UTC)
