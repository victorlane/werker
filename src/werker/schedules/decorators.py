"""The @schedule decorator: turns a @task into a periodic task.

Usage:
    @schedule(cron="*/5 * * * *", timezone="Europe/Amsterdam")
    @task
    def refresh_cache(): ...

    @schedule(every=timedelta(hours=1))
    @task
    def rebuild_stale(): ...

    @schedule(every=60, name="custom-name")
    @task
    def heartbeat(): ...

The decorated Task still works like any other @task (queue it imperatively,
trace it via result.id). @schedule only *additionally* registers it so
`manage.py syncschedules` copies it into the PeriodicTask table and the
worker's scheduler starts firing it.

Like werker's @at_most_once, @schedule accepts an already-built Task, so it
must wrap @task from the outside:

    @schedule(cron="...")   # outermost
    @task                   # innermost
    def my_task(): ...
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from zoneinfo import ZoneInfo

from croniter import croniter
from django.tasks.base import Task

from werker.models import ScheduleKind
from werker.schedules.registry import ScheduleDeclaration, register


def schedule(
    task: Task[..., Any] | None = None,
    *,
    every: timedelta | int | float | None = None,
    cron: str | None = None,
    name: str | None = None,
    timezone: str = "UTC",
    catchup: bool = False,
) -> Any:
    """Registers a periodic schedule for a @task.

    Exactly one of `every` or `cron` is required. `every` may be a
    `datetime.timedelta` or a number of seconds; `cron` is a 5-part cron
    expression validated with croniter.

    Supports both direct call (with a Task) and deferred call (bare as the
    @schedule(...) decorator factory)."""
    if every is None and cron is None:
        raise TypeError("schedule() requires one of 'every' or 'cron'.")
    if every is not None and cron is not None:
        raise TypeError("schedule() accepts only one of 'every' or 'cron'.")

    cron_expression = ""
    interval_seconds: int | None = None
    kind: ScheduleKind
    if cron is not None:
        kind = ScheduleKind.CRON
        cron_expression = _validate_cron(cron)
    elif every is not None:
        kind = ScheduleKind.INTERVAL
        interval_seconds = _validate_interval(every)
    else:  # pragma: no cover - guarded above, kept for mypy narrowing
        raise TypeError("schedule() requires one of 'every' or 'cron'.")

    def decorator(t: Task[..., Any]) -> Task[..., Any]:
        register(
            ScheduleDeclaration(
                name=name or t.module_path,
                task_path=t.module_path,
                kind=kind,
                args=(),
                kwargs={},
                queue_name=t.queue_name,
                priority=t.priority,
                cron_expression=cron_expression,
                interval_seconds=interval_seconds,
                timezone=timezone,
                catchup=catchup,
            )
        )
        return t

    return decorator(task) if task is not None else decorator


def _validate_cron(cron: str) -> str:
    if not croniter.is_valid(cron):
        raise ValueError(f"Invalid cron expression: {cron!r}.")
    return cron


def _validate_interval(every: timedelta | int | float) -> int:
    seconds = every.total_seconds() if isinstance(every, timedelta) else float(every)
    if seconds <= 0:
        raise ValueError(f"every must be a positive number of seconds, got {every!r}.")
    if int(seconds) != seconds:
        raise ValueError(f"every must be a whole number of seconds, got {every!r}.")
    return int(seconds)


def _validate_timezone(name: str) -> None:
    """Raises ZoneInfoNotFoundError for an unknown tz name. Exists to give
    syncschedules a cheap standalone check; not required by the registry."""
    ZoneInfo(name)
