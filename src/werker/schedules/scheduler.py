"""Computing when a periodic task should next fire.

Pure functions with no DB access so they're unit-testable without a
worker or database. The scheduler loop (werker/worker/scheduler.py) calls
these after it has locked a due PeriodicTask row.
"""

from datetime import datetime, timedelta
from typing import cast
from zoneinfo import ZoneInfo

from croniter import croniter
from django.utils import timezone

from werker.models import PeriodicTask, ScheduleKind


def next_run_after_fire(task: PeriodicTask, fired_at: datetime) -> datetime:
    """The next fire time strictly after `fired_at`, per this task's kind.

    CRON: the next cron occurrence after fired_at (croniter is naturally
    strict -> no double-fire at the boundary).
    INTERVAL: fired_at + interval_seconds.
    """
    if task.schedule_kind == ScheduleKind.CRON:
        tz = ZoneInfo(task.timezone or "UTC")
        local_fired = fired_at.astimezone(tz)
        return cast(datetime, croniter(task.cron_expression, local_fired).get_next(datetime))
    return fired_at + timedelta(seconds=int(task.interval_seconds or 0))


def next_run_after_sync(task: PeriodicTask) -> datetime:
    """The first fire time for a freshly-synced (or catch-up-realigned)
    schedule. Cron anchors to "now", interval anchors to "now" as well, so
    both kinds start cleanly rather than backfilling history."""
    now = timezone.now()
    if task.schedule_kind == ScheduleKind.CRON:
        tz = ZoneInfo(task.timezone or "UTC")
        local_now = now.astimezone(tz)
        return cast(datetime, croniter(task.cron_expression, local_now).get_next(datetime))
    return now + timedelta(seconds=int(task.interval_seconds or 0))
