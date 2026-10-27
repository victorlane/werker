"""Housekeeping shared by `prune_task_results` and the worker's automatic
cleanup (the RESULT_TTL option)."""

from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta
from typing import Any

from django.db import connections, router
from django.utils import timezone

from werker.models import DBTaskResult, TaskStatus, WorkerRecord

TERMINAL_STATUSES = (TaskStatus.SUCCESSFUL, TaskStatus.FAILED)
DEFAULT_BATCH_SIZE = 1000
#: Stopped or silent workers are forgotten after this long.
WORKER_RECORD_RETENTION = timedelta(days=7)
_CLEANUP_LOCK = "werker:cleanup"


def parse_duration(value: str | int | float | timedelta) -> timedelta:
    """Parses e.g. '30d', '12h', '90m', '60s', or a bare number of hours.
    A timedelta is returned unchanged."""
    if isinstance(value, timedelta):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return timedelta(hours=value)
    text = str(value).strip().lower()
    units = {"d": "days", "h": "hours", "m": "minutes", "s": "seconds"}
    if text and text[-1] in units:
        return timedelta(**{units[text[-1]]: int(text[:-1])})
    return timedelta(hours=int(text))


def terminal_statuses(statuses: Iterable[str]) -> tuple[str, ...]:
    """Upper-cases and validates statuses. Only SUCCESSFUL and FAILED rows
    may ever be pruned; READY/RUNNING raise ValueError."""
    wanted = tuple(s.strip().upper() for s in statuses if s.strip())
    for status in wanted:
        if status not in TERMINAL_STATUSES:
            raise ValueError(
                f"Can't prune status {status!r}; choose from "
                f"{', '.join(TERMINAL_STATUSES)}."
            )
    return wanted


def prunable(cutoff: datetime, statuses: Iterable[str]) -> Any:
    return DBTaskResult.objects.filter(
        status__in=terminal_statuses(statuses),
        finished_at__isnull=False,
        finished_at__lt=cutoff,
    )


def prune_results(
    cutoff: datetime, statuses: Iterable[str], *, batch_size: int = DEFAULT_BATCH_SIZE
) -> int:
    """Deletes finished task rows older than `cutoff`, in batches so each
    statement stays cheap on a large table. Returns the number deleted."""
    queryset = prunable(cutoff, statuses)
    total = 0
    while True:
        ids = list(queryset.values_list("id", flat=True)[:batch_size])
        if not ids:
            return total
        deleted, _ = DBTaskResult.objects.filter(id__in=ids).delete()
        total += deleted


def parse_result_ttl(ttl: Mapping[str, Any]) -> dict[str, timedelta]:
    """Validates the RESULT_TTL option, e.g. {"SUCCESSFUL": "7d"}."""
    statuses = terminal_statuses(ttl.keys())
    return {status: parse_duration(ttl[key]) for status, key in zip(statuses, ttl, strict=True)}


def run_cleanup(
    ttl: Mapping[str, timedelta], *, batch_size: int = DEFAULT_BATCH_SIZE
) -> int | None:
    """One cleanup pass: prunes results per their TTL and forgets old
    worker records. Only one process runs it at a time, cluster-wide;
    returns None if another one holds the lock."""
    # A session lock, not a transaction one, so each delete batch commits
    # on its own instead of holding every deleted row until the end.
    with connections[router.db_for_write(DBTaskResult)].cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(hashtext(%s))", [_CLEANUP_LOCK])
        (locked,) = cursor.fetchone()
        if not locked:
            return None
        try:
            now = timezone.now()
            deleted = sum(
                prune_results(now - age, [status], batch_size=batch_size)
                for status, age in ttl.items()
            )
            WorkerRecord.objects.filter(
                last_seen_at__lt=now - WORKER_RECORD_RETENTION
            ).delete()
            return deleted
        finally:
            cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", [_CLEANUP_LOCK])
