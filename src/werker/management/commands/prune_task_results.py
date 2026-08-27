"""Prune old task results to bound table growth.

    python manage.py prune_task_results --older-than=30d
    python manage.py prune_task_results --older-than=30d --status successful,failed
    python manage.py prune_task_results --dry-run

Only terminal rows (SUCCESSFUL/FAILED) are eligible for deletion; READY and
RUNNING rows are never touched regardless of age. Deletion is batched to
keep each statement cheap on large tables.
"""

from datetime import timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandParser
from django.db.models import QuerySet
from django.utils import timezone

from werker.models import DBTaskResult, TaskStatus

DEFAULT_RETENTION_HOURS = 24 * 30  # 30 days
DEFAULT_BATCH_SIZE = 1000
TERMINAL_STATUSES = (TaskStatus.SUCCESSFUL, TaskStatus.FAILED)


def parse_duration(value: str) -> timedelta:
    """Parses e.g. '30d', '12h', '90m', '60s', or a bare integer of hours."""
    value = value.strip().lower()
    if value.endswith("d"):
        return timedelta(days=int(value[:-1]))
    if value.endswith("h"):
        return timedelta(hours=int(value[:-1]))
    if value.endswith("m"):
        return timedelta(minutes=int(value[:-1]))
    if value.endswith("s"):
        return timedelta(seconds=int(value[:-1]))
    return timedelta(hours=int(value))


class Command(BaseCommand):
    help = "Delete terminal (SUCCESSFUL/FAILED) task results older than a retention window."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--older-than",
            default=None,
            help=(
                "Only delete rows whose finished_at is older than this. "
                "Accepts '30d', '12h', '90m', '60s', or bare hours. "
                "Default: 30d."
            ),
        )
        parser.add_argument(
            "--status",
            default=None,
            help=(
                "Comma-separated statuses to prune (successful,failed). "
                "Default: both terminal statuses."
            ),
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=DEFAULT_BATCH_SIZE,
            help="Number of rows to delete per DELETE statement.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report how many rows would be deleted without deleting.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        retention = (
            parse_duration(options["older_than"])
            if options["older_than"]
            else timedelta(hours=DEFAULT_RETENTION_HOURS)
        )
        cutoff = timezone.now() - retention

        statuses = self._normalize_statuses(options["status"])
        queryset = self._build_queryset(cutoff, statuses)

        if options["dry_run"]:
            count = queryset.count()
            self.stdout.write(
                self.style.WARNING(f"dry-run: {count} row(s) would be deleted.")
            )
            return

        total = self._delete_in_batches(queryset, options["batch_size"])
        self.stdout.write(
            self.style.SUCCESS(
                f"pruned {total} row(s) older than {retention} (cutoff {cutoff.isoformat()})."
            )
        )

    def _normalize_statuses(self, raw: str | None) -> tuple[str, ...]:
        if not raw:
            return TERMINAL_STATUSES
        wanted = [s.strip().upper() for s in raw.split(",") if s.strip()]
        for status in wanted:
            if status not in TaskStatus.values:
                raise ValueError(
                    f"Unknown status {status!r}; choose from {', '.join(TaskStatus.values)}."
                )
        return tuple(wanted)

    def _build_queryset(self, cutoff: Any, statuses: tuple[str, ...]) -> QuerySet[DBTaskResult]:
        return DBTaskResult.objects.filter(
            status__in=statuses,
            finished_at__isnull=False,
            finished_at__lt=cutoff,
        )

    def _delete_in_batches(self, queryset: QuerySet[DBTaskResult], batch_size: int) -> int:
        total = 0
        while True:
            ids = list(queryset.values_list("id", flat=True)[:batch_size])
            if not ids:
                return total
            deleted, _ = DBTaskResult.objects.filter(id__in=ids).delete()
            total += deleted
