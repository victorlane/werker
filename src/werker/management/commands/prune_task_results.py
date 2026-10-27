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

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.utils import timezone

from werker.maintenance import (
    DEFAULT_BATCH_SIZE,
    TERMINAL_STATUSES,
    parse_duration,
    prunable,
    prune_results,
)

DEFAULT_RETENTION = timedelta(days=30)


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
        try:
            retention = (
                parse_duration(options["older_than"])
                if options["older_than"]
                else DEFAULT_RETENTION
            )
            statuses = (
                options["status"].split(",") if options["status"] else list(TERMINAL_STATUSES)
            )
            cutoff = timezone.now() - retention
            queryset = prunable(cutoff, statuses)
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        if options["dry_run"]:
            count = queryset.count()
            self.stdout.write(
                self.style.WARNING(f"dry-run: {count} row(s) would be deleted.")
            )
            return

        total = prune_results(cutoff, statuses, batch_size=options["batch_size"])
        self.stdout.write(
            self.style.SUCCESS(
                f"pruned {total} row(s) older than {retention} (cutoff {cutoff.isoformat()})."
            )
        )
