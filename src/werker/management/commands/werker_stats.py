"""Queue health in one cheap query, for dashboards and alerting.

    python manage.py werker_stats
    python manage.py werker_stats --json

Per queue: due READY tasks, deferred READY tasks (run_after in the future),
RUNNING tasks, RUNNING tasks whose heartbeat is older than
STALE_RUNNING_TIMEOUT, and how long the oldest due task has been waiting.
Terminal rows are not counted; see prune_task_results.
"""

import json
from datetime import timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandParser
from django.db.models import Count, Min, Q
from django.tasks import task_backends
from django.utils import timezone

from werker.models import DBTaskResult, TaskStatus
from werker.worker.core import DEFAULT_STALE_RUNNING_TIMEOUT

COLUMNS = (
    "queue_name",
    "ready",
    "deferred",
    "running",
    "stale_running",
    "oldest_ready_age_seconds",
)


class Command(BaseCommand):
    help = "Report per-queue task counts and the age of the oldest waiting task."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--backend",
            default="default",
            help="TASKS alias whose STALE_RUNNING_TIMEOUT to use (default: 'default').",
        )
        parser.add_argument("--json", action="store_true", help="Print JSON instead of a table.")

    def handle(self, *args: Any, **options: Any) -> None:
        stale_timeout = task_backends[options["backend"]].options.get(
            "STALE_RUNNING_TIMEOUT", DEFAULT_STALE_RUNNING_TIMEOUT
        )
        queues = queue_stats(timedelta(seconds=stale_timeout))

        if options["json"]:
            self.stdout.write(json.dumps({"queues": queues}))
            return
        self.stdout.write("  ".join(COLUMNS))
        for queue in queues:
            self.stdout.write("  ".join(str(queue[column]) for column in COLUMNS))


def queue_stats(stale_after: timedelta) -> list[dict[str, Any]]:
    """Per-queue counts over READY/RUNNING rows only, which the claim index
    covers, so this stays cheap however many finished rows the table holds."""
    now = timezone.now()
    due = Q(status=TaskStatus.READY, run_after__lte=now)
    running = Q(status=TaskStatus.RUNNING)
    rows = (
        DBTaskResult.objects.filter(status__in=(TaskStatus.READY, TaskStatus.RUNNING))
        .values("queue_name")
        .annotate(
            ready=Count("id", filter=due),
            deferred=Count("id", filter=Q(status=TaskStatus.READY, run_after__gt=now)),
            running=Count("id", filter=running),
            stale_running=Count("id", filter=running & Q(last_heartbeat_at__lt=now - stale_after)),
            oldest_ready=Min("run_after", filter=due),
        )
        .order_by("queue_name")
    )
    return [
        {
            **{key: value for key, value in row.items() if key != "oldest_ready"},
            "oldest_ready_age_seconds": (
                round((now - row["oldest_ready"]).total_seconds()) if row["oldest_ready"] else None
            ),
        }
        for row in rows
    ]
