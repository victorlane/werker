"""Health check for werker workers, for container probes and monitoring.

    python manage.py werker_health --file /tmp/werker-alive --max-age 60
    python manage.py werker_health --db

Exits 0 when healthy and 1 otherwise. `--file` checks the health file a
worker touches on every loop pass (taskworker --health-file) and needs no
database query, so it suits a frequent liveness probe. `--db` checks that
at least one worker recorded a recent heartbeat.
"""

import time
from datetime import timedelta
from pathlib import Path
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.utils import timezone

from werker.models import WorkerRecord
from werker.worker.presence import alive_cutoff

DEFAULT_FILE_MAX_AGE = 60


class Command(BaseCommand):
    help = "Exit 0 if werker workers are healthy, 1 otherwise."

    def add_arguments(self, parser: CommandParser) -> None:
        target = parser.add_mutually_exclusive_group(required=True)
        target.add_argument("--file", help="Health file written by taskworker --health-file.")
        target.add_argument(
            "--db", action="store_true", help="Require a live worker in the database."
        )
        parser.add_argument(
            "--max-age",
            type=float,
            default=None,
            help=(
                "Seconds. --file: oldest acceptable file age (default 60). "
                "--db: oldest acceptable heartbeat (default 3 x HEARTBEAT_INTERVAL)."
            ),
        )
        parser.add_argument(
            "--hostname", default=None, help="--db only: only count workers on this host."
        )
        parser.add_argument("--backend", default="default", help="TASKS alias (default).")

    def handle(self, *args: Any, **options: Any) -> None:
        if options["file"]:
            self._check_file(Path(options["file"]), options["max_age"] or DEFAULT_FILE_MAX_AGE)
        else:
            self._check_db(options["max_age"], options["hostname"], options["backend"])

    def _check_file(self, path: Path, max_age: float) -> None:
        try:
            age = time.time() - path.stat().st_mtime
        except FileNotFoundError:
            raise CommandError(f"unhealthy: {path} does not exist") from None
        if age > max_age:
            raise CommandError(f"unhealthy: {path} is {age:.0f}s old (max {max_age:.0f}s)")
        self.stdout.write(f"healthy: {path} is {age:.0f}s old")

    def _check_db(self, max_age: float | None, hostname: str | None, alias: str) -> None:
        cutoff = (
            timezone.now() - timedelta(seconds=max_age)
            if max_age is not None
            else alive_cutoff(alias)
        )
        workers = WorkerRecord.objects.filter(stopped_at__isnull=True, last_seen_at__gte=cutoff)
        if hostname:
            workers = workers.filter(hostname=hostname)
        count = workers.count()
        if not count:
            raise CommandError("unhealthy: no live werker worker")
        self.stdout.write(f"healthy: {count} live worker(s)")
