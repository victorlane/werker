"""Creates/updates PeriodicTask rows from @schedule declarations.

Run after any deploy that touches @schedule decorators:

    python manage.py syncschedules

Idempotent and driven entirely by the in-process registry, so it requires
that every task module has been imported (import time is when @schedule
registers). The `--all` flag first imports `<app>.tasks` for every
installed app to discover any schedules that haven't been loaded yet.
"""

import contextlib
from typing import Any
from zoneinfo import ZoneInfoNotFoundError

from django.core.management.base import BaseCommand, CommandParser

from werker.models import PeriodicTask
from werker.schedules.decorators import _validate_timezone
from werker.schedules.discovery import discover_task_modules
from werker.schedules.registry import ScheduleDeclaration, get_declarations
from werker.schedules.scheduler import next_run_after_sync


class Command(BaseCommand):
    help = (
        "Sync @schedule declarations into PeriodicTask rows, "
        "creating/enabling and pruning/deactivating as needed."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--all",
            action="store_true",
            help=(
                "Import <app>.tasks for every INSTALLED_APPS entry first, "
                "to discover declarations that would otherwise need an import."
            ),
        )

    def handle(self, *args: Any, **options: Any) -> None:
        if options["all"]:
            discover_task_modules()

        declarations = get_declarations()

        existing = {pt.name: pt for pt in PeriodicTask.objects.all()}
        live_names = {d.name for d in declarations}

        if not declarations:
            # Empty registry is a valid state (all schedules removed): still
            # disable every previously-synced row rather than leaving them
            # enabled with nothing driving them.
            disabled = PeriodicTask.objects.filter(enabled=True).exclude(
                name__in=live_names
            ).update(enabled=False)
            self.stdout.write(
                self.style.SUCCESS(
                    f"synced 0 created, 0 updated, 0 re-enabled, {disabled} disabled "
                    f"(no declarations registered — import tasks modules or use --all)."
                )
            )
            return

        created = updated = reenabled = 0
        for declaration in declarations:
            _validate_declaration(declaration)

            pt = existing.get(declaration.name)
            if pt is None:
                _create_from(declaration)
                created += 1
                continue

            changed = _has_changed(pt, declaration)
            if changed:
                _apply_fields(pt, declaration)
                pt.next_run_at = next_run_after_sync(pt)
                updated += 1
            if not pt.enabled:
                pt.enabled = True
                reenabled += 1
            pt.save()

        # Disable rows whose declaration disappeared.
        disabled = PeriodicTask.objects.filter(enabled=True).exclude(
            name__in=live_names
        ).update(enabled=False)

        self.stdout.write(
            self.style.SUCCESS(
                f"synced {created} created, {updated} updated, "
                f"{reenabled} re-enabled, {disabled} disabled."
            )
        )


def _validate_declaration(declaration: ScheduleDeclaration) -> None:
    with contextlib.suppress(ZoneInfoNotFoundError):
        _validate_timezone(declaration.timezone)


def _create_from(declaration: ScheduleDeclaration) -> None:
    PeriodicTask.objects.create(
        name=declaration.name,
        task_path=declaration.task_path,
        args_json=list(declaration.args),
        kwargs_json=dict(declaration.kwargs),
        schedule_kind=declaration.kind,
        cron_expression=declaration.cron_expression,
        interval_seconds=declaration.interval_seconds,
        timezone=declaration.timezone,
        queue_name=declaration.queue_name,
        priority=declaration.priority,
        enabled=True,
        catchup=declaration.catchup,
        next_run_at=_initial_next_run(declaration),
        last_run_at=None,
    )


def _initial_next_run(declaration: ScheduleDeclaration) -> Any:
    # Build an unsaved template so next_run_after_sync can read the same
    # fields PeriodicTask rows carry (cron_expression/interval_seconds/etc.).
    template = PeriodicTask(
        schedule_kind=declaration.kind,
        cron_expression=declaration.cron_expression,
        interval_seconds=declaration.interval_seconds,
        timezone=declaration.timezone,
    )
    return next_run_after_sync(template)


def _has_changed(pt: PeriodicTask, declaration: ScheduleDeclaration) -> bool:
    return (
        pt.task_path != declaration.task_path
        or pt.args_json != list(declaration.args)
        or pt.kwargs_json != dict(declaration.kwargs)
        or pt.schedule_kind != declaration.kind
        or pt.cron_expression != declaration.cron_expression
        or pt.interval_seconds != declaration.interval_seconds
        or pt.timezone != declaration.timezone
        or pt.queue_name != declaration.queue_name
        or pt.priority != declaration.priority
        or pt.catchup != declaration.catchup
    )


def _apply_fields(pt: PeriodicTask, declaration: ScheduleDeclaration) -> None:
    pt.task_path = declaration.task_path
    pt.args_json = list(declaration.args)
    pt.kwargs_json = dict(declaration.kwargs)
    pt.schedule_kind = declaration.kind
    pt.cron_expression = declaration.cron_expression
    pt.interval_seconds = declaration.interval_seconds
    pt.timezone = declaration.timezone
    pt.queue_name = declaration.queue_name
    pt.priority = declaration.priority
    pt.catchup = declaration.catchup
