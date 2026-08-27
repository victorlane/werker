"""Schedule declarations collected at import time by werker's @schedule decorator.

A ScheduleDeclaration captures *how* a task should fire (cron or interval),
but not *when* it fired last: that mutable state lives in the PeriodicTask
table, synced by `manage.py syncschedules`. Keeping declarations in a live
dict lets both syncschedules and `WerkerConfig.ready()` read the truth
without querying storage.
"""

from dataclasses import dataclass, field

from werker.models import ScheduleKind


@dataclass(frozen=True)
class ScheduleDeclaration:
    name: str
    task_path: str
    kind: ScheduleKind
    args: tuple[object, ...] = field(default_factory=tuple)
    kwargs: dict[str, object] = field(default_factory=dict)
    queue_name: str = "default"
    priority: int = 0
    cron_expression: str = ""
    interval_seconds: int | None = None
    timezone: str = "UTC"
    catchup: bool = False


_declarations: dict[str, ScheduleDeclaration] = {}


def register(declaration: ScheduleDeclaration) -> None:
    """Records a declaration, index by name. A later registration with the
    same name wins — decorator evaluation order is import order, which is
    also the "closest definition" order a user would expect."""
    _declarations[declaration.name] = declaration


def get_declarations() -> list[ScheduleDeclaration]:
    """The current declarations in registration order (name index order is
    not meaningful, but deters accidental mutation by callers)."""
    return list(_declarations.values())


def get_declaration(name: str) -> ScheduleDeclaration | None:
    return _declarations.get(name)


def clear() -> None:
    """Test helper: reset the registry between tests that define schedules."""
    _declarations.clear()
