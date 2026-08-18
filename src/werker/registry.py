"""Module-level registries populated at import time by werker's own
decorators (werker.decorators). Kept as plain side-tables rather than
mutating django.tasks.Task instances (which are frozen dataclasses) — see
werker.decorators.at_most_once and, in a later phase, @schedule.
"""

from werker.models import DeliveryGuarantee

_AT_MOST_ONCE_TASK_PATHS: set[str] = set()


def mark_at_most_once(task_path: str) -> None:
    _AT_MOST_ONCE_TASK_PATHS.add(task_path)


def delivery_guarantee_for(task_path: str) -> DeliveryGuarantee:
    if task_path in _AT_MOST_ONCE_TASK_PATHS:
        return DeliveryGuarantee.AT_MOST_ONCE
    return DeliveryGuarantee.AT_LEAST_ONCE
