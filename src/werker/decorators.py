from __future__ import annotations

from typing import Any

from django.tasks.base import Task

from werker import registry


def at_most_once(task: Task[..., Any]) -> Task[..., Any]:
    """Opt a @task into AT_MOST_ONCE delivery: the reaper never retries a
    stale claim for this task. See werker.models.DeliveryGuarantee.

    Usage:
        @at_most_once
        @task
        def charge_customer(...): ...
    """
    registry.mark_at_most_once(task.module_path)
    return task
