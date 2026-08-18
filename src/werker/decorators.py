from __future__ import annotations

from typing import Any

from django.tasks.base import Task

from werker import registry


def at_most_once(task: Task[..., Any]) -> Task[..., Any]:
    """Opt a @task into AT_MOST_ONCE delivery.

    Default delivery (no decorator) is AT_LEAST_ONCE: if a worker dies
    mid-execution, werker's reaper reclaims the stale claim and retries it
    — task functions must be idempotent under this default.

    Under @at_most_once, the reaper does not retry a stale claim for this
    task — it's marked FAILED directly instead. This guarantees the task
    body never runs twice, at the cost of the task possibly never
    completing if the staleness was a false positive. See
    werker.models.DeliveryGuarantee for the full tradeoff.

    Usage:
        @at_most_once
        @task
        def charge_customer(...): ...
    """
    registry.mark_at_most_once(task.module_path)
    return task
