"""Due-schedule dispatch loop. Fire a due PeriodicTask row into the normal
task queue — the same path a manual enqueue takes — so a scheduled run is
just a @task run with job-level retry, claim, and result tracking.

Due rows are locked with SELECT ... FOR UPDATE SKIP LOCKED (consistent
with werker.broker's claim semantics) so many workers can share the
dispatch duty without double-firing, and a dispatch that crashes leaves
the row claimed so it can be recovered rather than fired twice.
"""

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any

from django.db import transaction
from django.utils import timezone

from werker.models import PeriodicTask, ScheduleKind
from werker.schedules.registry import ScheduleDeclaration
from werker.schedules.scheduler import next_run_after_fire

if TYPE_CHECKING:
    from werker.worker.core import Worker

logger = logging.getLogger("werker.worker")


async def scheduler_loop(worker: "Worker") -> None:
    """Runs alongside the claim/reaper loops. Polls for due PeriodicTask
    rows every scheduler_poll_interval seconds and fires them."""
    while not worker._shutdown.is_set():
        try:
            due = await worker.backend.broker.aclaim_due_schedules(
                limit=worker.claim_batch_size, worker_id=worker.worker_id
            )
        except Exception:
            # A transient DB error must not kill the worker; recover next poll.
            logger.exception("werker scheduler claim_due_schedules failed, retrying")
            due = []

        for schedule_id in due:
            try:
                await _dispatch(worker, schedule_id)
            except Exception:
                logger.exception(
                    "werker scheduler failed to dispatch schedule id=%s", schedule_id
                )

        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                worker._shutdown.wait(), timeout=worker.scheduler_poll_interval
            )


async def _dispatch(worker: "Worker", schedule_id: str) -> None:
    """Locks one due row, decides catch-up vs skip, fires it, and advances
    next_run_at."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(worker._executor, _dispatch_sync, worker, schedule_id)


def _dispatch_sync(worker: "Worker", schedule_id: str) -> None:
    with transaction.atomic():
        try:
            task = PeriodicTask.objects.select_for_update().get(id=schedule_id, enabled=True)
        except PeriodicTask.DoesNotExist:
            return  # deleted or disabled between claim and dispatch.

        now = timezone.now()

        # catchup=False -> skip any fully-missed occurrences: advance past
        # them and keep only a future next_run_at. A task that is merely
        # slightly overdue (polling latency, less than one full period) still
        # falls through and fires now.
        if not task.catchup and _periods_behind(task, now) >= 1:
            candidate = next_run_after_fire(task, task.next_run_at)
            while candidate <= now:
                task.next_run_at = candidate
                candidate = next_run_after_fire(task, task.next_run_at)
                if candidate <= task.next_run_at:
                    break  # degenerate schedule; avoid spinning forever
            task.next_run_at = candidate
            task.save(update_fields=["next_run_at"])
            return

        # Fire it: enqueue a normal DBTaskResult, then advance the
        # schedule's next_run_at and release the claim.
        declaration = _resolve_declaration(task)
        result_id = worker.backend.enqueue_schedule(declaration)

        next_run = next_run_after_fire(task, max(task.next_run_at, now))
        PeriodicTask.objects.filter(id=task.id).update(
            next_run_at=next_run,
            last_run_at=now,
            claimed_by="",
            last_heartbeat_at=None,
        )
        logger.info(
            "werker scheduler fired schedule=%s task=%s enqueued_id=%s next=%s",
            task.name,
            declaration.task_path,
            result_id,
            next_run,
        )


def _periods_behind(task: PeriodicTask, now: Any) -> int:
    """Whether the task has missed at least one complete fire. Returns 1
    if `next_run_at + one period <= now` (so we'd skip a missed run),
    else 0. Cron uses croniter to find the fire after next_run_at."""
    next_fire = next_run_after_fire(task, task.next_run_at)
    if next_fire <= task.next_run_at:
        # Degenerate schedule (e.g. interval=0): never treat as "behind".
        return 0
    return 1 if next_fire <= now else 0


def _resolve_declaration(task: PeriodicTask) -> ScheduleDeclaration:
    """Rebuild a ScheduleDeclaration from a PeriodicTask row, honoring any
    DB edits (queue/priority/etc.) even if the in-process registry has
    since been cleared or diverged."""
    return ScheduleDeclaration(
        name=task.name,
        task_path=task.task_path,
        kind=ScheduleKind(task.schedule_kind),
        args=tuple(task.args_json),
        kwargs=dict(task.kwargs_json),
        queue_name=task.queue_name,
        priority=task.priority,
        cron_expression=task.cron_expression,
        interval_seconds=task.interval_seconds,
        timezone=task.timezone,
        catchup=task.catchup,
    )
