"""Operations on queued and finished tasks: cancel and requeue.

Django's task API has no "cancelled" status, so a cancelled task becomes
FAILED with a werker.exceptions.TaskCancelled error. That keeps every
result readable through django.tasks.

    from werker.api import cancel, requeue

    cancel(result.id)
    requeue(result.id)
"""

import traceback

from asgiref.sync import sync_to_async
from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import F
from django.tasks.exceptions import TaskResultDoesNotExist
from django.utils import timezone

from werker.exceptions import TaskCancelled
from werker.models import DBTaskResult, TaskStatus


def cancelled_error() -> dict[str, str]:
    exc = TaskCancelled("cancelled")
    return {
        "exception_class_path": f"{TaskCancelled.__module__}.{TaskCancelled.__qualname__}",
        "traceback": "".join(traceback.format_exception(exc)),
    }


def cancel(result_id: str) -> bool:
    """Cancels a task. Returns False if it had already finished.

    - READY: becomes FAILED right away and never runs.
    - RUNNING: the worker notices on its next heartbeat. An async task is
      cancelled; a sync task can't be interrupted, so it finishes, but is
      never retried.
    """
    with transaction.atomic():
        row = _locked_row(result_id)
        if row.status == TaskStatus.READY:
            row.status = TaskStatus.FAILED
            row.finished_at = timezone.now()
            row.errors = [*row.errors, cancelled_error()]
            row.save(update_fields=["status", "finished_at", "errors"])
            return True
        if row.status == TaskStatus.RUNNING:
            if row.cancel_requested_at is None:
                row.cancel_requested_at = timezone.now()
                row.save(update_fields=["cancel_requested_at"])
            return True
        return False


def requeue(result_id: str, *, reset_attempts: bool = True) -> bool:
    """Puts a FAILED task back in the queue to run now. Returns False for
    any other status. Its error history is kept; `reset_attempts` gives it
    a fresh retry budget."""
    updates = {
        "status": TaskStatus.READY,
        "run_after": timezone.now(),
        "finished_at": None,
        "claimed_by": "",
        "claimed_at": None,
        "last_heartbeat_at": None,
        "cancel_requested_at": None,
        "attempts": 0 if reset_attempts else F("attempts"),
    }
    with transaction.atomic():
        _locked_row(result_id)
        updated = DBTaskResult.objects.filter(id=result_id, status=TaskStatus.FAILED).update(
            **updates
        )
    return bool(updated)


acancel = sync_to_async(cancel)
arequeue = sync_to_async(requeue)


def _locked_row(result_id: str) -> DBTaskResult:
    try:
        row = DBTaskResult.objects.select_for_update().filter(id=result_id).first()
    except (ValidationError, ValueError):
        # Not a valid UUID.
        raise TaskResultDoesNotExist(result_id) from None
    if row is None:
        raise TaskResultDoesNotExist(result_id)
    return row
