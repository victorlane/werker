from datetime import datetime
from typing import Any

from django.db import transaction
from django.utils import timezone

from werker.models import DBTaskResult, DeliveryGuarantee, TaskStatus
from werker.results import ResultStore


class DBResultStore(ResultStore):
    def create(
        self,
        *,
        id: str,
        task_path: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        queue_name: str,
        priority: int,
        run_after: datetime,
        enqueued_at: datetime,
        max_retries: int,
        delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE,
    ) -> None:
        DBTaskResult.objects.create(
            id=id,
            task_path=task_path,
            args_json=list(args),
            kwargs_json=dict(kwargs),
            queue_name=queue_name,
            priority=priority,
            status=TaskStatus.READY,
            run_after=run_after,
            enqueued_at=enqueued_at,
            max_retries=max_retries,
            delivery_guarantee=delivery_guarantee,
        )

    def mark_running(self, id: str, *, worker_id: str, attempt: int) -> bool:
        """Record a worker only while it still owns this exact claim attempt."""
        with transaction.atomic():
            result = DBTaskResult.objects.select_for_update().filter(id=id).first()
            if (
                result is None
                or result.status != TaskStatus.RUNNING
                or result.claimed_by != worker_id
                or result.attempts != attempt
            ):
                return False
            # The row lock makes this read-modify-write safe across workers.
            result.worker_ids = [*result.worker_ids, worker_id]
            result.save(update_fields=["worker_ids"])
        return True

    def mark_successful(
        self, id: str, *, worker_id: str, attempt: int, return_value: Any
    ) -> bool:
        updated = DBTaskResult.objects.filter(
            id=id, status=TaskStatus.RUNNING, claimed_by=worker_id, attempts=attempt
        ).update(
            status=TaskStatus.SUCCESSFUL,
            return_value=return_value,
            finished_at=timezone.now(),
        )
        return bool(updated)

    def mark_failed(
        self, id: str, *, worker_id: str, attempt: int, error: dict[str, str], will_retry: bool
    ) -> bool:
        # Locking prevents concurrent failures (including the stale-claim reaper)
        # from overwriting each other's JSON error history.
        with transaction.atomic():
            result = DBTaskResult.objects.select_for_update().filter(id=id).first()
            if (
                result is None
                or result.status != TaskStatus.RUNNING
                or result.claimed_by != worker_id
                or result.attempts != attempt
            ):
                return False
            result.errors = [*result.errors, error]
            update_fields = ["errors"]
            if not will_retry:
                result.status = TaskStatus.FAILED
                result.finished_at = timezone.now()
                update_fields.extend(["status", "finished_at"])
            result.save(update_fields=update_fields)
        return True

    def get(self, id: str) -> DBTaskResult:
        return DBTaskResult.objects.get(id=id)
