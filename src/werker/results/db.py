from datetime import datetime
from typing import Any

from django.utils import timezone

from werker.models import DBTaskResult, DeliveryGuarantee, TaskStatus
from werker.results import ResultStore


class DBResultStore(ResultStore):
    def create(
        self,
        *,
        id: str,
        task_path: str,
        args: tuple,
        kwargs: dict,
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

    def mark_running(self, id: str, *, worker_id: str, attempt: int) -> None:
        result = DBTaskResult.objects.get(id=id)
        result.worker_ids.append(worker_id)
        DBTaskResult.objects.filter(id=id).update(worker_ids=result.worker_ids)

    def mark_successful(self, id: str, *, return_value: Any) -> None:
        DBTaskResult.objects.filter(id=id).update(
            status=TaskStatus.SUCCESSFUL,
            return_value=return_value,
            finished_at=timezone.now(),
        )

    def mark_failed(self, id: str, *, error: dict, will_retry: bool) -> None:
        result = DBTaskResult.objects.get(id=id)
        result.errors.append(error)
        update_fields = {"errors": result.errors}
        if not will_retry:
            update_fields["status"] = TaskStatus.FAILED
            update_fields["finished_at"] = timezone.now()
        DBTaskResult.objects.filter(id=id).update(**update_fields)

    def get(self, id: str) -> DBTaskResult:
        return DBTaskResult.objects.get(id=id)
