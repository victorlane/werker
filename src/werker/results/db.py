from datetime import datetime
from typing import Any

from asgiref.sync import sync_to_async
from django.utils import timezone

from werker.models import DBTaskResult, TaskStatus
from werker.results import ResultStore


class DBResultStore(ResultStore):
    def _create_sync(
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
        )

    async def acreate(self, **kwargs) -> None:
        await sync_to_async(self._create_sync, thread_sensitive=True)(**kwargs)

    def _mark_running_sync(self, id: str, *, worker_id: str, attempt: int) -> None:
        result = DBTaskResult.objects.get(id=id)
        result.worker_ids.append(worker_id)
        DBTaskResult.objects.filter(id=id).update(worker_ids=result.worker_ids)

    async def amark_running(self, id: str, *, worker_id: str, attempt: int) -> None:
        await sync_to_async(self._mark_running_sync, thread_sensitive=True)(
            id, worker_id=worker_id, attempt=attempt
        )

    def _mark_successful_sync(self, id: str, *, return_value: Any) -> None:
        DBTaskResult.objects.filter(id=id).update(
            status=TaskStatus.SUCCESSFUL,
            return_value=return_value,
            finished_at=timezone.now(),
        )

    async def amark_successful(self, id: str, *, return_value: Any) -> None:
        await sync_to_async(self._mark_successful_sync, thread_sensitive=True)(
            id, return_value=return_value
        )

    def _mark_failed_sync(self, id: str, *, error: dict, will_retry: bool) -> None:
        result = DBTaskResult.objects.get(id=id)
        result.errors.append(error)
        update_fields = {"errors": result.errors}
        if not will_retry:
            update_fields["status"] = TaskStatus.FAILED
            update_fields["finished_at"] = timezone.now()
        DBTaskResult.objects.filter(id=id).update(**update_fields)

    async def amark_failed(self, id: str, *, error: dict, will_retry: bool) -> None:
        await sync_to_async(self._mark_failed_sync, thread_sensitive=True)(
            id, error=error, will_retry=will_retry
        )

    def _get_sync(self, id: str) -> DBTaskResult:
        return DBTaskResult.objects.get(id=id)

    async def aget(self, id: str) -> DBTaskResult:
        return await sync_to_async(self._get_sync, thread_sensitive=True)(id)
