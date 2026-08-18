"""The ResultStore ABC: TaskResult state, independent of queueing mechanics.

`get`/`aget` return the storage-layer werker.models.DBTaskResult row, not
django.tasks.TaskResult directly — converting into the public django.tasks
dataclass is werker.backend.PostgresTaskBackend's job, keeping this layer's
contract storage-shaped rather than coupled to django.tasks' public API.

Sync methods are canonical; async methods are sync_to_async(thread_sensitive=True)
derivations — see werker.broker's module docstring for why.
"""

import abc
from datetime import datetime
from typing import Any

from asgiref.sync import sync_to_async

from werker.models import DBTaskResult, DeliveryGuarantee


class ResultStore(abc.ABC):
    def __init__(self, options):
        self.options = options

    @abc.abstractmethod
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
    ) -> None: ...

    async def acreate(self, **kwargs) -> None:
        await sync_to_async(self.create, thread_sensitive=True)(**kwargs)

    @abc.abstractmethod
    def mark_running(self, id: str, *, worker_id: str, attempt: int) -> None: ...

    async def amark_running(self, id: str, *, worker_id: str, attempt: int) -> None:
        await sync_to_async(self.mark_running, thread_sensitive=True)(
            id, worker_id=worker_id, attempt=attempt
        )

    @abc.abstractmethod
    def mark_successful(self, id: str, *, return_value: Any) -> None: ...

    async def amark_successful(self, id: str, *, return_value: Any) -> None:
        await sync_to_async(self.mark_successful, thread_sensitive=True)(
            id, return_value=return_value
        )

    @abc.abstractmethod
    def mark_failed(self, id: str, *, error: dict, will_retry: bool) -> None: ...

    async def amark_failed(self, id: str, *, error: dict, will_retry: bool) -> None:
        await sync_to_async(self.mark_failed, thread_sensitive=True)(
            id, error=error, will_retry=will_retry
        )

    @abc.abstractmethod
    def get(self, id: str) -> DBTaskResult: ...

    async def aget(self, id: str) -> DBTaskResult:
        return await sync_to_async(self.get, thread_sensitive=True)(id)
