"""The ResultStore ABC: TaskResult state, independent of queueing mechanics.

`aget` returns the storage-layer werker.models.DBTaskResult row, not
django.tasks.TaskResult directly — converting into the public django.tasks
dataclass is werker.backend.PostgresTaskBackend's job (phase 3), keeping
this layer's contract storage-shaped rather than coupled to django.tasks'
public API surface.
"""

import abc
from datetime import datetime
from typing import Any

from asgiref.sync import async_to_sync

from werker.models import DBTaskResult


class ResultStore(abc.ABC):
    def __init__(self, options):
        self.options = options

    @abc.abstractmethod
    async def acreate(
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
    ) -> None: ...

    @abc.abstractmethod
    async def amark_running(self, id: str, *, worker_id: str, attempt: int) -> None: ...

    @abc.abstractmethod
    async def amark_successful(self, id: str, *, return_value: Any) -> None: ...

    @abc.abstractmethod
    async def amark_failed(
        self, id: str, *, error: dict, will_retry: bool
    ) -> None: ...

    @abc.abstractmethod
    async def aget(self, id: str) -> DBTaskResult: ...

    def get(self, id: str) -> DBTaskResult:
        return async_to_sync(self.aget)(id)
