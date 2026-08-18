"""The ResultStore ABC: TaskResult state, independent of queueing mechanics.

`get`/`aget` return the storage-layer werker.models.DBTaskResult row, not
django.tasks.TaskResult directly — converting into the public django.tasks
dataclass is werker.backend.PostgresTaskBackend's job, keeping this layer's
contract storage-shaped rather than coupled to django.tasks' public API.

Sync methods are canonical; async methods run on this ResultStore's own
dedicated executor (thread_sensitive=False), not asgiref's shared
single-thread thread_sensitive=True executor — see werker.broker's module
docstring for the full reasoning (identical here: DBResultStore holds no
non-thread-safe shared resource, so there's nothing for thread_sensitive=True
to protect, only a bottleneck for it to add).
"""

import abc
import functools
from collections.abc import Callable
from datetime import datetime
from typing import Any

from asgiref.sync import sync_to_async
from typing_extensions import ParamSpec, TypeVar

from werker.concurrency import (
    DEFAULT_BOOKKEEPING_CONCURRENCY,
    create_bounded_executor,
    get_option,
    run_with_connection_cleanup,
)
from werker.models import DBTaskResult, DeliveryGuarantee

_P = ParamSpec("_P")
_R = TypeVar("_R")


class ResultStore(abc.ABC):
    def __init__(self, options: Any):
        self.options = options
        concurrency = get_option(
            options, "BOOKKEEPING_CONCURRENCY", DEFAULT_BOOKKEEPING_CONCURRENCY
        )
        self._executor = create_bounded_executor(concurrency, thread_name_prefix="werker-results")

    async def _run_sync(
        self, func: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs
    ) -> _R:
        bound = functools.partial(run_with_connection_cleanup, func, *args, **kwargs)
        return await sync_to_async(bound, thread_sensitive=False, executor=self._executor)()

    @abc.abstractmethod
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
    ) -> None: ...

    async def acreate(self, **kwargs: Any) -> None:
        await self._run_sync(self.create, **kwargs)

    @abc.abstractmethod
    def mark_running(self, id: str, *, worker_id: str, attempt: int) -> None: ...

    async def amark_running(self, id: str, *, worker_id: str, attempt: int) -> None:
        await self._run_sync(self.mark_running, id, worker_id=worker_id, attempt=attempt)

    @abc.abstractmethod
    def mark_successful(self, id: str, *, return_value: Any) -> None: ...

    async def amark_successful(self, id: str, *, return_value: Any) -> None:
        await self._run_sync(self.mark_successful, id, return_value=return_value)

    @abc.abstractmethod
    def mark_failed(self, id: str, *, error: dict[str, str], will_retry: bool) -> None: ...

    async def amark_failed(self, id: str, *, error: dict[str, str], will_retry: bool) -> None:
        await self._run_sync(self.mark_failed, id, error=error, will_retry=will_retry)

    @abc.abstractmethod
    def get(self, id: str) -> DBTaskResult: ...

    async def aget(self, id: str) -> DBTaskResult:
        return await self._run_sync(self.get, id)
