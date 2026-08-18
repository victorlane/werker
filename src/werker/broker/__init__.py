"""The Broker ABC: queue/claim/lock semantics, independent of result storage.

Mirrors the split Celery draws between a broker and a result backend (see
werker.results). V1 ships one concrete Broker (PostgresBroker), backed by
the same table DBResultStore uses.

Sync methods are canonical; async methods run on this Broker's own
dedicated executor, not asgiref's shared thread_sensitive=True executor.
That executor is process-wide and single-threaded, so using it here would
serialize every claim/heartbeat/ack in the whole process onto one thread.
Broker/ResultStore hold no non-thread-safe resource, so there's nothing
for thread_sensitive=True to protect, only a bottleneck to add. See
tests/integration/test_broker_claim_concurrency.py for proof this is safe.
"""

import abc
import functools
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar

from asgiref.sync import sync_to_async
from typing_extensions import ParamSpec, TypeVar

from werker.concurrency import (
    DEFAULT_BOOKKEEPING_CONCURRENCY,
    create_bounded_executor,
    get_option,
    run_with_connection_cleanup,
)

_P = ParamSpec("_P")
_R = TypeVar("_R")


@dataclass(frozen=True)
class QueueItem:
    id: str
    queue_name: str
    priority: int
    run_after: datetime


@dataclass(frozen=True)
class ClaimedItem:
    id: str
    queue_name: str
    attempt: int


@dataclass(frozen=True)
class ReclaimedItem:
    id: str
    action: str  # "retried" or "failed", see Broker.reclaim_stale


class Broker(abc.ABC):
    #: False for a future backend (e.g. SQLite) without true SKIP LOCKED. Not used in v1.
    supports_skip_locked: ClassVar[bool] = True

    def __init__(self, options: Any):
        self.options = options
        concurrency = get_option(
            options, "BOOKKEEPING_CONCURRENCY", DEFAULT_BOOKKEEPING_CONCURRENCY
        )
        self._executor = create_bounded_executor(concurrency, thread_name_prefix="werker-broker")

    async def _run_sync(
        self, func: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs
    ) -> _R:
        """Runs func on this Broker's own dedicated executor, with connection
        cleanup. See the module docstring for why not thread_sensitive=True."""
        bound = functools.partial(run_with_connection_cleanup, func, *args, **kwargs)
        return await sync_to_async(bound, thread_sensitive=False, executor=self._executor)()

    @abc.abstractmethod
    def enqueue(self, item: QueueItem) -> None: ...

    async def aenqueue(self, item: QueueItem) -> None:
        await self._run_sync(self.enqueue, item)

    @abc.abstractmethod
    def claim(
        self, *, queue_names: Sequence[str], limit: int, worker_id: str
    ) -> list[ClaimedItem]: ...

    async def aclaim(
        self, *, queue_names: Sequence[str], limit: int, worker_id: str
    ) -> list[ClaimedItem]:
        return await self._run_sync(
            self.claim, queue_names=queue_names, limit=limit, worker_id=worker_id
        )

    @abc.abstractmethod
    def ack(self, item_id: str) -> None:
        """Release claim ownership of a finished item. Doesn't touch terminal
        status, that's ResultStore's job."""

    async def aack(self, item_id: str) -> None:
        await self._run_sync(self.ack, item_id)

    @abc.abstractmethod
    def nack(self, item_id: str, *, retry_after: datetime) -> None:
        """Release an item back to the queue, eligible for re-claim at retry_after."""

    async def anack(self, item_id: str, *, retry_after: datetime) -> None:
        await self._run_sync(self.nack, item_id, retry_after=retry_after)

    @abc.abstractmethod
    def heartbeat(self, item_id: str, *, worker_id: str) -> None:
        """Extends the claim's staleness window. See werker.worker.reaper."""

    async def aheartbeat(self, item_id: str, *, worker_id: str) -> None:
        await self._run_sync(self.heartbeat, item_id, worker_id=worker_id)

    @abc.abstractmethod
    def reclaim_stale(
        self, *, stale_before: datetime, limit: int, retry_backoff_seconds: float
    ) -> list[ReclaimedItem]:
        """Atomically reclaims RUNNING items with a stale heartbeat.
        AT_LEAST_ONCE items with retries left go back to READY (retried);
        everything else goes to FAILED (failed). Must lock-then-decide in
        one transaction per item so a late heartbeat can't race the decision."""

    async def areclaim_stale(
        self, *, stale_before: datetime, limit: int, retry_backoff_seconds: float
    ) -> list[ReclaimedItem]:
        return await self._run_sync(
            self.reclaim_stale,
            stale_before=stale_before,
            limit=limit,
            retry_backoff_seconds=retry_backoff_seconds,
        )
