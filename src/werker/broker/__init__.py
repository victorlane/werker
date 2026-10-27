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
from collections.abc import Callable, Mapping, Sequence
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
from werker.config import validate_options
from werker.registry import TaskPolicy

_P = ParamSpec("_P")
_R = TypeVar("_R")


@dataclass(frozen=True)
class QueueItem:
    id: str
    queue_name: str
    priority: int
    run_after: datetime


#: (task_path, attempt) -> seconds to wait before the next attempt.
RetryDelay = Callable[[str, int], float]


def retries_remaining(attempt: int, max_retries: int) -> bool:
    """True if a task that just finished attempt number `attempt` (1-based)
    may run again. MAX_RETRIES counts retries, so MAX_RETRIES=n allows n+1
    attempts in total and 0 disables retries."""
    return attempt <= max_retries


@dataclass(frozen=True)
class ClaimedItem:
    id: str
    queue_name: str
    task_path: str
    attempt: int
    #: Retry budget stored on the row at enqueue time, so a settings change
    #: doesn't alter tasks already in the queue.
    max_retries: int


class ClaimBatch(list[ClaimedItem]):
    """The claimed items. `wait_hint` is set when due work was held back by
    a rate limit: seconds until it can be claimed, so the worker can come
    back then instead of after a full poll interval."""

    wait_hint: float | None = None


@dataclass(frozen=True)
class ReclaimedItem:
    id: str
    action: str  # "retried" or "failed", see Broker.reclaim_stale


class Broker(abc.ABC):
    #: False for a future backend (e.g. SQLite) without true SKIP LOCKED. Not used in v1.
    supports_skip_locked: ClassVar[bool] = True

    def __init__(self, options: Any):
        validate_options(options)
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
        self,
        *,
        queue_names: Sequence[str],
        limit: int,
        worker_id: str,
        limits: Mapping[str, TaskPolicy] | None = None,
    ) -> "ClaimBatch":
        """Claims up to `limit` due items. `limits` maps task paths to
        their @rate_limit policy; those tasks are claimed only within it."""

    async def aclaim(
        self,
        *,
        queue_names: Sequence[str],
        limit: int,
        worker_id: str,
        limits: Mapping[str, TaskPolicy] | None = None,
    ) -> "ClaimBatch":
        return await self._run_sync(
            self.claim,
            queue_names=queue_names,
            limit=limit,
            worker_id=worker_id,
            limits=limits,
        )

    @abc.abstractmethod
    def ack(self, item_id: str, *, worker_id: str, attempt: int) -> None:
        """Release claim ownership of a finished item. Doesn't touch terminal
        status, that's ResultStore's job."""

    async def aack(self, item_id: str, *, worker_id: str, attempt: int) -> None:
        await self._run_sync(self.ack, item_id, worker_id=worker_id, attempt=attempt)

    @abc.abstractmethod
    def release(self, item: ClaimedItem, *, worker_id: str) -> bool:
        """Undo a claim the worker can't start yet: back to READY as if never
        claimed, without counting the attempt or spending a rate-limit token.
        False if this worker no longer holds the claim."""

    async def arelease(self, item: ClaimedItem, *, worker_id: str) -> bool:
        return await self._run_sync(self.release, item, worker_id=worker_id)

    @abc.abstractmethod
    def nack(self, item_id: str, *, worker_id: str, attempt: int, retry_after: datetime) -> None:
        """Release an item back to the queue, eligible for re-claim at retry_after."""

    async def anack(
        self, item_id: str, *, worker_id: str, attempt: int, retry_after: datetime
    ) -> None:
        await self._run_sync(
            self.nack, item_id, worker_id=worker_id, attempt=attempt, retry_after=retry_after
        )

    @abc.abstractmethod
    def heartbeat(self, item_id: str, *, worker_id: str, attempt: int) -> bool:
        """Extends the claim's staleness window. See werker.worker.reaper.
        Returns True if cancelling the task was requested (werker.api.cancel)."""

    async def aheartbeat(self, item_id: str, *, worker_id: str, attempt: int) -> bool:
        return await self._run_sync(
            self.heartbeat, item_id, worker_id=worker_id, attempt=attempt
        )

    @abc.abstractmethod
    def reclaim_stale(
        self, *, stale_before: datetime, limit: int, retry_delay: RetryDelay
    ) -> list[ReclaimedItem]:
        """Atomically reclaims RUNNING items with a stale heartbeat.
        AT_LEAST_ONCE items with retries left go back to READY (retried),
        delayed by retry_delay(task_path, attempt) seconds; everything else
        goes to FAILED (failed). Must lock-then-decide in one transaction per
        item so a late heartbeat can't race the decision."""

    async def areclaim_stale(
        self, *, stale_before: datetime, limit: int, retry_delay: RetryDelay
    ) -> list[ReclaimedItem]:
        return await self._run_sync(
            self.reclaim_stale,
            stale_before=stale_before,
            limit=limit,
            retry_delay=retry_delay,
        )

    @abc.abstractmethod
    def claim_due_schedules(self, *, limit: int, worker_id: str) -> list[str]:
        """Atomically claims due, enabled PeriodicTask rows (SKIP LOCKED)
        and returns their ids. A claim persists as a lease between this call
        and dispatch; a live lease excludes other schedulers, while a stale
        one can be recovered by a later claim."""

    async def aclaim_due_schedules(self, *, limit: int, worker_id: str) -> list[str]:
        return await self._run_sync(
            self.claim_due_schedules, limit=limit, worker_id=worker_id
        )
