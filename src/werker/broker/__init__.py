"""The Broker ABC: queue/claim/lock semantics, independent of result storage.

Mirrors the split Celery draws between a broker and a result backend (see
werker.results for the other half), plus the same split extended to
werker.schedules for periodic tasks. V1 ships one concrete Broker
(werker.broker.postgres.PostgresBroker) backed by the same Postgres table
werker.results.db.DBResultStore uses; see PostgresBroker's docstring for
why that's a deliberate v1 shortcut, not a structural requirement.

Sync methods are canonical (abstract); async methods are derived via
asgiref.sync.sync_to_async — mirroring Django's own BaseTaskBackend
(django.tasks.backends.base) and its documented guidance that ORM
transactions aren't async-safe as of Django 6.0. A worker's own event loop
awaits the async methods directly; a sync call site (e.g.
PostgresTaskBackend.enqueue) calls the sync methods with no asyncio
involved at all.

Deliberately thread_sensitive=False, with our own small dedicated
executor, rather than Django/asgiref's usual thread_sensitive=True:
asgiref routes thread_sensitive=True sync_to_async calls through a single
process-wide 1-worker executor (asgiref.sync.SyncToAsync.single_thread_executor)
when there's no enclosing sync context — which is our case, since a
werker worker is entered via plain asyncio.run(). That serializes *every*
thread-sensitive call in the whole process onto one OS thread. That
restriction exists to protect non-thread-safe resources (e.g. SQLite
connections); it buys us nothing since Broker/ResultStore are Postgres-only
and hold no such resource — proven directly by
tests/integration/test_broker_claim_concurrency.py, which shares one
PostgresBroker instance across real concurrent OS threads with no
Python-level locking at all (correctness comes from Postgres's own
SELECT ... FOR UPDATE SKIP LOCKED, not from thread serialization). Using
thread_sensitive=True here would only add an unnecessary and, worse,
*shared-with-everything-else* bottleneck — a slow bookkeeping call here
would contend with any other thread_sensitive=True call anywhere else in
the process, including this worker's own heartbeat sends, which are
latency-sensitive for the reaper (see werker.worker.reaper).
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
    action: str  # "retried" or "failed" — see Broker.reclaim_stale


class Broker(abc.ABC):
    #: False for a hypothetical future backend (e.g. SQLite) that cannot offer
    #: true non-blocking SKIP LOCKED semantics. Not exercised in v1 — see the
    #: "Known risks / non-goals" section of the project plan.
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
        """Runs func(*args, **kwargs) on this Broker's own dedicated executor
        (thread_sensitive=False), with connection cleanup — see the module
        docstring for why this isn't thread_sensitive=True."""
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
        """Release claim ownership of a finished item (succeeded or permanently
        failed). Does not touch terminal status — that's ResultStore's job."""

    async def aack(self, item_id: str) -> None:
        await self._run_sync(self.ack, item_id)

    @abc.abstractmethod
    def nack(self, item_id: str, *, retry_after: datetime) -> None:
        """Release an item back to the queue, eligible for re-claim at retry_after."""

    async def anack(self, item_id: str, *, retry_after: datetime) -> None:
        await self._run_sync(self.nack, item_id, retry_after=retry_after)

    @abc.abstractmethod
    def heartbeat(self, item_id: str, *, worker_id: str) -> None:
        """Extend the claim's staleness window for a still-running item. What
        distinguishes a slow-but-alive task from a crashed worker's stale claim
        — see werker.worker.reaper."""

    async def aheartbeat(self, item_id: str, *, worker_id: str) -> None:
        await self._run_sync(self.heartbeat, item_id, worker_id=worker_id)

    @abc.abstractmethod
    def reclaim_stale(
        self, *, stale_before: datetime, limit: int, retry_backoff_seconds: float
    ) -> list[ReclaimedItem]:
        """Atomically reclaim RUNNING items whose heartbeat is older than
        stale_before: AT_LEAST_ONCE items with retries remaining go back to
        READY (retried); AT_MOST_ONCE items, or items with no retries left,
        go to FAILED directly (failed) — never retried, per the delivery
        guarantee. The whole lock-then-decide-then-write must happen in one
        transaction per item so a second reaper (or the original "zombie"
        worker heartbeating late) can't race the decision."""

    async def areclaim_stale(
        self, *, stale_before: datetime, limit: int, retry_backoff_seconds: float
    ) -> list[ReclaimedItem]:
        return await self._run_sync(
            self.reclaim_stale,
            stale_before=stale_before,
            limit=limit,
            retry_backoff_seconds=retry_backoff_seconds,
        )
