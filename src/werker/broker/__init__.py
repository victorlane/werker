"""The Broker ABC: queue/claim/lock semantics, independent of result storage.

Mirrors the split Celery draws between a broker and a result backend (see
werker.results for the other half), plus the same split extended to
werker.schedules for periodic tasks. V1 ships one concrete Broker
(werker.broker.postgres.PostgresBroker) backed by the same Postgres table
werker.results.db.DBResultStore uses; see PostgresBroker's docstring for
why that's a deliberate v1 shortcut, not a structural requirement.

Sync methods are canonical (abstract); async methods are derived via
asgiref.sync.sync_to_async(thread_sensitive=True) — mirroring Django's own
BaseTaskBackend (django.tasks.backends.base) and its documented guidance
that ORM transactions aren't async-safe as of Django 6.0. A worker's own
event loop awaits the async methods directly; a sync call site (e.g.
PostgresTaskBackend.enqueue) calls the sync methods with no asyncio
involved at all.
"""

import abc
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from asgiref.sync import sync_to_async


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


class Broker(abc.ABC):
    #: False for a hypothetical future backend (e.g. SQLite) that cannot offer
    #: true non-blocking SKIP LOCKED semantics. Not exercised in v1 — see the
    #: "Known risks / non-goals" section of the project plan.
    supports_skip_locked: ClassVar[bool] = True

    def __init__(self, options):
        self.options = options

    @abc.abstractmethod
    def enqueue(self, item: QueueItem) -> None: ...

    async def aenqueue(self, item: QueueItem) -> None:
        await sync_to_async(self.enqueue, thread_sensitive=True)(item)

    @abc.abstractmethod
    def claim(
        self, *, queue_names: Sequence[str], limit: int, worker_id: str
    ) -> list[ClaimedItem]: ...

    async def aclaim(
        self, *, queue_names: Sequence[str], limit: int, worker_id: str
    ) -> list[ClaimedItem]:
        return await sync_to_async(self.claim, thread_sensitive=True)(
            queue_names=queue_names, limit=limit, worker_id=worker_id
        )

    @abc.abstractmethod
    def ack(self, item_id: str) -> None:
        """Release claim ownership of a finished item (succeeded or permanently
        failed). Does not touch terminal status — that's ResultStore's job."""

    async def aack(self, item_id: str) -> None:
        await sync_to_async(self.ack, thread_sensitive=True)(item_id)

    @abc.abstractmethod
    def nack(self, item_id: str, *, retry_after: datetime) -> None:
        """Release an item back to the queue, eligible for re-claim at retry_after."""

    async def anack(self, item_id: str, *, retry_after: datetime) -> None:
        await sync_to_async(self.nack, thread_sensitive=True)(item_id, retry_after=retry_after)

    @abc.abstractmethod
    def heartbeat(self, item_id: str, *, worker_id: str) -> None:
        """Extend the claim's staleness window for a still-running item. What
        distinguishes a slow-but-alive task from a crashed worker's stale claim
        — see werker.worker.reaper."""

    async def aheartbeat(self, item_id: str, *, worker_id: str) -> None:
        await sync_to_async(self.heartbeat, thread_sensitive=True)(item_id, worker_id=worker_id)
