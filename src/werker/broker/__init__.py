"""The Broker ABC: queue/claim/lock semantics, independent of result storage.

Mirrors the split Celery draws between a broker and a result backend (see
werker.results for the other half), plus the same split extended to
werker.schedules for periodic tasks — three independently swappable
concerns behind one werker.backend.PostgresTaskBackend, per the project
plan. V1 ships one concrete Broker (werker.broker.postgres.PostgresBroker)
backed by the same Postgres table werker.results.db.DBResultStore uses;
see PostgresBroker's docstring for why that's a deliberate v1 shortcut and
not a structural requirement of this ABC.
"""

import abc
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from asgiref.sync import async_to_sync


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
    async def aenqueue(self, item: QueueItem) -> None: ...

    def enqueue(self, item: QueueItem) -> None:
        return async_to_sync(self.aenqueue)(item)

    @abc.abstractmethod
    async def aclaim(
        self, *, queue_names: Sequence[str], limit: int, worker_id: str
    ) -> list[ClaimedItem]: ...

    def claim(
        self, *, queue_names: Sequence[str], limit: int, worker_id: str
    ) -> list[ClaimedItem]:
        return async_to_sync(self.aclaim)(
            queue_names=queue_names, limit=limit, worker_id=worker_id
        )

    @abc.abstractmethod
    async def aack(self, item_id: str) -> None:
        """Release claim ownership of a finished item (succeeded or permanently
        failed). Does not touch terminal status — that's ResultStore's job."""

    @abc.abstractmethod
    async def anack(self, item_id: str, *, retry_after: datetime) -> None:
        """Release an item back to the queue, eligible for re-claim at retry_after."""

    @abc.abstractmethod
    async def aheartbeat(self, item_id: str, *, worker_id: str) -> None:
        """Extend the claim's staleness window for a still-running item. What
        distinguishes a slow-but-alive task from a crashed worker's stale claim
        — see werker.worker.reaper."""
