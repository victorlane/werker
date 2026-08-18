"""PostgresBroker: SELECT ... FOR UPDATE SKIP LOCKED claim semantics.

v1 shortcut, documented deliberately (see werker.broker's module docstring):
this reads/writes the same werker.models.DBTaskResult table that
werker.results.db.DBResultStore uses, touching only the claim-relevant
columns (status, run_after, priority, queue_name, claimed_by, claimed_at,
last_heartbeat_at, attempts). A future non-Postgres Broker would own its
own physically separate storage.
"""

from datetime import datetime

from asgiref.sync import sync_to_async
from django.db import transaction
from django.db.models import F
from django.db.models.functions import Coalesce
from django.utils import timezone

from werker.broker import Broker, ClaimedItem, QueueItem
from werker.models import DBTaskResult, TaskStatus


class PostgresBroker(Broker):
    supports_skip_locked = True

    def _enqueue_sync(self, item: QueueItem) -> None:
        # The row is created by ResultStore.acreate before this runs (see
        # PostgresTaskBackend.aenqueue) — this call exists so the Broker
        # interface has something real to do even in the shared-table v1
        # layout, and so a future separate-storage Broker has a natural
        # place to actually insert a queue entry.
        DBTaskResult.objects.filter(id=item.id).update(
            queue_name=item.queue_name,
            priority=item.priority,
            run_after=item.run_after,
            status=TaskStatus.READY,
        )

    async def aenqueue(self, item: QueueItem) -> None:
        await sync_to_async(self._enqueue_sync, thread_sensitive=True)(item)

    def _claim_sync(
        self, *, queue_names: list[str], limit: int, worker_id: str
    ) -> list[ClaimedItem]:
        """The core correctness-critical claim query. Deliberately a plain
        sync function (see werker.backend's module docstring on why
        transactional DB logic is written sync-first, not async-first) —
        also directly callable from a thread pool in tests to exercise real
        concurrent Postgres transactions without going through asyncio."""
        now = timezone.now()
        with transaction.atomic():
            ids = list(
                DBTaskResult.objects.select_for_update(skip_locked=True)
                .filter(
                    status=TaskStatus.READY,
                    queue_name__in=queue_names,
                    run_after__lte=now,
                )
                .order_by("-priority", "run_after")
                .values_list("id", flat=True)[:limit]
            )
            if not ids:
                return []
            DBTaskResult.objects.filter(id__in=ids).update(
                status=TaskStatus.RUNNING,
                claimed_by=worker_id,
                claimed_at=now,
                last_heartbeat_at=now,
                started_at=Coalesce(F("started_at"), now),
                last_attempted_at=now,
                attempts=F("attempts") + 1,
            )
        rows = DBTaskResult.objects.filter(id__in=ids).values("id", "queue_name", "attempts")
        return [
            ClaimedItem(id=str(row["id"]), queue_name=row["queue_name"], attempt=row["attempts"])
            for row in rows
        ]

    async def aclaim(
        self, *, queue_names: list[str], limit: int, worker_id: str
    ) -> list[ClaimedItem]:
        return await sync_to_async(self._claim_sync, thread_sensitive=True)(
            queue_names=queue_names, limit=limit, worker_id=worker_id
        )

    def _ack_sync(self, item_id: str) -> None:
        DBTaskResult.objects.filter(id=item_id).update(
            claimed_by="", claimed_at=None, last_heartbeat_at=None
        )

    async def aack(self, item_id: str) -> None:
        await sync_to_async(self._ack_sync, thread_sensitive=True)(item_id)

    def _nack_sync(self, item_id: str, *, retry_after: datetime) -> None:
        DBTaskResult.objects.filter(id=item_id).update(
            status=TaskStatus.READY,
            run_after=retry_after,
            claimed_by="",
            claimed_at=None,
            last_heartbeat_at=None,
        )

    async def anack(self, item_id: str, *, retry_after: datetime) -> None:
        await sync_to_async(self._nack_sync, thread_sensitive=True)(
            item_id, retry_after=retry_after
        )

    def _heartbeat_sync(self, item_id: str, *, worker_id: str) -> None:
        DBTaskResult.objects.filter(id=item_id, claimed_by=worker_id).update(
            last_heartbeat_at=timezone.now()
        )

    async def aheartbeat(self, item_id: str, *, worker_id: str) -> None:
        await sync_to_async(self._heartbeat_sync, thread_sensitive=True)(
            item_id, worker_id=worker_id
        )
