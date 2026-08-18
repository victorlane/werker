"""PostgresBroker: SELECT ... FOR UPDATE SKIP LOCKED claim semantics.

v1 shortcut, documented deliberately (see werker.broker's module docstring):
this reads/writes the same werker.models.DBTaskResult table that
werker.results.db.DBResultStore uses, touching only the claim-relevant
columns (status, run_after, priority, queue_name, claimed_by, claimed_at,
last_heartbeat_at, attempts). A future non-Postgres Broker would own its
own physically separate storage.
"""

from collections.abc import Sequence
from datetime import datetime

from django.db import transaction
from django.db.models import F
from django.db.models.functions import Coalesce
from django.utils import timezone

from werker.broker import Broker, ClaimedItem, QueueItem
from werker.models import DBTaskResult, TaskStatus


class PostgresBroker(Broker):
    supports_skip_locked = True

    def enqueue(self, item: QueueItem) -> None:
        # The row is created by ResultStore.create before this runs (see
        # PostgresTaskBackend.enqueue) — this call exists so the Broker
        # interface has something real to do even in the shared-table v1
        # layout, and so a future separate-storage Broker has a natural
        # place to actually insert a queue entry.
        DBTaskResult.objects.filter(id=item.id).update(
            queue_name=item.queue_name,
            priority=item.priority,
            run_after=item.run_after,
            status=TaskStatus.READY,
        )

    def claim(
        self, *, queue_names: Sequence[str], limit: int, worker_id: str
    ) -> list[ClaimedItem]:
        """The core correctness-critical claim query. A plain sync Django ORM
        function (see werker.broker's module docstring) — also directly
        callable from a thread pool in tests to exercise real concurrent
        Postgres transactions without going through asyncio."""
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

    def ack(self, item_id: str) -> None:
        DBTaskResult.objects.filter(id=item_id).update(
            claimed_by="", claimed_at=None, last_heartbeat_at=None
        )

    def nack(self, item_id: str, *, retry_after: datetime) -> None:
        DBTaskResult.objects.filter(id=item_id).update(
            status=TaskStatus.READY,
            run_after=retry_after,
            claimed_by="",
            claimed_at=None,
            last_heartbeat_at=None,
        )

    def heartbeat(self, item_id: str, *, worker_id: str) -> None:
        DBTaskResult.objects.filter(id=item_id, claimed_by=worker_id).update(
            last_heartbeat_at=timezone.now()
        )
