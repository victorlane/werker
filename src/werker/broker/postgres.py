"""PostgresBroker: SELECT ... FOR UPDATE SKIP LOCKED claim semantics.

v1 shortcut: reads/writes the same DBTaskResult table DBResultStore uses,
touching only the claim-relevant columns. A future non-Postgres Broker
would own its own separate storage.
"""

from collections.abc import Sequence
from datetime import datetime, timedelta

from django.db import transaction
from django.db.models import F
from django.db.models.functions import Coalesce
from django.utils import timezone

from werker.broker import Broker, ClaimedItem, QueueItem, ReclaimedItem
from werker.models import DBTaskResult, DeliveryGuarantee, PeriodicTask, TaskStatus


class PostgresBroker(Broker):
    supports_skip_locked = True

    def enqueue(self, item: QueueItem) -> None:
        # The row is created by ResultStore.create before this runs.
        DBTaskResult.objects.filter(id=item.id).update(
            queue_name=item.queue_name,
            priority=item.priority,
            run_after=item.run_after,
            status=TaskStatus.READY,
        )

    def claim(
        self, *, queue_names: Sequence[str], limit: int, worker_id: str
    ) -> list[ClaimedItem]:
        """The core correctness-critical claim query. Also directly callable
        from a thread pool in tests to exercise real concurrent transactions."""
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

    def reclaim_stale(
        self, *, stale_before: datetime, limit: int, retry_backoff_seconds: float
    ) -> list[ReclaimedItem]:
        now = timezone.now()
        with transaction.atomic():
            ids = list(
                DBTaskResult.objects.select_for_update(skip_locked=True)
                .filter(status=TaskStatus.RUNNING, last_heartbeat_at__lt=stale_before)
                .order_by("last_heartbeat_at")
                .values_list("id", flat=True)[:limit]
            )
            if not ids:
                return []

            rows = list(
                DBTaskResult.objects.filter(id__in=ids).values(
                    "id", "delivery_guarantee", "attempts", "max_retries", "errors"
                )
            )

            retry_ids = []
            fail_rows = []
            for row in rows:
                can_retry = (
                    row["delivery_guarantee"] == DeliveryGuarantee.AT_LEAST_ONCE
                    and row["attempts"] < row["max_retries"]
                )
                if can_retry:
                    retry_ids.append(row["id"])
                else:
                    fail_rows.append(row)

            if retry_ids:
                DBTaskResult.objects.filter(id__in=retry_ids).update(
                    status=TaskStatus.READY,
                    run_after=now + timedelta(seconds=retry_backoff_seconds),
                    claimed_by="",
                    claimed_at=None,
                    last_heartbeat_at=None,
                )

            for row in fail_rows:
                errors = [*row["errors"], _reaper_error()]
                DBTaskResult.objects.filter(id=row["id"]).update(
                    status=TaskStatus.FAILED,
                    finished_at=now,
                    errors=errors,
                    claimed_by="",
                    claimed_at=None,
                    last_heartbeat_at=None,
                )

        fail_ids = {row["id"] for row in fail_rows}
        return [
            ReclaimedItem(
                id=str(row["id"]),
                action="failed" if row["id"] in fail_ids else "retried",
            )
            for row in rows
        ]

    def claim_due_schedules(self, *, limit: int, worker_id: str) -> list[str]:
        """Atomically claims due, enabled PeriodicTask rows and returns ids.
        Rows stay locked until _dispatch's transaction commits (the lock is
        held to the same transaction because we're inside one here too)."""
        now = timezone.now()
        with transaction.atomic():
            ids = list(
                PeriodicTask.objects.select_for_update(skip_locked=True)
                .filter(enabled=True, next_run_at__lte=now)
                .order_by("next_run_at")
                .values_list("id", flat=True)[:limit]
            )
            if not ids:
                return []
            PeriodicTask.objects.filter(id__in=ids).update(
                claimed_by=worker_id,
                last_heartbeat_at=now,
            )
        return [str(i) for i in ids]


def _reaper_error() -> dict[str, str]:
    return {
        "exception_class_path": "werker.exceptions.StaleClaimReclaimed",
        "traceback": (
            "werker.exceptions.StaleClaimReclaimed: no heartbeat received before "
            "STALE_RUNNING_TIMEOUT, the worker holding this claim is presumed dead."
        ),
    }
