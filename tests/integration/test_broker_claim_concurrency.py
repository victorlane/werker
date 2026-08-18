"""Proves the one correctness property this whole design rests on: N
concurrent claimers racing the same due rows never claim the same row
twice, and together claim every due row exactly once.

Calls PostgresBroker.claim (the canonical sync method) directly from real
OS threads via ThreadPoolExecutor, so each thread holds its own DB
connection and genuinely races the others in Postgres.
"""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from django.db import connections
from django.utils import timezone

from werker.broker.postgres import PostgresBroker
from werker.models import DBTaskResult, TaskStatus

NUM_ROWS = 40
NUM_CLAIMERS = 8


@pytest.mark.django_db(transaction=True)
def test_concurrent_claim_never_double_claims_and_claims_everything():
    now = timezone.now()
    DBTaskResult.objects.bulk_create(
        [
            DBTaskResult(
                task_path="tests.dummy_task",
                queue_name="default",
                status=TaskStatus.READY,
                run_after=now,
                enqueued_at=now,
            )
            for _ in range(NUM_ROWS)
        ]
    )

    broker = PostgresBroker(options=None)
    start_barrier = threading.Barrier(NUM_CLAIMERS)
    claimed_by_worker: dict[str, list[str]] = {}
    lock = threading.Lock()

    def claim_loop(worker_index: int) -> None:
        worker_id = f"worker-{worker_index}"
        claimed_ids: list[str] = []
        start_barrier.wait()
        try:
            while True:
                claimed = broker.claim(
                    queue_names=["default"], limit=1, worker_id=worker_id
                )
                if not claimed:
                    break
                claimed_ids.append(claimed[0].id)
        finally:
            connections.close_all()
        with lock:
            claimed_by_worker[worker_id] = claimed_ids

    with ThreadPoolExecutor(max_workers=NUM_CLAIMERS) as pool:
        list(pool.map(claim_loop, range(NUM_CLAIMERS)))

    all_claimed = [item_id for ids in claimed_by_worker.values() for item_id in ids]

    assert len(all_claimed) == NUM_ROWS, (
        f"expected exactly {NUM_ROWS} claims across all workers, got {len(all_claimed)}"
    )
    assert len(set(all_claimed)) == NUM_ROWS, "the same row was claimed more than once"

    remaining_ready = DBTaskResult.objects.filter(status=TaskStatus.READY).count()
    assert remaining_ready == 0

    running_count = DBTaskResult.objects.filter(status=TaskStatus.RUNNING).count()
    assert running_count == NUM_ROWS
