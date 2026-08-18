"""Stale-claim reclaim loop.

SELECT ... FOR UPDATE SKIP LOCKED is only a lock during the claiming
transaction; after commit, status=RUNNING is a soft lock with no DB
backing. A hard-killed worker leaves rows RUNNING forever without this
loop. Runs as a separate asyncio task alongside the claim loop in a
long-running `taskworker` process (not started under --once — a one-shot
drain has no reason to wait around checking for staleness).
"""

import asyncio
import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from django.utils import timezone

if TYPE_CHECKING:
    from werker.worker.core import Worker

logger = logging.getLogger("werker.worker")


async def reaper_loop(worker: "Worker") -> None:
    while not worker._shutdown.is_set():
        try:
            await asyncio.wait_for(
                worker._shutdown.wait(), timeout=worker.reaper_poll_interval
            )
        except TimeoutError:
            pass
        if worker._shutdown.is_set():
            break

        stale_before = timezone.now() - timedelta(seconds=worker.stale_running_timeout)
        reclaimed = await worker.backend.broker.areclaim_stale(
            stale_before=stale_before,
            limit=worker.claim_batch_size,
            retry_backoff_seconds=worker.retry_backoff_base,
        )
        for item in reclaimed:
            logger.warning(
                "werker reaper %s reclaimed stale claim id=%s action=%s",
                worker.worker_id,
                item.id,
                item.action,
            )
