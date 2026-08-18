"""Stale-claim reclaim loop. A hard-killed worker leaves rows RUNNING
forever without this. Runs alongside the claim loop, not under --once.
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
