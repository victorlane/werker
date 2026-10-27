"""Automatic result cleanup (RESULT_TTL). Every worker runs this loop, but
an advisory lock lets only one of them prune at a time."""

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING

from werker.maintenance import run_cleanup

if TYPE_CHECKING:
    from werker.worker.core import Worker

logger = logging.getLogger("werker.worker")


async def cleanup_loop(worker: "Worker") -> None:
    while not worker._shutdown.is_set():
        try:
            deleted = await worker.backend.broker._run_sync(run_cleanup, worker.result_ttl)
        except Exception:
            logger.exception("werker worker %s result cleanup failed", worker.worker_id)
        else:
            if deleted:
                logger.info(
                    "werker worker %s pruned %d finished task(s)", worker.worker_id, deleted
                )
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(worker._shutdown.wait(), timeout=worker.cleanup_interval)
