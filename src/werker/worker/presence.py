"""Worker presence: a WorkerRecord row per worker process, refreshed every
HEARTBEAT_INTERVAL, plus an optional health file for liveness probes.
"""

import asyncio
import contextlib
import logging
import os
import socket
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from django.tasks import task_backends
from django.utils import timezone

from werker.models import WorkerRecord

if TYPE_CHECKING:
    from werker.worker.core import Worker

logger = logging.getLogger("werker.worker")

DEFAULT_HEARTBEAT_INTERVAL = 15
#: A worker that missed this many heartbeats counts as gone.
MISSED_HEARTBEATS = 3


def alive_cutoff(alias: str = "default") -> datetime:
    """Workers last seen before this moment are treated as gone."""
    interval = task_backends[alias].options.get(
        "HEARTBEAT_INTERVAL", DEFAULT_HEARTBEAT_INTERVAL
    )
    return timezone.now() - timedelta(seconds=interval * MISSED_HEARTBEATS)


def _save(worker: "Worker", *, stopped: bool = False) -> None:
    now = timezone.now()
    defaults = {
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "queues": list(worker.queues),
        "last_seen_at": now,
        "stopped_at": now if stopped else None,
    }
    WorkerRecord.objects.update_or_create(
        worker_id=worker.worker_id,
        defaults=defaults,
        create_defaults={**defaults, "started_at": now},
    )


async def save_presence(worker: "Worker", *, stopped: bool = False) -> None:
    try:
        await worker.backend.broker._run_sync(_save, worker, stopped=stopped)
    except Exception:
        # Presence is informational; never let it stop the worker.
        logger.exception("werker worker %s presence update failed", worker.worker_id)


async def presence_loop(worker: "Worker") -> None:
    while not worker._shutdown.is_set():
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(worker._shutdown.wait(), timeout=worker.heartbeat_interval)
        if worker._shutdown.is_set():
            break
        await save_presence(worker)


def touch_health_file(path: str | None) -> None:
    if path is None:
        return
    try:
        Path(path).touch()
    except OSError:
        logger.exception("werker could not touch health file %s", path)
