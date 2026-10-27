"""LISTEN/NOTIFY wakeups. Enqueueing a task that is due now sends
`NOTIFY werker, '<queue>'` inside the enqueue transaction, so Postgres
delivers it only on commit. This loop listens on its own connection and
wakes the claim loop at once instead of waiting for the next poll.

Polling stays on as a safety net: for delayed tasks (run_after), for
notifications missed while reconnecting, and when LISTEN is unavailable
(e.g. psycopg missing, or pgbouncer in transaction mode; point
LISTEN_DATABASE at a direct connection for that).
"""

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any

from django.db import connections

from werker.broker.postgres import NOTIFY_CHANNEL

if TYPE_CHECKING:
    from werker.worker.core import Worker

logger = logging.getLogger("werker.worker")

MIN_RECONNECT_DELAY = 1.0
MAX_RECONNECT_DELAY = 30.0

# Django's own connection params that don't apply to a separate async connection.
_DJANGO_ONLY_PARAMS = {"cursor_factory", "context"}


def connection_params(alias: str) -> dict[str, Any]:
    params = connections[alias].get_connection_params()
    return {k: v for k, v in params.items() if k not in _DJANGO_ONLY_PARAMS}


async def listen_loop(worker: "Worker") -> None:
    try:
        import psycopg
    except ImportError:
        logger.warning("werker LISTEN/NOTIFY disabled: psycopg is not installed; polling only")
        return

    delay = MIN_RECONNECT_DELAY
    while not worker._shutdown.is_set():
        try:
            conn = await psycopg.AsyncConnection.connect(
                **connection_params(worker.listen_database), autocommit=True
            )
        except Exception as exc:
            logger.warning(
                "werker worker %s LISTEN connection failed (%s), polling; retrying in %ss",
                worker.worker_id,
                exc,
                delay,
            )
        else:
            try:
                async with conn:
                    await conn.execute(f"LISTEN {NOTIFY_CHANNEL}")
                    worker._listener_pid = conn.info.backend_pid
                    worker._listening = True
                    delay = MIN_RECONNECT_DELAY
                    # Anything enqueued while we weren't listening.
                    worker._wakeup.set()
                    await _consume_until_shutdown(worker, conn)
            except Exception as exc:
                if worker._shutdown.is_set():
                    break
                logger.warning(
                    "werker worker %s LISTEN connection lost (%s), polling; reconnecting in %ss",
                    worker.worker_id,
                    exc,
                    delay,
                )
            finally:
                worker._listening = False

        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(worker._shutdown.wait(), timeout=delay)
        delay = min(delay * 2, MAX_RECONNECT_DELAY)


async def _consume_until_shutdown(worker: "Worker", conn: Any) -> None:
    """Handles notifications until shutdown, which cancels the wait at once
    instead of after a poll timeout. Re-raises a lost connection."""

    async def consume() -> None:
        async for notify in conn.notifies():
            worker._notifications += 1
            if notify.payload in worker.queues:
                worker._wakeup.set()

    consumer = asyncio.ensure_future(consume())
    shutdown = asyncio.ensure_future(worker._shutdown.wait())
    try:
        done, _ = await asyncio.wait({consumer, shutdown}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        consumer.cancel()
        shutdown.cancel()
    if consumer in done:
        consumer.result()
