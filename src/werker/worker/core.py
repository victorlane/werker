"""The async worker orchestration loop.

Native `async def` task functions run directly on the loop, bounded by a
semaphore. Sync `def` task functions run in a bounded ThreadPoolExecutor,
the same async-on-loop/sync-in-threadpool split Starlette/FastAPI use.

Also handles graceful shutdown: SIGTERM/SIGINT set an asyncio.Event, and
in-flight work gets up to SHUTDOWN_GRACE_PERIOD before being abandoned. An
abandoned sync task can't be force-killed, so the reaper reclaims it once
its heartbeat goes stale, same as a genuine crash.
"""

import asyncio
import contextlib
import functools
import logging
import os
import signal
import socket
import traceback
import uuid
from datetime import timedelta
from inspect import iscoroutinefunction

from django.core.exceptions import ImproperlyConfigured
from django.tasks import task_backends
from django.tasks.base import TaskContext
from django.tasks.signals import task_finished, task_started
from django.utils import timezone
from django.utils.json import normalize_json

from werker.backend import PostgresTaskBackend
from werker.broker import ClaimedItem
from werker.worker.executor import create_executor, run_with_connection_cleanup
from werker.worker.reaper import reaper_loop
from werker.worker.scheduler import scheduler_loop

logger = logging.getLogger("werker.worker")

DEFAULT_CONCURRENCY = 8
DEFAULT_MAX_ASYNC_CONCURRENCY = 200
DEFAULT_POLL_INTERVAL = 1.0
DEFAULT_CLAIM_BATCH_SIZE = 10
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_BASE = 2.0
DEFAULT_RETRY_BACKOFF_MAX = 300
DEFAULT_HEARTBEAT_INTERVAL = 15
DEFAULT_STALE_RUNNING_TIMEOUT = 300
DEFAULT_REAPER_POLL_INTERVAL = 30
DEFAULT_SCHEDULER_POLL_INTERVAL = 30
DEFAULT_SHUTDOWN_GRACE_PERIOD = 30


def make_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


class Worker:
    def __init__(
        self, alias: str = "default", *, queues: list[str] | None = None, once: bool = False
    ):
        self.alias = alias
        backend = task_backends[alias]
        if not isinstance(backend, PostgresTaskBackend):
            raise ImproperlyConfigured(
                f"werker.worker.Worker requires TASKS[{alias!r}] to use "
                f"werker.backend.PostgresTaskBackend, got {type(backend).__name__}."
            )
        self.backend = backend
        self.queues = list(queues) if queues else list(self.backend.queues)
        self.once = once
        self.worker_id = make_worker_id()

        options = self.backend.options
        self.concurrency: int = options.get("CONCURRENCY", DEFAULT_CONCURRENCY)
        self.max_async_concurrency: int = options.get(
            "MAX_ASYNC_CONCURRENCY", DEFAULT_MAX_ASYNC_CONCURRENCY
        )
        self.poll_interval: float = options.get("POLL_INTERVAL", DEFAULT_POLL_INTERVAL)
        self.claim_batch_size: int = options.get("CLAIM_BATCH_SIZE", DEFAULT_CLAIM_BATCH_SIZE)
        self.max_retries: int = options.get("MAX_RETRIES", DEFAULT_MAX_RETRIES)
        self.retry_backoff_base: float = options.get(
            "RETRY_BACKOFF_BASE", DEFAULT_RETRY_BACKOFF_BASE
        )
        self.retry_backoff_max: float = options.get(
            "RETRY_BACKOFF_MAX", DEFAULT_RETRY_BACKOFF_MAX
        )
        self.heartbeat_interval: float = options.get(
            "HEARTBEAT_INTERVAL", DEFAULT_HEARTBEAT_INTERVAL
        )
        self.stale_running_timeout: float = options.get(
            "STALE_RUNNING_TIMEOUT", DEFAULT_STALE_RUNNING_TIMEOUT
        )
        self.reaper_poll_interval: float = options.get(
            "REAPER_POLL_INTERVAL", DEFAULT_REAPER_POLL_INTERVAL
        )
        self.scheduler_poll_interval: float = options.get(
            "SCHEDULER_POLL_INTERVAL", DEFAULT_SCHEDULER_POLL_INTERVAL
        )
        self.shutdown_grace_period: float = options.get(
            "SHUTDOWN_GRACE_PERIOD", DEFAULT_SHUTDOWN_GRACE_PERIOD
        )

        self._executor = create_executor(self.concurrency)
        self._async_semaphore = asyncio.Semaphore(self.max_async_concurrency)
        self._shutdown = asyncio.Event()
        self._inflight: set[asyncio.Task[None]] = set()

    async def run(self) -> None:
        self._install_signal_handlers()
        logger.info(
            "werker worker %s starting (backend=%s queues=%s)",
            self.worker_id,
            self.alias,
            self.queues,
        )
        try:
            loops = [self._task_claim_loop()]
            if not self.once:
                # --once drains and exits, no reason to wait for staleness.
                loops.append(reaper_loop(self))
                loops.append(scheduler_loop(self))
            await asyncio.gather(*loops)
        finally:
            # wait=False: _drain_inflight already waited up to
            # shutdown_grace_period. A thread that ignored cancellation
            # can't be force-killed from here, don't block on it too.
            self._executor.shutdown(wait=False, cancel_futures=True)
            logger.info("werker worker %s stopped", self.worker_id)

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._shutdown.set)
            except NotImplementedError:
                # e.g. Windows, or a non-main-thread event loop in tests.
                pass

    def _available_capacity(self) -> int:
        return (self.concurrency + self.max_async_concurrency) - len(self._inflight)

    async def _wait_or_shutdown(self, timeout: float) -> None:
        """Sleeps up to `timeout` seconds, or returns early if shutdown fires."""
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._shutdown.wait(), timeout=timeout)

    async def _task_claim_loop(self) -> None:
        while not self._shutdown.is_set():
            capacity = self._available_capacity()
            if capacity <= 0:
                await self._wait_or_shutdown(self.poll_interval)
                continue

            limit = min(capacity, self.claim_batch_size)
            claimed = await self.backend.broker.aclaim(
                queue_names=self.queues, limit=limit, worker_id=self.worker_id
            )

            if not claimed:
                if self.once:
                    break
                await self._wait_or_shutdown(self.poll_interval)
                continue

            for item in claimed:
                task_coro = asyncio.ensure_future(self._execute(item))
                self._inflight.add(task_coro)
                task_coro.add_done_callback(self._inflight.discard)

        await self._drain_inflight()

    async def _drain_inflight(self) -> None:
        if not self._inflight:
            return
        pending = list(self._inflight)
        try:
            await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True),
                timeout=None if self.once else self.shutdown_grace_period,
            )
        except TimeoutError:
            still_running = [t for t in pending if not t.done()]
            logger.warning(
                "werker worker %s shutdown grace period (%ss) elapsed with %d task(s) "
                "still running, abandoning them (the reaper will reclaim them once "
                "their heartbeat goes stale).",
                self.worker_id,
                self.shutdown_grace_period,
                len(still_running),
            )
            for t in still_running:
                t.cancel()

    async def _execute(self, item: ClaimedItem) -> None:
        result = await self.backend.aget_result(item.id)
        task = result.task

        await self.backend.result_store.amark_running(
            item.id, worker_id=self.worker_id, attempt=item.attempt
        )
        task_started.send(sender=type(self.backend), task_result=result)

        call_args = (
            (TaskContext(task_result=result), *result.args)
            if task.takes_context
            else tuple(result.args)
        )

        heartbeat_sender = asyncio.ensure_future(self._heartbeat_sender(item.id))
        try:
            if iscoroutinefunction(task.func):
                async with self._async_semaphore:
                    raw_return_value = await task.func(*call_args, **result.kwargs)
            else:
                loop = asyncio.get_running_loop()
                raw_return_value = await loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        run_with_connection_cleanup, task.func, *call_args, **result.kwargs
                    ),
                )
        except Exception as exc:
            # Deliberately catch-all: any task exception is a failed attempt,
            # not a worker bug.
            await self._handle_failure(item, exc)
        else:
            await self.backend.result_store.amark_successful(
                item.id, return_value=normalize_json(raw_return_value)
            )
            await self.backend.broker.aack(item.id)
        finally:
            heartbeat_sender.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_sender

        finished_result = await self.backend.aget_result(item.id)
        task_finished.send(sender=type(self.backend), task_result=finished_result)

    async def _heartbeat_sender(self, item_id: str) -> None:
        """Keeps last_heartbeat_at fresh while item_id runs. Cancelled as soon
        as execution finishes, see _execute's finally block."""
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            await self.backend.broker.aheartbeat(item_id, worker_id=self.worker_id)

    async def _handle_failure(self, item: ClaimedItem, exc: BaseException) -> None:
        exception_type = type(exc)
        error = {
            "exception_class_path": f"{exception_type.__module__}.{exception_type.__qualname__}",
            "traceback": "".join(traceback.format_exception(exc)),
        }
        will_retry = item.attempt < self.max_retries
        await self.backend.result_store.amark_failed(item.id, error=error, will_retry=will_retry)

        if will_retry:
            delay = min(
                self.retry_backoff_base**item.attempt,
                self.retry_backoff_max,
            )
            retry_after = timezone.now() + timedelta(seconds=delay)
            await self.backend.broker.anack(item.id, retry_after=retry_after)
        else:
            await self.backend.broker.aack(item.id)
