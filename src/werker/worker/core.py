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
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from datetime import timedelta
from inspect import iscoroutinefunction
from typing import Any

from django.core.exceptions import ImproperlyConfigured
from django.db import router
from django.tasks import task_backends
from django.tasks.base import TaskContext
from django.tasks.signals import task_finished, task_started
from django.utils import timezone
from django.utils.json import normalize_json
from django.utils.module_loading import import_string

from werker import registry
from werker.backend import PostgresTaskBackend
from werker.broker import ClaimedItem, retries_remaining
from werker.config import validate_options
from werker.exceptions import TaskCancelled, TaskTimeout
from werker.maintenance import parse_result_ttl
from werker.models import DBTaskResult, DeliveryGuarantee
from werker.retry import backoff_delay
from werker.worker.cleanup import cleanup_loop
from werker.worker.executor import create_executor, run_with_connection_cleanup
from werker.worker.listener import listen_loop
from werker.worker.presence import presence_loop, save_presence, touch_health_file
from werker.worker.reaper import reaper_loop
from werker.worker.scheduler import scheduler_loop

logger = logging.getLogger("werker.worker")

DEFAULT_CONCURRENCY = 8
DEFAULT_MAX_ASYNC_CONCURRENCY = 200
DEFAULT_POLL_INTERVAL = 1.0
DEFAULT_CLAIM_BATCH_SIZE = 10
DEFAULT_RETRY_BACKOFF_BASE = 2.0
DEFAULT_RETRY_BACKOFF_MAX = 300
DEFAULT_HEARTBEAT_INTERVAL = 15
DEFAULT_STALE_RUNNING_TIMEOUT = 300
DEFAULT_REAPER_POLL_INTERVAL = 30
DEFAULT_SCHEDULER_POLL_INTERVAL = 30
DEFAULT_SHUTDOWN_GRACE_PERIOD = 30
DEFAULT_CLEANUP_INTERVAL = 3600
MIN_RATE_LIMIT_WAIT = 0.01
#: Safety-net poll interval while LISTEN/NOTIFY is connected.
LISTENING_POLL_INTERVAL = 5.0


def make_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


def _needs_thread(task_path: str) -> bool:
    """Whether the task runs in the sync thread pool. An unimportable task
    doesn't: it fails fast in _run_claimed without one."""
    try:
        task = import_string(task_path)
    except ImportError:
        return False
    return not iscoroutinefunction(task.func)


class Worker:
    def __init__(
        self,
        alias: str = "default",
        *,
        queues: list[str] | None = None,
        once: bool = False,
        health_file: str | None = None,
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
        validate_options(options)
        self.concurrency: int = options.get("CONCURRENCY", DEFAULT_CONCURRENCY)
        self.max_async_concurrency: int = options.get(
            "MAX_ASYNC_CONCURRENCY", DEFAULT_MAX_ASYNC_CONCURRENCY
        )
        self.poll_interval: float = options.get("POLL_INTERVAL", DEFAULT_POLL_INTERVAL)
        self.claim_batch_size: int = options.get("CLAIM_BATCH_SIZE", DEFAULT_CLAIM_BATCH_SIZE)
        self.retry_backoff_base: float = options.get(
            "RETRY_BACKOFF_BASE", DEFAULT_RETRY_BACKOFF_BASE
        )
        self.retry_backoff_max: float = options.get(
            "RETRY_BACKOFF_MAX", DEFAULT_RETRY_BACKOFF_MAX
        )
        self.retry_jitter: bool = options.get("RETRY_JITTER", True)
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
        self.task_timeout: float | None = options.get("TASK_TIMEOUT")
        self.health_file: str | None = health_file or options.get("HEALTH_FILE")
        self.result_ttl = _result_ttl(options.get("RESULT_TTL"))
        self.cleanup_interval: float = options.get("CLEANUP_INTERVAL", DEFAULT_CLEANUP_INTERVAL)
        self.listen_notify: bool = options.get("LISTEN_NOTIFY", True)
        self.listen_database: str = options.get("LISTEN_DATABASE") or router.db_for_write(
            DBTaskResult
        )

        self._executor = create_executor(self.concurrency)
        self._async_semaphore = asyncio.Semaphore(self.max_async_concurrency)
        self._shutdown = asyncio.Event()
        self._inflight: set[asyncio.Task[None]] = set()
        # Sync task threads that timed out but haven't returned yet.
        self._abandoned = 0
        self._sync_inflight: set[asyncio.Task[None]] = set()
        # Ids of running tasks whose cancelling was requested.
        self._cancel_requested: set[str] = set()
        # Set by the listener on a NOTIFY for our queues, and when a task
        # finishes and frees capacity; wakes the claim loop early.
        self._wakeup = asyncio.Event()
        self._listening = False
        self._listener_pid: int | None = None
        self._notifications = 0

    async def run(self) -> None:
        self._install_signal_handlers()
        logger.info(
            "werker worker %s starting (backend=%s queues=%s)",
            self.worker_id,
            self.alias,
            self.queues,
        )
        await save_presence(self)
        try:
            loops = [self._task_claim_loop()]
            if not self.once:
                # --once drains and exits, no reason to wait for staleness.
                loops.append(reaper_loop(self))
                loops.append(scheduler_loop(self))
                loops.append(presence_loop(self))
                if self.listen_notify:
                    loops.append(listen_loop(self))
                if self.result_ttl:
                    loops.append(cleanup_loop(self))
            await asyncio.gather(*loops)
        finally:
            await save_presence(self, stopped=True)
            # wait=False: _drain_inflight already waited up to
            # shutdown_grace_period. A thread that ignored cancellation
            # can't be force-killed from here, don't block on it too.
            self._executor.shutdown(wait=False, cancel_futures=True)
            logger.info("werker worker %s stopped", self.worker_id)

    def retry_delay(self, task_path: str, attempt: int) -> float:
        """Seconds before the next attempt, from the task's @retry policy
        or the backend options."""
        policy = registry.policy_for(task_path)
        return backoff_delay(
            attempt,
            base=policy.backoff_base or self.retry_backoff_base,
            maximum=policy.backoff_max or self.retry_backoff_max,
            jitter=self.retry_jitter if policy.jitter is None else policy.jitter,
        )

    def request_shutdown(self) -> None:
        """Starts a graceful shutdown. Not thread-safe: from another thread,
        use loop.call_soon_threadsafe(worker.request_shutdown)."""
        self._shutdown.set()

    @property
    def idle_poll_interval(self) -> float:
        """How long to wait for work when the queue looked empty. While
        LISTEN is connected, polling is only a safety net."""
        if self._listening:
            return max(self.poll_interval, LISTENING_POLL_INTERVAL)
        return self.poll_interval

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.request_shutdown)
            except (NotImplementedError, RuntimeError, ValueError):
                # NotImplementedError on Windows. RuntimeError/ValueError when
                # the loop runs outside the main thread (runserver autostart,
                # tests): only the main thread may install signal handlers.
                pass

    def _task_done(self, task_coro: "asyncio.Task[None]") -> None:
        self._inflight.discard(task_coro)
        # Capacity freed up; claim more without waiting for the next poll.
        self._wakeup.set()

    def _free_sync_slots(self) -> int:
        # Abandoned (timed-out) sync threads still occupy the pool.
        return self.concurrency - len(self._sync_inflight) - self._abandoned

    def _available_capacity(self) -> int:
        async_inflight = len(self._inflight) - len(self._sync_inflight)
        return self._free_sync_slots() + self.max_async_concurrency - async_inflight

    async def _wait_or_shutdown(self, timeout: float) -> None:
        """Sleeps up to `timeout` seconds, or returns early if shutdown fires."""
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._shutdown.wait(), timeout=timeout)

    async def _wait_for_work(self, timeout: float) -> None:
        """Like _wait_or_shutdown, but also returns early on a wakeup."""
        waiters = {
            asyncio.ensure_future(self._shutdown.wait()),
            asyncio.ensure_future(self._wakeup.wait()),
        }
        try:
            await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in waiters:
                waiter.cancel()

    async def _task_claim_loop(self) -> None:
        while not self._shutdown.is_set():
            # Touched on every pass, so a liveness probe on it also catches a
            # stuck event loop, not only a dead process.
            touch_health_file(self.health_file)
            # Cleared before claiming, so a wakeup that arrives during the
            # claim isn't lost.
            self._wakeup.clear()
            capacity = self._available_capacity()
            if capacity <= 0:
                await self._wait_for_work(self.poll_interval)
                continue

            limit = min(capacity, self.claim_batch_size)
            try:
                claimed = await self.backend.broker.aclaim(
                    queue_names=self.queues,
                    limit=limit,
                    worker_id=self.worker_id,
                    limits=registry.limited_policies(),
                )
            except Exception:
                # A transient DB error (e.g. Postgres restart) must not kill
                # the worker; back off and retry next poll.
                logger.exception("werker worker %s claim failed, retrying", self.worker_id)
                await self._wait_or_shutdown(self.poll_interval)
                continue

            if not claimed:
                if self.once and claimed.wait_hint is None:
                    break
                wait = self.idle_poll_interval
                if claimed.wait_hint is not None:
                    # Due work is waiting on a rate limit.
                    wait = min(wait, max(claimed.wait_hint, MIN_RATE_LIMIT_WAIT))
                await self._wait_for_work(wait)
                continue

            released_any = False
            for item in claimed:
                needs_thread = _needs_thread(item.task_path)
                if needs_thread and self._free_sync_slots() <= 0:
                    # Hand it back for a worker with a free thread rather
                    # than hoarding it in our executor's queue.
                    await self._release(item)
                    released_any = True
                    continue
                self._start(item, needs_thread=needs_thread)

            if released_any:
                # Wait for a thread to free up (_task_done wakes us) instead
                # of re-claiming the same work straight away.
                await self._wait_for_work(self.poll_interval)

        await self._drain_inflight()

    def _start(self, item: ClaimedItem, *, needs_thread: bool) -> None:
        task_coro = asyncio.ensure_future(self._execute(item))
        self._inflight.add(task_coro)
        if needs_thread:
            self._sync_inflight.add(task_coro)
            task_coro.add_done_callback(self._sync_inflight.discard)
        task_coro.add_done_callback(self._task_done)

    async def _release(self, item: ClaimedItem) -> None:
        try:
            await self.backend.broker.arelease(item, worker_id=self.worker_id)
        except Exception:
            logger.exception(
                "werker worker %s failed to release id=%s; the reaper will reclaim it",
                self.worker_id,
                item.id,
            )

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
        """Runs one claimed item. Nobody awaits this future, so every error
        must be handled here or it would vanish and leave the row RUNNING
        until the reaper notices."""
        try:
            await self._run_claimed(item)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "werker worker %s failed handling task %s attempt %s",
                self.worker_id,
                item.id,
                item.attempt,
            )

    async def _run_claimed(self, item: ClaimedItem) -> None:
        # Before loading the TaskResult: django.tasks derives its `attempts`
        # (and so TaskContext.attempt) from the worker_ids this appends to.
        owns_claim = await self.backend.result_store.amark_running(
            item.id, worker_id=self.worker_id, attempt=item.attempt
        )
        if not owns_claim:
            logger.info(
                "worker %s no longer owns task %s attempt %s",
                self.worker_id,
                item.id,
                item.attempt,
            )
            return
        try:
            result = await self.backend.aget_result(item.id)
        except Exception as exc:
            # e.g. the task's module or function no longer exists. Record it
            # on the row like any other failed attempt.
            logger.exception(
                "werker worker %s could not load task %s", self.worker_id, item.id
            )
            await self._handle_failure(item, exc)
            return
        task = result.task

        task_started.send(sender=type(self.backend), task_result=result)

        call_args = (
            (TaskContext(task_result=result), *result.args)
            if task.takes_context
            else tuple(result.args)
        )

        task_timeout = self._timeout_for(item.task_path)
        is_async = iscoroutinefunction(task.func)
        if is_async:
            execution = asyncio.ensure_future(
                _outcome(
                    self._call_async(item, task.func, call_args, result.kwargs, task_timeout)
                )
            )
        else:
            execution = asyncio.ensure_future(
                _outcome(
                    self._call_sync(
                        item,
                        functools.partial(
                            run_with_connection_cleanup, task.func, *call_args, **result.kwargs
                        ),
                        task_timeout,
                    )
                )
            )
        # Only an async task can be interrupted when cancelling is requested.
        heartbeat_sender = asyncio.ensure_future(
            self._heartbeat_sender(item, execution if is_async else None)
        )
        try:
            try:
                succeeded, raw_return_value = await execution
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if item.id in self._cancel_requested and not (
                    current and current.cancelling()
                ):
                    # The heartbeat sender cancelled it on request, not shutdown.
                    raise TaskCancelled("cancelled while running") from None
                raise
            if not succeeded:
                raise raw_return_value
        except asyncio.CancelledError:
            # Shutdown abandoned this task; the reaper reclaims it.
            raise
        except BaseException as exc:
            # Deliberately catch-all, including SystemExit/KeyboardInterrupt
            # raised by task code: any task exception is a failed attempt,
            # not a worker bug.
            await self._handle_failure(item, exc)
        else:
            owns_claim = await self.backend.result_store.amark_successful(
                item.id,
                worker_id=self.worker_id,
                attempt=item.attempt,
                return_value=normalize_json(raw_return_value),
            )
            if not owns_claim:
                logger.info(
                    "worker %s no longer owns task %s attempt %s",
                    self.worker_id,
                    item.id,
                    item.attempt,
                )
                return
            await self.backend.broker.aack(
                item.id, worker_id=self.worker_id, attempt=item.attempt
            )
        finally:
            heartbeat_sender.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_sender
            self._cancel_requested.discard(item.id)

        finished_result = await self.backend.aget_result(item.id)
        task_finished.send(sender=type(self.backend), task_result=finished_result)

    def _timeout_for(self, task_path: str) -> float | None:
        policy_timeout = registry.policy_for(task_path).timeout
        return policy_timeout if policy_timeout is not None else self.task_timeout

    async def _call_async(
        self,
        item: ClaimedItem,
        func: Callable[..., Coroutine[Any, Any, Any]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        task_timeout: float | None,
    ) -> Any:
        """Awaits an async task, cancelling it cleanly past its timeout."""
        deadline = asyncio.timeout(task_timeout)
        try:
            async with self._async_semaphore, deadline:
                return await func(*args, **kwargs)
        except TimeoutError as exc:
            if deadline.expired():
                raise TaskTimeout(f"{item.task_path} exceeded {task_timeout}s") from exc
            raise

    async def _call_sync(
        self, item: ClaimedItem, call: Callable[[], Any], task_timeout: float | None
    ) -> Any:
        """Runs a sync task in the thread pool. A thread can't be killed, so
        past its timeout the task is recorded as failed and the thread is
        abandoned. It keeps a pool slot until it ends, so _abandoned lowers
        the claim capacity until then."""
        thread_future = self._executor.submit(call)
        future = asyncio.wrap_future(thread_future)
        done, _ = await asyncio.wait({future}, timeout=task_timeout)
        if done:
            return future.result()

        loop = asyncio.get_running_loop()
        self._abandoned += 1
        logger.warning(
            "werker worker %s abandoned task %s (%s) after %ss timeout; its thread "
            "keeps running until the function returns",
            self.worker_id,
            item.id,
            item.task_path,
            task_timeout,
        )
        # Retrieve the eventual outcome so asyncio doesn't log it as unhandled.
        future.add_done_callback(lambda f: f.cancelled() or f.exception())
        thread_future.add_done_callback(lambda _: self._call_in_loop(loop, self._release_abandoned))
        raise TaskTimeout(f"{item.task_path} exceeded {task_timeout}s")

    def _release_abandoned(self) -> None:
        self._abandoned -= 1

    @staticmethod
    def _call_in_loop(loop: asyncio.AbstractEventLoop, callback: Callable[[], None]) -> None:
        # The loop may already be closed if the worker stopped meanwhile.
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(callback)

    async def _heartbeat_sender(
        self, item: ClaimedItem, interruptible: "asyncio.Future[Any] | None"
    ) -> None:
        """Keeps this exact claim attempt's heartbeat fresh, and notices a
        cancel request (werker.api.cancel). Cancelled as soon as execution
        finishes, see _run_claimed's finally block."""
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            try:
                cancel_requested = await self.backend.broker.aheartbeat(
                    item.id, worker_id=self.worker_id, attempt=item.attempt
                )
            except Exception:
                # Keep trying: a dead heartbeat sender would let the reaper
                # reclaim a task that is still running.
                logger.exception("werker worker %s heartbeat failed", self.worker_id)
                continue
            if cancel_requested and item.id not in self._cancel_requested:
                logger.info("werker worker %s cancelling task %s", self.worker_id, item.id)
                self._cancel_requested.add(item.id)
                if interruptible is not None:
                    interruptible.cancel()

    async def _handle_failure(self, item: ClaimedItem, exc: BaseException) -> None:
        exception_type = type(exc)
        error = {
            "exception_class_path": f"{exception_type.__module__}.{exception_type.__qualname__}",
            "traceback": "".join(traceback.format_exception(exc)),
        }
        policy = registry.policy_for(item.task_path)
        will_retry = retries_remaining(item.attempt, item.max_retries)
        if policy.retry_on is not None and not isinstance(exc, policy.retry_on):
            will_retry = False
        if item.id in self._cancel_requested:
            will_retry = False
        if (
            isinstance(exc, TaskTimeout)
            and registry.delivery_guarantee_for(item.task_path)
            == DeliveryGuarantee.AT_MOST_ONCE
        ):
            # An abandoned sync thread may still be running it.
            will_retry = False
        owns_claim = await self.backend.result_store.amark_failed(
            item.id,
            worker_id=self.worker_id,
            attempt=item.attempt,
            error=error,
            will_retry=will_retry,
        )
        if not owns_claim:
            return

        if will_retry:
            delay = self.retry_delay(item.task_path, item.attempt)
            retry_after = timezone.now() + timedelta(seconds=delay)
            await self.backend.broker.anack(
                item.id, worker_id=self.worker_id, attempt=item.attempt, retry_after=retry_after
            )
        else:
            await self.backend.broker.aack(
                item.id, worker_id=self.worker_id, attempt=item.attempt
            )


def _result_ttl(raw: Any) -> dict[str, timedelta]:
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ImproperlyConfigured(
            'TASKS OPTIONS["RESULT_TTL"] must be a mapping like {"SUCCESSFUL": "7d"}.'
        )
    try:
        return parse_result_ttl(raw)
    except (ValueError, TypeError) as exc:
        raise ImproperlyConfigured(f'TASKS OPTIONS["RESULT_TTL"]: {exc}') from exc


async def _outcome(awaitable: Awaitable[Any]) -> tuple[bool, Any]:
    """(True, value) or (False, exception). asyncio re-raises SystemExit and
    KeyboardInterrupt out of the event loop when they leave a Task, so task
    code raising them must be caught inside the Task that runs it."""
    try:
        return True, await awaitable
    except asyncio.CancelledError:
        raise
    except BaseException as exc:
        return False, exc
