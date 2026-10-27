"""Per-task options for werker. Each decorator takes an already-built Task,
so it goes outside @task, and records the option in werker.registry:

    @retry(max_retries=5, on=(SMTPException,))
    @task
    def send_mail(...): ...
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from django.core.exceptions import ImproperlyConfigured
from django.tasks.base import Task

from werker import registry

type TaskDecorator = Callable[[Task[..., Any]], Task[..., Any]]


def at_most_once(task: Task[..., Any]) -> Task[..., Any]:
    """Opt a @task into AT_MOST_ONCE delivery: the reaper never retries a
    stale claim for this task. See werker.models.DeliveryGuarantee.

    Usage:
        @at_most_once
        @task
        def charge_customer(...): ...
    """
    registry.mark_at_most_once(task.module_path)
    return task


def retry(
    *,
    max_retries: int | None = None,
    on: type[BaseException] | tuple[type[BaseException], ...] | None = None,
    backoff_base: float | None = None,
    backoff_max: float | None = None,
    jitter: bool | None = None,
) -> TaskDecorator:
    """Overrides the backend's retry options for one task. `on` limits
    retries to these exception types; anything else fails immediately.
    Unset arguments fall back to the backend's OPTIONS."""
    if max_retries is not None and (
        not isinstance(max_retries, int) or isinstance(max_retries, bool) or max_retries < 0
    ):
        raise ImproperlyConfigured("retry(max_retries=...) must be zero or greater.")
    for name, value in (("backoff_base", backoff_base), ("backoff_max", backoff_max)):
        if value is not None and (isinstance(value, bool) or value <= 0):
            raise ImproperlyConfigured(f"retry({name}=...) must be greater than zero.")
    retry_on = (on,) if isinstance(on, type) else on
    if retry_on is not None and not all(
        isinstance(exc, type) and issubclass(exc, BaseException) for exc in retry_on
    ):
        raise ImproperlyConfigured("retry(on=...) must be exception classes.")

    def decorate(task: Task[..., Any]) -> Task[..., Any]:
        changes: dict[str, Any] = {
            "max_retries": max_retries,
            "retry_on": retry_on,
            "backoff_base": backoff_base,
            "backoff_max": backoff_max,
            "jitter": jitter,
        }
        registry.set_policy(
            task.module_path, **{k: v for k, v in changes.items() if v is not None}
        )
        return task

    return decorate


def timeout(seconds: float) -> TaskDecorator:
    """Fails an attempt that runs longer than `seconds`, then retries it per
    the retry policy. Overrides the backend's TASK_TIMEOUT option.

    An async task is cancelled cleanly. A sync task's thread can't be
    killed: it is abandoned and keeps running until the function returns,
    so a retry can overlap with it. An at-most-once task is never retried
    after a timeout for that reason."""
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or seconds <= 0:
        raise ImproperlyConfigured("timeout(seconds=...) must be greater than zero.")

    def decorate(task: Task[..., Any]) -> Task[..., Any]:
        registry.set_policy(task.module_path, timeout=float(seconds))
        return task

    return decorate


def unique(
    task: Task[..., Any] | None = None, *, key: Callable[..., Any] | None = None
) -> Any:
    """Prevents duplicate queued work: while a task with the same key is
    READY or RUNNING (including waiting for a retry), enqueueing it again
    returns the existing result instead of adding a row. Once it has
    finished, it can be enqueued again.

    The key defaults to the task's arguments. `key` receives the task's
    arguments and returns the key to dedupe on:

        @unique
        @task
        def rebuild_index(): ...

        @unique(key=lambda user_id, **_: user_id)
        @task
        def sync_user(user_id, force=False): ...
    """
    if key is not None and not callable(key):
        raise ImproperlyConfigured("unique(key=...) must be callable.")

    def decorate(task: Task[..., Any]) -> Task[..., Any]:
        registry.set_policy(task.module_path, unique=key or True)
        return task

    return decorate(task) if task is not None else decorate
