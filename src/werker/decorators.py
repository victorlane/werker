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
