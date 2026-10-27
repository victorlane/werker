"""PostgresTaskBackend: the django.tasks BaseTaskBackend implementation.

Composes a Broker + ResultStore, resolved from TASKS["<alias>"]["OPTIONS"].
`enqueue`/`get_result` are the canonical sync methods, matching
BaseTaskBackend itself.

`aenqueue`/`aget_result` are explicitly overridden too, not left as
Django's inherited thread_sensitive=True default, so the entire async path
uses Broker/ResultStore's own dedicated executors. See werker.broker.

Uses `from __future__ import annotations`: Task/TaskResult are typed
Generic in django-stubs but aren't Generic at runtime, so subscripting
them needs deferred annotation evaluation.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, cast

from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import Task, TaskError, TaskResult, TaskResultStatus
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_enqueued
from django.utils import timezone
from django.utils.module_loading import import_string
from typing_extensions import ParamSpec, TypeVar

from werker import registry
from werker.broker import QueueItem
from werker.config import validate_options
from werker.models import UNIQUE_ACTIVE_CONSTRAINT, DBTaskResult, TaskStatus
from werker.schedules.registry import ScheduleDeclaration

_P = ParamSpec("_P")
_R = TypeVar("_R")

DEFAULT_BROKER_CLASS = "werker.broker.postgres.PostgresBroker"
DEFAULT_RESULT_STORE_CLASS = "werker.results.db.DBResultStore"
DEFAULT_MAX_RETRIES = 3
UNIQUE_KEY_MAX_LENGTH = 255


class PostgresTaskBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = True
    supports_get_result = True
    supports_priority = True

    def __init__(self, alias: str, params: dict[str, Any]) -> None:
        super().__init__(alias, params)
        validate_options(self.options)
        broker_cls = import_string(self.options.get("BROKER_CLASS", DEFAULT_BROKER_CLASS))
        result_store_cls = import_string(
            self.options.get("RESULT_STORE_CLASS", DEFAULT_RESULT_STORE_CLASS)
        )
        self.broker = broker_cls(self.options)
        self.result_store = result_store_cls(self.options)

    def _max_retries_for(self, task_path: str) -> int:
        policy_max_retries = registry.policy_for(task_path).max_retries
        if policy_max_retries is not None:
            return policy_max_retries
        return int(self.options.get("MAX_RETRIES", DEFAULT_MAX_RETRIES))

    def enqueue_schedule(self, declaration: ScheduleDeclaration) -> str:
        """Enqueues one due periodic-task occurrence, mirroring enqueue() but
        seeded from a ScheduleDeclaration. Queues even if the underlying task
        isn't imported yet, since DBTaskResult stores task_path as a string."""
        now = timezone.now()
        result_id = str(uuid.uuid4())
        result_id, _ = self._write_enqueue(
            QueueItem(
                id=result_id,
                queue_name=declaration.queue_name,
                priority=declaration.priority,
                run_after=now,
            ),
            {
                "id": result_id,
                "task_path": declaration.task_path,
                "args": list(declaration.args),
                "kwargs": dict(declaration.kwargs),
                "queue_name": declaration.queue_name,
                "priority": declaration.priority,
                "run_after": now,
                "enqueued_at": now,
                "max_retries": self._max_retries_for(declaration.task_path),
                "delivery_guarantee": registry.delivery_guarantee_for(
                    declaration.task_path
                ),
                "unique_key": unique_key_for(
                    declaration.task_path, declaration.args, declaration.kwargs
                ),
            },
        )
        return result_id

    def _enqueue_plan(
        self, task: Task[_P, _R], args: list[Any], kwargs: dict[str, Any]
    ) -> tuple[str, QueueItem, dict[str, Any]]:
        """Shared by enqueue/aenqueue: everything about *what* to write that
        doesn't itself touch the DB, so neither copy has to duplicate it."""
        self.validate_task(task)
        result_id = str(uuid.uuid4())
        now = timezone.now()
        run_after = task.run_after or now
        queue_item = QueueItem(
            id=result_id, queue_name=task.queue_name, priority=task.priority, run_after=run_after
        )
        create_kwargs = {
            "id": result_id,
            "task_path": task.module_path,
            "args": args,
            "kwargs": kwargs,
            "queue_name": task.queue_name,
            "priority": task.priority,
            "run_after": run_after,
            "enqueued_at": now,
            "max_retries": self._max_retries_for(task.module_path),
            "delivery_guarantee": registry.delivery_guarantee_for(task.module_path),
            "unique_key": unique_key_for(task.module_path, args, kwargs),
        }
        return result_id, queue_item, create_kwargs

    def _write_enqueue(
        self, queue_item: QueueItem, create_kwargs: dict[str, Any]
    ) -> tuple[str, bool]:
        """create + enqueue in one transaction: a failed broker.enqueue must
        not leave a READY row with nothing pointing at it.

        Returns (result id, created). For a @unique task that already has a
        READY or RUNNING row with the same key, nothing is written and that
        row's id is returned with created=False."""
        unique_key = create_kwargs.get("unique_key")
        # Twice: the conflicting row can finish between our failed insert
        # and the lookup, which frees the key.
        for _ in range(2):
            try:
                with transaction.atomic():
                    self.result_store.create(**create_kwargs)
                    self.broker.enqueue(queue_item)
                return queue_item.id, True
            except IntegrityError as exc:
                if unique_key is None or UNIQUE_ACTIVE_CONSTRAINT not in str(exc):
                    raise
            existing = (
                DBTaskResult.objects.filter(
                    unique_key=unique_key, status__in=[TaskStatus.READY, TaskStatus.RUNNING]
                )
                .values_list("id", flat=True)
                .first()
            )
            if existing is not None:
                return str(existing), False
        raise IntegrityError(f"could not enqueue unique task {unique_key!r}")

    def enqueue(
        self, task: Task[_P, _R], args: list[Any], kwargs: dict[str, Any]
    ) -> TaskResult[_P, _R]:
        result_id, queue_item, create_kwargs = self._enqueue_plan(task, args, kwargs)
        result_id, created = self._write_enqueue(queue_item, create_kwargs)

        result = self.get_result(result_id)
        if created:
            task_enqueued.send(type(self), task_result=result)
        # get_result resolves Task generically, so its return type is
        # narrower than what we actually know here from `task`.
        return cast("TaskResult[_P, _R]", result)

    async def aenqueue(
        self, task: Task[_P, _R], args: list[Any], kwargs: dict[str, Any]
    ) -> TaskResult[_P, _R]:
        result_id, queue_item, create_kwargs = self._enqueue_plan(task, args, kwargs)
        # Run the sync create+enqueue block on the ResultStore's dedicated
        # executor so both land in one transaction, same as enqueue().
        result_id, created = await self.result_store._run_sync(
            self._write_enqueue, queue_item, create_kwargs
        )

        result = await self.aget_result(result_id)
        if created:
            task_enqueued.send(type(self), task_result=result)
        return cast("TaskResult[_P, _R]", result)

    def get_result(self, result_id: str) -> TaskResult[..., Any]:
        try:
            row = self.result_store.get(result_id)
        except (DBTaskResult.DoesNotExist, ValidationError, ValueError):
            raise TaskResultDoesNotExist(result_id) from None
        return _to_task_result(row, backend_alias=self.alias)

    async def aget_result(self, result_id: str) -> TaskResult[..., Any]:
        try:
            row = await self.result_store.aget(result_id)
        except (DBTaskResult.DoesNotExist, ValidationError, ValueError):
            raise TaskResultDoesNotExist(result_id) from None
        return _to_task_result(row, backend_alias=self.alias)


def _to_task_result(row: DBTaskResult, *, backend_alias: str) -> TaskResult[..., Any]:
    task = import_string(row.task_path)
    result: TaskResult[..., Any] = TaskResult(
        task=task,
        id=str(row.id),
        status=TaskResultStatus(row.status),
        enqueued_at=row.enqueued_at,
        started_at=row.started_at,
        last_attempted_at=row.last_attempted_at,
        finished_at=row.finished_at,
        args=row.args_json,
        kwargs=row.kwargs_json,
        backend=backend_alias,
        errors=[TaskError(**error) for error in row.errors],
        worker_ids=list(row.worker_ids),
    )
    if row.status == TaskStatus.SUCCESSFUL:
        object.__setattr__(result, "_return_value", row.return_value)
    return result


def unique_key_for(task_path: str, args: Sequence[Any], kwargs: Mapping[str, Any]) -> str | None:
    """The dedupe key for a @unique task, or None. Default: a hash of the
    arguments. A custom key function gets the task's arguments."""
    unique = registry.policy_for(task_path).unique
    if not unique:
        return None
    if callable(unique):
        raw = str(unique(*args, **kwargs))
    else:
        raw = json.dumps([list(args), dict(kwargs)], sort_keys=True, default=str)
    key = f"{task_path}:{raw}"
    if callable(unique) and len(key) <= UNIQUE_KEY_MAX_LENGTH:
        return key  # readable in the admin
    return f"{task_path}:{hashlib.sha256(raw.encode()).hexdigest()}"[:UNIQUE_KEY_MAX_LENGTH]
