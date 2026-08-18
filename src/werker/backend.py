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

import uuid
from typing import Any, cast

from django.core.exceptions import ValidationError
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import Task, TaskError, TaskResult, TaskResultStatus
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_enqueued
from django.utils import timezone
from django.utils.module_loading import import_string
from typing_extensions import ParamSpec, TypeVar

from werker import registry
from werker.broker import QueueItem
from werker.models import DBTaskResult, TaskStatus

_P = ParamSpec("_P")
_R = TypeVar("_R")

DEFAULT_BROKER_CLASS = "werker.broker.postgres.PostgresBroker"
DEFAULT_RESULT_STORE_CLASS = "werker.results.db.DBResultStore"
DEFAULT_MAX_RETRIES = 3


class PostgresTaskBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = True
    supports_get_result = True
    supports_priority = True

    def __init__(self, alias: str, params: dict[str, Any]) -> None:
        super().__init__(alias, params)
        broker_cls = import_string(self.options.get("BROKER_CLASS", DEFAULT_BROKER_CLASS))
        result_store_cls = import_string(
            self.options.get("RESULT_STORE_CLASS", DEFAULT_RESULT_STORE_CLASS)
        )
        self.broker = broker_cls(self.options)
        self.result_store = result_store_cls(self.options)

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
            "max_retries": self.options.get("MAX_RETRIES", DEFAULT_MAX_RETRIES),
            "delivery_guarantee": registry.delivery_guarantee_for(task.module_path),
        }
        return result_id, queue_item, create_kwargs

    def enqueue(
        self, task: Task[_P, _R], args: list[Any], kwargs: dict[str, Any]
    ) -> TaskResult[_P, _R]:
        result_id, queue_item, create_kwargs = self._enqueue_plan(task, args, kwargs)
        self.result_store.create(**create_kwargs)
        self.broker.enqueue(queue_item)

        result = self.get_result(result_id)
        task_enqueued.send(type(self), task_result=result)
        # get_result resolves Task generically, so its return type is
        # narrower than what we actually know here from `task`.
        return cast("TaskResult[_P, _R]", result)

    async def aenqueue(
        self, task: Task[_P, _R], args: list[Any], kwargs: dict[str, Any]
    ) -> TaskResult[_P, _R]:
        result_id, queue_item, create_kwargs = self._enqueue_plan(task, args, kwargs)
        await self.result_store.acreate(**create_kwargs)
        await self.broker.aenqueue(queue_item)

        result = await self.aget_result(result_id)
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
