"""PostgresTaskBackend: the django.tasks BaseTaskBackend implementation.

Composes a Broker + ResultStore (+, from phase 7, a ScheduleStore), all
resolved from TASKS["<alias>"]["OPTIONS"]. `enqueue`/`get_result` are the
canonical sync methods, matching django.tasks.backends.base.BaseTaskBackend
itself (its own `enqueue` is the abstract sync method; `aenqueue` is a
sync_to_async(thread_sensitive=True) wrapper Django provides for free) —
so this backend only needs to override the sync methods; the inherited
async wrappers are already correct and are not re-implemented here.

Settings resolution here is intentionally the plain dict.get(...) that
Django's own BaseTaskBackend.__init__ already sets up as self.options —
the DRF-api_settings-style lazy PgOptions wrapper (validation, IMPORT_STRINGS,
defaults) is phase 9's job, once the real set of needed options is known.
"""

import uuid

from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import TaskError, TaskResult, TaskResultStatus
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_enqueued
from django.utils import timezone
from django.utils.module_loading import import_string

from werker import registry
from werker.broker import QueueItem
from werker.models import DBTaskResult, TaskStatus

DEFAULT_BROKER_CLASS = "werker.broker.postgres.PostgresBroker"
DEFAULT_RESULT_STORE_CLASS = "werker.results.db.DBResultStore"
DEFAULT_MAX_RETRIES = 3


class PostgresTaskBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = True
    supports_get_result = True
    supports_priority = True

    def __init__(self, alias, params):
        super().__init__(alias, params)
        broker_cls = import_string(self.options.get("BROKER_CLASS", DEFAULT_BROKER_CLASS))
        result_store_cls = import_string(
            self.options.get("RESULT_STORE_CLASS", DEFAULT_RESULT_STORE_CLASS)
        )
        self.broker = broker_cls(self.options)
        self.result_store = result_store_cls(self.options)

    def enqueue(self, task, args, kwargs):
        self.validate_task(task)

        result_id = str(uuid.uuid4())
        now = timezone.now()
        run_after = task.run_after or now

        self.result_store.create(
            id=result_id,
            task_path=task.module_path,
            args=args,
            kwargs=kwargs,
            queue_name=task.queue_name,
            priority=task.priority,
            run_after=run_after,
            enqueued_at=now,
            max_retries=self.options.get("MAX_RETRIES", DEFAULT_MAX_RETRIES),
            delivery_guarantee=registry.delivery_guarantee_for(task.module_path),
        )
        self.broker.enqueue(
            QueueItem(
                id=result_id,
                queue_name=task.queue_name,
                priority=task.priority,
                run_after=run_after,
            )
        )

        result = self.get_result(result_id)
        task_enqueued.send(type(self), task_result=result)
        return result

    def get_result(self, result_id):
        try:
            row = self.result_store.get(result_id)
        except DBTaskResult.DoesNotExist:
            raise TaskResultDoesNotExist(result_id) from None
        return _to_task_result(row, backend_alias=self.alias)


def _to_task_result(row: DBTaskResult, *, backend_alias: str) -> TaskResult:
    task = import_string(row.task_path)
    result = TaskResult(
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
