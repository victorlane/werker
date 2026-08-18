"""PostgresTaskBackend: the django.tasks BaseTaskBackend implementation.

Phase 1 stub — models only so far. Wired up to django.tasks in phase 3.
"""

from django.tasks.backends.base import BaseTaskBackend


class PostgresTaskBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = True
    supports_get_result = True
    supports_priority = True

    def __init__(self, alias, params):
        super().__init__(alias, params)
        # Broker / ResultStore / ScheduleStore wiring lands in phase 2-3.

    def enqueue(self, task, args, kwargs):
        raise NotImplementedError("PostgresTaskBackend.enqueue lands in phase 3")

    async def aenqueue(self, task, args, kwargs):
        raise NotImplementedError("PostgresTaskBackend.aenqueue lands in phase 3")
