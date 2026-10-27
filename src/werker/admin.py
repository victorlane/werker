from django.contrib import admin, messages
from django.db.models import QuerySet
from django.http import HttpRequest

from werker import api
from werker.models import DBTaskResult, PeriodicTask


@admin.register(DBTaskResult)
class DBTaskResultAdmin(admin.ModelAdmin):  # type: ignore[type-arg]
    list_display = (
        "id",
        "task_path",
        "status",
        "queue_name",
        "priority",
        "attempts",
        "run_after",
        "claimed_by",
    )
    list_filter = ("status", "queue_name")
    search_fields = ("task_path", "id")
    readonly_fields = [f.name for f in DBTaskResult._meta.fields]
    actions = ["cancel_tasks", "requeue_tasks"]

    @admin.action(description="Cancel selected tasks")
    def cancel_tasks(self, request: HttpRequest, queryset: QuerySet[DBTaskResult]) -> None:
        done = sum(api.cancel(str(row.id)) for row in queryset)
        self._report(request, done, queryset.count(), "cancelled", "had already finished")

    @admin.action(description="Requeue selected failed tasks")
    def requeue_tasks(self, request: HttpRequest, queryset: QuerySet[DBTaskResult]) -> None:
        done = sum(api.requeue(str(row.id)) for row in queryset)
        self._report(request, done, queryset.count(), "requeued", "were not FAILED")

    def _report(
        self, request: HttpRequest, done: int, total: int, verb: str, skipped_reason: str
    ) -> None:
        self.message_user(request, f"{done} task(s) {verb}.", messages.SUCCESS)
        if total > done:
            self.message_user(
                request, f"{total - done} task(s) skipped: they {skipped_reason}.", messages.WARNING
            )


@admin.register(PeriodicTask)
class PeriodicTaskAdmin(admin.ModelAdmin):  # type: ignore[type-arg]
    list_display = (
        "name",
        "task_path",
        "schedule_kind",
        "enabled",
        "next_run_at",
        "last_run_at",
    )
    list_filter = ("enabled", "schedule_kind")
    search_fields = ("name", "task_path")
