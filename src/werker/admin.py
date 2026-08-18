from django.contrib import admin

from werker.models import DBTaskResult, PeriodicTask


@admin.register(DBTaskResult)
class DBTaskResultAdmin(admin.ModelAdmin):
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


@admin.register(PeriodicTask)
class PeriodicTaskAdmin(admin.ModelAdmin):
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
