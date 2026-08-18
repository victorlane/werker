import uuid

from django.db import models


class TaskStatus(models.TextChoices):
    """Mirrors django.tasks.TaskResultStatus."""

    READY = "READY", "Ready"
    RUNNING = "RUNNING", "Running"
    SUCCESSFUL = "SUCCESSFUL", "Successful"
    FAILED = "FAILED", "Failed"


class ScheduleKind(models.TextChoices):
    CRON = "CRON", "Cron expression"
    INTERVAL = "INTERVAL", "Fixed interval"


class DeliveryGuarantee(models.TextChoices):
    """AT_LEAST_ONCE (default) retries a stale claim; task functions must be
    idempotent. AT_MOST_ONCE (opt-in via @werker.at_most_once) never retries,
    so it can lose a task instead of duplicating it. Neither is exactly-once."""

    AT_LEAST_ONCE = "AT_LEAST_ONCE", "At least once"
    AT_MOST_ONCE = "AT_MOST_ONCE", "At most once"


class DBTaskResult(models.Model):
    """Backs both the Broker and the ResultStore in werker's v1 Postgres
    implementation (see werker.broker for why they share one table)."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    task_path = models.CharField(max_length=255)
    args_json = models.JSONField(default=list)
    kwargs_json = models.JSONField(default=dict)

    queue_name = models.CharField(max_length=100, default="default", db_index=True)
    priority = models.SmallIntegerField(default=0)

    status = models.CharField(
        max_length=10, choices=TaskStatus.choices, default=TaskStatus.READY
    )
    run_after = models.DateTimeField(db_index=True)

    enqueued_at = models.DateTimeField()
    started_at = models.DateTimeField(null=True, blank=True)
    last_attempted_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    attempts = models.PositiveIntegerField(default=0)
    max_retries = models.PositiveIntegerField(default=0)
    delivery_guarantee = models.CharField(
        max_length=20,
        choices=DeliveryGuarantee.choices,
        default=DeliveryGuarantee.AT_LEAST_ONCE,
    )

    errors = models.JSONField(default=list, blank=True)
    return_value = models.JSONField(null=True, blank=True)
    worker_ids = models.JSONField(default=list, blank=True)

    claimed_by = models.CharField(max_length=255, blank=True, default="")
    claimed_at = models.DateTimeField(null=True, blank=True)
    last_heartbeat_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(
                fields=["status", "queue_name", "-priority", "run_after"],
                name="werker_claim_idx",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.task_path} [{self.status}] ({self.id})"


class PeriodicTask(models.Model):
    """Declared via werker's own @schedule decorator, synced into this table
    by WerkerConfig.ready() / `manage.py syncschedules`."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255, unique=True)

    task_path = models.CharField(max_length=255)
    args_json = models.JSONField(default=list)
    kwargs_json = models.JSONField(default=dict)

    schedule_kind = models.CharField(max_length=10, choices=ScheduleKind.choices)
    cron_expression = models.CharField(max_length=100, blank=True, default="")
    interval_seconds = models.PositiveIntegerField(null=True, blank=True)
    timezone = models.CharField(max_length=100, default="UTC")

    queue_name = models.CharField(max_length=100, default="default")
    priority = models.SmallIntegerField(default=0)

    enabled = models.BooleanField(default=True)
    catchup = models.BooleanField(
        default=False,
        help_text=(
            "If the worker was down past a fire time: False (default) skips to the next "
            "future occurrence, True fires once immediately for the missed occurrence."
        ),
    )

    next_run_at = models.DateTimeField(db_index=True)
    last_run_at = models.DateTimeField(null=True, blank=True)

    claimed_by = models.CharField(max_length=255, blank=True, default="")
    last_heartbeat_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["enabled", "next_run_at"], name="werker_schedule_idx"),
        ]

    def __str__(self) -> str:
        return self.name
