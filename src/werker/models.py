import uuid

from django.db import models


class TaskStatus(models.TextChoices):
    """Mirrors django.tasks.TaskResultStatus so DBTaskResult.status maps 1:1
    onto the values django.tasks.TaskResult.status is expected to return."""

    READY = "READY", "Ready"
    RUNNING = "RUNNING", "Running"
    SUCCESSFUL = "SUCCESSFUL", "Successful"
    FAILED = "FAILED", "Failed"


class ScheduleKind(models.TextChoices):
    CRON = "CRON", "Cron expression"
    INTERVAL = "INTERVAL", "Fixed interval"


class DeliveryGuarantee(models.TextChoices):
    """AT_LEAST_ONCE (default): the reaper (werker.worker.reaper) reclaims a
    stale RUNNING row and retries it — task functions must be idempotent,
    since a crash after the task body finished but before the result was
    written can cause a genuine second execution.

    AT_MOST_ONCE (opt-in via @werker.at_most_once): the reaper does NOT
    reclaim a stale RUNNING row under this guarantee — it marks it FAILED
    directly instead. This guarantees the task body never runs twice, at
    the cost of the task possibly never completing if the "stale" claim
    was actually a false positive (worker alive but slow/paused).

    Neither mode is true exactly-once (guaranteed-runs-once-AND-never-lost)
    — that isn't achievable without the task's own side effects cooperating
    via an idempotency key, which is a pattern task authors can layer on
    top of either mode, not something a backend can provide unilaterally.
    """

    AT_LEAST_ONCE = "AT_LEAST_ONCE", "At least once"
    AT_MOST_ONCE = "AT_MOST_ONCE", "At most once"


class DBTaskResult(models.Model):
    """Backs both the Broker (claim-relevant columns) and the ResultStore
    (outcome-relevant columns) in werker's v1 Postgres implementation. See
    the broker/results package docstrings for why these two concerns share
    one table in v1 despite having separate ABCs."""

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
    """Declared via the `@schedule(...)` decorator (werker's own addition —
    django.tasks itself has no scheduling concept) and synced into this
    table idempotently by WerkerConfig.ready() / `manage.py syncschedules`."""

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
