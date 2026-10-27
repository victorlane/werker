# Werker

Werker is a self-contained, PostgreSQL-backed task queue and periodic scheduler for Django's native `django.tasks` framework. It stores queued work and results in your Django database: no Redis and no separate broker service. **PostgreSQL is required**; queue claims rely on `SELECT ... FOR UPDATE SKIP LOCKED`.

> **Status:** `0.1.0.dev0` is an early development preview. Review its delivery guarantees and run it against a supported PostgreSQL deployment before using it for critical work.

## Install and configure

Werker requires Python 3.12+, Django 6.0+, and PostgreSQL. Install it and add the application plus Django task backend configuration:

```bash
python -m pip install 'werker[postgres]'
```

```python
# settings.py
INSTALLED_APPS = [
    # ...
    "werker",
]

TASKS = {
    "default": {
        "BACKEND": "werker.backend.PostgresTaskBackend",
        "OPTIONS": {},
    },
}
```

Apply the migrations, then run a worker as a separately supervised process in production:

```bash
python manage.py migrate
python manage.py taskworker
```

The worker's database role needs normal read/write access to Werker's tables. Do not put secrets in task arguments, return values, or exception messages: they are persisted in PostgreSQL.

## Enqueue and inspect work

Use Django's `@task` API normally. Task modules are discovered from installed apps during startup, but explicitly importing tasks in code remains fine.

```python
# myapp/tasks.py
from django.tasks import task

@task
def send_welcome_email(user_id: int) -> str:
    # Send the message...
    return "sent"
```

```python
from myapp.tasks import send_welcome_email

result = send_welcome_email.enqueue(user_id=42)
print(result.id, result.status)  # READY, RUNNING, SUCCESSFUL, or FAILED

# From async code:
result = await send_welcome_email.aenqueue(user_id=42)
await result.arefresh()
```

`TaskResult` exposes Django's normal timestamps, errors, status, and, after a successful refresh, `return_value`. To wait without a blocking worker API, poll with a timeout appropriate to the request or job:

```python
import time

result = send_welcome_email.enqueue(user_id=42)
while not result.is_finished:
    time.sleep(0.5)
    result.refresh()
print(result.return_value)
```

Avoid waiting inside web requests for long-running work; return the result ID and inspect it asynchronously instead.

## Worker operation

```text
python manage.py taskworker [--backend ALIAS] [--queues first,second] [--once] [--health-file PATH]
```

- `--backend` selects the `TASKS` alias (default: `default`).
- `--queues` restricts claims to comma-separated queues; by default the backend's configured queues are used.
- `--health-file` touches a file on every pass of the worker loop, for liveness probes (see [Health checks](#health-checks)).
- `--once` drains work that is due *at invocation time* and exits. It does not run periodic scheduling or stale-claim recovery; use it for checks, not a long-running deployment.

Run at least one continuously running `taskworker` process. Multiple workers may share queues and schedules safely; PostgreSQL row locking prevents two workers from claiming the same ready task. Use your service manager (systemd, Kubernetes, etc.) to restart workers after failure and deploy schedule changes with `syncschedules` (below).

During `python manage.py runserver`, a daemon worker starts automatically for local development. Set `WERKER_AUTOSTART = False` to disable it. This is not a production process-management strategy.

### Instant pickup with LISTEN/NOTIFY

Enqueueing a task that is due now sends a Postgres `NOTIFY` inside the enqueue transaction, so it is delivered only when that transaction commits. Each worker holds one extra connection that `LISTEN`s and wakes the worker straight away, instead of waiting up to `POLL_INTERVAL`.

Polling stays on as a safety net, at least every 5 seconds while listening: it picks up delayed tasks (`run_after`) and anything enqueued while the listener was reconnecting. If `psycopg` is missing or the `LISTEN` connection fails, the worker logs a warning and keeps polling at `POLL_INTERVAL`.

`LISTEN` doesn't work through pgbouncer in transaction mode. Add a database alias with a direct connection and set `LISTEN_DATABASE` to it, or set `LISTEN_NOTIFY` to `False`.

### Health checks

Every worker keeps a row in the `WorkerRecord` table, refreshed every `HEARTBEAT_INTERVAL` and marked stopped on a graceful shutdown. The admin lists workers with an "alive" column.

`werker_health` exits 0 when healthy and 1 otherwise:

```bash
python manage.py werker_health --file /tmp/werker-alive --max-age 60  # no database query
python manage.py werker_health --db [--hostname HOST] [--max-age SECONDS]
```

`--file` checks the file written by `taskworker --health-file`. Because the worker touches it on every loop pass, a stale file also means a stuck worker, not only a dead one. `--db` checks that at least one worker sent a heartbeat recently (default: within 3 × `HEARTBEAT_INTERVAL`).

Kubernetes example:

```yaml
command: ["python", "manage.py", "taskworker", "--health-file", "/tmp/werker-alive"]
livenessProbe:
  exec:
    command: ["python", "manage.py", "werker_health", "--file", "/tmp/werker-alive", "--max-age", "60"]
  periodSeconds: 30
```

### Worker `OPTIONS`

Put options under `TASKS[alias]["OPTIONS"]`. All numeric options are validated when the backend/worker starts; positive values must be greater than zero, except the explicitly non-negative values below.

| Option | Default | Meaning |
| --- | ---: | --- |
| `CONCURRENCY` | `8` | Maximum synchronous `def` task executions, backed by a thread pool. |
| `MAX_ASYNC_CONCURRENCY` | `200` | Maximum native `async def` task executions. |
| `CLAIM_BATCH_SIZE` | `10` | Maximum rows claimed in one queue, reaper, or scheduler operation. |
| `POLL_INTERVAL` | `1.0` seconds | Delay before polling for more ready work. |
| `MAX_RETRIES` | `3` | Retry budget written to each newly enqueued task. See retry semantics below. `0` disables retries. |
| `RETRY_BACKOFF_BASE` | `2.0` seconds | Exponential retry base: an attempt numbered `n` waits `base ** n`. |
| `RETRY_BACKOFF_MAX` | `300` seconds | Maximum failure retry delay. |
| `LISTEN_NOTIFY` | `True` | Wake idle workers with Postgres `LISTEN/NOTIFY` as soon as a task is enqueued. See [Instant pickup](#instant-pickup-with-listennotify). |
| `LISTEN_DATABASE` | the task table's database | Database alias for the `LISTEN` connection. Point it at a direct connection when the main one goes through pgbouncer in transaction mode. |
| `RESULT_TTL` | `None` (keep everything) | Automatic cleanup: how long to keep finished tasks per status, e.g. `{"SUCCESSFUL": "7d", "FAILED": "30d"}`. See [Retention](#retention). |
| `CLEANUP_INTERVAL` | `3600` seconds | How often a worker runs the `RESULT_TTL` cleanup. |
| `HEALTH_FILE` | `None` | Path the worker touches on every loop pass. `taskworker --health-file` overrides it. |
| `TASK_TIMEOUT` | `None` (no limit) | Seconds an attempt may run before it fails with `werker.exceptions.TaskTimeout`. See [Timeouts](#timeouts). |
| `RETRY_JITTER` | `True` | Randomise each retry delay to between half and all of the computed delay, so tasks that failed together don't retry together. |
| `HEARTBEAT_INTERVAL` | `15` seconds | How often a running task renews its lease. |
| `STALE_RUNNING_TIMEOUT` | `300` seconds | A RUNNING task with an older heartbeat can be reclaimed. |
| `REAPER_POLL_INTERVAL` | `30` seconds | Stale-claim recovery polling interval. |
| `SCHEDULER_POLL_INTERVAL` | `30` seconds | Periodic-schedule polling interval. |
| `SHUTDOWN_GRACE_PERIOD` | `30` seconds | Time to drain in-flight work after `SIGTERM`/`SIGINT`; `0` is allowed. |
| `BOOKKEEPING_CONCURRENCY` | `4` | Dedicated thread-pool size for broker/result-store database bookkeeping. |
| `STALE_SCHEDULE_CLAIM_TIMEOUT` | `STALE_RUNNING_TIMEOUT` | Positive scheduler dispatch-lease staleness timeout. This option is consumed by the PostgreSQL broker. |
| `BROKER_CLASS` | `werker.broker.postgres.PostgresBroker` | Dotted custom broker class path. |
| `RESULT_STORE_CLASS` | `werker.results.db.DBResultStore` | Dotted custom result-store class path. |

For example:

```python
TASKS = {"default": {"BACKEND": "werker.backend.PostgresTaskBackend", "OPTIONS": {
    "CONCURRENCY": 16,
    "MAX_RETRIES": 5,
    "RETRY_BACKOFF_BASE": 2,
    "RETRY_BACKOFF_MAX": 600,
    "STALE_RUNNING_TIMEOUT": 600,
}}}
```

## Retries and delivery guarantees

An exception from a task is recorded (including its traceback). While retry budget remains, the task becomes READY again with an exponential delay; after the budget is exhausted, it is FAILED. A worker that crashes or is unable to finish before its heartbeat becomes stale is handled by the reaper similarly.

`MAX_RETRIES=n` allows `n` retries, so `n + 1` attempts in total. The budget is stored on each task when it is enqueued, so changing the setting later doesn't affect tasks already in the queue.

Override the retry options for a single task with `@retry`, outside `@task`. `on` limits retries to the listed exception types; any other exception fails the task immediately:

```python
from smtplib import SMTPException
from django.tasks import task
from werker.decorators import retry

@retry(max_retries=5, on=(SMTPException,), backoff_base=3, backoff_max=600)
@task
def send_invoice(invoice_id: int) -> None:
    ...
```

Arguments you leave out fall back to the backend `OPTIONS`.

The default is **at-least-once** delivery. A stale task with retries remaining may execute again, so task functions must be idempotent: use durable idempotency keys, unique constraints, transactional outboxes, or provider-side deduplication for externally visible effects. Werker does **not** provide exactly-once execution.

For an operation where avoiding duplicates is more important than eventual execution, opt into **at-most-once** delivery:

```python
from django.tasks import task
from werker.decorators import at_most_once

@at_most_once
@task
def charge_customer(payment_id: str) -> None:
    ...
```

An at-most-once task with a stale claim is failed rather than reclaimed. It can therefore be lost on a worker crash. The decorator must be outside `@task`.

## Duplicate prevention

`@unique` stops the same work from being queued twice. While a task with the same key is READY or RUNNING (including while it waits for a retry), enqueueing it again writes nothing and returns the existing result. Once that task has finished, it can be enqueued again.

```python
from django.tasks import task
from werker.decorators import unique

@unique  # key: the task's arguments
@task
def rebuild_search_index(section: str) -> None:
    ...

@unique(key=lambda user_id, **_: user_id)  # key: only user_id
@task
def sync_user(user_id: int, force: bool = False) -> None:
    ...
```

A Postgres partial unique index enforces this, so it also holds for concurrent enqueues from different processes. The `task_enqueued` signal is only sent when a new row is created. A periodic task with `@unique` doesn't pile up when a run takes longer than its interval. `requeue` returns `False` for a failed `@unique` task whose key is already queued again.

## Timeouts

Set a time limit for one task with `@timeout`, or for all tasks with the `TASK_TIMEOUT` option. An attempt that runs longer fails with `werker.exceptions.TaskTimeout` and is retried like any other failure:

```python
from django.tasks import task
from werker.decorators import timeout

@timeout(30)
@task
def render_report(report_id: int) -> None:
    ...
```

- **Async tasks** are cancelled cleanly at the deadline.
- **Sync tasks** run in a thread, and Python can't kill a thread. The attempt is recorded as failed, but the thread keeps running until the function returns. It keeps its worker slot until then, so the worker claims less new work. A retry can overlap with the abandoned thread, so sync tasks with a timeout must be idempotent.
- **At-most-once tasks** are never retried after a timeout, because the first attempt may still be running.

## Periodic schedules

Wrap a Django `Task` with `@schedule`, outside `@task`. A declaration is copied to the database by `syncschedules`, then every running worker participates in dispatching due schedules.

```python
from datetime import timedelta
from django.tasks import task
from werker.schedules.decorators import schedule

@schedule(every=timedelta(hours=1), name="reports.hourly")
@task
def hourly_report() -> None:
    ...

@schedule(cron="0 3 * * *", timezone="Europe/Amsterdam", catchup=True)
@task
def nightly_report() -> None:
    ...
```

`every` accepts a positive `timedelta` or a whole number of seconds. `cron` accepts a croniter-valid five-part expression. `timezone` must be an IANA zone name and defaults to `UTC`. `name` defaults to the task's module path and must be unique across schedules. Scheduled invocations use the task's queue and priority.

After every deploy that adds, changes, or removes `@schedule` declarations:

```bash
python manage.py syncschedules --all
```

`--all` imports `<app>.tasks` for installed apps before synchronization. Synchronization is idempotent: it creates/updates declared rows, re-enables returned declarations, and disables rows for declarations that disappeared. It does not delete historical rows. A normal `taskworker` is required to fire due schedules; `taskworker --once` intentionally does not run the scheduler.

With `catchup=False` (the default), fully missed occurrences are skipped and the next future occurrence is used. With `catchup=True`, one overdue occurrence is enqueued after recovery; it does not backfill every missed interval.

## Results, admin, and retention

Task rows contain arguments, results, recorded errors, lifecycle timestamps, claim ownership, and attempt history. Django admin registers task results as read-only and exposes periodic tasks for operational inspection. Treat both as sensitive operational data and restrict admin access accordingly.

### Cancel and requeue

```python
from werker.api import cancel, requeue  # async: acancel, arequeue

cancel(result.id)   # False if the task had already finished
requeue(result.id)  # FAILED tasks only; False otherwise
```

Django's task API has no "cancelled" status, so a cancelled task becomes FAILED with a `werker.exceptions.TaskCancelled` error.

- **READY** tasks are cancelled immediately and never run.
- **RUNNING** tasks are cancelled at the worker's next heartbeat (`HEARTBEAT_INTERVAL`). An async task is interrupted. A sync task can't be interrupted: it runs to the end and keeps its result, but is never retried.
- **Requeue** runs a FAILED task again now, with a fresh retry budget (`reset_attempts=False` keeps the old count). Its error history is kept.

The task result admin has "Cancel selected tasks" and "Requeue selected failed tasks" actions.

### Retention

Finished task rows are kept until you delete them. Set `RESULT_TTL` to let the workers do it:

```python
"OPTIONS": {"RESULT_TTL": {"SUCCESSFUL": "7d", "FAILED": "30d"}}
```

Every `CLEANUP_INTERVAL`, one worker (a Postgres advisory lock makes sure it's only one) deletes SUCCESSFUL and FAILED rows whose `finished_at` is older than their TTL, in batches. Leave a status out to keep those rows. READY and RUNNING rows are never deleted. The same pass forgets worker records not seen for 7 days.

To prune by hand or from your own scheduler instead, use the management command. READY and RUNNING rows are never deleted by it either:

```bash
python manage.py prune_task_results --older-than=30d      # default retention
python manage.py prune_task_results --dry-run             # preview count
python manage.py prune_task_results --status successful   # successful or failed
python manage.py prune_task_results --batch-size 1000
```

Durations support `d`, `h`, `m`, `s`, or bare hours (`30d`, `12h`, `90m`, `60s`, `72`). Run pruning from a scheduled system job and choose retention to meet your audit, privacy, and capacity requirements.

## Observability and troubleshooting

The `werker.worker` logger records worker lifecycle, stale-claim reclamation, schedule dispatches, and scheduler exceptions. Send it to your normal structured logging/alerting pipeline. Monitor worker availability, queue age, READY/RUNNING counts, FAILED counts, retries, stale reclaims, scheduler dispatch failures, and database connection saturation.

If work is not running, check: migrations are applied; a `taskworker` is alive; the selected backend alias and queues match the task; `run_after` is due; and workers can reach PostgreSQL. If scheduled work is absent, run `syncschedules --all` after verifying task-module imports and inspect the corresponding `PeriodicTask` row in admin. Do not manually change claim fields while a worker is running; restart workers or allow stale leases to recover.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup and checks. Security reporting and data-handling guidance are in [SECURITY.md](SECURITY.md). Changes are tracked in [CHANGELOG.md](CHANGELOG.md).

## License

Werker is released under the [MIT License](LICENSE).
