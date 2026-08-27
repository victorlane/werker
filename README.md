# werker

Self-contained, Postgres-backed background tasks for Django's native `django.tasks` framework. No Redis, no separate broker or infra, just your existing database.

## Quickstart

```bash
pip install werker
```

```python
# settings.py
INSTALLED_APPS = [
    ...
    "werker",
]

TASKS = {
    "default": {
        "BACKEND": "werker.backend.PostgresTaskBackend",
        "OPTIONS": {},
    },
}
```

```bash
python manage.py migrate
python manage.py taskworker
```

## Example

```python
# myapp/tasks.py
from django.tasks import task

@task
def send_welcome_email(user_id: int) -> str:
    ...
    return "sent"
```

```python
# somewhere else
from myapp.tasks import send_welcome_email

result = send_welcome_email.enqueue(user_id=42)
result.id       # track it
result.status   # READY, RUNNING, SUCCESSFUL, or FAILED

# or from an async view
result = await send_welcome_email.aenqueue(user_id=42)
await result.arefresh()
result.return_value
```

Waiting synchronously for a result to finish:

```python
import time

result = send_welcome_email.enqueue(user_id=42)
while not result.is_finished:
    time.sleep(0.5)
    result.refresh()
result.return_value
```

## Scheduling

```python
# myapp/tasks.py
from datetime import timedelta
from django.tasks import task
from werker.schedules.decorators import schedule

@schedule(every=timedelta(hours=1))      # or cron="0 3 * * *" + timezone="..."
@task
def nightly_report() -> str:
    ...
```

Sync declarations into the DB (run after deploy / whenever `@schedule`
decorators change), then the running worker's scheduler loop fires them:

```bash
python manage.py syncschedules
python manage.py taskworker
```

- `every` accepts a `timedelta` or whole seconds; `cron` validates with croniter.
- `catchup=False` (default) skips missed runs; `catchup=True` fires once
  immediately if the worker was down past a fire time.
- scheduler poll interval, worker capacity etc. are OPTIONS (e.g.
  `SCHEDULER_POLL_INTERVAL`).

## In-process worker (dev)

During `python manage.py runserver`, Werker starts a background worker
thread automatically (set `WERKER_AUTOSTART = False` to disable). For
production, run `manage.py taskworker` as a separate process.

## Pruning results

Bound table growth by deleting old terminal rows (never touches READY or
RUNNING):

```bash
python manage.py prune_task_results --older-than=30d      # default
python manage.py prune_task_results --dry-run             # preview
python manage.py prune_task_results --status successful   # only one status
```
