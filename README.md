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
