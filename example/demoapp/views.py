from django.http import JsonResponse
from django.tasks import task_backends
from django.tasks.base import TaskResultStatus
from django.tasks.exceptions import TaskResultDoesNotExist

from example.demoapp.tasks import say_hello, send_notification


def index(request):
    return JsonResponse(
        {
            "routes": {
                "GET /enqueue/via-code/<name>/": "enqueue a plain @task via .enqueue() in the view",
                "GET /enqueue/via-decorator/<user_id>/": "enqueue an @at_most_once @task",
                "GET /result/<task_id>/": "check a task's status/result by id",
            },
            "note": "run `python manage.py taskworker` in another shell to process enqueued tasks",
        }
    )


def enqueue_via_code(request, name):
    # A plain @task, queued imperatively from view logic.
    result = say_hello.enqueue(name)
    return JsonResponse({"id": result.id, "status": result.status})


def enqueue_via_decorator(request, user_id):
    # send_notification is wrapped with werker's @at_most_once decorator in
    # tasks.py. Enqueueing it looks identical; the decorator only changes
    # what happens if a worker crashes mid-execution (see the README).
    result = send_notification.enqueue(int(user_id))
    return JsonResponse({"id": result.id, "status": result.status})


def task_result(request, task_id):
    backend = task_backends["default"]
    try:
        result = backend.get_result(task_id)
    except TaskResultDoesNotExist:
        return JsonResponse({"error": "no task result with that id"}, status=404)

    data = {
        "id": result.id,
        "task": result.task.module_path,
        "status": result.status,
        "attempts": result.attempts,
    }
    if result.status == TaskResultStatus.SUCCESSFUL:
        data["return_value"] = result.return_value
    return JsonResponse(data)
