class StaleClaimReclaimed(Exception):
    """Recorded as the TaskError on a task the reaper reclaimed. Never
    actually raised, just referenced by dotted path in TaskError."""


class TaskCancelled(Exception):
    """Recorded as the TaskError on a task cancelled via werker.api.cancel."""


class TaskTimeout(Exception):
    """A task ran longer than its @timeout or the TASK_TIMEOUT option."""
