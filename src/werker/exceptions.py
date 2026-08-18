class StaleClaimReclaimed(Exception):
    """Recorded as the TaskError on a task the reaper reclaimed: no heartbeat
    was received before STALE_RUNNING_TIMEOUT, so the worker holding the
    claim is presumed dead. Never actually raised — its dotted path is
    stored as TaskError.exception_class_path so the failure is inspectable
    through the normal errors/TaskError machinery."""
