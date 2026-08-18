class StaleClaimReclaimed(Exception):
    """Recorded as the TaskError on a task the reaper reclaimed. Never
    actually raised, just referenced by dotted path in TaskError."""
