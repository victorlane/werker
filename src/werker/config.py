"""Validation for the numeric options that control worker behaviour."""

from collections.abc import Mapping
from typing import Any

from django.core.exceptions import ImproperlyConfigured

_POSITIVE_INTS = {
    "BOOKKEEPING_CONCURRENCY",
    "CONCURRENCY",
    "MAX_ASYNC_CONCURRENCY",
    "CLAIM_BATCH_SIZE",
}
_NON_NEGATIVE_INTS = {"MAX_RETRIES"}
_POSITIVE_NUMBERS = {
    "POLL_INTERVAL",
    "RETRY_BACKOFF_BASE",
    "RETRY_BACKOFF_MAX",
    "HEARTBEAT_INTERVAL",
    "STALE_RUNNING_TIMEOUT",
    "REAPER_POLL_INTERVAL",
    "SCHEDULER_POLL_INTERVAL",
    "STALE_SCHEDULE_CLAIM_TIMEOUT",
    "TASK_TIMEOUT",
}
_NON_NEGATIVE_NUMBERS = {"SHUTDOWN_GRACE_PERIOD"}
_BOOLEANS = {"RETRY_JITTER", "LISTEN_NOTIFY"}
#: May be None, meaning "no limit".
_OPTIONAL = {"TASK_TIMEOUT"}


def validate_options(options: Any) -> None:
    """Reject invalid configured capacity, retry, and timing values early.

    Unknown keys intentionally remain supported for custom broker/result-store
    implementations.  ``bool`` is excluded even though it is an ``int`` in
    Python: accepting ``True`` as a worker count is invariably a typo.
    """
    if options is None:
        return
    if not isinstance(options, Mapping):
        raise ImproperlyConfigured("TASKS OPTIONS must be a mapping.")

    for key, value in options.items():
        if value is None and key in _OPTIONAL:
            continue
        if key in _BOOLEANS:
            if not isinstance(value, bool):
                raise ImproperlyConfigured(f"TASKS OPTIONS[{key!r}] must be True or False.")
            continue
        if key in _POSITIVE_INTS | _NON_NEGATIVE_INTS:
            valid = isinstance(value, int) and not isinstance(value, bool)
            valid_range = value >= (1 if key in _POSITIVE_INTS else 0)
        elif key in _POSITIVE_NUMBERS | _NON_NEGATIVE_NUMBERS:
            valid = isinstance(value, (int, float)) and not isinstance(value, bool)
            valid_range = value > 0 if key in _POSITIVE_NUMBERS else value >= 0
        else:
            continue
        if not valid or not valid_range:
            positive_keys = _POSITIVE_INTS | _POSITIVE_NUMBERS
            comparison = "greater than zero" if key in positive_keys else "zero or greater"
            raise ImproperlyConfigured(f"TASKS OPTIONS[{key!r}] must be {comparison}.")