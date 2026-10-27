"""Retry timing shared by the worker's failure path and the reaper."""

import random


def backoff_delay(attempt: int, *, base: float, maximum: float, jitter: bool) -> float:
    """Seconds to wait before retrying after attempt number `attempt`.

    Exponential (`base ** attempt`, capped at `maximum`). With jitter it is
    a random value between half the delay and the full delay, so tasks that
    failed together don't all retry at the same moment."""
    try:
        delay = min(base**attempt, maximum)
    except OverflowError:
        delay = maximum
    if jitter:
        return random.uniform(delay / 2, delay)  # noqa: S311 - not security sensitive
    return float(delay)
