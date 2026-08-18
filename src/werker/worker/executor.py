"""Backwards-compatible re-export.

The actual implementation moved to werker.concurrency so it can be shared
with werker.broker/werker.results (see werker.broker's module docstring
for why those ABCs need their own dedicated executor too, not just the
worker's user-task pool this module originally served).
"""

from functools import partial

from werker.concurrency import create_bounded_executor, run_with_connection_cleanup

create_executor = partial(create_bounded_executor, thread_name_prefix="werker-sync")

__all__ = ["create_executor", "run_with_connection_cleanup"]
