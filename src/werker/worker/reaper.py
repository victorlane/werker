"""Stale-claim reclaim loop — phase 5, not yet implemented.

SELECT ... FOR UPDATE SKIP LOCKED is only a lock during the claiming
transaction; after commit, status=RUNNING is a soft lock with no DB
backing. A hard-killed worker leaves rows RUNNING forever without this.
See the "Delivery guarantee" section of the project plan for how this
loop's reclaim decision differs between AT_LEAST_ONCE (retry) and
AT_MOST_ONCE (mark FAILED, no retry) rows.
"""
