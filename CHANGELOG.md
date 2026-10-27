# Changelog

All notable changes to this project will be documented in this file. This
project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- `MAX_RETRIES` now counts retries, as documented: `MAX_RETRIES=n` allows
  `n + 1` attempts. Previously it allowed `n` attempts in total. The worker
  now uses the retry budget stored on each task row instead of the current
  setting.
- Python 3.12+ is required (Django 6 does not support 3.11).

### Fixed
- A failing heartbeat query no longer stops the task's heartbeats, which
  let the reaper reclaim a task that was still running.
- runserver autostart crashed at startup because signal handlers can't be
  installed off the main thread. It also only starts in the autoreloader's
  child process now, so runserver runs one worker instead of two.
- A transient database error in the claim or reaper loop no longer stops the
  worker.
- A task that can no longer be imported, or that raises `SystemExit`, is
  recorded as a failed attempt instead of staying RUNNING until the reaper
  reclaims it.

### Added
- `@retry` decorator: per-task retry budget, exception filter (`on=`) and
  backoff settings.
- `RETRY_JITTER` option (default on) that randomises retry delays.
- LISTEN/NOTIFY wakeups: idle workers pick up a newly enqueued task at
  once instead of at the next poll (`LISTEN_NOTIFY`, `LISTEN_DATABASE`).
  A finished task also wakes the claim loop.
- `@unique` decorator: enqueueing a task whose key is already READY or
  RUNNING returns the existing result instead of adding a duplicate.
- Worker presence: a `WorkerRecord` row per worker (listed in the admin),
  `taskworker --health-file`, and a `werker_health` command for liveness
  and readiness probes.
- `werker.api.cancel` / `requeue` (and async `acancel` / `arequeue`), plus
  matching admin actions. A cancelled task is FAILED with a
  `werker.exceptions.TaskCancelled` error.
- `@timeout` decorator and `TASK_TIMEOUT` option: an attempt that runs too
  long fails with `werker.exceptions.TaskTimeout`.
- The reaper now delays a reclaimed task with the same exponential backoff
  as a failed one, instead of a flat `RETRY_BACKOFF_BASE` seconds.
- `postgres` extra that installs `psycopg[binary]`.
- Initial Postgres-backed Django task backend, worker, result storage, retry
  handling, periodic schedules, and operational management commands.
- Packaging metadata, contributor and security guidance, and continuous
  integration checks.

## [0.1.0.dev0] - 2026-09-03

### Added
- First development preview.