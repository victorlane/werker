"""In-process worker autostart for `python manage.py runserver`.

Django's runserver has no official post-server-start hook that also works
hot-reload-safe, so Werker starts the worker in a daemon thread from
WerkerConfig.ready() — but ONLY when:

  * the active command is `runserver`, and
  * `WERKER_AUTOSTART` is truthy (defaults to True under runserver), and
  * a worker isn't already running in this process.

The thread runs an asyncio loop with `Worker(once=False).run()`. A
second `runserver` reload keeps the same process, so the idempotence
guard prevents a duplicate loop.
"""

import asyncio
import logging
import threading

from django.conf import settings

logger = logging.getLogger("werker.worker")


class AutostartManager:
    _thread: threading.Thread | None = None

    @classmethod
    def start_if_requested(cls) -> bool:
        """Starts the worker thread if the process is a runserver and
        WERKER_AUTOSTART is enabled. Returns True if (re)started."""
        if cls._thread is not None:
            return False
        if not _is_runserver() or not getattr(settings, "WERKER_AUTOSTART", True):
            return False

        from werker.worker.core import Worker

        def _run() -> None:
            worker: Worker = Worker()
            try:
                asyncio.run(worker.run())
            except Exception:
                logger.exception("werker autostart worker crashed")

        cls._thread = threading.Thread(
            target=_run, name="werker-autostart", daemon=True
        )
        cls._thread.start()
        logger.info("werker autostart worker started (runserver)")
        return True

    @classmethod
    def stop(cls) -> None:
        """Best-effort shutdown hook for tests/reloads. The daemon thread
        dies with the process, so this is optional."""
        cls._thread = None


def _is_runserver() -> bool:
    import sys

    return "runserver" in sys.argv
