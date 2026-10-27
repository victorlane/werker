"""In-process worker autostart for `python manage.py runserver`.

Django's runserver has no official post-server-start hook, so Werker
starts the worker in a daemon thread from WerkerConfig.ready(), but ONLY
when:

  * the active command is `runserver`, and
  * this is the process that actually serves requests: the autoreloader's
    child (RUN_MAIN=true), or the only process under `--noreload`, and
  * `WERKER_AUTOSTART` is truthy (defaults to True under runserver), and
  * a worker isn't already running in this process.

The autoreloader's parent process only watches files, so starting a worker
there too would run two workers. Each code reload starts a fresh child
process, which starts its own worker; the old one dies with its process.
"""

import asyncio
import contextlib
import logging
import os
import sys
import threading
from typing import TYPE_CHECKING

from django.conf import settings

if TYPE_CHECKING:
    from werker.worker.core import Worker

logger = logging.getLogger("werker.worker")


class AutostartManager:
    _thread: threading.Thread | None = None
    _worker: "Worker | None" = None
    _loop: asyncio.AbstractEventLoop | None = None

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
            try:
                worker = Worker()
                cls._worker = worker
                with asyncio.Runner() as runner:
                    cls._loop = runner.get_loop()
                    runner.run(worker.run())
            except Exception:
                logger.exception("werker autostart worker crashed")

        cls._thread = threading.Thread(target=_run, name="werker-autostart", daemon=True)
        cls._thread.start()
        logger.info("werker autostart worker started (runserver)")
        return True

    @classmethod
    def stop(cls, timeout: float = 10) -> None:
        """Asks the autostarted worker to shut down gracefully and waits up
        to `timeout` seconds. The daemon thread dies with the process anyway,
        so this mainly exists for tests."""
        thread, worker, loop = cls._thread, cls._worker, cls._loop
        cls._thread = cls._worker = cls._loop = None
        if worker is not None and loop is not None:
            # The loop may already be closed if the worker exited on its own.
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(worker.request_shutdown)
        if thread is not None:
            thread.join(timeout=timeout)


def _is_runserver() -> bool:
    if "runserver" not in sys.argv:
        return False
    return os.environ.get("RUN_MAIN") == "true" or "--noreload" in sys.argv
