import asyncio

from django.core.management.base import BaseCommand

from werker.worker.core import Worker


class Command(BaseCommand):
    help = "Run a werker task worker: claims and executes due tasks from the Postgres-backed queue."

    def add_arguments(self, parser):
        parser.add_argument(
            "--backend",
            default="default",
            help="TASKS alias to use (default: 'default').",
        )
        parser.add_argument(
            "--queues",
            default=None,
            help=(
                "Comma-separated queue names to claim from "
                "(default: the backend's configured queues)."
            ),
        )
        parser.add_argument(
            "--once",
            action="store_true",
            help=(
                "Drain currently-due work once and exit, instead of running forever. "
                "Used by tests/CI, not intended for long-running deployment."
            ),
        )

    def handle(self, *args, **options):
        queues = options["queues"].split(",") if options["queues"] else None
        worker = Worker(alias=options["backend"], queues=queues, once=options["once"])
        asyncio.run(worker.run())
