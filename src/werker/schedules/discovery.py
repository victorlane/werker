"""Loading task/schedule modules for installed apps.

Django only imports the models and AppConfig of each INSTALLED_APPS entry;
a module that is *only* imported to register @schedule/@at_most_once
decorations would otherwise never run. autodiscover() imports each app's
`tasks` module (the same convention the example app and Django's own docs
use), which executes the decorators.
"""


from django.utils.module_loading import autodiscover_modules


def discover_task_modules() -> None:
    """Imports `<app>.tasks` for every installed app, side-effecting the
    decorator registries. Idempotent (import_module caches)."""
    autodiscover_modules("tasks")


def discover_schedules() -> None:
    """Backwards-compatible alias. Schedule declarations live in `tasks`
    modules today, so this is the same discovery pass."""
    discover_task_modules()
