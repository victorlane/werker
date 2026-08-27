from django.apps import AppConfig


class WerkerConfig(AppConfig):
    name = "werker"
    label = "werker"
    verbose_name = "Werker"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        # Import `<app>.tasks` for every installed app so @task/@schedule/
        # @at_most_once decorators register at startup, even when nothing
        # else imports those modules. Called once the app registry is ready.
        from werker.schedules.discovery import discover_task_modules

        discover_task_modules()

        # runserver-only in-process worker autostart (see werker.worker.autostart).
        from werker.worker.autostart import AutostartManager

        AutostartManager.start_if_requested()