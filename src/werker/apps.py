from django.apps import AppConfig


class WerkerConfig(AppConfig):
    name = "werker"
    label = "werker"
    verbose_name = "Werker"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        # Phase 1: models only. Autodiscovery of `tasks.py` (task/schedule
        # registration) and in-process worker autostart land in later phases.
        pass
