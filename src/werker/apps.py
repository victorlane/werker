from django.apps import AppConfig


class WerkerConfig(AppConfig):
    name = "werker"
    label = "werker"
    verbose_name = "Werker"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        # Autodiscovery and in-process autostart land in later phases.
        pass
