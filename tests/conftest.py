import pytest
from testcontainers.community.postgres import PostgresContainer


@pytest.fixture(scope="session")
def django_db_setup(django_db_blocker):
    """Overrides pytest-django's default django_db_setup: instead of creating
    a test_* database on whatever server settings.DATABASES points at, spin
    up a disposable real Postgres via testcontainers and point Django at it.
    Matches this project's no-mocked-infra testing convention — SKIP LOCKED
    concurrency behavior can only be proven against a real Postgres."""
    with PostgresContainer("postgres:17-alpine") as postgres:
        from django.conf import settings
        from django.db import connections

        # .update() in place (not a wholesale dict replacement): ConnectionHandler
        # caches the *result* of running its one-time defaulting pass (TIME_ZONE,
        # CONN_MAX_AGE, etc.) over these exact dict objects the first time
        # `connections.settings` is accessed. Swapping in a brand-new dict here
        # would bypass that pass entirely and blow up on the missing defaults.
        settings.DATABASES["default"].update(
            {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": postgres.dbname,
                "USER": postgres.username,
                "PASSWORD": postgres.password,
                "HOST": postgres.get_container_host_ip(),
                "PORT": postgres.get_exposed_port(5432),
            }
        )
        connections.close_all()

        with django_db_blocker.unblock():
            from django.core.management import call_command

            call_command("migrate", "--noinput", verbosity=0)

        yield
