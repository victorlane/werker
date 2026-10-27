# Contributing

Thanks for improving Werker. By contributing, you agree that your work may be
distributed under the [MIT License](LICENSE).

## Development setup

Werker supports Python 3.11+ and Django 6.0+. Its integration tests require
Docker because they deliberately exercise PostgreSQL locking semantics.

```bash
git clone https://github.com/victorlane/werker.git
cd werker
python -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

The example project expects PostgreSQL on `localhost:5544`; its settings are
intended for local development only. Use a disposable database and run:

```bash
python example/manage.py migrate
python example/manage.py check
```

## Checks

Run these before opening a pull request:

```bash
ruff check .
mypy
pytest tests/unit
pytest                 # requires Docker/Testcontainers and PostgreSQL image access
python -m build
twine check dist/*
```

Please add focused tests for behaviour changes. Integration tests are the
right place for transaction, claim, concurrency, and PostgreSQL-specific
behaviour. Do not change broker claim, worker execution, retry, or delivery
semantics incidentally while making refactors or documentation changes.

## Pull requests

- Keep changes small and explain user-visible behaviour and operational impact.
- Update `README.md` and `CHANGELOG.md` for public API or configuration changes.
- Include migrations for intentional model changes; do not edit existing
  migrations after release.
- Never commit credentials, database dumps, build artifacts, or local virtual
  environments.

For security vulnerabilities, do **not** open a public issue; see
[SECURITY.md](SECURITY.md).