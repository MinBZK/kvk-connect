# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

KVK-Connect is a Python library and Docker microservice suite for integrating with the Dutch Chamber of Commerce (KVK) API. It serves three purposes:
1. A pip-installable package for fetching KVK data without deep API knowledge
2. A local mirror of KVK data (basisprofiel, vestigingen, vestigingsprofiel) kept up-to-date in a database
3. An optional mutation service that polls KVK API for company changes and syncs them to the local mirror

## Commands

Before every commit, always run `just check-all` twice. The first run may auto-fix files (ruff); the second run validates the result is clean. Only commit if the second run passes fully.

```bash
# Install dependencies
just install          # uv sync

# Quality checks
just test             # pytest
just cov              # pytest with coverage report
just lint             # ruff check + format
just typing           # pyright type checking
just check-all        # all checks (lint, cov, typing, pre-commit)

# Run a single test
uv run pytest tests/path/to/test_file.py::test_function_name

# Docker
just docker-build     # build containers
just docker-up        # start all services
just docker-down      # stop all services
```

## Architecture

### Three Model Layers

Data flows through three distinct model layers (all in `src/kvk_connect/models/`):
- **API models** (`models/api/`): Raw dataclasses deserialized from KVK API JSON responses. Each has `from_dict()` and `to_dict()` methods.
- **Domain models** (`models/domain/`): Business logic representations, also dataclasses with `from_dict()`/`to_dict()`.
- **ORM models** (`models/orm/`): SQLAlchemy mapped tables for database persistence.

### Mappers and Services

`src/kvk_connect/mappers/` contains functions that convert API models → Domain models. `src/kvk_connect/services/record_service.py` (`KVKRecordService`) is the public-facing high-level API that orchestrates fetching and mapping. Both `KVKApiClient` and `KVKRecordService` are exported from `kvk_connect.__init__` as the library's public API.

### Database Layer

`src/kvk_connect/db/` has separate reader (`*_reader.py`) and writer (`*_writer.py`) classes. Schema is auto-initialized via `ensure_database_initialized()` in `db/init.py` — no Alembic. Use direct SQL for any schema changes.

### Docker Apps

Five Docker services in `apps/`, each with a `main.py` and `Dockerfile`:
- `gateway`: NGINX rate limiter (port 8080) — all apps route API calls through this
- `basisprofiel`, `vestigingen`, `vestigingsprofiel`: Fetch and persist respective KVK data types
- `mutatie-reader`: Polls KVK mutation API and writes change signals to DB

Apps depend on each other in order: mutatie-reader → basisprofiel → vestigingen → vestigingsprofiel. Compose files: `docker-compose.local.yaml` (SQLite/local) and `docker-compose.db.yaml` (PostgreSQL).

### Error Handling Pattern

`src/kvk_connect/exceptions.py` defines two error types:
- `KVKPermanentError`: Company doesn't exist (e.g., error code IPD0005) — long retry delay (24h default)
- `KVKTemporaryError`: Temporary unavailability (e.g., IPD1002, IPD1003) — short retry delay (10m)

`KVKApiClient` uses `@global_rate_limit()` decorator (`utils/rate_limit.py`) on all API methods.

## Coding Standards

- **Python 3.13+**, PEP 8, full type hints everywhere
- Use `Mapped[T]` for SQLAlchemy ORM models
- All dataclasses must have `from_dict()` static method and `to_dict()` using `asdict()`
- Use `from __future__ import annotations` for forward references in dataclasses
- Log with lazy `%s` formatting, not f-strings: `logging.info('Fetched %d records', count)` (Pylint W1203)
- Use `uv` for package management, not `pip`
- Database agnostic via SQLAlchemy — no Redis, no Alembic
- Business domain terms in comments/docstrings may use Dutch; all code (variables, functions) in English

### Git Workflow

Always work on branches — never commit directly to `main`. Two branch types:

- **`feature/...`** — new functionality. Always bump minor version before merging: `just bump minor`
- **`fix/...`** — bug fixes. Always bump patch version before merging: `just bump patch`

After bumping, commit the version change on the same branch as part of the PR.

**Always create new branches from an up-to-date `main`:**

```bash
git checkout main
git pull
git checkout -b feature/my-feature   # or fix/my-fix
```

Never branch from another feature or fix branch. If you are already on a different branch when starting new work, switch to main and pull first. Branching from a non-main branch causes the PR to include unrelated commits and leads to merge conflicts.

### Git Worktrees

When using git worktrees, create them **inside the project folder** (e.g., `.worktrees/`).
