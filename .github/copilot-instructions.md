# Copilot Instructions for KVKConnect Project

## Project opzet
* Dit project is een kvkconnector naar KVK API. Documentatie staat hier: https://developers.kvk.nl/documentation
* Doel is meerledig:
  * pip install package om snel aan de slag te kunnen met ophalen van kvk informatie zonder zelf de api te doorgronden
  * Als lokale mirror van kvk data, waarbij bedrijfsprofiel, vestigingen en details van de vestiging(vestigingsprofiel) opgeslagen worden en actueel gehouden wordt.
  * Klanten hebben optioneel de keuze om ook de mutatieservice te gebruiken, waarbij mutaties opgehaald worden en verwerkt in de lokale mirror.

## Architecture Constraints
- **No Redis**: Do not suggest Redis.
- **Minimize infrastructure**: Solve problems with existing stack (Python, SQLAlchemy, PostgreSQL, NGINX, Docker) first.
- **Database Agnostic**: Use SQLAlchemy for database interactions. Make all solutions database agnostic.
- **No Alembic**: Do not suggest Alembic for migrations, always provide direct SQL commands for creating or altering tables.
- **English Language**: Always use English for code, variables, functions.
- **Concise Comments**: Use short sentences in English for comments.

## General Guidelines
- Code must be written for **Python 3.11+**.
- Follow **PEP 8** style conventions.
- Use **type hints** everywhere (`Mapped[int]`, `Mapped[str]` for SQLAlchemy models).
- Keep functions small and single-responsibility.
- Always prefer clarity and maintainability over cleverness.
# Copilot Instructions for KVKConnect Project

## Project opzet
* Dit project is een kvk connector naar KVK API. Documentatie staat hier: https://developers.kvk.nl/documentation
* Doel is meerledig:
  * pip install package om snel aan de slag te kunnen met ophalen van kvk informatie zonder zelf de api te doorgronden
  * Als lokale mirror van kvk data, waarbij bedrijfsprofiel, vestigingen en details van de vestiging(vestigingsprofiel) opgeslagen worden en actueel gehouden wordt.
  * Klanten hebben optioneel de keuze om ook de mutatieservice te gebruiken, waarbij mutaties opgehaald worden en verwerkt in de lokale mirror.

## Architecture Constraints
- **No Redis**: Do not suggest Redis.
- **Minimize infrastructure**: Solve problems with existing stack (Python, SQLAlchemy, PostgreSQL, NGINX, Docker) first.
- **Database Agnostic**: Use SQLAlchemy for database interactions. Make all solutions database agnostic.
- **No Alembic**: Do not suggest Alembic for migrations, always provide direct SQL commands for creating or altering tables.
- **English Language**: Always use English for code, variables, functions.
- **Concise Comments**: Use short sentences in English for comments.

## General Guidelines
- Code must be written for **Python 3.13+**.
- Follow **PEP 8** style conventions.
- Use **type hints** everywhere (`Mapped[int]`, `Mapped[str]` for SQLAlchemy models).
- Keep functions small and single-responsibility.
- Always prefer clarity and maintainability over cleverness.

## Project Structure
- Source code lives in `src/`.

## Documentation
- Write **docstrings** for all public classes and functions.
- Keep documentation Dutch where relevant for business terms.

## Testing
- Use **pytest** for all tests.
- Tests live in `tests/` and mirror the `src/` structure.
- Use fixtures for DB setup/teardown.
- Test names start with `test_`.

## Logging & Error Handling
- Use the standard `logging` module, not `print()`.
- Log at appropriate levels: `info` for normal ops, `warning` for recoverable issues, `error` for failures.
- Raise custom exceptions for domain-specific errors.

## Dataclass Pattern
- Use `@dataclass` from `dataclasses` for all domain and API models.
- Always include a `from_dict` static method for deserialization.
- Always include a `to_dict` method using `asdict()` for serialization.
- Use `from __future__ import annotations` for forward references.
- Keep dataclasses simple: no complex validation logic in the class itself.

## Output Formatting
- When generating solutions, always give the full file content.

## Linting / Pylint
- Aim to adhere to Pylint rules where practical.
- Enforce Pylint W1203: use lazy formatting in logging calls. Do not use f-strings or `str.format()` inside logging calls.
  - Good: `logging.info('Processed %d records for %s', count, name)`
  - Bad: `logging.info(f'Processed {count} records for {name}')`

## UV instread of pip
- When suggesting package installation commands, prefer `uv` over `pip` for installing packages.
## Project Structure
- Source code lives in `src/`.

## Documentation
- Write **docstrings** for all public classes and functions.
- Keep documentation Dutch where relevant for business terms.

## Testing
- Use **pytest** for all tests.
- Tests live in `tests/` and mirror the `src/` structure.
- Use fixtures for DB setup/teardown.
- Test names start with `test_`.

## Logging & Error Handling
- Use the standard `logging` module, not `print()`.
- Log at appropriate levels: `info` for normal ops, `warning` for recoverable issues, `error` for failures.
- Raise custom exceptions for domain-specific errors.

## Dataclass Pattern
- Use `@dataclass` from `dataclasses` for all domain and API models.
- Always include a `from_dict` static method for deserialization.
- Always include a `to_dict` method using `asdict()` for serialization.
- Use `from __future__ import annotations` for forward references.
- Keep dataclasses simple: no complex validation logic in the class itself.

## Output Formatting
- When generating solutions, always give the full file content.

## Linting / Pylint
- Aim to adhere to Pylint rules where practical.
- Enforce Pylint W1203: use lazy formatting in logging calls. Do not use f-strings or `str.format()` inside logging calls.
  - Good: `logging.info('Processed %d records for %s', count, name)`
  - Bad: `logging.info(f'Processed {count} records for {name}')`

## UV instread of pip
- When suggesting package installation commands, prefer `uv` over `pip` for installing packages.
