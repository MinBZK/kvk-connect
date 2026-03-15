from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import inspect, text

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import DeclarativeBase

logger = logging.getLogger(__name__)


def _migrate_missing_columns(engine: Engine, base: type[DeclarativeBase]) -> None:
    """Voeg ontbrekende kolommen toe aan bestaande tabellen (Watchtower-compatibel).

    Na een image-update via Watchtower kunnen bestaande tabellen kolommen missen
    die in het nieuwe ORM-model zijn toegevoegd. Deze functie detecteert en migreert
    ze automatisch bij startup. Alle nieuwe kolommen zijn nullable, dus veilig te
    toevoegen zonder dataverlies.
    """
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())

    for table_name, table in base.metadata.tables.items():
        if table_name not in existing_tables:
            continue  # nieuwe tabel is al aangemaakt door create_all

        existing_cols = {col["name"] for col in inspector.get_columns(table_name)}
        for col in table.columns:
            if col.name not in existing_cols:
                col_type = col.type.compile(dialect=engine.dialect)
                with engine.connect() as conn:
                    conn.execute(text(f"ALTER TABLE {table_name} ADD {col.name} {col_type}"))
                    conn.commit()
                logger.info("Migrated: ALTER TABLE %s ADD %s %s", table_name, col.name, col_type)


def ensure_database_initialized(engine: Engine, base: type[DeclarativeBase]) -> None:
    """Ensure all tables for the given Base exist in the database.

    This is safe to run multiple times - existing tables are skipped.
    New columns added in a schema update are automatically migrated (Watchtower-compatible).
    """
    logger.info("Ensuring tables exist for %s...", base.__name__)
    base.metadata.create_all(engine)
    _migrate_missing_columns(engine, base)

    inspector = inspect(engine)
    table_count = len([t for t in inspector.get_table_names() if t in base.metadata.tables])
    logger.info("Database initialized: %s table(s) ready", table_count)
