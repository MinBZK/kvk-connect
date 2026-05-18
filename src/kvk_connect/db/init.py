from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import inspect, text

from kvk_connect.models.enums import KVKStatus

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


def _migrate_backfill_status(engine: Engine) -> None:
    """Vul de status-kolom retroactief in voor bestaande records.

    Eenmalig nodig voor omgevingen die voor de introductie van de status-kolom bestonden.
    Mag worden verwijderd zodra alle omgevingen zijn bijgewerkt.

    Idempotent: stap 1-3 raken alleen rijen aan waar status IS NULL;
    stap 4a/4b raken alleen actief-geclassificeerde rijen met een einddatum.
    """
    tables = ["basisprofielen", "vestigingsprofielen", "vestigingen"]
    inspector = inspect(engine)
    with engine.connect() as conn:
        for tabel in tables:
            existing_cols = {col["name"] for col in inspector.get_columns(tabel)}
            if not {"status", "niet_leverbaar_code", "retry_after"}.issubset(existing_cols):
                continue

            conn.execute(
                text(f"UPDATE {tabel} SET status = :s WHERE status IS NULL AND niet_leverbaar_code IS NOT NULL"),  # noqa: S608
                {"s": KVKStatus.UITGESCHREVEN.value},
            )
            conn.execute(
                text(f"UPDATE {tabel} SET status = :s WHERE status IS NULL AND retry_after > CURRENT_TIMESTAMP"),  # noqa: S608
                {"s": KVKStatus.TIJDELIJK_NIET_BESCHIKBAAR.value},
            )
            conn.execute(
                text(f"UPDATE {tabel} SET status = :s WHERE status IS NULL"),  # noqa: S608
                {"s": KVKStatus.ACTIEF.value},
            )

            if tabel == "basisprofielen":
                # Bedrijven met een einddatum zijn uitgeschreven, ook als ze geen API-fout hadden
                conn.execute(
                    text(
                        "UPDATE basisprofielen SET status = :s"  # noqa: S608
                        ' WHERE status = :a AND "RegistratieDatumEinde" IS NOT NULL'
                    ),
                    {"s": KVKStatus.UITGESCHREVEN.value, "a": KVKStatus.ACTIEF.value},
                )
            elif tabel == "vestigingsprofielen":
                # Vestigingen met een einddatum zijn uitgeschreven, ook als ze geen API-fout hadden
                conn.execute(
                    text(
                        "UPDATE vestigingsprofielen SET status = :s"  # noqa: S608
                        ' WHERE status = :a AND "RegistratieDatumEindeVestiging" IS NOT NULL'
                    ),
                    {"s": KVKStatus.UITGESCHREVEN.value, "a": KVKStatus.ACTIEF.value},
                )

            conn.commit()
            logger.info("Backfilled status voor tabel: %s", tabel)


def ensure_database_initialized(engine: Engine, base: type[DeclarativeBase]) -> None:
    """Ensure all tables for the given Base exist in the database.

    This is safe to run multiple times - existing tables are skipped.
    New columns added in a schema update are automatically migrated (Watchtower-compatible).
    """
    logger.info("Ensuring tables exist for %s...", base.__name__)
    base.metadata.create_all(engine)
    _migrate_missing_columns(engine, base)
    _migrate_backfill_status(engine)

    inspector = inspect(engine)
    table_count = len([t for t in inspector.get_table_names() if t in base.metadata.tables])
    logger.info("Database initialized: %s table(s) ready", table_count)
