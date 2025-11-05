from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import inspect

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import DeclarativeBase

logger = logging.getLogger(__name__)


def ensure_database_initialized(engine: Engine, base: type[DeclarativeBase]) -> None:
    """Ensure all tables for the given Base exist in the database.

    This is safe to run multiple times - existing tables are skipped.
    """
    logger.info("Ensuring tables exist for %s...", base.__name__)
    base.metadata.create_all(engine)

    inspector = inspect(engine)
    table_count = len([t for t in inspector.get_table_names() if t in base.metadata.tables])
    logger.info("Database initialized: %s table(s) ready", table_count)
