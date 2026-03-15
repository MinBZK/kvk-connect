"""Tests for ensure_database_initialized."""

from __future__ import annotations

import logging

import pytest
from sqlalchemy import create_engine, inspect, text

from kvk_connect.db.init import ensure_database_initialized
from kvk_connect.models.orm.base import Base

logger = logging.getLogger(__name__)


@pytest.fixture
def fresh_engine():
    """Create a fresh in-memory SQLite engine (no tables yet)."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    yield engine
    engine.dispose()


class TestEnsureDatabaseInitialized:
    """Test suite for ensure_database_initialized."""

    def test_creates_tables_on_first_call(self, fresh_engine) -> None:
        """Tables are created after the first call."""
        inspector = inspect(fresh_engine)
        assert inspector.get_table_names() == []

        ensure_database_initialized(fresh_engine, Base)

        table_names = inspect(fresh_engine).get_table_names()
        assert len(table_names) > 0

    def test_creates_expected_tables(self, fresh_engine) -> None:
        """Known ORM tables are present after initialization."""
        ensure_database_initialized(fresh_engine, Base)

        table_names = set(inspect(fresh_engine).get_table_names())
        assert "basisprofielen" in table_names
        assert "vestigingen" in table_names
        assert "vestigingsprofielen" in table_names
        assert "signalen" in table_names

    def test_idempotent_second_call_does_not_raise(self, fresh_engine) -> None:
        """Calling twice does not raise an error (create_all is idempotent)."""
        ensure_database_initialized(fresh_engine, Base)
        ensure_database_initialized(fresh_engine, Base)  # should not raise

        table_names = inspect(fresh_engine).get_table_names()
        assert len(table_names) > 0

    def test_table_count_matches_metadata(self, fresh_engine) -> None:
        """Number of created tables matches number of tables in Base.metadata."""
        ensure_database_initialized(fresh_engine, Base)

        inspector = inspect(fresh_engine)
        created = {t for t in inspector.get_table_names() if t in Base.metadata.tables}
        assert len(created) == len(Base.metadata.tables)

    def test_migrate_adds_missing_column(self, fresh_engine) -> None:
        """Missing column is added via ALTER TABLE on second initialization."""
        # Create the table without the 'naam' column (simulate old schema)
        with fresh_engine.connect() as conn:
            conn.execute(text("CREATE TABLE basisprofielen (kvkNummer TEXT PRIMARY KEY)"))
            conn.commit()

        ensure_database_initialized(fresh_engine, Base)

        cols = {c["name"] for c in inspect(fresh_engine).get_columns("basisprofielen")}
        assert "naam" in cols
        assert "handelsnamen" in cols

    def test_migrate_is_idempotent(self, fresh_engine) -> None:
        """Calling ensure_database_initialized twice does not raise or duplicate columns."""
        ensure_database_initialized(fresh_engine, Base)
        ensure_database_initialized(fresh_engine, Base)  # should not raise

        cols = [c["name"] for c in inspect(fresh_engine).get_columns("basisprofielen")]
        assert len(cols) == len(set(cols)), "Duplicate columns detected"

    def test_migrate_logs_added_columns(self, fresh_engine, caplog) -> None:
        """A log message is emitted for each column added via migration."""
        import logging

        with fresh_engine.connect() as conn:
            conn.execute(text("CREATE TABLE basisprofielen (kvkNummer TEXT PRIMARY KEY)"))
            conn.commit()

        with caplog.at_level(logging.INFO):
            ensure_database_initialized(fresh_engine, Base)

        assert "Migrated" in caplog.text

    def test_migrate_does_not_touch_complete_tables(self, fresh_engine, caplog) -> None:
        """No ALTER TABLE is issued when all columns already exist."""
        import logging

        ensure_database_initialized(fresh_engine, Base)
        caplog.clear()

        with caplog.at_level(logging.INFO):
            ensure_database_initialized(fresh_engine, Base)

        assert "Migrated" not in caplog.text
