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
