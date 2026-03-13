"""Tests for SignaalReader database reader."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from kvk_connect.db.signaal_reader import SignaalReader
from kvk_connect.models.orm.signaal_orm import SignaalORM

logger = logging.getLogger(__name__)


def _add_signaal(
    session: Session,
    id: str,
    timestamp: datetime,
    kvknummer: str = "12345678",
    signaal_type: str = "WIJZIGING",
) -> SignaalORM:
    """Insert a SignaalORM row and commit."""
    orm = SignaalORM(
        id=id,
        timestamp=timestamp,
        kvknummer=kvknummer,
        signaal_type=signaal_type,
    )
    session.add(orm)
    session.commit()
    return orm


class TestSignaalReader:
    """Test suite for SignaalReader."""

    @pytest.fixture
    def reader(self, db_engine: Engine) -> SignaalReader:
        """Create reader instance."""
        return SignaalReader(db_engine)

    # --- get_last_timestamp ---

    def test_get_last_timestamp_empty_table_returns_none(
        self, reader: SignaalReader
    ) -> None:
        """Returns None when no signals exist."""
        assert reader.get_last_timestamp() is None

    def test_get_last_timestamp_single_record(
        self, reader: SignaalReader, db_session: Session
    ) -> None:
        """Returns the timestamp of the only record."""
        ts = datetime(2024, 6, 15, 10, 0, 0)
        _add_signaal(db_session, "s1", ts)

        result = reader.get_last_timestamp()
        assert result is not None
        # Compare naive datetimes (SQLite strips tz)
        assert result.replace(tzinfo=None) == ts.replace(tzinfo=None)

    def test_get_last_timestamp_returns_maximum(
        self, reader: SignaalReader, db_session: Session
    ) -> None:
        """Returns the latest timestamp when multiple records exist."""
        ts_early = datetime(2024, 1, 1, 0, 0, 0)
        ts_late = datetime(2024, 12, 31, 23, 59, 59)
        ts_mid = datetime(2024, 6, 15, 12, 0, 0)

        _add_signaal(db_session, "s1", ts_early)
        _add_signaal(db_session, "s2", ts_late)
        _add_signaal(db_session, "s3", ts_mid)

        result = reader.get_last_timestamp()
        assert result is not None
        assert result.replace(tzinfo=None) == ts_late.replace(tzinfo=None)

    # --- get_first_timestamp ---

    def test_get_first_timestamp_empty_table_returns_none(
        self, reader: SignaalReader
    ) -> None:
        """Returns None when no signals exist."""
        assert reader.get_first_timestamp() is None

    def test_get_first_timestamp_single_record(
        self, reader: SignaalReader, db_session: Session
    ) -> None:
        """Returns the timestamp of the only record."""
        ts = datetime(2024, 3, 10, 8, 30, 0)
        _add_signaal(db_session, "s1", ts)

        result = reader.get_first_timestamp()
        assert result is not None
        assert result.replace(tzinfo=None) == ts.replace(tzinfo=None)

    def test_get_first_timestamp_returns_minimum(
        self, reader: SignaalReader, db_session: Session
    ) -> None:
        """Returns the earliest timestamp when multiple records exist."""
        ts_early = datetime(2024, 1, 1, 0, 0, 0)
        ts_late = datetime(2024, 12, 31, 23, 59, 59)
        ts_mid = datetime(2024, 6, 15, 12, 0, 0)

        _add_signaal(db_session, "s1", ts_early)
        _add_signaal(db_session, "s2", ts_late)
        _add_signaal(db_session, "s3", ts_mid)

        result = reader.get_first_timestamp()
        assert result is not None
        assert result.replace(tzinfo=None) == ts_early.replace(tzinfo=None)

    # --- first vs last ---

    def test_first_before_last_with_multiple_records(
        self, reader: SignaalReader, db_session: Session
    ) -> None:
        """get_first_timestamp < get_last_timestamp when records span a range."""
        _add_signaal(db_session, "s1", datetime(2024, 1, 1))
        _add_signaal(db_session, "s2", datetime(2024, 6, 1))
        _add_signaal(db_session, "s3", datetime(2024, 12, 1))

        first = reader.get_first_timestamp()
        last = reader.get_last_timestamp()

        assert first is not None
        assert last is not None
        assert first.replace(tzinfo=None) < last.replace(tzinfo=None)

    def test_first_equals_last_with_single_record(
        self, reader: SignaalReader, db_session: Session
    ) -> None:
        """get_first_timestamp == get_last_timestamp when only one record exists."""
        ts = datetime(2024, 5, 20, 9, 0, 0)
        _add_signaal(db_session, "s1", ts)

        first = reader.get_first_timestamp()
        last = reader.get_last_timestamp()

        assert first is not None
        assert last is not None
        assert first.replace(tzinfo=None) == last.replace(tzinfo=None)
