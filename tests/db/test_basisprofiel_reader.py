"""Tests for Basisprofiel database reader."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from kvk_connect.db.basisprofiel_reader import BasisProfielReader
from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.models.orm.signaal_orm import SignaalORM

logger = logging.getLogger(__name__)


class TestBasisProfielReader:
    """Test suite for BasisProfielReader."""

    @pytest.fixture
    def reader(self, db_engine: Engine) -> BasisProfielReader:
        """Create reader instance with test database engine."""
        return BasisProfielReader(db_engine)

    @pytest.fixture
    def setup_test_data(self, db_session: Session) -> None:
        """Set up test data in database."""
        # Add some signals
        signaal1 = SignaalORM(
            id="signal-1",
            kvknummer="12345678",
            timestamp=datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            signaal_type="UPDATE",
            vestigingsnummer=None,
        )
        signaal2 = SignaalORM(
            id="signal-2",
            kvknummer="87654321",
            timestamp=datetime(2024, 1, 2, 10, 0, 0, tzinfo=UTC),
            signaal_type="UPDATE",
            vestigingsnummer=None,
        )
        db_session.add(signaal1)
        db_session.add(signaal2)
        db_session.commit()

    def test_get_missing_kvk_nummers_empty_database(self, reader: BasisProfielReader) -> None:
        """Test with empty database returns empty list."""
        missing = reader.get_missing_kvk_nummers(limit=10)
        assert missing == []
        logger.info("Empty database test passed")

    def test_get_missing_kvk_nummers_no_missing(self, db_session: Session, reader: BasisProfielReader) -> None:
        """Test when all signals have corresponding profiles."""
        # Add signal
        signaal = SignaalORM(
            id="signal-1",
            kvknummer="12345678",
            timestamp=datetime.now(UTC),
            signaal_type="UPDATE",
        )
        db_session.add(signaal)

        # Add matching profile
        profile = BasisProfielORM(
            kvk_nummer="12345678",
            naam="Test Company",
        )
        db_session.add(profile)
        db_session.commit()

        missing = reader.get_missing_kvk_nummers(limit=10)
        assert "12345678" not in missing
        logger.info("No missing profiles test passed")

    def test_get_missing_kvk_nummers_with_missing(self, db_session: Session, reader: BasisProfielReader) -> None:
        """Test finding missing profiles."""
        # Add signal without profile
        signaal = SignaalORM(
            id="signal-1",
            kvknummer="12345678",
            timestamp=datetime.now(UTC),
            signaal_type="UPDATE",
        )
        db_session.add(signaal)
        db_session.commit()

        missing = reader.get_missing_kvk_nummers(limit=10)
        assert "12345678" in missing
        logger.info("Found missing profile: %s", missing)

    def test_get_missing_kvk_nummers_respects_limit(self, db_session: Session, reader: BasisProfielReader) -> None:
        """Test that limit is respected."""
        # Add 5 signals without profiles
        for i in range(5):
            signaal = SignaalORM(
                id=f"signal-{i}",
                kvknummer=f"1234567{i}",
                timestamp=datetime.now(UTC),
                signaal_type="UPDATE",
            )
            db_session.add(signaal)
        db_session.commit()

        missing = reader.get_missing_kvk_nummers(limit=3)
        assert len(missing) <= 3
        logger.info("Limit respected: got %d results with limit 3", len(missing))

    def test_get_missing_kvk_nummers_count_empty(self, reader: BasisProfielReader) -> None:
        """Test count with empty database."""
        count = reader.get_missing_kvk_nummers_count()
        assert count == 0

    def test_get_missing_kvk_nummers_count_with_data(self, db_session: Session, reader: BasisProfielReader) -> None:
        """Test counting missing profiles."""
        # Add 3 signals without profiles
        for i in range(3):
            signaal = SignaalORM(
                id=f"signal-{i}",
                kvknummer=f"1234567{i}",
                timestamp=datetime.now(UTC),
                signaal_type="UPDATE",
            )
            db_session.add(signaal)
        db_session.commit()

        count = reader.get_missing_kvk_nummers_count()
        assert count == 3
        logger.info("Missing count: %d", count)

    def test_get_outdated_kvk_nummers_empty_database(self, reader: BasisProfielReader) -> None:
        """Test with empty database."""
        outdated = reader.get_outdated_kvk_nummers()
        assert outdated == []

    def test_get_outdated_kvk_nummers_signal_newer(
        self, db_session: Session, reader: BasisProfielReader
    ) -> None:
        """Test finding outdated profiles when signal is newer."""
        kvk = "12345678"

        # Add profile with old timestamp
        profile = BasisProfielORM(
            kvk_nummer=kvk,
            naam="Test Company",
            last_updated=datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
        )
        db_session.add(profile)

        # Add signal with newer timestamp
        signaal = SignaalORM(
            id="signal-1",
            kvknummer=kvk,
            timestamp=datetime(2024, 1, 2, 10, 0, 0, tzinfo=UTC),
            signaal_type="UPDATE",
            vestigingsnummer=None,  # Profile update, not vestiging
        )
        db_session.add(signaal)
        db_session.commit()

        outdated = reader.get_outdated_kvk_nummers()
        assert kvk in outdated
        logger.info("Found outdated profile: %s", kvk)

    def test_get_outdated_kvk_nummers_profile_newer(self, db_session: Session, reader: BasisProfielReader) -> None:
        """Test that newer profiles are not marked outdated."""
        kvk = "12345678"

        # Add profile with new timestamp
        profile = BasisProfielORM(
            kvk_nummer=kvk,
            naam="Test Company",
            last_updated=datetime(2024, 1, 2, 10, 0, 0, tzinfo=UTC),
        )
        db_session.add(profile)

        # Add signal with older timestamp
        signaal = SignaalORM(
            id="signal-1",
            kvknummer=kvk,
            timestamp=datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            signaal_type="UPDATE",
            vestigingsnummer=None,
        )
        db_session.add(signaal)
        db_session.commit()

        outdated = reader.get_outdated_kvk_nummers()
        assert kvk not in outdated
        logger.info("New profile not marked outdated")

    def test_get_outdated_ignores_vestiging_updates(self, db_session: Session, reader: BasisProfielReader) -> None:
        """Test that vestiging-specific signals are ignored."""
        kvk = "12345678"
        vestig_nummer = "000050074695"

        # Add profile
        profile = BasisProfielORM(
            kvk_nummer=kvk,
            naam="Test Company",
            last_updated=datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
        )
        db_session.add(profile)

        # Add signal for vestiging (not profile)
        signaal = SignaalORM(
            id="signal-1",
            kvknummer=kvk,
            timestamp=datetime(2024, 1, 2, 10, 0, 0, tzinfo=UTC),
            signaal_type="UPDATE",
            vestigingsnummer=vestig_nummer,  # Vestiging update, not profile
        )
        db_session.add(signaal)
        db_session.commit()

        outdated = reader.get_outdated_kvk_nummers()
        assert kvk not in outdated  # Should not be marked outdated
        logger.info("Vestiging-only signal ignored correctly")

    def test_get_outdated_multiple_signals_same_kvk(self, db_session: Session, reader: BasisProfielReader) -> None:
        """Test with multiple signals for same KVK."""
        kvk = "12345678"

        # Add profile
        profile = BasisProfielORM(
            kvk_nummer=kvk,
            naam="Test Company",
            last_updated=datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
        )
        db_session.add(profile)

        # Add multiple signals with different timestamps
        signaal1 = SignaalORM(
            id="signal-1",
            kvknummer=kvk,
            timestamp=datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC),
            signaal_type="UPDATE",
            vestigingsnummer=None,
        )
        signaal2 = SignaalORM(
            id="signal-2",
            kvknummer=kvk,
            timestamp=datetime(2024, 1, 2, 10, 0, 0, tzinfo=UTC),  # Newest
            signaal_type="UPDATE",
            vestigingsnummer=None,
        )
        db_session.add_all([signaal1, signaal2])
        db_session.commit()

        outdated = reader.get_outdated_kvk_nummers()
        assert kvk in outdated
        logger.info("Multiple signals handled correctly")
