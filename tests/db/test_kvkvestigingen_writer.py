"""Tests for KvKVestigingenWriter database writer."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from kvk_connect.db.kvkvestigingen_writer import KvKVestigingenWriter
from kvk_connect.models.domain.kvkvestigingsnummersdomain import KvKVestigingsNummersDomain
from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.models.orm.vestigingen_orm import VestigingenORM

logger = logging.getLogger(__name__)


class TestKvKVestigingenWriter:
    """Test suite for KvKVestigingenWriter."""

    @pytest.fixture
    def writer(self, db_engine: Engine) -> KvKVestigingenWriter:
        """Create writer instance with test database engine."""
        return KvKVestigingenWriter(db_engine)

    @pytest.fixture
    def basisprofiel(self, db_session: Session) -> BasisProfielORM:
        """Insert a BasisProfielORM to satisfy the FK constraint on vestigingen."""
        bp = BasisProfielORM(kvk_nummer="12345678", last_updated=datetime(2024, 1, 1, tzinfo=UTC))
        db_session.add(bp)
        db_session.commit()
        return bp

    # --- init ---

    def test_init_with_default_batch_size(self, writer: KvKVestigingenWriter) -> None:
        """Test writer initialization with default batch size."""
        assert writer.batch_size == 1
        assert writer._session is None
        assert writer._count == 0

    def test_init_with_custom_batch_size(self, db_engine: Engine) -> None:
        """Test writer initialization with custom batch size."""
        writer = KvKVestigingenWriter(db_engine, batch_size=50)
        assert writer.batch_size == 50

    # --- context manager ---

    def test_context_manager_creates_session(self, writer: KvKVestigingenWriter) -> None:
        """Test context manager creates session on entry."""
        with writer:
            assert writer._session is not None

    def test_context_manager_closes_session(self, writer: KvKVestigingenWriter) -> None:
        """Test context manager closes session on exit."""
        with writer:
            pass
        assert writer._session is None

    def test_context_manager_rollback_on_exception(
        self,
        db_engine: Engine,
        db_session: Session,
        basisprofiel: BasisProfielORM,
    ) -> None:
        """Test context manager rolls back on exception."""
        writer = KvKVestigingenWriter(db_engine, batch_size=10)
        domain = KvKVestigingsNummersDomain(kvk_nummer="12345678", vestigingsnummers=["000000000001"])

        try:
            with writer:
                writer.add(domain)
                raise ValueError("Test error")
        except ValueError:
            pass

        from sqlalchemy.orm import sessionmaker
        fresh_session = sessionmaker(bind=db_engine)()
        try:
            record = fresh_session.query(VestigingenORM).filter_by(kvk_nummer="12345678").first()
            assert record is None
        finally:
            fresh_session.close()

    # --- add ---

    def test_add_single_vestiging(
        self,
        writer: KvKVestigingenWriter,
        db_session: Session,
        basisprofiel: BasisProfielORM,
    ) -> None:
        """Test adding a single vestiging."""
        domain = KvKVestigingsNummersDomain(
            kvk_nummer="12345678", vestigingsnummers=["000000000001"]
        )

        with writer:
            writer.add(domain)

        record = db_session.query(VestigingenORM).filter_by(
            kvk_nummer="12345678", vestigingsnummer="000000000001"
        ).first()
        assert record is not None

    def test_add_multiple_vestigingen(
        self,
        writer: KvKVestigingenWriter,
        db_session: Session,
        basisprofiel: BasisProfielORM,
    ) -> None:
        """Test adding multiple vestigingen for one KvK nummer."""
        domain = KvKVestigingsNummersDomain(
            kvk_nummer="12345678",
            vestigingsnummers=["000000000001", "000000000002", "000000000003"],
        )

        with writer:
            writer.add(domain)

        records = db_session.query(VestigingenORM).filter_by(kvk_nummer="12345678").all()
        vestigingsnummers = {r.vestigingsnummer for r in records}
        assert "000000000001" in vestigingsnummers
        assert "000000000002" in vestigingsnummers
        assert "000000000003" in vestigingsnummers

    def test_add_empty_vestigingsnummers_writes_sentinel(
        self,
        writer: KvKVestigingenWriter,
        db_session: Session,
        basisprofiel: BasisProfielORM,
    ) -> None:
        """Test that empty vestigingsnummers writes sentinel value."""
        domain = KvKVestigingsNummersDomain(kvk_nummer="12345678", vestigingsnummers=[])

        with writer:
            writer.add(domain)

        record = db_session.query(VestigingenORM).filter_by(
            kvk_nummer="12345678", vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER
        ).first()
        assert record is not None

    def test_add_sets_last_updated(
        self,
        writer: KvKVestigingenWriter,
        db_session: Session,
        basisprofiel: BasisProfielORM,
    ) -> None:
        """Test that add sets last_updated timestamp."""
        domain = KvKVestigingsNummersDomain(
            kvk_nummer="12345678", vestigingsnummers=["000000000001"]
        )
        before = datetime.now(UTC).replace(tzinfo=None)

        with writer:
            writer.add(domain)

        record = db_session.query(VestigingenORM).filter_by(
            kvk_nummer="12345678", vestigingsnummer="000000000001"
        ).first()
        assert record is not None
        assert record.last_updated is not None

    def test_add_without_context_manager_raises(self, writer: KvKVestigingenWriter) -> None:
        """Test add raises RuntimeError when called outside context manager."""
        domain = KvKVestigingsNummersDomain(kvk_nummer="12345678", vestigingsnummers=[])
        with pytest.raises(RuntimeError, match="Session not initialized"):
            writer.add(domain)

    def test_add_upserts_existing_record(
        self,
        writer: KvKVestigingenWriter,
        db_session: Session,
        basisprofiel: BasisProfielORM,
    ) -> None:
        """Test add updates (upserts) an existing record with same primary key."""
        domain = KvKVestigingsNummersDomain(
            kvk_nummer="12345678", vestigingsnummers=["000000000001"]
        )

        with writer:
            writer.add(domain)

        with writer:
            writer.add(domain)

        records = db_session.query(VestigingenORM).filter_by(
            kvk_nummer="12345678", vestigingsnummer="000000000001"
        ).all()
        assert len(records) == 1

    # --- batch commit ---

    def test_batch_commit_at_batch_size(
        self,
        db_engine: Engine,
        db_session: Session,
    ) -> None:
        """Test batch commit triggers when batch_size reached."""
        writer = KvKVestigingenWriter(db_engine, batch_size=3)

        for i in range(3):
            bp = BasisProfielORM(kvk_nummer=f"1234567{i}", last_updated=datetime.now(UTC))
            db_session.add(bp)
        db_session.commit()

        with writer:
            for i in range(3):
                domain = KvKVestigingsNummersDomain(
                    kvk_nummer=f"1234567{i}",
                    vestigingsnummers=[VestigingenORM.SENTINEL_VESTIGINGSNUMMER],
                )
                writer.add(domain)

        records = db_session.query(VestigingenORM).all()
        assert len(records) == 3

    def test_counter_increments_per_vestigingsnummer(
        self,
        db_engine: Engine,
        basisprofiel: BasisProfielORM,
    ) -> None:
        """Test _count increments by number of vestigingsnummers added (no mid-batch commit)."""
        writer = KvKVestigingenWriter(db_engine, batch_size=100)
        domain = KvKVestigingsNummersDomain(
            kvk_nummer="12345678",
            vestigingsnummers=["000000000001", "000000000002"],
        )

        with writer:
            assert writer._count == 0
            writer.add(domain)
            assert writer._count == 2

    # --- mark_niet_leverbaar ---

    def test_mark_niet_leverbaar_writes_tombstone(
        self,
        writer: KvKVestigingenWriter,
        db_session: Session,
        basisprofiel: BasisProfielORM,
    ) -> None:
        """Test tombstone record written with correct code."""
        with writer:
            writer.mark_niet_leverbaar("12345678", "IPD0005")

        record = db_session.query(VestigingenORM).filter_by(
            kvk_nummer="12345678",
            vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
        ).first()
        assert record is not None
        assert record.niet_leverbaar_code == "IPD0005"

    def test_mark_niet_leverbaar_has_no_retry_after(
        self,
        writer: KvKVestigingenWriter,
        db_session: Session,
        basisprofiel: BasisProfielORM,
    ) -> None:
        """Test tombstone does not set retry_after."""
        with writer:
            writer.mark_niet_leverbaar("12345678", "IPD0005")

        record = db_session.query(VestigingenORM).filter_by(
            kvk_nummer="12345678",
            vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
        ).first()
        assert record.retry_after is None

    def test_mark_niet_leverbaar_without_context_raises(
        self, writer: KvKVestigingenWriter
    ) -> None:
        """Test mark_niet_leverbaar without context manager raises RuntimeError."""
        with pytest.raises(RuntimeError, match="Session not initialized"):
            writer.mark_niet_leverbaar("12345678", "IPD0005")

    # --- mark_retry_after ---

    def test_mark_retry_after_writes_record(
        self,
        writer: KvKVestigingenWriter,
        db_session: Session,
        basisprofiel: BasisProfielORM,
    ) -> None:
        """Test retry_after record is written."""
        with writer:
            writer.mark_retry_after("12345678", timedelta(hours=1))

        record = db_session.query(VestigingenORM).filter_by(
            kvk_nummer="12345678",
            vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
        ).first()
        assert record is not None
        assert record.retry_after is not None

    def test_mark_retry_after_timestamp_in_future(
        self,
        writer: KvKVestigingenWriter,
        db_session: Session,
        basisprofiel: BasisProfielORM,
    ) -> None:
        """Test retry_after is set in the future."""
        before = datetime.now(UTC).replace(tzinfo=None)

        with writer:
            writer.mark_retry_after("12345678", timedelta(hours=10))

        record = db_session.query(VestigingenORM).filter_by(
            kvk_nummer="12345678",
            vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
        ).first()
        assert record.retry_after > before + timedelta(hours=1)

    def test_mark_retry_after_has_no_niet_leverbaar_code(
        self,
        writer: KvKVestigingenWriter,
        db_session: Session,
        basisprofiel: BasisProfielORM,
    ) -> None:
        """Test retry record does not set niet_leverbaar_code."""
        with writer:
            writer.mark_retry_after("12345678", timedelta(hours=1))

        record = db_session.query(VestigingenORM).filter_by(
            kvk_nummer="12345678",
            vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
        ).first()
        assert record.niet_leverbaar_code is None

    def test_mark_retry_after_without_context_raises(
        self, writer: KvKVestigingenWriter
    ) -> None:
        """Test mark_retry_after without context manager raises RuntimeError."""
        with pytest.raises(RuntimeError, match="Session not initialized"):
            writer.mark_retry_after("12345678", timedelta(hours=1))
