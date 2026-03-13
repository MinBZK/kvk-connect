"""Tests for SignaalWriter database writer."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from kvk_connect.db.signaal_writer import SignaalWriter
from kvk_connect.models.api.mutatiesignalen_api import MutatieSignaal
from kvk_connect.models.orm.signaal_orm import SignaalORM

logger = logging.getLogger(__name__)


def _make_signaal(
    id: str | None = None,
    kvknummer: str = "12345678",
    signaal_type: str = "WIJZIGING",
    vestigingsnummer: str | None = None,
) -> MutatieSignaal:
    """Helper to create a MutatieSignaal test object."""
    return MutatieSignaal(
        id=id or str(uuid4()),
        kvknummer=kvknummer,
        signaal_type=signaal_type,
        timestamp=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
        vestigingsnummer=vestigingsnummer,
    )


class TestSignaalWriter:
    """Test suite for SignaalWriter."""

    @pytest.fixture
    def writer(self, db_engine: Engine) -> SignaalWriter:
        """Create writer with default (upsert) mode."""
        return SignaalWriter(db_engine)

    @pytest.fixture
    def bulk_writer(self, db_engine: Engine) -> SignaalWriter:
        """Create writer with bulk (non-upsert) mode."""
        return SignaalWriter(db_engine, batch_size=10, upsert=False)

    # --- init ---

    def test_init_defaults(self, writer: SignaalWriter) -> None:
        """Test default initialization values."""
        assert writer.batch_size == 10
        assert writer.upsert is True
        assert writer._session is None
        assert writer._buffer == []
        assert writer._count == 0

    def test_init_bulk_mode(self, bulk_writer: SignaalWriter) -> None:
        """Test bulk mode initialization."""
        assert bulk_writer.upsert is False

    # --- context manager ---

    def test_context_manager_creates_session(self, writer: SignaalWriter) -> None:
        """Test session created on entry."""
        with writer:
            assert writer._session is not None

    def test_context_manager_closes_session(self, writer: SignaalWriter) -> None:
        """Test session closed on exit."""
        with writer:
            pass
        assert writer._session is None

    def test_context_manager_rollback_on_exception(
        self, db_engine: Engine, db_session: Session
    ) -> None:
        """Test context manager rolls back on exception."""
        writer = SignaalWriter(db_engine, batch_size=100)
        signaal = _make_signaal(id="fixed-id-1")

        try:
            with writer:
                writer.add(signaal)
                raise ValueError("Test error")
        except ValueError:
            pass

        from sqlalchemy.orm import sessionmaker
        fresh_session = sessionmaker(bind=db_engine)()
        try:
            record = fresh_session.query(SignaalORM).filter_by(id="fixed-id-1").first()
            assert record is None
        finally:
            fresh_session.close()

    # --- add (upsert mode) ---

    def test_add_upsert_persists_record(
        self, writer: SignaalWriter, db_session: Session
    ) -> None:
        """Test add persists a record in upsert mode."""
        signaal = _make_signaal(id="test-id-1", kvknummer="12345678")

        with writer:
            writer.add(signaal)

        record = db_session.query(SignaalORM).filter_by(id="test-id-1").first()
        assert record is not None
        assert record.kvknummer == "12345678"
        assert record.signaal_type == "WIJZIGING"

    def test_add_upsert_updates_existing(
        self, writer: SignaalWriter, db_session: Session
    ) -> None:
        """Test add updates an existing record in upsert mode."""
        shared_id = "shared-id"
        signaal_v1 = _make_signaal(id=shared_id, signaal_type="NIEUW")
        signaal_v2 = _make_signaal(id=shared_id, signaal_type="WIJZIGING")

        with writer:
            writer.add(signaal_v1)

        with writer:
            writer.add(signaal_v2)

        all_records = db_session.query(SignaalORM).filter_by(id=shared_id).all()
        assert len(all_records) == 1
        assert all_records[0].signaal_type == "WIJZIGING"

    def test_add_without_context_raises(self, writer: SignaalWriter) -> None:
        """Test add raises RuntimeError outside context manager."""
        with pytest.raises(RuntimeError, match="Session not initialized"):
            writer.add(_make_signaal())

    def test_add_with_vestigingsnummer(
        self, writer: SignaalWriter, db_session: Session
    ) -> None:
        """Test add persists optional vestigingsnummer."""
        signaal = _make_signaal(id="vest-test", vestigingsnummer="000000000001")

        with writer:
            writer.add(signaal)

        record = db_session.query(SignaalORM).filter_by(id="vest-test").first()
        assert record.vestigingsnummer == "000000000001"

    def test_add_upsert_batch_commits_at_batch_size(
        self, db_engine: Engine, db_session: Session
    ) -> None:
        """Test batch commit triggers at batch_size in upsert mode."""
        writer = SignaalWriter(db_engine, batch_size=3)
        signalen = [_make_signaal() for _ in range(3)]

        with writer:
            for s in signalen:
                writer.add(s)

        records = db_session.query(SignaalORM).all()
        assert len(records) == 3

    # --- add (bulk/non-upsert mode) ---

    def test_add_bulk_persists_record_on_flush(
        self, bulk_writer: SignaalWriter, db_session: Session
    ) -> None:
        """Test bulk mode add persists record after flush."""
        signaal = _make_signaal(id="bulk-id-1")

        with bulk_writer:
            bulk_writer.add(signaal)

        record = db_session.query(SignaalORM).filter_by(id="bulk-id-1").first()
        assert record is not None

    def test_add_bulk_buffers_before_batch_size(
        self, bulk_writer: SignaalWriter
    ) -> None:
        """Test bulk mode buffers records before batch_size is reached."""
        with bulk_writer:
            for _ in range(5):
                bulk_writer.add(_make_signaal())
            assert len(bulk_writer._buffer) == 5

    def test_add_bulk_flushes_at_batch_size(
        self, db_engine: Engine, db_session: Session
    ) -> None:
        """Test bulk mode flushes buffer when batch_size reached."""
        writer = SignaalWriter(db_engine, batch_size=3, upsert=False)
        signalen = [_make_signaal() for _ in range(3)]

        with writer:
            for s in signalen:
                writer.add(s)
            assert len(writer._buffer) == 0  # buffer cleared after batch commit

    # --- flush ---

    def test_flush_commits_remaining_upsert(
        self, writer: SignaalWriter, db_session: Session
    ) -> None:
        """Test explicit flush commits remaining records in upsert mode."""
        signaal = _make_signaal(id="flush-test")

        with writer:
            writer.add(signaal)
            writer.flush()

        record = db_session.query(SignaalORM).filter_by(id="flush-test").first()
        assert record is not None

    def test_flush_commits_remaining_bulk(
        self, bulk_writer: SignaalWriter, db_session: Session
    ) -> None:
        """Test explicit flush commits buffered records in bulk mode."""
        signaal = _make_signaal(id="flush-bulk-test")

        with bulk_writer:
            bulk_writer.add(signaal)
            bulk_writer.flush()

        record = db_session.query(SignaalORM).filter_by(id="flush-bulk-test").first()
        assert record is not None

    # --- _to_orm ---

    def test_to_orm_maps_all_fields(self) -> None:
        """Test _to_orm maps all fields correctly."""
        ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        signaal = MutatieSignaal(
            id="map-test",
            kvknummer="87654321",
            signaal_type="OPRICHTING",
            timestamp=ts,
            vestigingsnummer="000000000099",
        )
        orm_obj = SignaalWriter._to_orm(signaal)

        assert orm_obj.id == "map-test"
        assert orm_obj.kvknummer == "87654321"
        assert orm_obj.signaal_type == "OPRICHTING"
        assert orm_obj.timestamp == ts
        assert orm_obj.vestigingsnummer == "000000000099"

    def test_to_orm_none_vestigingsnummer(self) -> None:
        """Test _to_orm handles None vestigingsnummer."""
        signaal = _make_signaal(id="none-vest", vestigingsnummer=None)
        orm_obj = SignaalWriter._to_orm(signaal)
        assert orm_obj.vestigingsnummer is None
