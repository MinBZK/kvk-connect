"""Tests for Basisprofiel database writer."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from kvk_connect.db.basisprofiel_writer import BasisProfielWriter
from kvk_connect.mappers.kvk_record_mapper import map_kvkbasisprofiel_api_to_kvkrecord
from kvk_connect.models.api.basisprofiel_api import BasisProfielAPI
from kvk_connect.models.domain.basisprofiel import BasisProfielDomain
from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM

logger = logging.getLogger(__name__)


class TestBasisProfielWriter:
    """Test suite for BasisProfielWriter."""

    @pytest.fixture
    def writer(self, db_engine: Engine) -> BasisProfielWriter:
        """Create writer instance with test database engine."""
        return BasisProfielWriter(db_engine)

    def test_init_with_default_batch_size(self, writer: BasisProfielWriter) -> None:
        """Test writer initialization with default batch size."""
        assert writer.batch_size == 1
        assert writer._session is None
        assert writer._count == 0
        logger.info("Writer initialized with default batch size")

    def test_init_with_custom_batch_size(self, db_engine: Engine) -> None:
        """Test writer initialization with custom batch size."""
        writer = BasisProfielWriter(db_engine, batch_size=100)
        assert writer.batch_size == 100
        logger.info("Writer initialized with custom batch size %d", 100)

    def test_context_manager_creates_session(self, writer: BasisProfielWriter) -> None:
        """Test context manager creates session."""
        with writer:
            assert writer._session is not None
            assert isinstance(writer._session, Session)

    def test_context_manager_closes_session(self, writer: BasisProfielWriter) -> None:
        """Test context manager closes session after use."""
        with writer:
            session = writer._session
            assert session is not None
        assert writer._session is None
        logger.info("Session properly closed by context manager")

    def test_add_single_basisprofiel(
        self,
        writer: BasisProfielWriter,
        db_session: Session,
        mock_kvk_basisprofiel_response: dict,
    ) -> None:
        """Test adding single basisprofiel record."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        with writer:
            writer.add(domain)

        record = db_session.query(BasisProfielORM).filter_by(
            kvk_nummer="12345678"
        ).first()
        assert record is not None
        assert record.naam == "Test B.V."
        logger.info("Successfully added basisprofiel for kvk_nummer %s", "12345678")

    def test_add_without_context_manager_raises_error(
        self,
        writer: BasisProfielWriter,
    ) -> None:
        """Test adding without context manager raises RuntimeError."""
        domain = BasisProfielDomain(
            kvk_nummer="12345678",
            naam="Test",
            rechtsvorm="B.V.",
        )
        with pytest.raises(RuntimeError, match="Session not initialized"):
            writer.add(domain)
        logger.warning("Attempted to add record without active session")

    def test_flush_commits_changes(
        self,
        writer: BasisProfielWriter,
        db_session: Session,
        mock_kvk_basisprofiel_response: dict,
    ) -> None:
        """Test flush method commits changes."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        with writer:
            writer.add(domain)
            writer.flush()

        record = db_session.query(BasisProfielORM).filter_by(
            kvk_nummer="12345678"
        ).first()
        assert record is not None
        logger.info("Flush successfully committed changes")

    def test_add_multiple_basisprofielen(
        self,
        db_engine: Engine,
        db_session: Session,
    ) -> None:
        """Test adding multiple basisprofiel records."""
        writer = BasisProfielWriter(db_engine, batch_size=2)
        domains = [
            BasisProfielDomain(
                kvk_nummer=f"1234567{i}",
                naam=f"Company {i}",
                rechtsvorm="B.V.",
            )
            for i in range(3)
        ]

        with writer:
            for domain in domains:
                writer.add(domain)

        records = db_session.query(BasisProfielORM).all()
        assert len(records) == 3
        logger.info("Successfully added %d basisprofielen", 3)

    def test_batch_commit_triggers_at_batch_size(
        self,
        db_engine: Engine,
        db_session: Session,
    ) -> None:
        """Test batch commit triggers when batch size reached."""
        writer = BasisProfielWriter(db_engine, batch_size=2)
        domains = [
            BasisProfielDomain(
                kvk_nummer=f"1234567{i}",
                naam=f"Company {i}",
                rechtsvorm="B.V.",
            )
            for i in range(2)
        ]

        with writer:
            for domain in domains:
                writer.add(domain)

        records = db_session.query(BasisProfielORM).all()
        assert len(records) == 2
        logger.info("Batch commit triggered at batch size threshold")

    def test_update_existing_basisprofiel(
        self,
        writer: BasisProfielWriter,
        db_session: Session,
        mock_kvk_basisprofiel_response: dict,
    ) -> None:
        """Test updating existing basisprofiel via merge (upsert)."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        initial_domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        with writer:
            writer.add(initial_domain)

        updated_response = mock_kvk_basisprofiel_response.copy()
        updated_response["naam"] = "Updated Company B.V."
        updated_response["totaalWerkzamePersonen"] = 10
        api_updated = BasisProfielAPI.from_dict(updated_response)
        updated_domain = map_kvkbasisprofiel_api_to_kvkrecord(api_updated)

        with writer:
            writer.add(updated_domain)

        record = db_session.query(BasisProfielORM).filter_by(
            kvk_nummer="12345678"
        ).first()
        assert record.naam == "Updated Company B.V."
        assert record.totaal_werkzame_personen == 10

        all_records = db_session.query(BasisProfielORM).all()
        assert len(all_records) == 1
        logger.info("Successfully updated existing basisprofiel")

    def test_to_orm_conversion(
        self,
        mock_kvk_basisprofiel_response: dict,
    ) -> None:
        """Test domain to ORM conversion."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)
        orm_obj = BasisProfielWriter._to_orm(domain)

        assert isinstance(orm_obj, BasisProfielORM)
        assert orm_obj.kvk_nummer == "12345678"
        assert orm_obj.naam == "Test B.V."
        logger.info("Domain to ORM conversion successful")

    def test_to_orm_handles_null_fields(self) -> None:
        """Test ORM conversion handles null optional fields."""
        domain = BasisProfielDomain(
            kvk_nummer="12345678",
            naam=None,
            rechtsvorm="B.V.",
        )

        orm_obj = BasisProfielWriter._to_orm(domain)
        assert orm_obj.naam is None
        assert orm_obj.kvk_nummer == "12345678"
        logger.info("NULL fields properly handled in ORM conversion")

    def test_to_orm_converts_websites_string(self) -> None:
        """Test ORM conversion handles websites as string."""
        domain = BasisProfielDomain(
            kvk_nummer="12345678",
            naam="Test",
            rechtsvorm="B.V.",
            websites="https://test.nl, https://example.com",
        )

        orm_obj = BasisProfielWriter._to_orm(domain)
        assert orm_obj.websites == "https://test.nl, https://example.com"
        logger.info("Websites string properly handled in ORM conversion")

    def test_to_orm_handles_date_strings(self) -> None:
        """Test ORM conversion preserves date string fields."""
        domain = BasisProfielDomain(
            kvk_nummer="12345678",
            naam="Test",
            rechtsvorm="B.V.",
            registratie_datum_aanvang="2020-01-15",
            registratie_datum_einde="2025-12-31",
        )

        orm_obj = BasisProfielWriter._to_orm(domain)
        assert orm_obj.kvk_nummer == "12345678"
        logger.info("Date fields properly handled in ORM conversion")

    def test_last_updated_timestamp_set_on_add(
        self,
        writer: BasisProfielWriter,
        db_session: Session,
        mock_kvk_basisprofiel_response: dict,
    ) -> None:
        """Test last_updated timestamp set when adding record."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)
        before_add = datetime.now(UTC).replace(tzinfo=None)

        with writer:
            writer.add(domain)

        after_add = datetime.now(UTC).replace(tzinfo=None)
        record = db_session.query(BasisProfielORM).filter_by(
            kvk_nummer="12345678"
        ).first()

        assert record.last_updated is not None
        assert before_add <= record.last_updated <= after_add
        logger.info("last_updated timestamp properly set")

    def test_context_manager_rollback_on_exception(
        self,
        db_engine: Engine,
        mock_kvk_basisprofiel_response: dict,
    ) -> None:
        """Test context manager rolls back on exception."""
        writer = BasisProfielWriter(db_engine, batch_size=10)
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        try:
            with writer:
                writer.add(domain)
                raise ValueError("Test error")
        except ValueError:
            pass

        # Use fresh session to verify rollback occurred.
        from sqlalchemy.orm import sessionmaker
        SessionLocal = sessionmaker(bind=db_engine)
        fresh_session = SessionLocal()
        try:
            record = fresh_session.query(BasisProfielORM).filter_by(
                kvk_nummer="12345678"
            ).first()
            assert record is None
            logger.info("Transaction properly rolled back on exception")
        finally:
            fresh_session.close()

    def test_counter_increments_on_add(
        self,
        writer: BasisProfielWriter,
        mock_kvk_basisprofiel_response: dict,
    ) -> None:
        """Test counter increments on each add."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        with writer:
            assert writer._count == 0
            writer.add(domain)
            assert writer._count == 1
            writer.add(domain)
            assert writer._count == 2
            logger.info("Counter properly incremented to %d", writer._count)

    # --- mark_niet_leverbaar ---

    def test_mark_niet_leverbaar_writes_tombstone(
        self,
        writer: BasisProfielWriter,
        db_session: Session,
    ) -> None:
        """Test tombstone record is written with correct code."""
        with writer:
            writer.mark_niet_leverbaar("12345678", "IPD0005")

        record = db_session.query(BasisProfielORM).filter_by(kvk_nummer="12345678").first()
        assert record is not None
        assert record.niet_leverbaar_code == "IPD0005"

    def test_mark_niet_leverbaar_has_no_retry_after(
        self,
        writer: BasisProfielWriter,
        db_session: Session,
    ) -> None:
        """Test tombstone does not set retry_after."""
        with writer:
            writer.mark_niet_leverbaar("12345678", "IPD0005")

        record = db_session.query(BasisProfielORM).filter_by(kvk_nummer="12345678").first()
        assert record.retry_after is None

    def test_mark_niet_leverbaar_sets_last_updated(
        self,
        writer: BasisProfielWriter,
        db_session: Session,
    ) -> None:
        """Test tombstone has last_updated timestamp."""
        with writer:
            writer.mark_niet_leverbaar("12345678", "IPD0005")

        record = db_session.query(BasisProfielORM).filter_by(kvk_nummer="12345678").first()
        assert record.last_updated is not None

    def test_mark_niet_leverbaar_without_context_raises(
        self,
        writer: BasisProfielWriter,
    ) -> None:
        """Test mark_niet_leverbaar without context manager raises RuntimeError."""
        with pytest.raises(RuntimeError, match="Session not initialized"):
            writer.mark_niet_leverbaar("12345678", "IPD0005")

    # --- mark_retry_after ---

    def test_mark_retry_after_writes_record(
        self,
        writer: BasisProfielWriter,
        db_session: Session,
    ) -> None:
        """Test retry_after record is written."""
        with writer:
            writer.mark_retry_after("12345678", timedelta(hours=24))

        record = db_session.query(BasisProfielORM).filter_by(kvk_nummer="12345678").first()
        assert record is not None
        assert record.retry_after is not None

    def test_mark_retry_after_has_no_niet_leverbaar_code(
        self,
        writer: BasisProfielWriter,
        db_session: Session,
    ) -> None:
        """Test retry record does not set niet_leverbaar_code."""
        with writer:
            writer.mark_retry_after("12345678", timedelta(hours=24))

        record = db_session.query(BasisProfielORM).filter_by(kvk_nummer="12345678").first()
        assert record.niet_leverbaar_code is None

    def test_mark_retry_after_timestamp_is_in_future(
        self,
        writer: BasisProfielWriter,
        db_session: Session,
    ) -> None:
        """Test retry_after timestamp is set in the future."""
        before = datetime.now(UTC).replace(tzinfo=None)

        with writer:
            writer.mark_retry_after("12345678", timedelta(hours=24))

        record = db_session.query(BasisProfielORM).filter_by(kvk_nummer="12345678").first()
        # retry_after should be at least 1 hour from now (we set 24h)
        assert record.retry_after > before + timedelta(hours=1)

    def test_mark_retry_after_without_context_raises(
        self,
        writer: BasisProfielWriter,
    ) -> None:
        """Test mark_retry_after without context manager raises RuntimeError."""
        with pytest.raises(RuntimeError, match="Session not initialized"):
            writer.mark_retry_after("12345678", timedelta(hours=24))

    def test_mark_niet_leverbaar_preserves_existing_data(
        self,
        writer: BasisProfielWriter,
        db_session: Session,
        mock_kvk_basisprofiel_response: dict,
    ) -> None:
        """Tombstone must not erase previously stored fields (regression: merge() data loss)."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        with writer:
            writer.add(domain)

        with writer:
            writer.mark_niet_leverbaar("12345678", "IPD0005")

        record = db_session.query(BasisProfielORM).filter_by(kvk_nummer="12345678").first()
        assert record.niet_leverbaar_code == "IPD0005"
        assert record.naam is not None
        assert record.rechtsvorm is not None
        assert record.registratie_datum_einde is not None

    def test_mark_retry_after_preserves_existing_data(
        self,
        writer: BasisProfielWriter,
        db_session: Session,
        mock_kvk_basisprofiel_response: dict,
    ) -> None:
        """mark_retry_after must not erase previously stored fields (regression: merge() data loss)."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        with writer:
            writer.add(domain)

        with writer:
            writer.mark_retry_after("12345678", timedelta(hours=10))

        record = db_session.query(BasisProfielORM).filter_by(kvk_nummer="12345678").first()
        assert record.retry_after is not None
        assert record.naam is not None
        assert record.registratie_datum_einde is not None

    # --- new field coverage ---

    def test_to_orm_includes_ind_non_mailing(self, mock_kvk_basisprofiel_response: dict) -> None:
        """ind_non_mailing is stored in ORM."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)
        orm_obj = BasisProfielWriter._to_orm(domain)

        assert orm_obj.ind_non_mailing == "Nee"

    def test_to_orm_includes_formele_registratiedatum(self, mock_kvk_basisprofiel_response: dict) -> None:
        """formele_registratiedatum is parsed and stored as Date."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)
        orm_obj = BasisProfielWriter._to_orm(domain)

        assert orm_obj.formele_registratiedatum is not None
        assert orm_obj.formele_registratiedatum.year == 2020

    def test_to_orm_includes_handelsnamen(self, mock_kvk_basisprofiel_response: dict) -> None:
        """handelsnamen is stored as sorted comma-separated string."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)
        orm_obj = BasisProfielWriter._to_orm(domain)

        assert orm_obj.handelsnamen == "Test Company, Test Services"

    def test_add_all_new_fields_persisted(
        self,
        writer: BasisProfielWriter,
        db_session: Session,
        mock_kvk_basisprofiel_response: dict,
    ) -> None:
        """End-to-end: all 3 new fields survive the full API → mapper → writer → DB roundtrip."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        with writer:
            writer.add(domain)

        record = db_session.query(BasisProfielORM).filter_by(kvk_nummer="12345678").first()
        assert record is not None
        assert record.ind_non_mailing == "Nee"
        assert record.formele_registratiedatum is not None
        assert record.handelsnamen == "Test Company, Test Services"
