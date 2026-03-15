"""Tests for VestigingsProfielWriter database writer."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from kvk_connect.db.vestigingsprofiel_writer import VestigingsProfielWriter
from kvk_connect.models.domain.vestigingsprofiel_domain import VestigingsProfielDomain
from kvk_connect.models.orm.vestigingsprofiel_orm import VestigingsProfielORM

logger = logging.getLogger(__name__)


def _make_domain(vestigingsnummer: str = "000000000001", **kwargs) -> VestigingsProfielDomain:
    """Helper to create a minimal VestigingsProfielDomain."""
    return VestigingsProfielDomain(vestigingsnummer=vestigingsnummer, **kwargs)


class TestVestigingsProfielWriter:
    """Test suite for VestigingsProfielWriter."""

    @pytest.fixture
    def writer(self, db_engine: Engine) -> VestigingsProfielWriter:
        """Create writer instance with test database engine."""
        return VestigingsProfielWriter(db_engine)

    # --- init ---

    def test_init_with_default_batch_size(self, writer: VestigingsProfielWriter) -> None:
        """Test default batch size and initial state."""
        assert writer.batch_size == 1
        assert writer._session is None
        assert writer._count == 0

    def test_init_with_custom_batch_size(self, db_engine: Engine) -> None:
        """Test custom batch size is stored."""
        writer = VestigingsProfielWriter(db_engine, batch_size=50)
        assert writer.batch_size == 50

    # --- context manager ---

    def test_context_manager_creates_session(self, writer: VestigingsProfielWriter) -> None:
        """Test session created on context manager entry."""
        with writer:
            assert writer._session is not None

    def test_context_manager_closes_session(self, writer: VestigingsProfielWriter) -> None:
        """Test session closed on context manager exit."""
        with writer:
            pass
        assert writer._session is None

    def test_context_manager_rollback_on_exception(
        self, db_engine: Engine, db_session: Session
    ) -> None:
        """Test context manager rolls back on exception."""
        writer = VestigingsProfielWriter(db_engine, batch_size=10)
        domain = _make_domain("000000000001")

        try:
            with writer:
                writer.add(domain)
                raise ValueError("Test error")
        except ValueError:
            pass

        from sqlalchemy.orm import sessionmaker
        fresh_session = sessionmaker(bind=db_engine)()
        try:
            record = fresh_session.query(VestigingsProfielORM).filter_by(
                vestigingsnummer="000000000001"
            ).first()
            assert record is None
        finally:
            fresh_session.close()

    # --- add ---

    def test_add_persists_record(
        self, writer: VestigingsProfielWriter, db_session: Session
    ) -> None:
        """Test add persists a record to the database."""
        domain = _make_domain(
            "000000000001",
            bzk_adres_straatnaam="Teststraat",
            bzk_adres_postcode="1234AB",
            bzk_adres_plaats="Amsterdam",
        )

        with writer:
            writer.add(domain)

        record = db_session.query(VestigingsProfielORM).filter_by(
            vestigingsnummer="000000000001"
        ).first()
        assert record is not None
        assert record.bzk_adres_straatnaam == "Teststraat"
        assert record.bzk_adres_postcode == "1234AB"
        assert record.bzk_adres_plaats == "Amsterdam"

    def test_add_without_context_manager_raises(
        self, writer: VestigingsProfielWriter
    ) -> None:
        """Test add raises RuntimeError outside context manager."""
        domain = _make_domain()
        with pytest.raises(RuntimeError, match="Session not initialized"):
            writer.add(domain)

    def test_add_sets_last_updated(
        self, writer: VestigingsProfielWriter, db_session: Session
    ) -> None:
        """Test add sets last_updated timestamp."""
        domain = _make_domain("000000000001")

        with writer:
            writer.add(domain)

        record = db_session.query(VestigingsProfielORM).filter_by(
            vestigingsnummer="000000000001"
        ).first()
        assert record.last_updated is not None

    def test_add_upserts_existing_record(
        self, writer: VestigingsProfielWriter, db_session: Session
    ) -> None:
        """Test add updates an existing record (upsert)."""
        domain_v1 = _make_domain("000000000001", bzk_adres_straatnaam="Straat A")

        with writer:
            writer.add(domain_v1)

        domain_v2 = _make_domain("000000000001", bzk_adres_straatnaam="Straat B")

        with writer:
            writer.add(domain_v2)

        all_records = db_session.query(VestigingsProfielORM).filter_by(
            vestigingsnummer="000000000001"
        ).all()
        assert len(all_records) == 1
        assert all_records[0].bzk_adres_straatnaam == "Straat B"

    # --- GPS coordinate parsing ---

    def test_to_orm_parses_gps_with_dot(self) -> None:
        """Test GPS coordinates with dot decimal separator are parsed correctly."""
        domain = _make_domain(
            bzk_adres_gps_latitude="52.3731",
            bzk_adres_gps_longitude="4.8922",
        )
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.bzk_adres_gps_latitude == pytest.approx(52.3731)
        assert orm_obj.bzk_adres_gps_longitude == pytest.approx(4.8922)

    def test_to_orm_parses_gps_with_comma(self) -> None:
        """Test GPS coordinates with comma decimal separator are parsed correctly."""
        domain = _make_domain(
            bzk_adres_gps_latitude="52,3731",
            bzk_adres_gps_longitude="4,8922",
        )
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.bzk_adres_gps_latitude == pytest.approx(52.3731)
        assert orm_obj.bzk_adres_gps_longitude == pytest.approx(4.8922)

    def test_to_orm_invalid_gps_returns_none(self, caplog) -> None:
        """Test invalid GPS values result in None with a warning logged."""
        domain = _make_domain(
            bzk_adres_gps_latitude="not-a-number",
            bzk_adres_gps_longitude="also-bad",
        )
        with caplog.at_level(logging.WARNING):
            orm_obj = VestigingsProfielWriter._to_orm(domain)

        assert orm_obj.bzk_adres_gps_latitude is None
        assert orm_obj.bzk_adres_gps_longitude is None
        assert "Invalid latitude" in caplog.text or "Invalid longitude" in caplog.text

    def test_to_orm_none_gps_returns_none(self) -> None:
        """Test None GPS values result in None without error."""
        domain = _make_domain(bzk_adres_gps_latitude=None, bzk_adres_gps_longitude=None)
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.bzk_adres_gps_latitude is None
        assert orm_obj.bzk_adres_gps_longitude is None

    # --- date parsing ---

    def test_to_orm_parses_datum_aanvang(self) -> None:
        """Test registratie_datum_aanvang_vestiging is parsed."""
        domain = _make_domain(registratie_datum_aanvang_vestiging="20200115")
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.registratie_datum_aanvang_vestiging is not None

    def test_to_orm_none_datum_stays_none(self) -> None:
        """Test None datum fields remain None."""
        domain = _make_domain(
            registratie_datum_aanvang_vestiging=None,
            registratie_datum_einde_vestiging=None,
        )
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.registratie_datum_aanvang_vestiging is None
        assert orm_obj.registratie_datum_einde_vestiging is None

    # --- batch commit ---

    def test_batch_commit_at_batch_size(
        self, db_engine: Engine, db_session: Session
    ) -> None:
        """Test batch commits when batch_size is reached."""
        writer = VestigingsProfielWriter(db_engine, batch_size=3)
        domains = [_make_domain(f"00000000000{i}") for i in range(3)]

        with writer:
            for d in domains:
                writer.add(d)

        records = db_session.query(VestigingsProfielORM).all()
        assert len(records) == 3

    # --- mark_niet_leverbaar ---

    def test_mark_niet_leverbaar_writes_tombstone(
        self, writer: VestigingsProfielWriter, db_session: Session
    ) -> None:
        """Test tombstone record written with correct code."""
        with writer:
            writer.mark_niet_leverbaar("000000000001", "IPD0005")

        record = db_session.query(VestigingsProfielORM).filter_by(
            vestigingsnummer="000000000001"
        ).first()
        assert record is not None
        assert record.niet_leverbaar_code == "IPD0005"

    def test_mark_niet_leverbaar_no_retry_after(
        self, writer: VestigingsProfielWriter, db_session: Session
    ) -> None:
        """Test tombstone does not set retry_after."""
        with writer:
            writer.mark_niet_leverbaar("000000000001", "IPD0005")

        record = db_session.query(VestigingsProfielORM).filter_by(
            vestigingsnummer="000000000001"
        ).first()
        assert record.retry_after is None

    def test_mark_niet_leverbaar_without_context_raises(
        self, writer: VestigingsProfielWriter
    ) -> None:
        """Test mark_niet_leverbaar raises without context manager."""
        with pytest.raises(RuntimeError, match="Session not initialized"):
            writer.mark_niet_leverbaar("000000000001", "IPD0005")

    # --- mark_retry_after ---

    def test_mark_retry_after_writes_record(
        self, writer: VestigingsProfielWriter, db_session: Session
    ) -> None:
        """Test retry_after record is written."""
        with writer:
            writer.mark_retry_after("000000000001", timedelta(hours=1))

        record = db_session.query(VestigingsProfielORM).filter_by(
            vestigingsnummer="000000000001"
        ).first()
        assert record is not None
        assert record.retry_after is not None

    def test_mark_retry_after_timestamp_in_future(
        self, writer: VestigingsProfielWriter, db_session: Session
    ) -> None:
        """Test retry_after is in the future."""
        before = datetime.now(UTC).replace(tzinfo=None)

        with writer:
            writer.mark_retry_after("000000000001", timedelta(hours=10))

        record = db_session.query(VestigingsProfielORM).filter_by(
            vestigingsnummer="000000000001"
        ).first()
        assert record.retry_after > before + timedelta(hours=1)

    def test_mark_retry_after_no_niet_leverbaar_code(
        self, writer: VestigingsProfielWriter, db_session: Session
    ) -> None:
        """Test retry record has no niet_leverbaar_code."""
        with writer:
            writer.mark_retry_after("000000000001", timedelta(hours=1))

        record = db_session.query(VestigingsProfielORM).filter_by(
            vestigingsnummer="000000000001"
        ).first()
        assert record.niet_leverbaar_code is None

    def test_mark_retry_after_without_context_raises(
        self, writer: VestigingsProfielWriter
    ) -> None:
        """Test mark_retry_after raises without context manager."""
        with pytest.raises(RuntimeError, match="Session not initialized"):
            writer.mark_retry_after("000000000001", timedelta(hours=1))

    def test_mark_niet_leverbaar_preserves_existing_data(
        self, writer: VestigingsProfielWriter, db_session: Session
    ) -> None:
        """Tombstone must not erase previously stored fields (regression: merge() data loss)."""
        domain = _make_domain(
            "000000000001",
            bzk_adres_straatnaam="Teststraat",
            bzk_adres_postcode="1234AB",
            registratie_datum_einde_vestiging="31-12-2025",
        )

        with writer:
            writer.add(domain)

        with writer:
            writer.mark_niet_leverbaar("000000000001", "IPD0005")

        record = db_session.query(VestigingsProfielORM).filter_by(
            vestigingsnummer="000000000001"
        ).first()
        assert record.niet_leverbaar_code == "IPD0005"
        assert record.bzk_adres_straatnaam is not None
        assert record.registratie_datum_einde_vestiging is not None

    def test_mark_retry_after_preserves_existing_data(
        self, writer: VestigingsProfielWriter, db_session: Session
    ) -> None:
        """mark_retry_after must not erase previously stored fields (regression: merge() data loss)."""
        domain = _make_domain(
            "000000000001",
            bzk_adres_straatnaam="Teststraat",
            registratie_datum_einde_vestiging="31-12-2025",
        )

        with writer:
            writer.add(domain)

        with writer:
            writer.mark_retry_after("000000000001", timedelta(hours=10))

        record = db_session.query(VestigingsProfielORM).filter_by(
            vestigingsnummer="000000000001"
        ).first()
        assert record.retry_after is not None
        assert record.bzk_adres_straatnaam is not None
        assert record.registratie_datum_einde_vestiging is not None

    # --- new field coverage ---

    def test_to_orm_includes_kvk_nummer(self) -> None:
        domain = _make_domain(kvk_nummer="12345678")
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.kvk_nummer == "12345678"

    def test_to_orm_includes_rsin(self) -> None:
        domain = _make_domain(rsin="123456789")
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.rsin == "123456789"

    def test_to_orm_includes_ind_non_mailing(self) -> None:
        domain = _make_domain(ind_non_mailing="Nee")
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.ind_non_mailing == "Nee"

    def test_to_orm_includes_formele_registratiedatum(self) -> None:
        domain = _make_domain(formele_registratiedatum="01-01-2020")
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.formele_registratiedatum is not None
        assert orm_obj.formele_registratiedatum.year == 2020

    def test_to_orm_includes_statutaire_naam(self) -> None:
        domain = _make_domain(statutaire_naam="Test B.V.")
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.statutaire_naam == "Test B.V."

    def test_to_orm_includes_eerste_handelsnaam(self) -> None:
        domain = _make_domain(eerste_handelsnaam="Test Company")
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.eerste_handelsnaam == "Test Company"

    def test_to_orm_includes_ind_hoofdvestiging(self) -> None:
        domain = _make_domain(ind_hoofdvestiging="Ja")
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.ind_hoofdvestiging == "Ja"

    def test_to_orm_includes_ind_commerciele_vestiging(self) -> None:
        domain = _make_domain(ind_commerciele_vestiging="Ja")
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.ind_commerciele_vestiging == "Ja"

    def test_to_orm_includes_werkzame_personen(self) -> None:
        domain = _make_domain(voltijd_werkzame_personen=5, deeltijd_werkzame_personen=3, totaal_werkzame_personen=8)
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.voltijd_werkzame_personen == 5
        assert orm_obj.deeltijd_werkzame_personen == 3
        assert orm_obj.totaal_werkzame_personen == 8

    def test_to_orm_includes_handelsnamen(self) -> None:
        domain = _make_domain(handelsnamen="Alpha BV, Zebra BV")
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.handelsnamen == "Alpha BV, Zebra BV"

    def test_to_orm_includes_sbi_activiteiten(self) -> None:
        domain = _make_domain(
            hoofdactiviteit="62010",
            hoofdactiviteit_omschrijving="Computer programming",
            activiteit_overig="62020, 62090",
        )
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.hoofdactiviteit == "62010"
        assert orm_obj.hoofdactiviteit_omschrijving == "Computer programming"
        assert orm_obj.activiteit_overig == "62020, 62090"

    def test_to_orm_includes_websites(self) -> None:
        domain = _make_domain(websites="https://example.com")
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.websites == "https://example.com"

    def test_to_orm_includes_cor_adres_volledig_fields(self) -> None:
        domain = _make_domain(
            cor_adres_volledig="Postbus 100 1012AB Amsterdam",
            cor_adres_straatnaam="Teststraat",
            cor_adres_huisnummer=5,
            cor_adres_postcode="1012AB",
            cor_adres_postbusnummer=100,
            cor_adres_plaats="Amsterdam",
            cor_adres_land="Nederland",
        )
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.cor_adres_volledig == "Postbus 100 1012AB Amsterdam"
        assert orm_obj.cor_adres_straatnaam == "Teststraat"
        assert orm_obj.cor_adres_huisnummer == 5
        assert orm_obj.cor_adres_postcode == "1012AB"
        assert orm_obj.cor_adres_postbusnummer == 100
        assert orm_obj.cor_adres_plaats == "Amsterdam"
        assert orm_obj.cor_adres_land == "Nederland"

    def test_to_orm_cor_gps_parses_dot_separator(self) -> None:
        domain = _make_domain(cor_adres_gps_latitude="52.3676", cor_adres_gps_longitude="4.9041")
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.cor_adres_gps_latitude == pytest.approx(52.3676)
        assert orm_obj.cor_adres_gps_longitude == pytest.approx(4.9041)

    def test_to_orm_cor_gps_parses_comma_separator(self) -> None:
        domain = _make_domain(cor_adres_gps_latitude="52,3676", cor_adres_gps_longitude="4,9041")
        orm_obj = VestigingsProfielWriter._to_orm(domain)
        assert orm_obj.cor_adres_gps_latitude == pytest.approx(52.3676)
        assert orm_obj.cor_adres_gps_longitude == pytest.approx(4.9041)

    def test_add_all_new_fields_persisted(
        self,
        writer: VestigingsProfielWriter,
        db_session: Session,
        mock_kvk_vestigingsprofiel_response: dict,
    ) -> None:
        """End-to-end: all new fields survive the full API → mapper → writer → DB roundtrip."""
        from kvk_connect.mappers.map_vestigingsprofiel_api_to_vestigingsprofiel_domain import (
            map_vestigingsprofiel_api_to_vestigingsprofiel_domain,
        )
        from kvk_connect.models.api.vestigingsprofiel_api import VestigingsProfielAPI

        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)

        with writer:
            writer.add(domain)

        record = db_session.query(VestigingsProfielORM).filter_by(
            vestigingsnummer="000000000001"
        ).first()
        assert record is not None
        assert record.kvk_nummer == "12345678"
        assert record.rsin == "123456789"
        assert record.ind_non_mailing == "Nee"
        assert record.formele_registratiedatum is not None
        assert record.statutaire_naam == "Test B.V."
        assert record.eerste_handelsnaam == "Test Company"
        assert record.handelsnamen == "Test Company, Test Services"
        assert record.ind_hoofdvestiging == "Ja"
        assert record.ind_commerciele_vestiging == "Ja"
        assert record.voltijd_werkzame_personen == 5
        assert record.deeltijd_werkzame_personen == 5
        assert record.totaal_werkzame_personen == 10
        assert record.hoofdactiviteit == "62010"
        assert record.activiteit_overig == "62020, 62090"
        assert record.websites == "https://example.com"
        assert record.cor_adres_volledig == "Postbus 100 1012AB Amsterdam"
        assert record.cor_adres_gps_latitude is None  # GPS 0.0 → None
        assert record.bzk_adres_gps_latitude == pytest.approx(52.3676, abs=1e-3)
