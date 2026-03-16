"""Tests voor VestigingsProfielWriter historielogica.

Alle tests gebruiken echte in-memory SQLite — geen mocks.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from kvk_connect.db.vestigingsprofiel_writer import VestigingsProfielWriter
from kvk_connect.db.historie_utils import _VESTIGINGSPROFIEL_BUSINESS_FIELDS
from kvk_connect.models.domain.vestigingsprofiel_domain import VestigingsProfielDomain
from kvk_connect.models.orm.vestigingsprofiel_historie_orm import VestigingsProfielHistorieORM
from kvk_connect.models.orm.vestigingsprofiel_orm import VestigingsProfielORM


def _domain(vestigingsnummer: str = "000012345678", **kwargs) -> VestigingsProfielDomain:
    return VestigingsProfielDomain(vestigingsnummer=vestigingsnummer, **kwargs)


class TestVestigingsProfielHistorie:
    """Historielogica voor VestigingsProfielWriter."""

    @pytest.fixture
    def writer(self, db_engine: Engine) -> VestigingsProfielWriter:
        return VestigingsProfielWriter(db_engine)

    @pytest.fixture
    def session(self, db_engine: Engine) -> Session:
        s = sessionmaker(bind=db_engine)()
        yield s
        s.close()

    def _historie(
        self, session: Session, vestigingsnummer: str = "000012345678"
    ) -> list[VestigingsProfielHistorieORM]:
        return (
            session.query(VestigingsProfielHistorieORM)
            .filter_by(vestigingsnummer=vestigingsnummer)
            .order_by(VestigingsProfielHistorieORM.gewijzigd_op, VestigingsProfielHistorieORM.id)
            .all()
        )

    # --- Nieuw record ---

    def test_new_record_creates_initial_history_entry(self, writer: VestigingsProfielWriter, session: Session) -> None:
        """Nieuw record geeft 1 historierij met alle non-None veldnamen."""
        domain = _domain(statutaire_naam="Test Vestiging B.V.", ind_hoofdvestiging="Ja", totaal_werkzame_personen=3)

        with writer:
            writer.add(domain)

        rows = self._historie(session)
        assert len(rows) == 1
        changed = set(rows[0].gewijzigde_velden.split(","))
        assert "statutaire_naam" in changed
        assert "ind_hoofdvestiging" in changed
        assert "totaal_werkzame_personen" in changed
        assert "websites" not in changed  # None-veld

    def test_new_record_history_gewijzigde_velden_only_non_none(
        self, writer: VestigingsProfielWriter, session: Session
    ) -> None:
        """gewijzigde_velden bevat uitsluitend non-None business-velden."""
        domain = _domain(statutaire_naam="Alleen naam")

        with writer:
            writer.add(domain)

        rows = self._historie(session)
        assert len(rows) == 1
        changed = set(rows[0].gewijzigde_velden.split(","))
        assert "statutaire_naam" in changed
        # Geen None-velden
        for f in _VESTIGINGSPROFIEL_BUSINESS_FIELDS:
            if f != "statutaire_naam":
                assert f not in changed

    # --- Update met echte wijziging ---

    def test_update_single_field_creates_second_history_entry(
        self, writer: VestigingsProfielWriter, session: Session
    ) -> None:
        """Update van 1 veld levert 2e historierij met precies dat veld."""
        with writer:
            writer.add(_domain(statutaire_naam="Oud"))
        with writer:
            writer.add(_domain(statutaire_naam="Nieuw"))

        rows = self._historie(session)
        assert len(rows) == 2
        changed = set(rows[1].gewijzigde_velden.split(","))
        assert changed == {"statutaire_naam"}

    def test_update_stores_new_value_in_history_row(
        self, writer: VestigingsProfielWriter, session: Session
    ) -> None:
        """De historierij bevat de nieuwe waarde na de wijziging."""
        with writer:
            writer.add(_domain(statutaire_naam="Oud"))
        with writer:
            writer.add(_domain(statutaire_naam="Nieuw"))

        rows = self._historie(session)
        assert rows[1].statutaire_naam == "Nieuw"

    # --- Re-fetch zonder wijziging ---

    def test_refetch_identical_no_history_entry(
        self, writer: VestigingsProfielWriter, session: Session
    ) -> None:
        """Identieke re-fetch genereert géén nieuwe historierij."""
        domain = _domain(statutaire_naam="Stable", ind_hoofdvestiging="Ja")

        with writer:
            writer.add(domain)
        with writer:
            writer.add(domain)

        rows = self._historie(session)
        assert len(rows) == 1

    def test_three_identical_writes_single_history_entry(
        self, writer: VestigingsProfielWriter, session: Session
    ) -> None:
        """3× dezelfde data → nog steeds 1 historierij."""
        domain = _domain(statutaire_naam="Stable")

        for _ in range(3):
            with writer:
                writer.add(domain)

        rows = self._historie(session)
        assert len(rows) == 1

    # --- Meerdere opeenvolgende wijzigingen ---

    def test_three_consecutive_changes_three_rows(
        self, writer: VestigingsProfielWriter, session: Session
    ) -> None:
        """3 opeenvolgende wijzigingen → 3 historierijen in chronologische volgorde."""
        with writer:
            writer.add(_domain(statutaire_naam="V1"))
        with writer:
            writer.add(_domain(statutaire_naam="V2"))
        with writer:
            writer.add(_domain(statutaire_naam="V3"))

        rows = self._historie(session)
        assert len(rows) == 3
        assert rows[0].statutaire_naam == "V1"
        assert rows[1].statutaire_naam == "V2"
        assert rows[2].statutaire_naam == "V3"

    # --- None ↔ waarde transities ---

    def test_none_to_value_tracked(self, writer: VestigingsProfielWriter, session: Session) -> None:
        """Veld verandert van None naar waarde → wordt getrackt."""
        with writer:
            writer.add(_domain(statutaire_naam="Test"))
        with writer:
            writer.add(_domain(statutaire_naam="Test", websites="https://vestiging.nl"))

        rows = self._historie(session)
        assert len(rows) == 2
        assert "websites" in rows[1].gewijzigde_velden.split(",")
        assert rows[1].websites == "https://vestiging.nl"

    def test_value_to_none_tracked(self, writer: VestigingsProfielWriter, session: Session) -> None:
        """Veld verandert van waarde naar None → wordt getrackt."""
        with writer:
            writer.add(_domain(statutaire_naam="Test", websites="https://vestiging.nl"))
        with writer:
            writer.add(_domain(statutaire_naam="Test"))

        rows = self._historie(session)
        assert len(rows) == 2
        assert "websites" in rows[1].gewijzigde_velden.split(",")
        assert rows[1].websites is None

    # --- Integer en datum velden ---

    def test_integer_field_change_tracked_correctly(
        self, writer: VestigingsProfielWriter, session: Session
    ) -> None:
        """Integer-veld voltijd_werkzame_personen correct als integer opgeslagen."""
        with writer:
            writer.add(_domain(statutaire_naam="Test", voltijd_werkzame_personen=10))
        with writer:
            writer.add(_domain(statutaire_naam="Test", voltijd_werkzame_personen=20))

        rows = self._historie(session)
        assert len(rows) == 2
        assert "voltijd_werkzame_personen" in rows[1].gewijzigde_velden.split(",")
        assert rows[1].voltijd_werkzame_personen == 20
        assert isinstance(rows[1].voltijd_werkzame_personen, int)

    def test_date_field_change_tracked_correctly(
        self, writer: VestigingsProfielWriter, session: Session
    ) -> None:
        """Datum-veld wijziging correct opgeslagen als Date."""
        with writer:
            writer.add(_domain(statutaire_naam="Test", registratie_datum_aanvang_vestiging="01-01-2018"))
        with writer:
            writer.add(_domain(statutaire_naam="Test", registratie_datum_aanvang_vestiging="15-03-2020"))

        rows = self._historie(session)
        assert len(rows) == 2
        assert "registratie_datum_aanvang_vestiging" in rows[1].gewijzigde_velden.split(",")
        assert rows[1].registratie_datum_aanvang_vestiging is not None

    # --- GPS velden ---

    def test_gps_float_field_tracked(self, writer: VestigingsProfielWriter, session: Session) -> None:
        """GPS-coördinaat (float) wijziging correct opgeslagen."""
        with writer:
            writer.add(_domain(statutaire_naam="Test", cor_adres_gps_latitude="52.3676"))
        with writer:
            writer.add(_domain(statutaire_naam="Test", cor_adres_gps_latitude="52.0000"))

        rows = self._historie(session)
        assert len(rows) == 2
        assert "cor_adres_gps_latitude" in rows[1].gewijzigde_velden.split(",")
        assert isinstance(rows[1].cor_adres_gps_latitude, float)

    # --- Rollback ---

    def test_rollback_no_history_persisted(self, db_engine: Engine) -> None:
        """Exception in writer-context → géén historierij in DB."""
        writer = VestigingsProfielWriter(db_engine, batch_size=10)

        try:
            with writer:
                writer.add(_domain(statutaire_naam="Test"))
                raise ValueError("Simulated error")
        except ValueError:
            pass

        SessionLocal = sessionmaker(bind=db_engine)
        fresh = SessionLocal()
        try:
            rows = fresh.query(VestigingsProfielHistorieORM).all()
            assert rows == []
        finally:
            fresh.close()

    # --- Batch ---

    def test_batch_size_3_five_vestigingen_correct_history(
        self, db_engine: Engine, session: Session
    ) -> None:
        """batch_size=3, 5 vestigingen elk 1 nieuw → elk 1 historierij."""
        writer = VestigingsProfielWriter(db_engine, batch_size=3)

        with writer:
            for i in range(5):
                writer.add(_domain(vestigingsnummer=f"00001234567{i}", statutaire_naam=f"Vestiging {i}"))

        all_rows = session.query(VestigingsProfielHistorieORM).all()
        assert len(all_rows) == 5

    # --- kvk_nummer in historierij ---

    def test_kvk_nummer_stored_in_history_row(self, writer: VestigingsProfielWriter, session: Session) -> None:
        """kvk_nummer wordt opgeslagen in historierij voor bedrijfsniveau-queries."""
        with writer:
            writer.add(_domain(kvk_nummer="12345678", statutaire_naam="Test"))

        row = session.query(VestigingsProfielHistorieORM).first()
        assert row is not None
        assert row.kvk_nummer == "12345678"
        assert row.vestigingsnummer == "000012345678"

    # --- Exactheid van gewijzigde_velden ---

    def test_gewijzigde_velden_exact_match(self, writer: VestigingsProfielWriter, session: Session) -> None:
        """gewijzigde_velden bevat precies de afwijkende velden, niet meer."""
        with writer:
            writer.add(_domain(statutaire_naam="A", ind_hoofdvestiging="Ja", totaal_werkzame_personen=5))
        with writer:
            writer.add(_domain(statutaire_naam="A", ind_hoofdvestiging="Nee", totaal_werkzame_personen=5))

        rows = self._historie(session)
        assert len(rows) == 2
        changed = set(rows[1].gewijzigde_velden.split(","))
        assert changed == {"ind_hoofdvestiging"}

    # --- Flat kolommen correct ---

    def test_flat_columns_match_orm_obj(self, writer: VestigingsProfielWriter, session: Session) -> None:
        """Alle flat kolommen in historierij bevatten de juiste waarden."""
        domain = _domain(
            kvk_nummer="12345678",
            statutaire_naam="Test Vestiging",
            ind_hoofdvestiging="Ja",
            ind_commerciele_vestiging="Ja",
            totaal_werkzame_personen=7,
            cor_adres_postcode="1234AB",
            cor_adres_plaats="Amsterdam",
        )

        with writer:
            writer.add(domain)

        row = session.query(VestigingsProfielHistorieORM).first()
        assert row is not None
        assert row.vestigingsnummer == "000012345678"
        assert row.kvk_nummer == "12345678"
        assert row.statutaire_naam == "Test Vestiging"
        assert row.ind_hoofdvestiging == "Ja"
        assert row.ind_commerciele_vestiging == "Ja"
        assert row.totaal_werkzame_personen == 7
        assert row.cor_adres_postcode == "1234AB"
        assert row.cor_adres_plaats == "Amsterdam"
        assert row.gewijzigd_op is not None

    # --- Meerdere vestigingen onafhankelijk ---

    def test_history_for_multiple_vestigingen_independent(
        self, writer: VestigingsProfielWriter, session: Session
    ) -> None:
        """Historierijen voor verschillende vestigingen zijn onafhankelijk."""
        with writer:
            writer.add(_domain(vestigingsnummer="000011111111", statutaire_naam="Vestiging A"))
            writer.add(_domain(vestigingsnummer="000022222222", statutaire_naam="Vestiging B"))

        rows_a = self._historie(session, "000011111111")
        rows_b = self._historie(session, "000022222222")
        assert len(rows_a) == 1
        assert len(rows_b) == 1
        assert rows_a[0].statutaire_naam == "Vestiging A"
        assert rows_b[0].statutaire_naam == "Vestiging B"

    # --- Vestigingsprofiel blijft intact ---

    def test_base_record_still_exists_after_history_write(
        self, writer: VestigingsProfielWriter, session: Session
    ) -> None:
        """Na het schrijven van historie is het vestigingsprofiel zelf correct bijgewerkt."""
        with writer:
            writer.add(_domain(statutaire_naam="Oud"))
        with writer:
            writer.add(_domain(statutaire_naam="Bijgewerkt"))

        vp = session.query(VestigingsProfielORM).filter_by(vestigingsnummer="000012345678").first()
        assert vp is not None
        assert vp.statutaire_naam == "Bijgewerkt"

        hist = self._historie(session)
        assert len(hist) == 2
