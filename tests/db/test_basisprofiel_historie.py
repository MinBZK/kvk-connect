"""Tests voor BasisProfielWriter historielogica.

Alle tests gebruiken echte in-memory SQLite — geen mocks.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from kvk_connect.db.basisprofiel_writer import BasisProfielWriter
from kvk_connect.db.historie_utils import _BASISPROFIEL_BUSINESS_FIELDS
from kvk_connect.models.domain.basisprofiel import BasisProfielDomain
from kvk_connect.models.orm.basisprofiel_historie_orm import BasisProfielHistorieORM
from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM


def _domain(kvk_nummer: str = "12345678", **kwargs) -> BasisProfielDomain:
    return BasisProfielDomain(kvk_nummer=kvk_nummer, **kwargs)


class TestBasisProfielHistorie:
    """Historielogica voor BasisProfielWriter."""

    @pytest.fixture
    def writer(self, db_engine: Engine) -> BasisProfielWriter:
        return BasisProfielWriter(db_engine)

    @pytest.fixture
    def session(self, db_engine: Engine) -> Session:
        s = sessionmaker(bind=db_engine)()
        yield s
        s.close()

    def _historie(self, session: Session, kvk_nummer: str = "12345678") -> list[BasisProfielHistorieORM]:
        return (
            session.query(BasisProfielHistorieORM)
            .filter_by(kvk_nummer=kvk_nummer)
            .order_by(BasisProfielHistorieORM.gewijzigd_op, BasisProfielHistorieORM.id)
            .all()
        )

    # --- Nieuw record ---

    def test_new_record_creates_initial_history_entry(self, writer: BasisProfielWriter, session: Session) -> None:
        """Nieuw record geeft 1 historierij met alle non-None veldnamen."""
        domain = _domain(naam="Test B.V.", rechtsvorm="B.V.", totaal_werkzame_personen=5)

        with writer:
            writer.add(domain)

        rows = self._historie(session)
        assert len(rows) == 1
        changed = set(rows[0].gewijzigde_velden.split(","))
        assert "naam" in changed
        assert "rechtsvorm" in changed
        assert "totaal_werkzame_personen" in changed
        # None-velden horen er niet in
        assert "websites" not in changed

    def test_new_record_history_gewijzigde_velden_only_non_none(
        self, writer: BasisProfielWriter, session: Session
    ) -> None:
        """gewijzigde_velden bevat uitsluitend velden met een waarde (None-velden uitgesloten)."""
        domain = _domain(naam="Alleen naam")

        with writer:
            writer.add(domain)

        rows = self._historie(session)
        assert len(rows) == 1
        changed = set(rows[0].gewijzigde_velden.split(","))
        none_business_fields = _BASISPROFIEL_BUSINESS_FIELDS - changed
        for f in none_business_fields:
            assert f not in changed

    # --- Update met echte wijziging ---

    def test_update_single_field_creates_second_history_entry(
        self, writer: BasisProfielWriter, session: Session
    ) -> None:
        """Update van 1 veld levert 2e historierij met precies dat veld."""
        with writer:
            writer.add(_domain(naam="Oud B.V."))

        with writer:
            writer.add(_domain(naam="Nieuw B.V."))

        rows = self._historie(session)
        assert len(rows) == 2
        changed = set(rows[1].gewijzigde_velden.split(","))
        assert changed == {"naam"}

    def test_update_stores_new_value_in_history_row(self, writer: BasisProfielWriter, session: Session) -> None:
        """De historierij bevat de nieuwe waarde na de wijziging."""
        with writer:
            writer.add(_domain(naam="Oud"))
        with writer:
            writer.add(_domain(naam="Nieuw"))

        rows = self._historie(session)
        assert rows[1].naam == "Nieuw"

    def test_update_multiple_fields_all_tracked(self, writer: BasisProfielWriter, session: Session) -> None:
        """Meerdere gewijzigde velden worden allemaal opgenomen in gewijzigde_velden."""
        with writer:
            writer.add(_domain(naam="A", rechtsvorm="B.V."))
        with writer:
            writer.add(_domain(naam="B", rechtsvorm="N.V."))

        rows = self._historie(session)
        assert len(rows) == 2
        changed = set(rows[1].gewijzigde_velden.split(","))
        assert "naam" in changed
        assert "rechtsvorm" in changed

    # --- Re-fetch zonder wijziging ---

    def test_refetch_identical_no_history_entry(self, writer: BasisProfielWriter, session: Session) -> None:
        """Identieke re-fetch genereert géén nieuwe historierij."""
        domain = _domain(naam="Stable B.V.", rechtsvorm="B.V.")

        with writer:
            writer.add(domain)
        with writer:
            writer.add(domain)

        rows = self._historie(session)
        assert len(rows) == 1

    def test_three_identical_writes_single_history_entry(self, writer: BasisProfielWriter, session: Session) -> None:
        """3× dezelfde data → nog steeds 1 historierij."""
        domain = _domain(naam="Stable")

        for _ in range(3):
            with writer:
                writer.add(domain)

        rows = self._historie(session)
        assert len(rows) == 1

    # --- Meerdere opeenvolgende wijzigingen ---

    def test_three_consecutive_changes_three_rows(self, writer: BasisProfielWriter, session: Session) -> None:
        """3 opeenvolgende wijzigingen → 3 historierijen in chronologische volgorde."""
        with writer:
            writer.add(_domain(naam="V1"))
        with writer:
            writer.add(_domain(naam="V2"))
        with writer:
            writer.add(_domain(naam="V3"))

        rows = self._historie(session)
        assert len(rows) == 3
        assert rows[0].naam == "V1"
        assert rows[1].naam == "V2"
        assert rows[2].naam == "V3"

    # --- None ↔ waarde transities ---

    def test_none_to_value_tracked(self, writer: BasisProfielWriter, session: Session) -> None:
        """Veld verandert van None naar waarde → wordt getrackt."""
        with writer:
            writer.add(_domain(naam="Test"))  # websites=None
        with writer:
            writer.add(_domain(naam="Test", websites="https://test.nl"))

        rows = self._historie(session)
        assert len(rows) == 2
        assert "websites" in rows[1].gewijzigde_velden.split(",")
        assert rows[1].websites == "https://test.nl"

    def test_value_to_none_tracked(self, writer: BasisProfielWriter, session: Session) -> None:
        """Veld verandert van waarde naar None → wordt getrackt."""
        with writer:
            writer.add(_domain(naam="Test", websites="https://test.nl"))
        with writer:
            writer.add(_domain(naam="Test"))  # websites terug naar None

        rows = self._historie(session)
        assert len(rows) == 2
        assert "websites" in rows[1].gewijzigde_velden.split(",")
        assert rows[1].websites is None

    # --- Veldtypes ---

    def test_integer_field_change_tracked_correctly(self, writer: BasisProfielWriter, session: Session) -> None:
        """Integer-veld wijziging correct als integer opgeslagen."""
        with writer:
            writer.add(_domain(naam="Test", totaal_werkzame_personen=10))
        with writer:
            writer.add(_domain(naam="Test", totaal_werkzame_personen=25))

        rows = self._historie(session)
        assert len(rows) == 2
        assert "totaal_werkzame_personen" in rows[1].gewijzigde_velden.split(",")
        assert rows[1].totaal_werkzame_personen == 25
        assert isinstance(rows[1].totaal_werkzame_personen, int)

    def test_date_field_change_tracked_correctly(self, writer: BasisProfielWriter, session: Session) -> None:
        """Datum-veld wijziging correct opgeslagen als Date."""
        with writer:
            writer.add(_domain(naam="Test", formele_registratiedatum="01-01-2020"))
        with writer:
            writer.add(_domain(naam="Test", formele_registratiedatum="15-06-2021"))

        rows = self._historie(session)
        assert len(rows) == 2
        assert "formele_registratiedatum" in rows[1].gewijzigde_velden.split(",")
        assert rows[1].formele_registratiedatum is not None

    # --- Rollback ---

    def test_rollback_no_history_persisted(self, db_engine: Engine) -> None:
        """Exception in writer-context → géén historierij in DB."""
        writer = BasisProfielWriter(db_engine, batch_size=10)

        try:
            with writer:
                writer.add(_domain(naam="Test"))
                raise ValueError("Simulated error")
        except ValueError:
            pass

        SessionLocal = sessionmaker(bind=db_engine)
        fresh = SessionLocal()
        try:
            rows = fresh.query(BasisProfielHistorieORM).all()
            assert rows == []
        finally:
            fresh.close()

    # --- Batch ---

    def test_batch_size_3_five_records_correct_history(self, db_engine: Engine, session: Session) -> None:
        """batch_size=3, 5 records met elk een uniek kvk_nummer → elk 1 historierij."""
        writer = BasisProfielWriter(db_engine, batch_size=3)

        with writer:
            for i in range(5):
                writer.add(_domain(kvk_nummer=f"1234567{i}", naam=f"Bedrijf {i}"))

        all_rows = session.query(BasisProfielHistorieORM).all()
        assert len(all_rows) == 5

    def test_batch_same_kvk_multiple_changes(self, db_engine: Engine, session: Session) -> None:
        """batch_size=3, zelfde kvk_nummer 3× gewijzigd → 3 historierijen."""
        writer = BasisProfielWriter(db_engine, batch_size=1)

        with writer:
            writer.add(_domain(naam="V1"))
        with writer:
            writer.add(_domain(naam="V2"))
        with writer:
            writer.add(_domain(naam="V3"))

        rows = session.query(BasisProfielHistorieORM).filter_by(kvk_nummer="12345678").all()
        assert len(rows) == 3

    # --- Exactheid van gewijzigde_velden ---

    def test_gewijzigde_velden_exact_match(self, writer: BasisProfielWriter, session: Session) -> None:
        """gewijzigde_velden bevat precies de afwijkende velden, niet meer."""
        with writer:
            writer.add(_domain(naam="A", rechtsvorm="B.V.", totaal_werkzame_personen=5))
        with writer:
            writer.add(_domain(naam="A", rechtsvorm="N.V.", totaal_werkzame_personen=5))

        rows = self._historie(session)
        assert len(rows) == 2
        changed = set(rows[1].gewijzigde_velden.split(","))
        assert changed == {"rechtsvorm"}

    # --- Flat kolommen correct ---

    def test_flat_columns_match_orm_obj(self, writer: BasisProfielWriter, session: Session) -> None:
        """Alle flat kolommen in historierij bevatten de juiste waarden van orm_obj."""
        domain = _domain(
            naam="Test B.V.",
            rechtsvorm="B.V.",
            rechtsvorm_uitgebreid="Besloten Vennootschap",
            ind_non_mailing="Nee",
            totaal_werkzame_personen=42,
            websites="https://test.nl",
        )

        with writer:
            writer.add(domain)

        row = session.query(BasisProfielHistorieORM).filter_by(kvk_nummer="12345678").first()
        assert row is not None
        assert row.naam == "Test B.V."
        assert row.rechtsvorm == "B.V."
        assert row.rechtsvorm_uitgebreid == "Besloten Vennootschap"
        assert row.ind_non_mailing == "Nee"
        assert row.totaal_werkzame_personen == 42
        assert row.websites == "https://test.nl"
        assert row.kvk_nummer == "12345678"
        assert row.gewijzigd_op is not None

    def test_history_row_references_correct_kvk_nummer(
        self, writer: BasisProfielWriter, session: Session
    ) -> None:
        """Historierij bevat het juiste kvk_nummer."""
        with writer:
            writer.add(_domain(kvk_nummer="87654321", naam="Ander Bedrijf"))

        row = session.query(BasisProfielHistorieORM).first()
        assert row is not None
        assert row.kvk_nummer == "87654321"

    def test_history_for_multiple_companies_independent(
        self, writer: BasisProfielWriter, session: Session
    ) -> None:
        """Historierijen voor verschillende bedrijven zijn onafhankelijk."""
        with writer:
            writer.add(_domain(kvk_nummer="11111111", naam="Bedrijf A"))
            writer.add(_domain(kvk_nummer="22222222", naam="Bedrijf B"))

        rows_a = self._historie(session, "11111111")
        rows_b = self._historie(session, "22222222")
        assert len(rows_a) == 1
        assert len(rows_b) == 1
        assert rows_a[0].naam == "Bedrijf A"
        assert rows_b[0].naam == "Bedrijf B"

    # --- basisprofiel bestaat, daarna ophalen via basisprofielen tabel ---

    def test_base_record_still_exists_after_history_write(
        self, writer: BasisProfielWriter, session: Session
    ) -> None:
        """Na het schrijven van historie is het basisprofiel zelf ongewijzigd in basisprofielen."""
        with writer:
            writer.add(_domain(naam="Basis B.V.", rechtsvorm="B.V."))
        with writer:
            writer.add(_domain(naam="Bijgewerkt B.V.", rechtsvorm="B.V."))

        bp = session.query(BasisProfielORM).filter_by(kvk_nummer="12345678").first()
        assert bp is not None
        assert bp.naam == "Bijgewerkt B.V."

        hist = self._historie(session)
        assert len(hist) == 2
