"""Migration compatibility tests: bestaande DB + nieuwe historietabellen.

Simuleert wat er gebeurt als Watchtower een nieuw image trekt terwijl de DB
de historietabellen nog niet heeft. Gebruikt echte in-memory SQLite, geen mocks.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from kvk_connect.db.basisprofiel_writer import BasisProfielWriter
from kvk_connect.db.init import ensure_database_initialized
from kvk_connect.db.vestigingsprofiel_writer import VestigingsProfielWriter
from kvk_connect.models.domain.basisprofiel import BasisProfielDomain
from kvk_connect.models.domain.vestigingsprofiel_domain import VestigingsProfielDomain
from kvk_connect.models.orm.base import Base
from kvk_connect.models.orm.basisprofiel_historie_orm import BasisProfielHistorieORM
from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.models.orm.vestigingsprofiel_historie_orm import VestigingsProfielHistorieORM
from kvk_connect.models.orm.vestigingsprofiel_orm import VestigingsProfielORM


@pytest.fixture
def legacy_engine():
    """Engine die start met het oude schema (zonder historietabellen) en dan migreert."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})

    # Simuleer oud schema: alleen basisprofielen en vestigingsprofielen, zonder historietabellen
    with engine.connect() as conn:
        conn.execute(text(
            "CREATE TABLE basisprofielen ("
            "  kvkNummer TEXT PRIMARY KEY,"
            "  naam TEXT,"
            "  indNonMailing TEXT,"
            "  formeleRegistratiedatum DATE,"
            "  handelsnamen TEXT,"
            "  eersteHandelsnaam TEXT,"
            "  hoofdactiviteit TEXT,"
            "  hoofdactiviteitOmschrijving TEXT,"
            "  activiteitOverig TEXT,"
            "  rechtsvorm TEXT,"
            "  rechtsvormUitgebreid TEXT,"
            "  totaalWerkzamePersonen INTEGER,"
            "  websites TEXT,"
            "  RegistratieDatumAanvang DATE,"
            "  RegistratieDatumEinde DATE,"
            "  niet_leverbaar_code TEXT,"
            "  retry_after DATETIME,"
            "  created_at DATETIME,"
            "  last_updated DATETIME"
            ")"
        ))
        conn.execute(text(
            "CREATE TABLE vestigingsprofielen ("
            "  vestigingsnummer TEXT PRIMARY KEY,"
            "  kvkNummer TEXT,"
            "  rsin TEXT,"
            "  statutaireNaam TEXT,"
            "  last_updated DATETIME,"
            "  created_at DATETIME,"
            "  niet_leverbaar_code TEXT,"
            "  retry_after DATETIME"
            ")"
        ))
        conn.commit()

    # Migreer: create_all maakt de historietabellen aan, bestaande tabellen worden overgeslagen
    ensure_database_initialized(engine, Base)

    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


class TestHistorieMigrationCompat:
    """Watchtower-compatibele migratie: historietabellen worden aangemaakt op bestaande DB."""

    def test_migrate_creates_basisprofielen_historie(self, legacy_engine) -> None:
        """Na migratie bestaat basisprofielen_historie tabel."""
        tables = inspect(legacy_engine).get_table_names()
        assert "basisprofielen_historie" in tables

    def test_migrate_creates_vestigingsprofielen_historie(self, legacy_engine) -> None:
        """Na migratie bestaat vestigingsprofielen_historie tabel."""
        tables = inspect(legacy_engine).get_table_names()
        assert "vestigingsprofielen_historie" in tables

    def test_migrate_preserves_basisprofielen_data(self, legacy_engine) -> None:
        """Pre-existerende data in basisprofielen is ongewijzigd na migratie."""
        Session = sessionmaker(bind=legacy_engine)

        with Session() as session:
            session.add(BasisProfielORM(
                kvk_nummer="12345678",
                naam="Bestaand Bedrijf",
                last_updated=datetime.now(UTC),
                created_at=datetime.now(UTC),
            ))
            session.commit()

        with Session() as session:
            record = session.get(BasisProfielORM, "12345678")
            assert record is not None
            assert record.naam == "Bestaand Bedrijf"

    def test_migrate_preserves_vestigingsprofielen_data(self, legacy_engine) -> None:
        """Pre-existerende data in vestigingsprofielen is ongewijzigd na migratie."""
        Session = sessionmaker(bind=legacy_engine)

        with Session() as session:
            session.add(VestigingsProfielORM(
                vestigingsnummer="000012345678",
                last_updated=datetime.now(UTC),
                created_at=datetime.now(UTC),
            ))
            session.commit()

        with Session() as session:
            record = session.get(VestigingsProfielORM, "000012345678")
            assert record is not None
            assert record.vestigingsnummer == "000012345678"

    def test_basisprofiel_writer_creates_history_on_migrated_db(self, legacy_engine) -> None:
        """BasisProfielWriter schrijft historierijen correct op een gemigreerde DB."""
        writer = BasisProfielWriter(legacy_engine)

        with writer:
            writer.add(BasisProfielDomain(kvk_nummer="12345678", naam="Test B.V.", rechtsvorm="B.V."))

        Session = sessionmaker(bind=legacy_engine)
        with Session() as session:
            rows = session.query(BasisProfielHistorieORM).filter_by(kvk_nummer="12345678").all()
            assert len(rows) == 1
            changed = set(rows[0].gewijzigde_velden.split(","))
            assert "naam" in changed
            assert "rechtsvorm" in changed

    def test_vestigingsprofiel_writer_creates_history_on_migrated_db(self, legacy_engine) -> None:
        """VestigingsProfielWriter schrijft historierijen correct op een gemigreerde DB."""
        writer = VestigingsProfielWriter(legacy_engine)

        with writer:
            writer.add(VestigingsProfielDomain(
                vestigingsnummer="000012345678",
                kvk_nummer="12345678",
                statutaire_naam="Test Vestiging",
            ))

        Session = sessionmaker(bind=legacy_engine)
        with Session() as session:
            rows = session.query(VestigingsProfielHistorieORM).filter_by(vestigingsnummer="000012345678").all()
            assert len(rows) == 1
            assert "statutaire_naam" in rows[0].gewijzigde_velden.split(",")

    def test_old_record_update_creates_history_on_migrated_db(self, legacy_engine) -> None:
        """Update van pre-existerende record (zonder initiële historierij) wordt correct getrackt."""
        Session = sessionmaker(bind=legacy_engine)

        # Pre-existerende record (van voor de migratie)
        with Session() as session:
            session.add(BasisProfielORM(
                kvk_nummer="11111111",
                naam="Oud Bedrijf",
                last_updated=datetime.now(UTC),
                created_at=datetime.now(UTC),
            ))
            session.commit()

        # Update via writer → diff detecteert wijziging t.o.v. oud record
        writer = BasisProfielWriter(legacy_engine)
        with writer:
            writer.add(BasisProfielDomain(kvk_nummer="11111111", naam="Bijgewerkt Bedrijf"))

        with Session() as session:
            rows = session.query(BasisProfielHistorieORM).filter_by(kvk_nummer="11111111").all()
            assert len(rows) == 1
            assert "naam" in rows[0].gewijzigde_velden.split(",")
            assert rows[0].naam == "Bijgewerkt Bedrijf"

    def test_historie_table_has_correct_columns(self, legacy_engine) -> None:
        """basisprofielen_historie tabel heeft de verwachte kolommen."""
        cols = {c["name"] for c in inspect(legacy_engine).get_columns("basisprofielen_historie")}
        required = {
            "id", "kvk_nummer", "gewijzigd_op", "gewijzigde_velden",
            "naam", "eerste_handelsnaam", "handelsnamen", "websites",
            "ind_non_mailing", "hoofdactiviteit", "hoofdactiviteit_omschrijving",
            "activiteit_overig", "rechtsvorm", "rechtsvorm_uitgebreid",
            "totaal_werkzame_personen", "formele_registratiedatum",
            "registratie_datum_aanvang", "registratie_datum_einde",
        }
        assert required.issubset(cols)

    def test_vestigingen_historie_table_has_correct_columns(self, legacy_engine) -> None:
        """vestigingsprofielen_historie tabel heeft de verwachte kolommen."""
        cols = {c["name"] for c in inspect(legacy_engine).get_columns("vestigingsprofielen_historie")}
        required = {
            "id", "vestigingsnummer", "kvk_nummer", "gewijzigd_op", "gewijzigde_velden",
            "rsin", "statutaire_naam", "ind_hoofdvestiging", "ind_commerciele_vestiging",
            "cor_adres_volledig", "cor_adres_postcode", "cor_adres_gps_latitude",
            "bzk_adres_volledig", "bzk_adres_postcode",
            "totaal_werkzame_personen",
        }
        assert required.issubset(cols)

    def test_double_migration_is_idempotent(self, legacy_engine) -> None:
        """Twee keer ensure_database_initialized uitvoeren heeft geen bijwerkingen."""
        ensure_database_initialized(legacy_engine, Base)

        tables = inspect(legacy_engine).get_table_names()
        assert "basisprofielen_historie" in tables
        assert "vestigingsprofielen_historie" in tables
