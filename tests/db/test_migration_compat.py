"""Migration compatibility tests: old schema → new schema coexistence.

These tests simulate what happens when Watchtower pulls a new Docker image while
the existing database still has the old schema (missing new columns).  They use
real in-memory SQLite with no mocks.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from kvk_connect.db.basisprofiel_writer import BasisProfielWriter
from kvk_connect.db.init import _migrate_backfill_status, ensure_database_initialized
from kvk_connect.db.vestigingsprofiel_writer import VestigingsProfielWriter
from kvk_connect.mappers.kvk_record_mapper import map_kvkbasisprofiel_api_to_kvkrecord
from kvk_connect.models.api.basisprofiel_api import BasisProfielAPI
from kvk_connect.models.domain.basisprofiel import BasisProfielDomain
from kvk_connect.models.enums import KVKStatus
from kvk_connect.models.orm.base import Base
from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.models.orm.vestigingsprofiel_orm import VestigingsProfielORM


@pytest.fixture
def migrated_engine():
    """Engine that starts with an old schema (missing new columns) and then migrates."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})

    # Simulate old schema: only the original columns of basisprofielen
    with engine.connect() as conn:
        conn.execute(text(
            "CREATE TABLE basisprofielen ("
            "  kvkNummer TEXT PRIMARY KEY,"
            "  naam TEXT,"
            "  hoofdactiviteit TEXT,"
            "  hoofdactiviteitOmschrijving TEXT,"
            "  activiteitOverig TEXT,"
            "  rechtsvorm TEXT,"
            "  rechtsvormUitgebreid TEXT,"
            "  eersteHandelsnaam TEXT,"
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
        conn.commit()

    # Run migration — new columns (indNonMailing, formeleRegistratiedatum, handelsnamen) are added
    ensure_database_initialized(engine, Base)

    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


class TestMigrationCompat:
    """Tests that verify old and new schema records coexist correctly."""

    def test_migrate_adds_new_basisprofiel_columns(self, migrated_engine) -> None:
        """After migration, new columns exist in basisprofielen."""
        cols = {c["name"] for c in inspect(migrated_engine).get_columns("basisprofielen")}
        assert "indNonMailing" in cols
        assert "formeleRegistratiedatum" in cols
        assert "handelsnamen" in cols
        assert "status" in cols

    def test_old_record_coexists_with_new_record(
        self, migrated_engine, mock_kvk_basisprofiel_response: dict
    ) -> None:
        """A record written before migration and one after both remain readable."""
        Session = sessionmaker(bind=migrated_engine)

        # Write a 'pre-migration' record: new fields are NULL
        with Session() as session:
            old_record = BasisProfielORM(
                kvk_nummer="11111111",
                naam="Old Company",
                last_updated=datetime.now(UTC),
                created_at=datetime.now(UTC),
            )
            session.add(old_record)
            session.commit()

        # Write a 'post-migration' record: all fields filled
        writer = BasisProfielWriter(migrated_engine)
        api = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api)

        with writer:
            writer.add(domain)

        with Session() as session:
            old = session.get(BasisProfielORM, "11111111")
            assert old is not None
            assert old.naam == "Old Company"
            assert old.ind_non_mailing is None  # pre-migration NULL

            new = session.get(BasisProfielORM, "12345678")
            assert new is not None
            assert new.ind_non_mailing == "Nee"
            assert new.handelsnamen == "Test Company, Test Services"

    def test_merge_adds_new_fields_to_existing_record(self, migrated_engine) -> None:
        """Updating an existing 'old' record fills in the new fields."""
        Session = sessionmaker(bind=migrated_engine)

        # Insert old-style record with NULL for new fields
        with Session() as session:
            session.add(BasisProfielORM(
                kvk_nummer="12345678",
                naam="Test B.V.",
                last_updated=datetime.now(UTC),
                created_at=datetime.now(UTC),
            ))
            session.commit()

        # Now write updated domain with new fields
        writer = BasisProfielWriter(migrated_engine)
        domain = BasisProfielDomain(
            kvk_nummer="12345678",
            naam="Test B.V.",
            ind_non_mailing="Nee",
            handelsnamen="Test Company",
        )
        with writer:
            writer.add(domain)

        with Session() as session:
            record = session.get(BasisProfielORM, "12345678")
            assert record is not None
            assert record.ind_non_mailing == "Nee"
            assert record.handelsnamen == "Test Company"

    def test_merge_preserves_new_fields_after_tombstone(
        self, migrated_engine, mock_kvk_basisprofiel_response: dict
    ) -> None:
        """Tombstoning a record must not erase the new fields (regression guard)."""
        writer = BasisProfielWriter(migrated_engine)
        api = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api)

        with writer:
            writer.add(domain)

        with writer:
            writer.mark_uitgeschreven("12345678", "IPD0005")

        Session = sessionmaker(bind=migrated_engine)
        with Session() as session:
            record = session.get(BasisProfielORM, "12345678")
            assert record is not None
            assert record.niet_leverbaar_code == "IPD0005"
            assert record.ind_non_mailing == "Nee"
            assert record.handelsnamen == "Test Company, Test Services"


class TestBackfillStatus:
    """Tests die de _migrate_backfill_status() logica verifiëren op pre-migratie data."""

    def _insert_basisprofiel(self, engine, kvk_nummer: str, **extra_cols) -> None:
        col_names = ", ".join(["kvkNummer", "naam", "last_updated", *extra_cols.keys()])
        placeholders = ", ".join([":kvk", ":naam", "CURRENT_TIMESTAMP", *[f":{k}" for k in extra_cols]])
        with engine.connect() as conn:
            conn.execute(
                text(f"INSERT INTO basisprofielen ({col_names}) VALUES ({placeholders})"),  # noqa: S608
                {"kvk": kvk_nummer, "naam": "Test", **extra_cols},
            )
            conn.commit()

    def _insert_vestigingsprofiel(self, engine, vestigingsnummer: str, **extra_cols) -> None:
        col_names = ", ".join(["vestigingsnummer", "created_at", "last_updated", *extra_cols.keys()])
        placeholders = ", ".join([":v", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP", *[f":{k}" for k in extra_cols]])
        with engine.connect() as conn:
            conn.execute(
                text(f"INSERT INTO vestigingsprofielen ({col_names}) VALUES ({placeholders})"),  # noqa: S608
                {"v": vestigingsnummer, **extra_cols},
            )
            conn.commit()

    def test_backfill_actief_voor_normaal_record(self, migrated_engine) -> None:
        """Record zonder code en zonder retry_after krijgt status ACTIEF."""
        self._insert_basisprofiel(migrated_engine, "99999991")
        _migrate_backfill_status(migrated_engine)

        Session = sessionmaker(bind=migrated_engine)
        with Session() as session:
            assert session.get(BasisProfielORM, "99999991").status == KVKStatus.ACTIEF

    def test_backfill_uitgeschreven_voor_niet_leverbaar_code(self, migrated_engine) -> None:
        """Record met niet_leverbaar_code krijgt status UITGESCHREVEN."""
        self._insert_basisprofiel(migrated_engine, "99999992", niet_leverbaar_code="IPD0005")
        _migrate_backfill_status(migrated_engine)

        Session = sessionmaker(bind=migrated_engine)
        with Session() as session:
            assert session.get(BasisProfielORM, "99999992").status == KVKStatus.UITGESCHREVEN

    def test_backfill_tijdelijk_voor_actieve_retry(self, migrated_engine) -> None:
        """Record met retry_after in de toekomst krijgt status TIJDELIJK_NIET_BESCHIKBAAR."""
        future = (datetime.now(UTC) + timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        self._insert_basisprofiel(migrated_engine, "99999993", retry_after=future)
        _migrate_backfill_status(migrated_engine)

        Session = sessionmaker(bind=migrated_engine)
        with Session() as session:
            assert session.get(BasisProfielORM, "99999993").status == KVKStatus.TIJDELIJK_NIET_BESCHIKBAAR

    def test_backfill_actief_voor_verlopen_retry(self, migrated_engine) -> None:
        """Record met verlopen retry_after (geen code) krijgt status ACTIEF."""
        past = (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        self._insert_basisprofiel(migrated_engine, "99999994", retry_after=past)
        _migrate_backfill_status(migrated_engine)

        Session = sessionmaker(bind=migrated_engine)
        with Session() as session:
            assert session.get(BasisProfielORM, "99999994").status == KVKStatus.ACTIEF

    def test_backfill_uitgeschreven_voor_registratie_datum_einde(self, migrated_engine) -> None:
        """Basisprofiel met RegistratieDatumEinde gevuld krijgt status UITGESCHREVEN."""
        self._insert_basisprofiel(migrated_engine, "99999995", RegistratieDatumEinde="2020-01-01")
        _migrate_backfill_status(migrated_engine)

        Session = sessionmaker(bind=migrated_engine)
        with Session() as session:
            assert session.get(BasisProfielORM, "99999995").status == KVKStatus.UITGESCHREVEN

    def test_backfill_uitgeschreven_voor_vestigingsprofiel_datum_einde(self, migrated_engine) -> None:
        """Vestigingsprofiel met RegistratieDatumEindeVestiging gevuld krijgt status UITGESCHREVEN."""
        self._insert_vestigingsprofiel(
            migrated_engine, "123456789012", RegistratieDatumEindeVestiging="2020-01-01"
        )
        _migrate_backfill_status(migrated_engine)

        Session = sessionmaker(bind=migrated_engine)
        with Session() as session:
            assert session.get(VestigingsProfielORM, "123456789012").status == KVKStatus.UITGESCHREVEN

    def test_backfill_idempotent(self, migrated_engine) -> None:
        """Twee keer draaien geeft hetzelfde resultaat — bestaande statussen worden niet overschreven."""
        self._insert_basisprofiel(migrated_engine, "99999996", niet_leverbaar_code="IPD0005")
        _migrate_backfill_status(migrated_engine)
        _migrate_backfill_status(migrated_engine)

        Session = sessionmaker(bind=migrated_engine)
        with Session() as session:
            assert session.get(BasisProfielORM, "99999996").status == KVKStatus.UITGESCHREVEN
