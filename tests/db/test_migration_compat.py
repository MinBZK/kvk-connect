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
from kvk_connect.db.init import ensure_database_initialized
from kvk_connect.db.vestigingsprofiel_writer import VestigingsProfielWriter
from kvk_connect.mappers.kvk_record_mapper import map_kvkbasisprofiel_api_to_kvkrecord
from kvk_connect.models.api.basisprofiel_api import BasisProfielAPI
from kvk_connect.models.domain.basisprofiel import BasisProfielDomain
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
            writer.mark_niet_leverbaar("12345678", "IPD0005")

        Session = sessionmaker(bind=migrated_engine)
        with Session() as session:
            record = session.get(BasisProfielORM, "12345678")
            assert record is not None
            assert record.niet_leverbaar_code == "IPD0005"
            assert record.ind_non_mailing == "Nee"
            assert record.handelsnamen == "Test Company, Test Services"
