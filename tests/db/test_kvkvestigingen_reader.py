"""Tests for KvKVestigingen database reader."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from kvk_connect.db.kvkvestigingen_reader import KvKVestigingenReader
from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.models.orm.vestigingen_orm import VestigingenORM


class TestKvKVestigingenReader:
    """Test suite for KvKVestigingenReader."""

    @pytest.fixture
    def reader(self, db_engine: Engine) -> KvKVestigingenReader:
        return KvKVestigingenReader(db_engine)

    @pytest.fixture
    def basisprofiel(self, db_session: Session) -> BasisProfielORM:
        bp = BasisProfielORM(kvk_nummer="12345678", last_updated=datetime(2024, 1, 1, tzinfo=UTC))
        db_session.add(bp)
        db_session.commit()
        return bp

    # --- get_missing_kvk_nummers ---

    def test_get_missing_kvk_nummers_empty_database(self, reader: KvKVestigingenReader) -> None:
        assert reader.get_missing_kvk_nummers(limit=10) == []

    def test_get_missing_kvk_nummers_found(
        self, db_session: Session, reader: KvKVestigingenReader, basisprofiel: BasisProfielORM
    ) -> None:
        """BasisProfielORM without vestigingen record is returned as missing."""
        missing = reader.get_missing_kvk_nummers(limit=10)
        assert "12345678" in missing

    def test_get_missing_kvk_nummers_not_missing_when_present(
        self, db_session: Session, reader: KvKVestigingenReader, basisprofiel: BasisProfielORM
    ) -> None:
        sentinel = VestigingenORM(
            kvk_nummer="12345678", vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER
        )
        db_session.add(sentinel)
        db_session.commit()

        missing = reader.get_missing_kvk_nummers(limit=10)
        assert "12345678" not in missing

    def test_get_missing_kvk_nummers_respects_limit(
        self, db_session: Session, reader: KvKVestigingenReader
    ) -> None:
        for i in range(5):
            db_session.add(BasisProfielORM(kvk_nummer=f"1234567{i}"))
        db_session.commit()

        missing = reader.get_missing_kvk_nummers(limit=3)
        assert len(missing) <= 3

    def test_get_missing_kvk_nummers_count_empty(self, reader: KvKVestigingenReader) -> None:
        assert reader.get_missing_kvk_nummers_count() == 0

    def test_get_missing_kvk_nummers_count(
        self, db_session: Session, reader: KvKVestigingenReader
    ) -> None:
        for i in range(3):
            db_session.add(BasisProfielORM(kvk_nummer=f"1234567{i}"))
        db_session.commit()

        assert reader.get_missing_kvk_nummers_count() == 3

    def test_get_missing_excludes_tombstone(
        self, db_session: Session, reader: KvKVestigingenReader
    ) -> None:
        """BasisProfielORM with niet_leverbaar_code is not returned as missing."""
        db_session.add(BasisProfielORM(kvk_nummer="12345678", niet_leverbaar_code="IPD0005"))
        db_session.commit()

        assert "12345678" not in reader.get_missing_kvk_nummers(limit=10)

    def test_get_missing_count_excludes_tombstone(
        self, db_session: Session, reader: KvKVestigingenReader
    ) -> None:
        db_session.add(BasisProfielORM(kvk_nummer="12345678", niet_leverbaar_code="IPD0005"))
        db_session.commit()

        assert reader.get_missing_kvk_nummers_count() == 0

    # --- get_outdated_vestigingen ---

    def test_get_outdated_vestigingen_empty_database(self, reader: KvKVestigingenReader) -> None:
        assert reader.get_outdated_vestigingen(limit=10) == []

    def test_get_outdated_vestigingen_count_empty(self, reader: KvKVestigingenReader) -> None:
        assert reader.get_outdated_vestigingen_count() == 0

    def test_get_outdated_vestigingen_found(
        self, db_session: Session, reader: KvKVestigingenReader
    ) -> None:
        """BasisProfielORM newer than vestiging is returned as outdated."""
        db_session.add(
            BasisProfielORM(kvk_nummer="12345678", last_updated=datetime(2024, 2, 1, tzinfo=UTC))
        )
        db_session.add(
            VestigingenORM(
                kvk_nummer="12345678",
                vestigingsnummer="123456789012",
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.commit()

        assert "12345678" in reader.get_outdated_vestigingen(limit=10)

    def test_get_outdated_vestigingen_not_outdated_when_up_to_date(
        self, db_session: Session, reader: KvKVestigingenReader
    ) -> None:
        """BasisProfielORM older than vestiging is not returned as outdated."""
        db_session.add(
            BasisProfielORM(kvk_nummer="12345678", last_updated=datetime(2024, 1, 1, tzinfo=UTC))
        )
        db_session.add(
            VestigingenORM(
                kvk_nummer="12345678",
                vestigingsnummer="123456789012",
                last_updated=datetime(2024, 2, 1, tzinfo=UTC),
            )
        )
        db_session.commit()

        assert "12345678" not in reader.get_outdated_vestigingen(limit=10)

    def test_get_outdated_vestigingen_count(
        self, db_session: Session, reader: KvKVestigingenReader
    ) -> None:
        db_session.add(
            BasisProfielORM(kvk_nummer="12345678", last_updated=datetime(2024, 2, 1, tzinfo=UTC))
        )
        db_session.add(
            VestigingenORM(
                kvk_nummer="12345678",
                vestigingsnummer="123456789012",
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.commit()

        assert reader.get_outdated_vestigingen_count() == 1

    def test_get_outdated_vestigingen_respects_limit(
        self, db_session: Session, reader: KvKVestigingenReader
    ) -> None:
        for i in range(5):
            db_session.add(
                BasisProfielORM(
                    kvk_nummer=f"1234567{i}", last_updated=datetime(2024, 2, 1, tzinfo=UTC)
                )
            )
            db_session.add(
                VestigingenORM(
                    kvk_nummer=f"1234567{i}",
                    vestigingsnummer=f"00000000000{i}",
                    last_updated=datetime(2024, 1, 1, tzinfo=UTC),
                )
            )
        db_session.commit()

        assert len(reader.get_outdated_vestigingen(limit=3)) <= 3

    # --- tombstone filtering for get_outdated_vestigingen ---

    def test_get_outdated_excludes_tombstone(
        self, db_session: Session, reader: KvKVestigingenReader
    ) -> None:
        """Sentinel row with niet_leverbaar_code blocks the KVK nummer from outdated list."""
        db_session.add(
            BasisProfielORM(kvk_nummer="12345678", last_updated=datetime(2024, 2, 1, tzinfo=UTC))
        )
        db_session.add(
            VestigingenORM(
                kvk_nummer="12345678",
                vestigingsnummer="123456789012",
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.add(
            VestigingenORM(
                kvk_nummer="12345678",
                vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
                niet_leverbaar_code="IPD0005",
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.commit()

        assert "12345678" not in reader.get_outdated_vestigingen(limit=10)

    def test_get_outdated_count_excludes_tombstone(
        self, db_session: Session, reader: KvKVestigingenReader
    ) -> None:
        db_session.add(
            BasisProfielORM(kvk_nummer="12345678", last_updated=datetime(2024, 2, 1, tzinfo=UTC))
        )
        db_session.add(
            VestigingenORM(
                kvk_nummer="12345678",
                vestigingsnummer="123456789012",
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.add(
            VestigingenORM(
                kvk_nummer="12345678",
                vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
                niet_leverbaar_code="IPD0005",
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.commit()

        assert reader.get_outdated_vestigingen_count() == 0

    # --- retry_after filtering for get_outdated_vestigingen ---

    def test_get_outdated_excludes_active_retry_after(
        self, db_session: Session, reader: KvKVestigingenReader
    ) -> None:
        """Sentinel row with future retry_after blocks the KVK nummer."""
        db_session.add(
            BasisProfielORM(kvk_nummer="12345678", last_updated=datetime(2024, 2, 1, tzinfo=UTC))
        )
        db_session.add(
            VestigingenORM(
                kvk_nummer="12345678",
                vestigingsnummer="123456789012",
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.add(
            VestigingenORM(
                kvk_nummer="12345678",
                vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
                retry_after=datetime.now(UTC) + timedelta(hours=24),
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.commit()

        assert "12345678" not in reader.get_outdated_vestigingen(limit=10)

    def test_get_outdated_includes_expired_retry_after(
        self, db_session: Session, reader: KvKVestigingenReader
    ) -> None:
        """Sentinel row with expired retry_after does not block the KVK nummer."""
        db_session.add(
            BasisProfielORM(kvk_nummer="12345678", last_updated=datetime(2024, 2, 1, tzinfo=UTC))
        )
        db_session.add(
            VestigingenORM(
                kvk_nummer="12345678",
                vestigingsnummer="123456789012",
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.add(
            VestigingenORM(
                kvk_nummer="12345678",
                vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
                retry_after=datetime.now(UTC) - timedelta(hours=1),
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.commit()

        assert "12345678" in reader.get_outdated_vestigingen(limit=10)
