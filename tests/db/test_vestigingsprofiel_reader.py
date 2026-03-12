"""Tests for VestigingsProfiel database reader."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from kvk_connect.db.vestigingenprofiel_reader import VestigingsProfielReader
from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.models.orm.signaal_orm import SignaalORM
from kvk_connect.models.orm.vestigingen_orm import VestigingenORM
from kvk_connect.models.orm.vestigingsprofiel_orm import VestigingsProfielORM


class TestVestigingsProfielReader:
    """Test suite for VestigingsProfielReader."""

    @pytest.fixture
    def reader(self, db_engine: Engine) -> VestigingsProfielReader:
        return VestigingsProfielReader(db_engine)

    @pytest.fixture
    def vestiging(self, db_session: Session) -> VestigingenORM:
        """Create a basisprofiel + vestiging for use as FK parent."""
        bp = BasisProfielORM(kvk_nummer="12345678")
        db_session.add(bp)
        db_session.flush()
        v = VestigingenORM(kvk_nummer="12345678", vestigingsnummer="123456789012")
        db_session.add(v)
        db_session.commit()
        return v

    # --- get_vestigingen_zonder_vestigingsprofielen ---

    def test_get_vestigingen_zonder_profielen_empty(self, reader: VestigingsProfielReader) -> None:
        assert reader.get_vestigingen_zonder_vestigingsprofielen(limit=10) == []

    def test_get_vestigingen_zonder_profielen_found(
        self, db_session: Session, reader: VestigingsProfielReader, vestiging: VestigingenORM
    ) -> None:
        """Vestiging without a profile is returned as missing."""
        missing = reader.get_vestigingen_zonder_vestigingsprofielen(limit=10)
        assert "123456789012" in missing

    def test_get_vestigingen_zonder_profielen_not_missing_when_present(
        self, db_session: Session, reader: VestigingsProfielReader, vestiging: VestigingenORM
    ) -> None:
        db_session.add(VestigingsProfielORM(vestigingsnummer="123456789012"))
        db_session.commit()

        missing = reader.get_vestigingen_zonder_vestigingsprofielen(limit=10)
        assert "123456789012" not in missing

    def test_get_vestigingen_zonder_profielen_excludes_sentinel(
        self, db_session: Session, reader: VestigingsProfielReader
    ) -> None:
        """Sentinel vestigingsnummer never appears as missing."""
        bp = BasisProfielORM(kvk_nummer="12345678")
        db_session.add(bp)
        db_session.flush()
        db_session.add(
            VestigingenORM(
                kvk_nummer="12345678",
                vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
            )
        )
        db_session.commit()

        missing = reader.get_vestigingen_zonder_vestigingsprofielen(limit=10)
        assert VestigingenORM.SENTINEL_VESTIGINGSNUMMER not in missing

    def test_get_vestigingen_zonder_profielen_count_empty(
        self, reader: VestigingsProfielReader
    ) -> None:
        assert reader.get_vestigingen_zonder_vestigingsprofielen_count() == 0

    def test_get_vestigingen_zonder_profielen_count(
        self, db_session: Session, reader: VestigingsProfielReader
    ) -> None:
        bp = BasisProfielORM(kvk_nummer="12345678")
        db_session.add(bp)
        db_session.flush()
        for i in range(1, 4):
            db_session.add(
                VestigingenORM(kvk_nummer="12345678", vestigingsnummer=f"00000000000{i}")
            )
        db_session.commit()

        assert reader.get_vestigingen_zonder_vestigingsprofielen_count() == 3

    def test_get_vestigingen_zonder_profielen_respects_limit(
        self, db_session: Session, reader: VestigingsProfielReader
    ) -> None:
        bp = BasisProfielORM(kvk_nummer="12345678")
        db_session.add(bp)
        db_session.flush()
        for i in range(5):
            db_session.add(
                VestigingenORM(kvk_nummer="12345678", vestigingsnummer=f"00000000000{i}")
            )
        db_session.commit()

        assert len(reader.get_vestigingen_zonder_vestigingsprofielen(limit=3)) <= 3

    # --- get_outdated_vestigingen ---

    def test_get_outdated_vestigingen_empty(self, reader: VestigingsProfielReader) -> None:
        assert reader.get_outdated_vestigingen(limit=10) == []

    def test_get_outdated_vestigingen_count_empty(self, reader: VestigingsProfielReader) -> None:
        assert reader.get_outdated_vestigingen_count() == 0

    def test_get_outdated_vestigingen_found(
        self, db_session: Session, reader: VestigingsProfielReader, vestiging: VestigingenORM
    ) -> None:
        """Vestiging newer than its profile is returned as outdated."""
        vestiging.last_updated = datetime(2024, 2, 1, tzinfo=UTC)
        db_session.add(
            VestigingsProfielORM(
                vestigingsnummer="123456789012",
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.commit()

        assert "123456789012" in reader.get_outdated_vestigingen(limit=10)

    def test_get_outdated_vestigingen_not_outdated_when_up_to_date(
        self, db_session: Session, reader: VestigingsProfielReader, vestiging: VestigingenORM
    ) -> None:
        vestiging.last_updated = datetime(2024, 1, 1, tzinfo=UTC)
        db_session.add(
            VestigingsProfielORM(
                vestigingsnummer="123456789012",
                last_updated=datetime(2024, 2, 1, tzinfo=UTC),
            )
        )
        db_session.commit()

        assert "123456789012" not in reader.get_outdated_vestigingen(limit=10)

    def test_get_outdated_vestigingen_count(
        self, db_session: Session, reader: VestigingsProfielReader, vestiging: VestigingenORM
    ) -> None:
        vestiging.last_updated = datetime(2024, 2, 1, tzinfo=UTC)
        db_session.add(
            VestigingsProfielORM(
                vestigingsnummer="123456789012",
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.commit()

        assert reader.get_outdated_vestigingen_count() == 1

    def test_get_outdated_vestigingen_excludes_tombstone(
        self, db_session: Session, reader: VestigingsProfielReader, vestiging: VestigingenORM
    ) -> None:
        """Profile with niet_leverbaar_code is excluded from outdated."""
        vestiging.last_updated = datetime(2024, 2, 1, tzinfo=UTC)
        db_session.add(
            VestigingsProfielORM(
                vestigingsnummer="123456789012",
                niet_leverbaar_code="IPD0005",
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.commit()

        assert "123456789012" not in reader.get_outdated_vestigingen(limit=10)
        assert reader.get_outdated_vestigingen_count() == 0

    def test_get_outdated_vestigingen_excludes_active_retry_after(
        self, db_session: Session, reader: VestigingsProfielReader, vestiging: VestigingenORM
    ) -> None:
        """Profile with unexpired retry_after is excluded from outdated."""
        vestiging.last_updated = datetime(2024, 2, 1, tzinfo=UTC)
        db_session.add(
            VestigingsProfielORM(
                vestigingsnummer="123456789012",
                retry_after=datetime.now(UTC) + timedelta(hours=24),
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.commit()

        assert "123456789012" not in reader.get_outdated_vestigingen(limit=10)

    def test_get_outdated_vestigingen_includes_expired_retry_after(
        self, db_session: Session, reader: VestigingsProfielReader, vestiging: VestigingenORM
    ) -> None:
        """Profile with expired retry_after reappears as outdated."""
        vestiging.last_updated = datetime(2024, 2, 1, tzinfo=UTC)
        db_session.add(
            VestigingsProfielORM(
                vestigingsnummer="123456789012",
                retry_after=datetime.now(UTC) - timedelta(hours=1),
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.commit()

        assert "123456789012" in reader.get_outdated_vestigingen(limit=10)

    # --- get_outdated_vestigingen_signaal ---

    def test_get_outdated_vestigingen_signaal_empty(
        self, reader: VestigingsProfielReader
    ) -> None:
        assert reader.get_outdated_vestigingen_signaal(limit=10) == []

    def test_get_outdated_vestigingen_signaal_count_empty(
        self, reader: VestigingsProfielReader
    ) -> None:
        assert reader.get_outdated_vestigingen_signaal_count() == 0

    def test_get_outdated_vestigingen_signaal_found(
        self, db_session: Session, reader: VestigingsProfielReader
    ) -> None:
        """Vestiging with signal newer than its profile is returned."""
        db_session.add(
            VestigingsProfielORM(
                vestigingsnummer="123456789012",
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.add(
            SignaalORM(
                id="s1",
                kvknummer="12345678",
                vestigingsnummer="123456789012",
                timestamp=datetime(2024, 2, 1, tzinfo=UTC),
                signaal_type="UPDATE",
            )
        )
        db_session.commit()

        result = reader.get_outdated_vestigingen_signaal(limit=10)
        assert "123456789012" in result

    def test_get_outdated_vestigingen_signaal_excludes_tombstone(
        self, db_session: Session, reader: VestigingsProfielReader
    ) -> None:
        db_session.add(
            VestigingsProfielORM(
                vestigingsnummer="123456789012",
                niet_leverbaar_code="IPD0005",
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.add(
            SignaalORM(
                id="s1",
                kvknummer="12345678",
                vestigingsnummer="123456789012",
                timestamp=datetime(2024, 2, 1, tzinfo=UTC),
                signaal_type="UPDATE",
            )
        )
        db_session.commit()

        assert "123456789012" not in reader.get_outdated_vestigingen_signaal(limit=10)
        assert reader.get_outdated_vestigingen_signaal_count() == 0

    def test_get_outdated_vestigingen_signaal_excludes_active_retry_after(
        self, db_session: Session, reader: VestigingsProfielReader
    ) -> None:
        db_session.add(
            VestigingsProfielORM(
                vestigingsnummer="123456789012",
                retry_after=datetime.now(UTC) + timedelta(hours=24),
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.add(
            SignaalORM(
                id="s1",
                kvknummer="12345678",
                vestigingsnummer="123456789012",
                timestamp=datetime(2024, 2, 1, tzinfo=UTC),
                signaal_type="UPDATE",
            )
        )
        db_session.commit()

        assert "123456789012" not in reader.get_outdated_vestigingen_signaal(limit=10)

    def test_get_outdated_vestigingen_signaal_includes_expired_retry_after(
        self, db_session: Session, reader: VestigingsProfielReader
    ) -> None:
        db_session.add(
            VestigingsProfielORM(
                vestigingsnummer="123456789012",
                retry_after=datetime.now(UTC) - timedelta(hours=1),
                last_updated=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        db_session.add(
            SignaalORM(
                id="s1",
                kvknummer="12345678",
                vestigingsnummer="123456789012",
                timestamp=datetime(2024, 2, 1, tzinfo=UTC),
                signaal_type="UPDATE",
            )
        )
        db_session.commit()

        assert "123456789012" in reader.get_outdated_vestigingen_signaal(limit=10)
