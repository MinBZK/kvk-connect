"""Tests for VestigingenORM and foreign key constraints."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.models.orm.vestigingen_orm import VestigingenORM
from kvk_connect.models.orm.vestigingsprofiel_orm import VestigingsProfielORM


@pytest.fixture
def basisprofiel(db_session: Session) -> BasisProfielORM:
    """Create a test basisprofiel."""
    bp = BasisProfielORM(kvk_nummer="12345678")
    db_session.add(bp)
    db_session.commit()
    return bp


@pytest.fixture
def vestiging(db_session: Session, basisprofiel: BasisProfielORM) -> VestigingenORM:
    """Create a test vestiging."""
    v = VestigingenORM(
        kvk_nummer=basisprofiel.kvk_nummer,
        vestigingsnummer="123456789012"
    )
    db_session.add(v)
    db_session.commit()
    return v


class TestVestigingenForeignKey:
    """Test foreign key constraints on VestigingenORM."""

    def test_vestiging_requires_existing_basisprofiel(
        self, db_session: Session
    ) -> None:
        """Vestiging must reference existing basisprofiel."""
        v = VestigingenORM(kvk_nummer="99999999", vestigingsnummer="000000000000")
        db_session.add(v)

        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_vestiging_cascade_delete_on_basisprofiel(
        self, db_session: Session, basisprofiel: BasisProfielORM, vestiging: VestigingenORM
    ) -> None:
        """Deleting basisprofiel cascades to vestigingen."""
        vestiging_id = vestiging.vestigingsnummer
        db_session.delete(basisprofiel)
        db_session.commit()

        result = db_session.query(VestigingenORM).filter_by(
            vestigingsnummer=vestiging_id
        ).first()
        assert result is None