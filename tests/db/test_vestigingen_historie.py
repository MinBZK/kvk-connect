"""Tests voor KvKVestigingenWriter historielogica.

Alle tests gebruiken echte in-memory SQLite — geen mocks.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from kvk_connect.db.kvkvestigingen_writer import KvKVestigingenWriter
from kvk_connect.models.domain.kvkvestigingsnummersdomain import KvKVestigingsNummersDomain
from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.models.orm.vestigingen_historie_orm import VestigingenHistorieORM
from kvk_connect.models.orm.vestigingen_orm import VestigingenORM


def _domain(kvk_nummer: str = "12345678", vestigingsnummers: list[str] | None = None) -> KvKVestigingsNummersDomain:
    return KvKVestigingsNummersDomain(kvk_nummer=kvk_nummer, vestigingsnummers=vestigingsnummers or [])


class TestVestigingenHistorie:
    """Historielogica voor KvKVestigingenWriter."""

    @pytest.fixture
    def writer(self, db_engine: Engine) -> KvKVestigingenWriter:
        return KvKVestigingenWriter(db_engine)

    @pytest.fixture
    def session(self, db_engine: Engine) -> Session:
        s = sessionmaker(bind=db_engine)()
        yield s
        s.close()

    @pytest.fixture(autouse=True)
    def basisprofiel(self, db_engine: Engine) -> None:
        """Insert BasisProfielORM rijen om aan de FK-constraint te voldoen."""
        s = sessionmaker(bind=db_engine)()
        for kvk in ("12345678", "11111111", "22222222"):
            s.merge(BasisProfielORM(kvk_nummer=kvk, last_updated=datetime(2024, 1, 1, tzinfo=UTC)))
        s.commit()
        s.close()

    def _historie(
        self, session: Session, kvk_nummer: str = "12345678"
    ) -> list[VestigingenHistorieORM]:
        return (
            session.query(VestigingenHistorieORM)
            .filter_by(kvk_nummer=kvk_nummer)
            .order_by(VestigingenHistorieORM.gewijzigd_op, VestigingenHistorieORM.id)
            .all()
        )

    # --- Eerste fetch ---

    def test_first_fetch_two_vestigingen_creates_two_toevoegd_rows(
        self, writer: KvKVestigingenWriter, session: Session
    ) -> None:
        """Eerste fetch met 2 vestigingen → 2 'toevoegd' historierijen."""
        with writer:
            writer.add(_domain(vestigingsnummers=["000011111111", "000022222222"]))

        rows = self._historie(session)
        assert len(rows) == 2
        assert all(r.event_type == "toevoegd" for r in rows)
        vestigingsnummers = {r.vestigingsnummer for r in rows}
        assert vestigingsnummers == {"000011111111", "000022222222"}

    def test_first_fetch_kvk_nummer_stored_in_history(
        self, writer: KvKVestigingenWriter, session: Session
    ) -> None:
        """kvk_nummer wordt correct opgeslagen in historierij."""
        with writer:
            writer.add(_domain(kvk_nummer="12345678", vestigingsnummers=["000011111111"]))

        row = session.query(VestigingenHistorieORM).first()
        assert row is not None
        assert row.kvk_nummer == "12345678"
        assert row.vestigingsnummer == "000011111111"
        assert row.gewijzigd_op is not None

    # --- Re-fetch zonder wijziging ---

    def test_refetch_identical_set_no_history_rows(
        self, writer: KvKVestigingenWriter, session: Session
    ) -> None:
        """Identieke re-fetch genereert géén nieuwe historierij."""
        with writer:
            writer.add(_domain(vestigingsnummers=["000011111111"]))
        with writer:
            writer.add(_domain(vestigingsnummers=["000011111111"]))

        rows = self._historie(session)
        assert len(rows) == 1
        assert rows[0].event_type == "toevoegd"

    def test_three_identical_fetches_single_history_row(
        self, writer: KvKVestigingenWriter, session: Session
    ) -> None:
        """3× dezelfde set → nog steeds 1 historierij."""
        for _ in range(3):
            with writer:
                writer.add(_domain(vestigingsnummers=["000011111111"]))

        rows = self._historie(session)
        assert len(rows) == 1

    # --- Vestiging toegevoegd ---

    def test_new_vestiging_added_creates_toevoegd_row(
        self, writer: KvKVestigingenWriter, session: Session
    ) -> None:
        """Nieuwe vestiging in de set → 'toevoegd' historierij."""
        with writer:
            writer.add(_domain(vestigingsnummers=["000011111111"]))
        with writer:
            writer.add(_domain(vestigingsnummers=["000011111111", "000022222222"]))

        rows = self._historie(session)
        assert len(rows) == 2
        event_types = {r.vestigingsnummer: r.event_type for r in rows}
        assert event_types["000011111111"] == "toevoegd"
        assert event_types["000022222222"] == "toevoegd"

    # --- Vestiging verwijderd ---

    def test_vestiging_removed_creates_verwijderd_row(
        self, writer: KvKVestigingenWriter, session: Session
    ) -> None:
        """Vestiging verdwijnt uit de set → 'verwijderd' historierij."""
        with writer:
            writer.add(_domain(vestigingsnummers=["000011111111", "000022222222"]))
        with writer:
            writer.add(_domain(vestigingsnummers=["000011111111"]))

        rows = self._historie(session)
        assert len(rows) == 3
        verwijderd = [r for r in rows if r.event_type == "verwijderd"]
        assert len(verwijderd) == 1
        assert verwijderd[0].vestigingsnummer == "000022222222"

    def test_removed_vestiging_deleted_from_vestigingen_table(
        self, writer: KvKVestigingenWriter, session: Session
    ) -> None:
        """Verwijderde vestiging wordt ook uit de vestigingen tabel verwijderd."""
        with writer:
            writer.add(_domain(vestigingsnummers=["000011111111", "000022222222"]))
        with writer:
            writer.add(_domain(vestigingsnummers=["000011111111"]))

        remaining = (
            session.query(VestigingenORM)
            .filter(
                VestigingenORM.kvk_nummer == "12345678",
                VestigingenORM.vestigingsnummer != VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
            )
            .all()
        )
        assert len(remaining) == 1
        assert remaining[0].vestigingsnummer == "000011111111"

    def test_all_vestigingen_removed_creates_verwijderd_rows(
        self, writer: KvKVestigingenWriter, session: Session
    ) -> None:
        """Alle vestigingen verwijderd → alle 'verwijderd' historierijen."""
        with writer:
            writer.add(_domain(vestigingsnummers=["000011111111", "000022222222"]))
        with writer:
            writer.add(_domain(vestigingsnummers=[]))

        rows = self._historie(session)
        verwijderd = [r for r in rows if r.event_type == "verwijderd"]
        assert len(verwijderd) == 2

    # --- Sentinel ---

    def test_sentinel_not_in_history(
        self, writer: KvKVestigingenWriter, session: Session
    ) -> None:
        """Sentinel-vestiging (000000000000) verschijnt nooit in historietabel."""
        with writer:
            writer.add(_domain(vestigingsnummers=[VestigingenORM.SENTINEL_VESTIGINGSNUMMER]))

        rows = session.query(VestigingenHistorieORM).all()
        assert rows == []

    def test_empty_list_uses_sentinel_no_history(
        self, writer: KvKVestigingenWriter, session: Session
    ) -> None:
        """Lege vestigingsnummers-lijst → sentinel in tabel, géén historierij."""
        with writer:
            writer.add(_domain(vestigingsnummers=[]))

        rows = session.query(VestigingenHistorieORM).all()
        assert rows == []

        sentinel = (
            session.query(VestigingenORM)
            .filter_by(
                kvk_nummer="12345678",
                vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
            )
            .first()
        )
        assert sentinel is not None

    def test_sentinel_not_deleted_when_vestigingen_removed(
        self, writer: KvKVestigingenWriter, db_engine: Engine, session: Session
    ) -> None:
        """Sentinel-rij wordt nooit verwijderd door set-diff logica."""
        # Zet sentinel expliciet in de tabel
        s = sessionmaker(bind=db_engine)()
        s.merge(
            VestigingenORM(
                kvk_nummer="12345678",
                vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
                last_updated=datetime.now(UTC),
            )
        )
        s.commit()
        s.close()

        # Voeg echte vestiging toe dan verwijder ze weer
        with writer:
            writer.add(_domain(vestigingsnummers=["000011111111"]))
        with writer:
            writer.add(_domain(vestigingsnummers=[]))

        sentinel = (
            session.query(VestigingenORM)
            .filter_by(
                kvk_nummer="12345678",
                vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
            )
            .first()
        )
        assert sentinel is not None

    # --- Rollback ---

    def test_rollback_no_history_persisted(self, db_engine: Engine) -> None:
        """Exception in writer-context → géén historierij in DB."""
        writer = KvKVestigingenWriter(db_engine, batch_size=10)

        try:
            with writer:
                writer.add(_domain(vestigingsnummers=["000011111111"]))
                raise ValueError("Simulated error")
        except ValueError:
            pass

        fresh = sessionmaker(bind=db_engine)()
        try:
            rows = fresh.query(VestigingenHistorieORM).all()
            assert rows == []
        finally:
            fresh.close()

    # --- Meerdere bedrijven onafhankelijk ---

    def test_history_for_multiple_companies_independent(
        self, writer: KvKVestigingenWriter, session: Session
    ) -> None:
        """Historierijen voor verschillende bedrijven zijn onafhankelijk."""
        with writer:
            writer.add(_domain(kvk_nummer="11111111", vestigingsnummers=["000011111111"]))
            writer.add(_domain(kvk_nummer="22222222", vestigingsnummers=["000022222222"]))

        rows_a = self._historie(session, "11111111")
        rows_b = self._historie(session, "22222222")
        assert len(rows_a) == 1
        assert len(rows_b) == 1
        assert rows_a[0].vestigingsnummer == "000011111111"
        assert rows_b[0].vestigingsnummer == "000022222222"

    # --- Vestigingen tabel intact ---

    def test_vestigingen_table_updated_after_history_write(
        self, writer: KvKVestigingenWriter, session: Session
    ) -> None:
        """Na het schrijven van historie zijn de vestigingen correct bijgewerkt."""
        with writer:
            writer.add(_domain(vestigingsnummers=["000011111111", "000022222222"]))
        with writer:
            writer.add(_domain(vestigingsnummers=["000011111111", "000033333333"]))

        remaining = (
            session.query(VestigingenORM)
            .filter(
                VestigingenORM.kvk_nummer == "12345678",
                VestigingenORM.vestigingsnummer != VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
            )
            .all()
        )
        vestigingsnummers = {r.vestigingsnummer for r in remaining}
        assert vestigingsnummers == {"000011111111", "000033333333"}
