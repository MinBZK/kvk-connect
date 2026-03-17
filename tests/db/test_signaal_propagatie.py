"""Integratietest: signaal → basisprofiel → vestigingen → vestigingsprofiel keten.

Gebruikt echte SQLite in-memory database. Alleen KVKApiClient is gemockt.
Simuleert opeenvolgende daemon-cycles om te verifiëren dat signalen correct
propageren door de volledige keten.

Geanonimiseerde signalen gebaseerd op productie-data:
- SignaalNieuweInschrijving (nieuw bedrijf, geen vestigingsnummer)
- SignaalGewijzigdeInschrijving (bestaand bedrijf, geen vestigingsnummer)
- SignaalGewijzigdeVestiging (bestaand bedrijf, met vestigingsnummer)
"""

from __future__ import annotations

import copy
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from kvk_connect.api.client import KVKApiClient
from kvk_connect.db.basisprofiel_reader import BasisProfielReader
from kvk_connect.db.basisprofiel_writer import BasisProfielWriter
from kvk_connect.db.kvkvestigingen_reader import KvKVestigingenReader
from kvk_connect.db.kvkvestigingen_writer import KvKVestigingenWriter
from kvk_connect.db.vestigingenprofiel_reader import VestigingsProfielReader
from kvk_connect.db.vestigingsprofiel_writer import VestigingsProfielWriter
from kvk_connect.models.api.basisprofiel_api import BasisProfielAPI
from kvk_connect.models.api.vestigingen_api import VestigingenAPI
from kvk_connect.models.api.vestigingsprofiel_api import VestigingsProfielAPI
from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.models.orm.signaal_orm import SignaalORM
from kvk_connect.models.orm.vestigingen_orm import VestigingenORM
from kvk_connect.models.orm.vestigingsprofiel_orm import VestigingsProfielORM
from kvk_connect.services.record_service import KVKRecordService

logger = logging.getLogger(__name__)

# Timestamps voor test-signalen (naive — SQLite slaat geen timezone-info op)
T0 = datetime(2025, 10, 24, 8, 0, 0)  # Initieel signaal
T1 = datetime(2025, 12, 17, 12, 0, 0)  # Later signaal (updates)

DATA_DIR = Path(__file__).parent.parent / "data"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_json(filename: str) -> dict:
    with open(DATA_DIR / filename, encoding="utf-8") as f:
        return json.load(f)


def _insert_signaal(
    session: Session,
    *,
    id: str,
    timestamp: datetime,
    kvknummer: str,
    signaal_type: str,
    vestigingsnummer: str | None = None,
) -> None:
    session.add(
        SignaalORM(
            id=id,
            timestamp=timestamp,
            kvknummer=kvknummer,
            signaal_type=signaal_type,
            vestigingsnummer=vestigingsnummer,
        )
    )
    session.commit()


def _create_mock_client(
    bp_json: dict,
    vest_json: dict,
    vestprof_json: dict,
) -> KVKApiClient:
    """Maak een mock KVKApiClient die vaste API-model responses retourneert.

    get_vestigingsprofiel retourneert per vestigingsnummer een uniek profiel
    (kopie van de template met aangepast vestigingsnummer).
    """
    client = MagicMock(spec=KVKApiClient)
    client.get_basisprofiel.return_value = BasisProfielAPI.from_dict(bp_json)
    client.get_vestigingen.return_value = VestigingenAPI.from_dict(vest_json)

    def _get_vestprof(vestigingsnummer: str, geo_data: bool = True) -> VestigingsProfielAPI:
        data = copy.deepcopy(vestprof_json)
        data["vestigingsnummer"] = vestigingsnummer
        return VestigingsProfielAPI.from_dict(data)

    client.get_vestigingsprofiel.side_effect = _get_vestprof
    return client


def _run_basisprofiel_cycle(engine: Engine, mock_client: KVKApiClient) -> int:
    """Simuleer 1 basisprofiel daemon cycle (outdated + missing)."""
    reader = BasisProfielReader(engine)
    count = 0
    with BasisProfielWriter(engine) as writer:
        for kvk in reader.get_outdated_kvk_nummers():
            record = KVKRecordService(mock_client).get_basisprofiel(kvk)
            if record:
                writer.add(record)
                count += 1
        for kvk in reader.get_missing_kvk_nummers():
            record = KVKRecordService(mock_client).get_basisprofiel(kvk)
            if record:
                writer.add(record)
                count += 1
        writer.flush()
    return count


def _run_vestigingen_cycle(engine: Engine, mock_client: KVKApiClient) -> int:
    """Simuleer 1 vestigingen daemon cycle (outdated + missing)."""
    reader = KvKVestigingenReader(engine)
    count = 0
    with KvKVestigingenWriter(engine) as writer:
        for kvk in reader.get_outdated_vestigingen():
            record = KVKRecordService(mock_client).get_vestigingen(kvk)
            if record:
                writer.add(record)
                count += 1
        for kvk in reader.get_missing_kvk_nummers():
            record = KVKRecordService(mock_client).get_vestigingen(kvk)
            if record:
                writer.add(record)
                count += 1
        writer.flush()
    return count


def _run_vestigingsprofiel_cycle(engine: Engine, mock_client: KVKApiClient) -> int:
    """Simuleer 1 vestigingsprofiel daemon cycle (signaal + missing)."""
    reader = VestigingsProfielReader(engine)
    count = 0
    with VestigingsProfielWriter(engine) as writer:
        for vestnr in reader.get_outdated_vestigingen_signaal():
            record = KVKRecordService(mock_client).get_vestigingsprofiel(vestnr)
            if record:
                writer.add(record)
                count += 1
        for vestnr in reader.get_vestigingen_zonder_vestigingsprofielen():
            record = KVKRecordService(mock_client).get_vestigingsprofiel(vestnr)
            if record:
                writer.add(record)
                count += 1
        writer.flush()
    return count


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSignaalPropagatie:
    """Integratietest: signaal-propagatie door de volledige keten."""

    @pytest.fixture
    def bp_json(self) -> dict:
        return _load_json("test_input_basisprofiel.json")

    @pytest.fixture
    def vest_json(self) -> dict:
        return _load_json("test_input_vestigingen.json")

    @pytest.fixture
    def vestprof_json(self) -> dict:
        return _load_json("test_input_vestigingsprofielen.json")

    @pytest.fixture
    def mock_client(self, bp_json: dict, vest_json: dict, vestprof_json: dict) -> KVKApiClient:
        return _create_mock_client(bp_json, vest_json, vestprof_json)

    def test_nieuw_bedrijf_volledige_keten(
        self, db_engine: Engine, db_session: Session, mock_client: KVKApiClient
    ) -> None:
        """SignaalNieuweInschrijving zonder vestigingsnummer → volledige keten.

        Verwacht: basisprofiel → vestigingen → vestigingsprofielen allemaal aangemaakt.
        """
        _insert_signaal(
            db_session,
            id="aaa00001-0000-0000-0000-000000000001",
            timestamp=T0,
            kvknummer="12345678",
            signaal_type="SignaalNieuweInschrijving",
        )

        # Cycle 1: basisprofiel pikt nieuw kvknummer op via missing
        bp_reader = BasisProfielReader(db_engine)
        assert "12345678" in bp_reader.get_missing_kvk_nummers()
        _run_basisprofiel_cycle(db_engine, mock_client)

        # Verify: basisprofiel aangemaakt
        bp = db_session.query(BasisProfielORM).filter_by(kvk_nummer="12345678").first()
        assert bp is not None
        assert bp.naam == "Test B.V."

        # Cycle 2: vestigingen pikt kvknummer op via missing
        vest_reader = KvKVestigingenReader(db_engine)
        assert "12345678" in vest_reader.get_missing_kvk_nummers()
        _run_vestigingen_cycle(db_engine, mock_client)

        # Verify: vestigingen aangemaakt (5 uit test fixture)
        vest_count = db_session.query(VestigingenORM).filter(
            VestigingenORM.kvk_nummer == "12345678",
            VestigingenORM.vestigingsnummer != VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
        ).count()
        assert vest_count == 5

        # Cycle 3: vestigingsprofiel pikt vestigingsnummers op via missing
        vp_reader = VestigingsProfielReader(db_engine)
        missing_profielen = vp_reader.get_vestigingen_zonder_vestigingsprofielen()
        assert len(missing_profielen) == 5
        _run_vestigingsprofiel_cycle(db_engine, mock_client)

        # Verify: vestigingsprofiel aangemaakt
        vp = db_session.query(VestigingsProfielORM).filter_by(vestigingsnummer="000000000001").first()
        assert vp is not None
        assert vp.kvk_nummer == "12345678"

    def test_bestaand_bedrijf_bedrijfswijziging(
        self, db_engine: Engine, db_session: Session, mock_client: KVKApiClient
    ) -> None:
        """SignaalGewijzigdeInschrijving zonder vestigingsnummer → BP outdated → keten.

        Setup: bedrijf al volledig in DB met oude timestamps.
        Verwacht: basisprofiel + vestigingen re-fetched via keten.
        """
        # Setup: bedrijf al in DB
        old_ts = T0 - timedelta(days=30)
        db_session.add(BasisProfielORM(kvk_nummer="12345678", naam="Oud B.V.", last_updated=old_ts))
        db_session.add(
            VestigingenORM(kvk_nummer="12345678", vestigingsnummer="000000000001", last_updated=old_ts)
        )
        db_session.commit()

        # Signaal: bedrijfswijziging (geen vestigingsnummer)
        _insert_signaal(
            db_session,
            id="bbb00001-0000-0000-0000-000000000001",
            timestamp=T0,
            kvknummer="12345678",
            signaal_type="SignaalGewijzigdeInschrijving",
        )

        # Cycle 1: basisprofiel outdated
        bp_reader = BasisProfielReader(db_engine)
        assert "12345678" in bp_reader.get_outdated_kvk_nummers()
        _run_basisprofiel_cycle(db_engine, mock_client)

        # Verify: basisprofiel bijgewerkt
        db_session.expire_all()
        bp = db_session.query(BasisProfielORM).filter_by(kvk_nummer="12345678").first()
        assert bp is not None
        assert bp.last_updated > old_ts

        # Cycle 2: vestigingen keten-outdated (bp nieuwer dan vest)
        vest_reader = KvKVestigingenReader(db_engine)
        assert "12345678" in vest_reader.get_outdated_vestigingen()
        _run_vestigingen_cycle(db_engine, mock_client)

    def test_bestaand_bedrijf_vestigingswijziging(
        self, db_engine: Engine, db_session: Session, mock_client: KVKApiClient
    ) -> None:
        """SignaalGewijzigdeVestiging MET vestigingsnummer → BP outdated triggert.

        Dit is de kerntest: ZONDER de fix zou basisprofiel dit signaal negeren
        vanwege de vestigingsnummer IS NULL filter. Met de fix triggert basisprofiel
        op alle signalen, waardoor de keten correct propageert.
        """
        # Setup: bedrijf volledig in DB
        old_ts = T0 - timedelta(days=30)
        db_session.add(BasisProfielORM(kvk_nummer="12345678", naam="Test B.V.", last_updated=old_ts))
        db_session.add(
            VestigingenORM(kvk_nummer="12345678", vestigingsnummer="000000000001", last_updated=old_ts)
        )
        db_session.add(
            VestigingsProfielORM(
                vestigingsnummer="000000000001", kvk_nummer="12345678", last_updated=old_ts
            )
        )
        db_session.commit()

        # Signaal: vestigingswijziging (MET vestigingsnummer)
        _insert_signaal(
            db_session,
            id="ccc00001-0000-0000-0000-000000000001",
            timestamp=T0,
            kvknummer="12345678",
            signaal_type="SignaalGewijzigdeVestiging",
            vestigingsnummer="000000000001",
        )

        # KERNCHECK: basisprofiel outdated MOET dit kvknummer bevatten
        bp_reader = BasisProfielReader(db_engine)
        outdated = bp_reader.get_outdated_kvk_nummers()
        assert "12345678" in outdated, (
            "Basisprofiel outdated moet triggeren op vestiging-signalen "
            "(vestigingsnummer IS NULL filter moet verwijderd zijn)"
        )

        # Cycle 1: basisprofiel
        _run_basisprofiel_cycle(db_engine, mock_client)

        # Cycle 2: vestigingen keten-outdated
        vest_reader = KvKVestigingenReader(db_engine)
        assert "12345678" in vest_reader.get_outdated_vestigingen()
        _run_vestigingen_cycle(db_engine, mock_client)

        # Cycle 3: vestigingsprofiel signaal-triggered
        vp_reader = VestigingsProfielReader(db_engine)
        signaal_outdated = vp_reader.get_outdated_vestigingen_signaal()
        assert "000000000001" in signaal_outdated
        _run_vestigingsprofiel_cycle(db_engine, mock_client)

        # Verify: vestigingsprofiel bijgewerkt
        db_session.expire_all()
        vp = db_session.query(VestigingsProfielORM).filter_by(vestigingsnummer="000000000001").first()
        assert vp is not None
        assert vp.last_updated > old_ts

    def test_nieuw_vestigingsnummer_via_keten(
        self, db_engine: Engine, db_session: Session, mock_client: KVKApiClient
    ) -> None:
        """Signaal met onbekend vestigingsnummer → keten → VestProfiel missing.

        Setup: bedrijf in DB met 1 vestiging. API retourneert 5 vestigingen.
        Verwacht: na keten-propagatie worden ontbrekende vestigingsprofielen opgepikt.
        """
        old_ts = T0 - timedelta(days=30)
        db_session.add(BasisProfielORM(kvk_nummer="12345678", naam="Test B.V.", last_updated=old_ts))
        db_session.add(
            VestigingenORM(kvk_nummer="12345678", vestigingsnummer="000000000001", last_updated=old_ts)
        )
        db_session.add(
            VestigingsProfielORM(
                vestigingsnummer="000000000001", kvk_nummer="12345678", last_updated=old_ts
            )
        )
        db_session.commit()

        # Signaal: nieuw vestigingsnummer (niet in vestigingen tabel)
        _insert_signaal(
            db_session,
            id="ddd00001-0000-0000-0000-000000000001",
            timestamp=T0,
            kvknummer="12345678",
            signaal_type="SignaalGewijzigdeVestiging",
            vestigingsnummer="000000000099",
        )

        # Cycle 1-2: basisprofiel + vestigingen (mock retourneert 5 vestigingsnummers)
        _run_basisprofiel_cycle(db_engine, mock_client)
        _run_vestigingen_cycle(db_engine, mock_client)

        # Verify: nieuwe vestigingsnummers in DB
        db_session.expire_all()
        vest_count = db_session.query(VestigingenORM).filter(
            VestigingenORM.kvk_nummer == "12345678",
            VestigingenORM.vestigingsnummer != VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
        ).count()
        assert vest_count == 5

        # Cycle 3: vestigingsprofiel missing pikt nieuwe vestigingsnummers op
        vp_reader = VestigingsProfielReader(db_engine)
        missing = vp_reader.get_vestigingen_zonder_vestigingsprofielen()
        # 000000000001 heeft al een profiel, de rest (002-005) mist er een
        assert len(missing) == 4
        assert "000000000001" not in missing

    def test_geen_openstaand_work_na_verwerking(
        self, db_engine: Engine, db_session: Session, mock_client: KVKApiClient
    ) -> None:
        """Na volledige verwerking retourneren alle readers lege lijsten."""
        # Insert signaal
        _insert_signaal(
            db_session,
            id="eee00001-0000-0000-0000-000000000001",
            timestamp=T0,
            kvknummer="12345678",
            signaal_type="SignaalNieuweInschrijving",
        )

        # Run volledige keten
        _run_basisprofiel_cycle(db_engine, mock_client)
        _run_vestigingen_cycle(db_engine, mock_client)
        _run_vestigingsprofiel_cycle(db_engine, mock_client)

        # Verify: geen openstaand work
        bp_reader = BasisProfielReader(db_engine)
        vest_reader = KvKVestigingenReader(db_engine)
        vp_reader = VestigingsProfielReader(db_engine)

        assert bp_reader.get_missing_kvk_nummers() == []
        assert bp_reader.get_outdated_kvk_nummers() == []
        assert vest_reader.get_missing_kvk_nummers() == []
        assert vest_reader.get_outdated_vestigingen() == []
        assert vp_reader.get_vestigingen_zonder_vestigingsprofielen() == []
