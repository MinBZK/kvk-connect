import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session, sessionmaker

from kvk_connect.models.domain import KvKVestigingsNummersDomain
from kvk_connect.models.orm.vestigingen_historie_orm import VestigingenHistorieORM
from kvk_connect.models.orm.vestigingen_orm import VestigingenORM

logger = logging.getLogger(__name__)


class KvKVestigingenWriter:
    # Low batch size by default to avoid locking issues
    def __init__(self, engine, batch_size: int = 1):
        logger.info("Initializing KvKVestigingenWriter, met batch size: %d", batch_size)
        self.Session = sessionmaker(bind=engine)
        self.batch_size = batch_size
        self._session: Session | None = None
        self._count = 0

    def __enter__(self):
        """Start een nieuwe database sessie."""
        self._session = self.Session()
        return self

    def __exit__(self, exc_type, exc, tb):
        """Commit of rollback de sessie en sluit deze af."""
        try:
            if exc is None:
                self.flush()
            else:
                if self._session:
                    self._session.rollback()
        finally:
            if self._session:
                self._session.close()
            self._session = None

    def flush(self) -> None:
        """Schrijf openstaande wijzigingen naar de database."""
        if self._session:
            self._session.commit()

    def mark_niet_leverbaar(self, kvk_nummer: str, code: str) -> None:
        """Schrijf tombstone voor permanent niet-leverbaar KVK nummer (vestigingen)."""
        if not self._session:
            raise RuntimeError("Session not initialized. Use context manager.")
        orm_obj = VestigingenORM(
            kvk_nummer=kvk_nummer,
            vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
            niet_leverbaar_code=code,
            last_updated=datetime.now(UTC),
        )
        self._session.merge(orm_obj)
        self._session.commit()

    def mark_retry_after(self, kvk_nummer: str, delay: timedelta) -> None:
        """Stel retry_after in voor tijdelijk niet-leverbaar KVK nummer (vestigingen)."""
        if not self._session:
            raise RuntimeError("Session not initialized. Use context manager.")
        orm_obj = VestigingenORM(
            kvk_nummer=kvk_nummer,
            vestigingsnummer=VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
            retry_after=datetime.now(UTC) + delay,
            last_updated=datetime.now(UTC),
        )
        self._session.merge(orm_obj)
        self._session.commit()

    def add(self, domain_kvkvestigingen: KvKVestigingsNummersDomain) -> None:
        """Schrijf alle vestigingsnummers uit het domeinmodel weg naar de database.

        Creëert een apart database-record per vestigingsnummer met het bijbehorende kvkNummer.
        Als er geen vestigingsnummers zijn, wordt een record met vestigingsnummer=NULL weggeschreven.
        Bijhoudt een event-log in vestigingen_historie voor toe- en afname van vestigingen.

        Params:
            domain_kvkvestigingen: KvKVestigingsNummersDomain - Domain object met kvkNummer en lijst vestigingsnummers
        """
        if not self._session:
            raise RuntimeError("Session not initialized. Use context manager.")

        timestamp = datetime.now(UTC)

        # Bepaal de nieuwe set vestigingsnummers (excl. sentinel)
        new_set = {
            v for v in (domain_kvkvestigingen.vestigingsnummers or []) if v != VestigingenORM.SENTINEL_VESTIGINGSNUMMER
        }

        # Haal bestaande vestigingsnummers op uit de database (excl. sentinel)
        existing_set = {
            row.vestigingsnummer
            for row in self._session.query(VestigingenORM)
            .filter(
                VestigingenORM.kvk_nummer == domain_kvkvestigingen.kvk_nummer,
                VestigingenORM.vestigingsnummer != VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
            )
            .all()
        }

        added = sorted(new_set - existing_set)
        removed = sorted(existing_set - new_set)

        # Schrijf historierijen voor toevoegingen
        for vestigingsnummer in added:
            self._session.add(
                VestigingenHistorieORM(
                    kvk_nummer=domain_kvkvestigingen.kvk_nummer,
                    vestigingsnummer=vestigingsnummer,
                    event_type="toevoegd",
                    gewijzigd_op=timestamp,
                )
            )

        # Schrijf historierijen voor verwijderingen en verwijder stale rijen
        for vestigingsnummer in removed:
            self._session.add(
                VestigingenHistorieORM(
                    kvk_nummer=domain_kvkvestigingen.kvk_nummer,
                    vestigingsnummer=vestigingsnummer,
                    event_type="verwijderd",
                    gewijzigd_op=timestamp,
                )
            )
            self._session.query(VestigingenORM).filter_by(
                kvk_nummer=domain_kvkvestigingen.kvk_nummer, vestigingsnummer=vestigingsnummer
            ).delete()

        # Merge alle vestigingen (incl. sentinel als new_set leeg is)
        vestigingsnummers = domain_kvkvestigingen.vestigingsnummers or [VestigingenORM.SENTINEL_VESTIGINGSNUMMER]
        for vestigingsnummer in vestigingsnummers:
            orm_obj = VestigingenORM(
                kvk_nummer=domain_kvkvestigingen.kvk_nummer, vestigingsnummer=vestigingsnummer, last_updated=timestamp
            )
            self._session.merge(orm_obj)

        # Verhoog counter met totaal aantal vestigingen van dit KvK
        self._count += len(vestigingsnummers)

        # Commit als batch_size bereikt
        if self._count >= self.batch_size:
            self._session.commit()
            self._count = 0
