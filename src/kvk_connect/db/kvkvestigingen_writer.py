import logging
from datetime import UTC, datetime

from sqlalchemy.orm import Session, sessionmaker

from kvk_connect.models.domain import KvKVestigingsNummersDomain
from kvk_connect.models.orm.kvkvestigingen_orm import VestigingenORM

logger = logging.getLogger(__name__)


class KvKVestigingenWriter:
    # Low batch size by default to avoid locking issues
    def __init__(self, engine, batch_size: int = 1):
        logger.info("Initializing BasisProfielWriter, met batch size: %d", batch_size)
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

    def flush(self) -> None:  # noqa: D102
        if self._session:
            self._session.commit()

    def add(self, domain_kvkvestigingen: KvKVestigingsNummersDomain) -> None:
        """Schrijf alle vestigingsnummers uit het domeinmodel weg naar de database.

        Creëert een apart database-record per vestigingsnummer met het bijbehorende kvkNummer.
        Als er geen vestigingsnummers zijn, wordt een record met vestigingsnummer=NULL weggeschreven.

        Params:
            domain_kvkvestigingen: KvKVestigingsNummersDomain - Domain object met kvkNummer en lijst vestigingsnummers
        """
        if not self._session:
            raise RuntimeError("Session not initialized. Use context manager.")

        timestamp = datetime.now(UTC)
        vestigingsnummers = domain_kvkvestigingen.vestigingsnummers or [
            VestigingenORM.SENTINEL_VESTIGINGSNUMMER
        ]  # Gebruik Sentinel waarde als er geen vestigingen zijn

        # Merge alle vestigingen van dit KvK nummer
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
