import logging
from datetime import UTC, datetime

from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from kvk_connect.models.domain import BasisProfielDomain
from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.utils.tools import parse_kvk_datum

logger = logging.getLogger(__name__)


class BasisProfielWriter:
    # lage default batch size op 1 om db locking te minimaliseren
    def __init__(self, engine: Engine, batch_size: int = 1):
        logger.info("Initializing BasisProfielWriter, met batch size: %d", batch_size)
        self.Session = sessionmaker(bind=engine)
        self.batch_size = batch_size
        self._session: Session | None = None
        self._count = 0

    def __enter__(self):
        """Create a new session for the context."""
        self._session = self.Session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager, commit or rollback based on exception state."""
        if self._session is None:
            return

        try:
            if exc_type is None:
                # No exception: commit the transaction
                self._session.commit()
                logger.debug("Session committed successfully")
            else:
                # Exception occurred: rollback the transaction
                self._session.rollback()
                logger.warning("Session rolled back due to exception: %s", exc_type.__name__)
        finally:
            self._session.close()
            self._session = None

    def flush(self) -> None:  # noqa: D102
        if self._session:
            self._session.commit()

    def add(self, domain_basisprofiel: BasisProfielDomain) -> None:  # noqa: D102
        if not self._session:
            raise RuntimeError("Session not initialized. Use context manager.")

        orm_obj = self._to_orm(domain_basisprofiel)
        orm_obj.last_updated = datetime.now(UTC)

        self._session.merge(orm_obj)
        self._count += 1

        if self._count % self.batch_size == 0:
            self._session.commit()

    @staticmethod
    def _to_orm(api_obj: BasisProfielDomain) -> BasisProfielORM:
        return BasisProfielORM(
            kvk_nummer=api_obj.kvk_nummer,
            naam=api_obj.naam,
            hoofdactiviteit=api_obj.hoofdactiviteit,
            hoofdactiviteit_omschrijving=api_obj.hoofdactiviteit_omschrijving,
            activiteit_overig=api_obj.activiteit_overig,
            rechtsvorm=api_obj.rechtsvorm,
            rechtsvorm_uitgebreid=api_obj.rechtsvorm_uitgebreid,
            eerste_handelsnaam=api_obj.eerste_handelsnaam,
            totaal_werkzame_personen=api_obj.totaal_werkzame_personen,
            websites=api_obj.websites,
            registratie_datum_aanvang=parse_kvk_datum(api_obj.registratie_datum_aanvang),
            registratie_datum_einde=parse_kvk_datum(api_obj.registratie_datum_einde),
        )
