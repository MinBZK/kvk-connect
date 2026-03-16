import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from kvk_connect.db.historie_utils import _BASISPROFIEL_BUSINESS_FIELDS, compute_changed_fields
from kvk_connect.models.domain import BasisProfielDomain
from kvk_connect.models.orm.basisprofiel_historie_orm import BasisProfielHistorieORM
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

        # ⚠️ Diff VÓÓR merge — merge overschrijft de identity-map instance
        existing = self._session.get(BasisProfielORM, orm_obj.kvk_nummer)
        changed = compute_changed_fields(existing, orm_obj, _BASISPROFIEL_BUSINESS_FIELDS)

        self._session.merge(orm_obj)

        if changed:
            self._session.add(
                BasisProfielHistorieORM(
                    kvk_nummer=orm_obj.kvk_nummer,
                    gewijzigd_op=orm_obj.last_updated,
                    gewijzigde_velden=",".join(changed),
                    naam=orm_obj.naam,
                    eerste_handelsnaam=orm_obj.eerste_handelsnaam,
                    handelsnamen=orm_obj.handelsnamen,
                    websites=orm_obj.websites,
                    ind_non_mailing=orm_obj.ind_non_mailing,
                    hoofdactiviteit=orm_obj.hoofdactiviteit,
                    hoofdactiviteit_omschrijving=orm_obj.hoofdactiviteit_omschrijving,
                    activiteit_overig=orm_obj.activiteit_overig,
                    rechtsvorm=orm_obj.rechtsvorm,
                    rechtsvorm_uitgebreid=orm_obj.rechtsvorm_uitgebreid,
                    totaal_werkzame_personen=orm_obj.totaal_werkzame_personen,
                    formele_registratiedatum=orm_obj.formele_registratiedatum,
                    registratie_datum_aanvang=orm_obj.registratie_datum_aanvang,
                    registratie_datum_einde=orm_obj.registratie_datum_einde,
                )
            )

        self._count += 1

        if self._count % self.batch_size == 0:
            self._session.commit()

    def mark_niet_leverbaar(self, kvk_nummer: str, code: str) -> None:
        """Schrijf tombstone voor permanent niet-leverbaar KVK nummer (bijv. IPD0005)."""
        if not self._session:
            raise RuntimeError("Session not initialized. Use context manager.")
        existing = self._session.get(BasisProfielORM, kvk_nummer)
        if existing:
            existing.niet_leverbaar_code = code
            existing.last_updated = datetime.now(UTC)
        else:
            self._session.add(
                BasisProfielORM(kvk_nummer=kvk_nummer, niet_leverbaar_code=code, last_updated=datetime.now(UTC))
            )
        self._session.commit()

    def mark_retry_after(self, kvk_nummer: str, delay: timedelta) -> None:
        """Stel retry_after in voor tijdelijk niet-leverbaar KVK nummer (bijv. IPD1002/IPD1003)."""
        if not self._session:
            raise RuntimeError("Session not initialized. Use context manager.")
        existing = self._session.get(BasisProfielORM, kvk_nummer)
        if existing:
            existing.retry_after = datetime.now(UTC) + delay
            existing.last_updated = datetime.now(UTC)
        else:
            self._session.add(
                BasisProfielORM(
                    kvk_nummer=kvk_nummer, retry_after=datetime.now(UTC) + delay, last_updated=datetime.now(UTC)
                )
            )
        self._session.commit()

    @staticmethod
    def _to_orm(api_obj: BasisProfielDomain) -> BasisProfielORM:
        return BasisProfielORM(
            kvk_nummer=api_obj.kvk_nummer,
            naam=api_obj.naam,
            ind_non_mailing=api_obj.ind_non_mailing,
            formele_registratiedatum=parse_kvk_datum(api_obj.formele_registratiedatum),
            hoofdactiviteit=api_obj.hoofdactiviteit,
            hoofdactiviteit_omschrijving=api_obj.hoofdactiviteit_omschrijving,
            activiteit_overig=api_obj.activiteit_overig,
            rechtsvorm=api_obj.rechtsvorm,
            rechtsvorm_uitgebreid=api_obj.rechtsvorm_uitgebreid,
            eerste_handelsnaam=api_obj.eerste_handelsnaam,
            handelsnamen=api_obj.handelsnamen,
            totaal_werkzame_personen=api_obj.totaal_werkzame_personen,
            websites=api_obj.websites,
            registratie_datum_aanvang=parse_kvk_datum(api_obj.registratie_datum_aanvang),
            registratie_datum_einde=parse_kvk_datum(api_obj.registratie_datum_einde),
        )
