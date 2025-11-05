# ruff: noqa: D102
import logging
from datetime import UTC, datetime

from sqlalchemy.orm import Session, sessionmaker

from kvk_connect.models.domain.vestigingsprofiel_domain import VestigingsProfielDomain
from kvk_connect.models.orm.vestigingsprofiel_orm import VestigingsProfielORM
from kvk_connect.utils.tools import parse_kvk_datum

logger = logging.getLogger(__name__)


class VestigingsProfielWriter:
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
        """Commit changes on successful exit, rollback on exception."""
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
        if self._session:
            self._session.commit()

    def add(self, domain_vestigingsprofiel: VestigingsProfielDomain) -> None:
        if not self._session:
            raise RuntimeError("Session not initialized. Use context manager.")

        orm_obj = self._to_orm(domain_vestigingsprofiel)
        orm_obj.last_updated = datetime.now(UTC)

        self._session.merge(orm_obj)
        self._count += 1

        if self._count % self.batch_size == 0:
            self._session.commit()

    @staticmethod
    def _to_orm(domein_obj: VestigingsProfielDomain) -> VestigingsProfielORM:
        # Convert GPS coordinates from string to float (handle comma decimal separator)
        gps_lat = None
        gps_lon = None

        if domein_obj.bzk_adres_gps_latitude:
            try:
                gps_lat = float(domein_obj.bzk_adres_gps_latitude.replace(",", "."))
            except (ValueError, AttributeError):
                logger.warning("Invalid latitude value: %s", domein_obj.bzk_adres_gps_latitude)

        if domein_obj.bzk_adres_gps_longitude:
            try:
                gps_lon = float(domein_obj.bzk_adres_gps_longitude.replace(",", "."))
            except (ValueError, AttributeError):
                logger.warning("Invalid longitude value: %s", domein_obj.bzk_adres_gps_longitude)

        return VestigingsProfielORM(
            vestigingsnummer=domein_obj.vestigingsnummer,
            cor_adres_volledig=domein_obj.cor_adres_volledig,
            cor_adres_postcode=domein_obj.cor_adres_postcode,
            cor_adres_postbusnummer=domein_obj.cor_adres_postbusnummer,
            cor_adres_plaats=domein_obj.cor_adres_plaats,
            cor_adres_land=domein_obj.cor_adres_land,
            bzk_adres_volledig=domein_obj.bzk_adres_volledig,
            bzk_adres_straatnaam=domein_obj.bzk_adres_straatnaam,
            bzk_adres_huisnummer=domein_obj.bzk_adres_huisnummer,
            bzk_adres_postcode=domein_obj.bzk_adres_postcode,
            bzk_adres_plaats=domein_obj.bzk_adres_plaats,
            bzk_adres_land=domein_obj.bzk_adres_land,
            bzk_adres_gps_latitude=gps_lat,
            bzk_adres_gps_longitude=gps_lon,
            registratie_datum_aanvang_vestiging=parse_kvk_datum(domein_obj.registratie_datum_aanvang_vestiging),
            registratie_datum_einde_vestiging=parse_kvk_datum(domein_obj.registratie_datum_einde_vestiging),
        )
