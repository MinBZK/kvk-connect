# ruff: noqa: D102
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session, sessionmaker

from kvk_connect.models.domain.vestigingsprofiel_domain import VestigingsProfielDomain
from kvk_connect.models.orm.vestigingsprofiel_orm import VestigingsProfielORM
from kvk_connect.utils.tools import parse_kvk_datum

logger = logging.getLogger(__name__)


class VestigingsProfielWriter:
    def __init__(self, engine, batch_size: int = 1):
        logger.info("Initializing VestigingsProfielWriter, met batch size: %d", batch_size)
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

    def mark_niet_leverbaar(self, vestigingsnummer: str, code: str) -> None:
        """Schrijf tombstone voor permanent niet-leverbaar vestigingsnummer."""
        if not self._session:
            raise RuntimeError("Session not initialized. Use context manager.")
        existing = self._session.get(VestigingsProfielORM, vestigingsnummer)
        if existing:
            existing.niet_leverbaar_code = code
            existing.last_updated = datetime.now(UTC)
        else:
            self._session.add(
                VestigingsProfielORM(
                    vestigingsnummer=vestigingsnummer, niet_leverbaar_code=code, last_updated=datetime.now(UTC)
                )
            )
        self._session.commit()

    def mark_retry_after(self, vestigingsnummer: str, delay: timedelta) -> None:
        """Stel retry_after in voor tijdelijk niet-leverbaar vestigingsnummer."""
        if not self._session:
            raise RuntimeError("Session not initialized. Use context manager.")
        existing = self._session.get(VestigingsProfielORM, vestigingsnummer)
        if existing:
            existing.retry_after = datetime.now(UTC) + delay
            existing.last_updated = datetime.now(UTC)
        else:
            self._session.add(
                VestigingsProfielORM(
                    vestigingsnummer=vestigingsnummer,
                    retry_after=datetime.now(UTC) + delay,
                    last_updated=datetime.now(UTC),
                )
            )
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
    def _parse_gps(value: str | None, label: str) -> float | None:
        if not value:
            return None
        try:
            return float(str(value).replace(",", "."))
        except (ValueError, AttributeError):
            logger.warning("Invalid %s value: %s", label, value)
            return None

    @staticmethod
    def _to_orm(domein_obj: VestigingsProfielDomain) -> VestigingsProfielORM:
        return VestigingsProfielORM(
            vestigingsnummer=domein_obj.vestigingsnummer,
            kvk_nummer=domein_obj.kvk_nummer,
            rsin=domein_obj.rsin,
            ind_non_mailing=domein_obj.ind_non_mailing,
            formele_registratiedatum=parse_kvk_datum(domein_obj.formele_registratiedatum),
            statutaire_naam=domein_obj.statutaire_naam,
            eerste_handelsnaam=domein_obj.eerste_handelsnaam,
            handelsnamen=domein_obj.handelsnamen,
            ind_hoofdvestiging=domein_obj.ind_hoofdvestiging,
            ind_commerciele_vestiging=domein_obj.ind_commerciele_vestiging,
            voltijd_werkzame_personen=domein_obj.voltijd_werkzame_personen,
            deeltijd_werkzame_personen=domein_obj.deeltijd_werkzame_personen,
            totaal_werkzame_personen=domein_obj.totaal_werkzame_personen,
            hoofdactiviteit=domein_obj.hoofdactiviteit,
            hoofdactiviteit_omschrijving=domein_obj.hoofdactiviteit_omschrijving,
            activiteit_overig=domein_obj.activiteit_overig,
            websites=domein_obj.websites,
            cor_adres_volledig=domein_obj.cor_adres_volledig,
            cor_adres_straatnaam=domein_obj.cor_adres_straatnaam,
            cor_adres_huisnummer=domein_obj.cor_adres_huisnummer,
            cor_adres_postcode=domein_obj.cor_adres_postcode,
            cor_adres_postbusnummer=domein_obj.cor_adres_postbusnummer,
            cor_adres_plaats=domein_obj.cor_adres_plaats,
            cor_adres_land=domein_obj.cor_adres_land,
            cor_adres_gps_latitude=VestigingsProfielWriter._parse_gps(domein_obj.cor_adres_gps_latitude, "latitude"),
            cor_adres_gps_longitude=VestigingsProfielWriter._parse_gps(domein_obj.cor_adres_gps_longitude, "longitude"),
            bzk_adres_volledig=domein_obj.bzk_adres_volledig,
            bzk_adres_straatnaam=domein_obj.bzk_adres_straatnaam,
            bzk_adres_huisnummer=domein_obj.bzk_adres_huisnummer,
            bzk_adres_postcode=domein_obj.bzk_adres_postcode,
            bzk_adres_plaats=domein_obj.bzk_adres_plaats,
            bzk_adres_land=domein_obj.bzk_adres_land,
            bzk_adres_gps_latitude=VestigingsProfielWriter._parse_gps(domein_obj.bzk_adres_gps_latitude, "latitude"),
            bzk_adres_gps_longitude=VestigingsProfielWriter._parse_gps(domein_obj.bzk_adres_gps_longitude, "longitude"),
            registratie_datum_aanvang_vestiging=parse_kvk_datum(domein_obj.registratie_datum_aanvang_vestiging),
            registratie_datum_einde_vestiging=parse_kvk_datum(domein_obj.registratie_datum_einde_vestiging),
        )
