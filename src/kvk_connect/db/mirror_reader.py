from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from kvk_connect.models.domain.basisprofiel import BasisProfielDomain
from kvk_connect.models.domain.vestigingsprofiel_domain import VestigingsProfielDomain
from kvk_connect.models.orm.basisprofiel_historie_orm import BasisProfielHistorieORM
from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.models.orm.vestigingen_orm import VestigingenORM
from kvk_connect.models.orm.vestigingsprofiel_historie_orm import VestigingsProfielHistorieORM
from kvk_connect.models.orm.vestigingsprofiel_orm import VestigingsProfielORM


def _basisprofiel_orm_to_domain(orm: BasisProfielORM) -> BasisProfielDomain:
    return BasisProfielDomain(
        kvk_nummer=orm.kvk_nummer,
        naam=orm.naam,
        ind_non_mailing=orm.ind_non_mailing,
        formele_registratiedatum=orm.formele_registratiedatum.isoformat() if orm.formele_registratiedatum else None,
        hoofdactiviteit=orm.hoofdactiviteit,
        hoofdactiviteit_omschrijving=orm.hoofdactiviteit_omschrijving,
        activiteit_overig=orm.activiteit_overig,
        rechtsvorm=orm.rechtsvorm,
        rechtsvorm_uitgebreid=orm.rechtsvorm_uitgebreid,
        eerste_handelsnaam=orm.eerste_handelsnaam,
        handelsnamen=orm.handelsnamen,
        vestigingsnummer=None,  # niet opgeslagen in basisprofielen-tabel
        totaal_werkzame_personen=orm.totaal_werkzame_personen,
        websites=orm.websites,
        registratie_datum_aanvang=orm.registratie_datum_aanvang.isoformat() if orm.registratie_datum_aanvang else None,
        registratie_datum_einde=orm.registratie_datum_einde.isoformat() if orm.registratie_datum_einde else None,
        niet_leverbaar_code=orm.niet_leverbaar_code,
    )


def _vestigingsprofiel_orm_to_domain(orm: VestigingsProfielORM) -> VestigingsProfielDomain:
    return VestigingsProfielDomain(
        vestigingsnummer=orm.vestigingsnummer,
        kvk_nummer=orm.kvk_nummer,
        rsin=orm.rsin,
        ind_non_mailing=orm.ind_non_mailing,
        formele_registratiedatum=orm.formele_registratiedatum.isoformat() if orm.formele_registratiedatum else None,
        statutaire_naam=orm.statutaire_naam,
        eerste_handelsnaam=orm.eerste_handelsnaam,
        handelsnamen=orm.handelsnamen,
        ind_hoofdvestiging=orm.ind_hoofdvestiging,
        ind_commerciele_vestiging=orm.ind_commerciele_vestiging,
        voltijd_werkzame_personen=orm.voltijd_werkzame_personen,
        deeltijd_werkzame_personen=orm.deeltijd_werkzame_personen,
        totaal_werkzame_personen=orm.totaal_werkzame_personen,
        hoofdactiviteit=orm.hoofdactiviteit,
        hoofdactiviteit_omschrijving=orm.hoofdactiviteit_omschrijving,
        activiteit_overig=orm.activiteit_overig,
        websites=orm.websites,
        cor_adres_volledig=orm.cor_adres_volledig,
        cor_adres_straatnaam=orm.cor_adres_straatnaam,
        cor_adres_huisnummer=orm.cor_adres_huisnummer,
        cor_adres_postcode=orm.cor_adres_postcode,
        cor_adres_postbusnummer=orm.cor_adres_postbusnummer,
        cor_adres_plaats=orm.cor_adres_plaats,
        cor_adres_land=orm.cor_adres_land,
        cor_adres_gps_latitude=str(orm.cor_adres_gps_latitude) if orm.cor_adres_gps_latitude is not None else None,
        cor_adres_gps_longitude=str(orm.cor_adres_gps_longitude) if orm.cor_adres_gps_longitude is not None else None,
        bzk_adres_volledig=orm.bzk_adres_volledig,
        bzk_adres_straatnaam=orm.bzk_adres_straatnaam,
        bzk_adres_huisnummer=orm.bzk_adres_huisnummer,
        bzk_adres_postcode=orm.bzk_adres_postcode,
        bzk_adres_plaats=orm.bzk_adres_plaats,
        bzk_adres_land=orm.bzk_adres_land,
        bzk_adres_gps_latitude=str(orm.bzk_adres_gps_latitude) if orm.bzk_adres_gps_latitude is not None else None,
        bzk_adres_gps_longitude=str(orm.bzk_adres_gps_longitude) if orm.bzk_adres_gps_longitude is not None else None,
        registratie_datum_aanvang_vestiging=orm.registratie_datum_aanvang_vestiging.isoformat()
        if orm.registratie_datum_aanvang_vestiging
        else None,
        registratie_datum_einde_vestiging=orm.registratie_datum_einde_vestiging.isoformat()
        if orm.registratie_datum_einde_vestiging
        else None,
        niet_leverbaar_code=orm.niet_leverbaar_code,
    )


class KVKMirrorReader:
    """Adapter-laag voor directe lookups uit de lokale KVK-datamirror.

    Converteert ORM-objecten intern naar domain-objecten.
    Bestaande readers (*_reader.py) blijven ongewijzigd — die bedienen sync-queues.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def get_basisprofiel(self, kvk_nummer: str) -> BasisProfielDomain | None:
        """Geeft basisprofiel voor kvk_nummer, of None als niet gevonden."""
        with Session(self.engine) as session:
            orm = session.get(BasisProfielORM, kvk_nummer)
            return _basisprofiel_orm_to_domain(orm) if orm is not None else None

    def get_vestigingsnummers(self, kvk_nummer: str) -> list[str]:
        """Geeft vestigingsnummers voor kvk_nummer, sentinel uitgesloten."""
        with Session(self.engine) as session:
            stmt = select(VestigingenORM.vestigingsnummer).where(
                VestigingenORM.kvk_nummer == kvk_nummer,
                VestigingenORM.vestigingsnummer != VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
            )
            return list(session.execute(stmt).scalars().all())

    def get_vestigingsprofiel(self, vestigingsnummer: str) -> VestigingsProfielDomain | None:
        """Geeft vestigingsprofiel voor vestigingsnummer, of None als niet gevonden."""
        with Session(self.engine) as session:
            orm = session.get(VestigingsProfielORM, vestigingsnummer)
            return _vestigingsprofiel_orm_to_domain(orm) if orm is not None else None

    def get_vestigingsprofielen(self, vestigingsnummers: list[str]) -> list[VestigingsProfielDomain]:
        """Geeft vestigingsprofielen voor een lijst vestigingsnummers."""
        if not vestigingsnummers:
            return []
        with Session(self.engine) as session:
            stmt = select(VestigingsProfielORM).where(VestigingsProfielORM.vestigingsnummer.in_(vestigingsnummers))
            return [_vestigingsprofiel_orm_to_domain(orm) for orm in session.execute(stmt).scalars().all()]

    def get_basisprofiel_historie(self, kvk_nummer: str) -> list[BasisProfielHistorieORM]:
        """Geeft wijzigingsgeschiedenis voor kvk_nummer, meest recent eerst."""
        with Session(self.engine) as session:
            stmt = (
                select(BasisProfielHistorieORM)
                .where(BasisProfielHistorieORM.kvk_nummer == kvk_nummer)
                .order_by(BasisProfielHistorieORM.gewijzigd_op.desc())
            )
            return list(session.execute(stmt).scalars().all())

    def get_vestigingsprofiel_historie(self, vestigingsnummer: str) -> list[VestigingsProfielHistorieORM]:
        """Geeft wijzigingsgeschiedenis voor vestigingsnummer, meest recent eerst."""
        with Session(self.engine) as session:
            stmt = (
                select(VestigingsProfielHistorieORM)
                .where(VestigingsProfielHistorieORM.vestigingsnummer == vestigingsnummer)
                .order_by(VestigingsProfielHistorieORM.gewijzigd_op.desc())
            )
            return list(session.execute(stmt).scalars().all())

    def zoek_op_naam_prefix(self, prefix: str, limit: int) -> list[BasisProfielDomain]:
        """Zoekt basisprofielen op naam-prefix (database-agnostisch LIKE)."""
        with Session(self.engine) as session:
            stmt = select(BasisProfielORM).where(BasisProfielORM.naam.like(f"{prefix}%")).limit(limit)
            return [_basisprofiel_orm_to_domain(orm) for orm in session.execute(stmt).scalars().all()]

    def filter_op_sbi(self, sbi_prefix: str, gemeente: str | None, limit: int) -> list[VestigingsProfielDomain]:
        """Geeft vestigingsprofielen gefilterd op SBI-prefix en optioneel gemeente."""
        with Session(self.engine) as session:
            stmt = (
                select(VestigingsProfielORM)
                .where(VestigingsProfielORM.hoofdactiviteit.like(f"{sbi_prefix}%"))
                .limit(limit)
            )
            if gemeente is not None:
                stmt = stmt.where(VestigingsProfielORM.cor_adres_plaats == gemeente)
            return [_vestigingsprofiel_orm_to_domain(orm) for orm in session.execute(stmt).scalars().all()]

    def get_kvk_nummers_op_vestigingsnummers(
        self, vestigingsnummers: list[str], exclude_kvk_nummer: str
    ) -> list[tuple[str, str]]:
        """Geeft (kvk_nummer, vestigingsnummer) paren voor vestigingsnummers, excl. exclude_kvk_nummer."""
        if not vestigingsnummers:
            return []
        with Session(self.engine) as session:
            stmt = select(VestigingenORM.kvk_nummer, VestigingenORM.vestigingsnummer).where(
                VestigingenORM.vestigingsnummer.in_(vestigingsnummers),
                VestigingenORM.kvk_nummer != exclude_kvk_nummer,
                VestigingenORM.vestigingsnummer != VestigingenORM.SENTINEL_VESTIGINGSNUMMER,
            )
            return [(row.kvk_nummer, row.vestigingsnummer) for row in session.execute(stmt).all()]

    def check_actiefstatus_batch(self, kvk_nummers: list[str]) -> list[BasisProfielDomain]:
        """Geeft basisprofielen voor een lijst kvk_nummers (max 200)."""
        with Session(self.engine) as session:
            stmt = select(BasisProfielORM).where(BasisProfielORM.kvk_nummer.in_(kvk_nummers))
            return [_basisprofiel_orm_to_domain(orm) for orm in session.execute(stmt).scalars().all()]
