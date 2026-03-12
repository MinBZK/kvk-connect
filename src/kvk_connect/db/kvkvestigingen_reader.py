from sqlalchemy import exists, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.models.orm.vestigingen_orm import VestigingenORM


class KvKVestigingenReader:
    def __init__(self, engine: Engine):
        self.engine = engine

    def get_missing_kvk_nummers(self, limit: int = 1000) -> list[str]:
        """Retourneert unieke KVK nummers die wel in basisprofielen staan maar nog niet in kvkvestigingen."""
        with Session(self.engine) as session:
            stmt = (
                select(BasisProfielORM.kvk_nummer)
                .select_from(BasisProfielORM)
                .outerjoin(VestigingenORM, BasisProfielORM.kvk_nummer == VestigingenORM.kvk_nummer)
                .where(VestigingenORM.kvk_nummer.is_(None))
                .where(BasisProfielORM.niet_leverbaar_code.is_(None))
                .distinct()
                .limit(limit)  # maximaal limit nieuwe per keer ophalen
            )

            result = session.execute(stmt).scalars().all()
            return list(result)

    def get_missing_kvk_nummers_count(self) -> int:
        """Retourneert het totaal aantal KVK nummers die wel in basisprofielen staan maar nog niet in kvkvestigingen."""
        with Session(self.engine) as session:
            stmt = (
                select(func.count(func.distinct(BasisProfielORM.kvk_nummer)))
                .select_from(BasisProfielORM)
                .outerjoin(VestigingenORM, BasisProfielORM.kvk_nummer == VestigingenORM.kvk_nummer)
                .where(VestigingenORM.kvk_nummer.is_(None))
                .where(BasisProfielORM.niet_leverbaar_code.is_(None))
            )
            result = session.execute(stmt).scalar()
            return result or 0

    def get_outdated_vestigingen_count(self) -> int:
        """Retourneert het totaal aantal KVK nummers waarvan de vestigingen verouderd zijn."""
        with Session(self.engine) as session:
            blocked = (
                exists()
                .where(VestigingenORM.kvk_nummer == BasisProfielORM.kvk_nummer)
                .where(VestigingenORM.vestigingsnummer == VestigingenORM.SENTINEL_VESTIGINGSNUMMER)
                .where(
                    VestigingenORM.niet_leverbaar_code.is_not(None)
                    | (VestigingenORM.retry_after.is_not(None) & (VestigingenORM.retry_after > func.now()))
                )
            )
            stmt = (
                select(func.count(func.distinct(BasisProfielORM.kvk_nummer)))
                .join(VestigingenORM, BasisProfielORM.kvk_nummer == VestigingenORM.kvk_nummer)
                .where(BasisProfielORM.last_updated > VestigingenORM.last_updated)
                .where(~blocked)
            )
            result = session.execute(stmt).scalar()
            return result or 0

    def get_outdated_vestigingen(self, limit: int = 1000) -> list[str]:
        """Geen een lijst van unieke kvknummers terug waarvan de vestigingen verouderd zijn.

        Dit is gedefinieerd als basisprofielen die nieuwer zijn dan de laatste update van de vestigingen.
        """
        with Session(self.engine) as session:
            blocked = (
                exists()
                .where(VestigingenORM.kvk_nummer == BasisProfielORM.kvk_nummer)
                .where(VestigingenORM.vestigingsnummer == VestigingenORM.SENTINEL_VESTIGINGSNUMMER)
                .where(
                    VestigingenORM.niet_leverbaar_code.is_not(None)
                    | (VestigingenORM.retry_after.is_not(None) & (VestigingenORM.retry_after > func.now()))
                )
            )
            stmt = (
                select(BasisProfielORM.kvk_nummer)
                .join(VestigingenORM, BasisProfielORM.kvk_nummer == VestigingenORM.kvk_nummer)
                .where(BasisProfielORM.last_updated > VestigingenORM.last_updated)
                .where(~blocked)
                .distinct()
                .limit(limit)  # maximaal limit nieuwe per keer ophalen
            )

            result = session.execute(stmt).scalars().all()
            return list(result)
