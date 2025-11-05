from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.models.orm.kvkvestigingen_orm import VestigingenORM


class KvKVestigingenReader:
    def __init__(self, engine: Engine):
        self.engine = engine

    def get_missing_kvk_nummers(self) -> list[str]:
        """Retourneert unieke KVK nummers die wel in basisprofielen staan maar nog niet in kvkvestigingen."""
        with Session(self.engine) as session:
            stmt = (
                select(BasisProfielORM.kvk_nummer)
                .select_from(BasisProfielORM)
                .outerjoin(VestigingenORM, BasisProfielORM.kvk_nummer == VestigingenORM.kvk_nummer)
                .where(VestigingenORM.kvk_nummer.is_(None))
                .distinct()
            )

            result = session.execute(stmt).scalars().all()
            return list(result)

    def get_outdated_vestigingen(self) -> list[str]:
        """Geen een lijst van unieke kvknummers terug waarvan de vestigingen verouderd zijn.

        Dit is gedefinieerd als basisprofielen die nieuwer zijn dan de laatste update van de vestigingen.
        """
        with Session(self.engine) as session:
            stmt = (
                select(BasisProfielORM.kvk_nummer)
                .join(VestigingenORM, BasisProfielORM.kvk_nummer == VestigingenORM.kvk_nummer)
                .where(BasisProfielORM.last_updated > VestigingenORM.last_updated)
                .distinct()
            )

            result = session.execute(stmt).scalars().all()
            return list(result)
