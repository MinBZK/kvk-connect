import random

from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.models.orm.signaal_orm import SignaalORM


class BasisProfielReader:
    def __init__(self, engine: Engine):
        self.engine = engine

    def get_missing_kvk_nummers(self, limit: int = 50) -> list[str]:
        """Retourneert random sample van KVK nummers die wel in signalen staan maar nog niet in basisprofielen.

        Hiermee halen we kvk nummers op die wel uit signalen komen, maar mogelijk nog niet bekend zijn.
        Hierdoor beperking op aantal op te halen nummers per keer (limit), zodat we langzaam over tijd inlopen.
        """
        fetch_size = limit * 5
        with Session(self.engine) as session:
            stmt = (
                select(SignaalORM.kvknummer)
                .outerjoin(BasisProfielORM, SignaalORM.kvknummer == BasisProfielORM.kvk_nummer)
                .where(BasisProfielORM.kvk_nummer.is_(None))
                .distinct()
                .limit(fetch_size)  # maximaal limit nieuwe per keer ophalen
            )

            result = session.execute(stmt).scalars().all()
            all_kvk_nrs = list(result)

            # Random sample uit de opgehaalde resultaten
            return random.sample(all_kvk_nrs, min(limit, len(all_kvk_nrs)))

    def get_missing_kvk_nummers_count(self) -> int:
        """Retourneert het totaal aantal KVK nummers die wel in signalen staan maar nog niet in basisprofielen."""
        with Session(self.engine) as session:
            stmt = (
                select(func.count(func.distinct(SignaalORM.kvknummer)))
                .outerjoin(BasisProfielORM, SignaalORM.kvknummer == BasisProfielORM.kvk_nummer)
                .where(BasisProfielORM.kvk_nummer.is_(None))
            )

            result = session.execute(stmt).scalar()
            return result or 0

    def get_outdated_kvk_nummers(self) -> list[str]:
        """Retourneert unieke KVK nummers die zowel in signalen als basisprofielen staan.

        Hierbij worden alleen basisprofielen bekeken de signaal timestamp nieuwer is
        dan het basisprofiel (update nodig).
        """
        with Session(self.engine) as session:
            stmt = (
                select(SignaalORM.kvknummer)
                .join(BasisProfielORM, SignaalORM.kvknummer == BasisProfielORM.kvk_nummer)
                .where(
                    SignaalORM.timestamp > BasisProfielORM.last_updated,
                    SignaalORM.vestigingsnummer.is_(None),  # Alleen basisprofiel updates, geen vestigingsprofielen
                )
                .distinct()
            )

            result = session.execute(stmt).scalars().all()
            return list(result)
