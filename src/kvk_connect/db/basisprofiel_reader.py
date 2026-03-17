import random

from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from kvk_connect.models.orm.basisprofiel_orm import BasisProfielORM
from kvk_connect.models.orm.signaal_orm import SignaalORM


class BasisProfielReader:
    def __init__(self, engine: Engine):
        self.engine = engine

    def get_missing_kvk_nummers(self, limit: int = 1000) -> list[str]:
        """Retourneert random sample van KVK nummers die wel in signalen staan maar nog niet in basisprofielen.

        Hiermee halen we kvk nummers op die wel uit signalen komen, maar mogelijk nog niet bekend zijn.
        Hierdoor beperking op aantal op te halen nummers per keer (limit), zodat we langzaam over tijd inlopen.

        Bevat ook records waarvan de retry_after verstreken is (tijdelijk niet-leverbaar).
        Tombstones (niet_leverbaar_code IS NOT NULL) worden altijd uitgesloten.
        """
        with Session(self.engine) as session:
            stmt = (
                select(SignaalORM.kvknummer)
                .outerjoin(BasisProfielORM, SignaalORM.kvknummer == BasisProfielORM.kvk_nummer)
                .where(
                    # Nooit eerder opgehaald
                    BasisProfielORM.kvk_nummer.is_(None)
                    # OF: tijdelijk geblokkeerd maar retry_after verstreken
                    | (
                        BasisProfielORM.niet_leverbaar_code.is_(None)
                        & BasisProfielORM.retry_after.is_not(None)
                        & (BasisProfielORM.retry_after <= func.now())
                    )
                )
                .distinct()
                .limit(limit)  # maximaal limit nieuwe per keer ophalen
            )

            result = session.execute(stmt).scalars().all()
            all_kvk_nrs = list(result)

            # Random sample uit de opgehaalde resultaten
            return random.sample(all_kvk_nrs, min(limit, len(all_kvk_nrs)))

    def get_missing_kvk_nummers_count(self, limit: int = 1000) -> int:
        """Retourneert het totaal aantal KVK nummers die wel in signalen staan maar nog niet in basisprofielen."""
        with Session(self.engine) as session:
            stmt = (
                select(func.count(func.distinct(SignaalORM.kvknummer)))
                .outerjoin(BasisProfielORM, SignaalORM.kvknummer == BasisProfielORM.kvk_nummer)
                .where(
                    BasisProfielORM.kvk_nummer.is_(None)
                    | (
                        BasisProfielORM.niet_leverbaar_code.is_(None)
                        & BasisProfielORM.retry_after.is_not(None)
                        & (BasisProfielORM.retry_after <= func.now())
                    )
                )
                .limit(limit)  # maximaal limit nieuwe per keer ophalen
            )

            result = session.execute(stmt).scalar()
            return result or 0

    def get_outdated_kvk_nummers(self, limit: int = 1000) -> list[str]:
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
                    BasisProfielORM.niet_leverbaar_code.is_(None),  # Geen tombstones updaten
                    BasisProfielORM.retry_after.is_(None)
                    | (BasisProfielORM.retry_after <= func.now()),  # Geen actieve blokkade
                )
                .distinct()
                .limit(limit)  # maximaal limit nieuwe per keer ophalen
            )

            result = session.execute(stmt).scalars().all()
            return list(result)

    def get_outdated_kvk_nummers_count(self) -> int:
        """Retourneert het totaal aantal KVK nummers met een nieuwer signaal dan het opgeslagen basisprofiel."""
        with Session(self.engine) as session:
            stmt = (
                select(func.count(func.distinct(SignaalORM.kvknummer)))
                .join(BasisProfielORM, SignaalORM.kvknummer == BasisProfielORM.kvk_nummer)
                .where(
                    SignaalORM.timestamp > BasisProfielORM.last_updated,
                    BasisProfielORM.niet_leverbaar_code.is_(None),
                    BasisProfielORM.retry_after.is_(None) | (BasisProfielORM.retry_after <= func.now()),
                )
            )
            result = session.execute(stmt).scalar()
            return result or 0

    def kvk_nummer_exists(self, kvk_nummer: str) -> bool:
        """Check if KVK number exists in basisprofiel.

        Optimized to use LIMIT 1 for early termination.

        Args:
            kvk_nummer: 8-digit KVK number.

        Returns:
            True if KVK number exists, False otherwise.
        """
        with Session(self.engine) as session:
            stmt = select(BasisProfielORM.kvk_nummer).where(BasisProfielORM.kvk_nummer == kvk_nummer).limit(1)
            result = session.execute(stmt).scalar()
            return result is not None
