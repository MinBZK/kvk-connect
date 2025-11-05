from sqlalchemy import select
from sqlalchemy.orm import Session

from kvk_connect.models.orm.kvkvestigingen_orm import VestigingenORM
from kvk_connect.models.orm.signaal_orm import SignaalORM
from kvk_connect.models.orm.vestigingsprofiel_orm import VestigingsProfielORM


class VestigingsProfielReader:
    def __init__(self, engine):
        self.engine = engine

    def get_vestigingen_zonder_vestigingsprofielen(self) -> list[str]:
        """Haalt alle vestigingsnummers op die wel in kvk_vestigingen staan maar nog niet in vestigingsprofielen.

        Returns:
            list[str]: Lijst met vestigingsnummers zonder vestigingsprofiel
        """
        with Session(self.engine) as session:
            stmt = (
                select(VestigingenORM.vestigingsnummer)
                .outerjoin(
                    VestigingsProfielORM, VestigingenORM.vestigingsnummer == VestigingsProfielORM.vestigingsnummer
                )
                .where(VestigingsProfielORM.vestigingsnummer.is_(None))
                .where(VestigingenORM.vestigingsnummer != VestigingenORM.SENTINEL_VESTIGINGSNUMMER)
                .distinct()
            )

            result = session.execute(stmt)
            return [row[0] for row in result.fetchall()]

    def get_outdated_vestigingen(self) -> list[str]:
        """Return lijst van vestigingsnummers met vestiging nieuwer dan de lastupdated van het vestigingenprofiel."""

        with Session(self.engine) as session:
            stmt = (
                select(VestigingenORM.vestigingsnummer)
                .join(VestigingsProfielORM, VestigingenORM.vestigingsnummer == VestigingsProfielORM.vestigingsnummer)
                .where(VestigingenORM.last_updated > VestigingsProfielORM.last_updated)
                .where(VestigingenORM.vestigingsnummer != VestigingenORM.SENTINEL_VESTIGINGSNUMMER)
                .distinct()
            )

            result = session.execute(stmt).scalars().all()
            return list(result)

    def get_outdated_vestigingen_signaal(self) -> list[str]:
        """Return lijst van vestigingsnummers met signaal nieuwer is dan de last_updated van het vestigingenprofiel.

        Returns:
            list[str]: Lijst met vestigingsnummers die een update nodig hebben
        """
        with Session(self.engine) as session:
            stmt = (
                select(SignaalORM.vestigingsnummer)
                .join(VestigingsProfielORM, SignaalORM.vestigingsnummer == VestigingsProfielORM.vestigingsnummer)
                .where(
                    SignaalORM.timestamp > VestigingsProfielORM.last_updated,
                    SignaalORM.vestigingsnummer.is_not(None),  # Alleen vestigingsprofielen, geen basisprofielen
                )
                .distinct()
            )

            result = session.execute(stmt).scalars().all()
            return [v for v in result if v is not None]  # filter out possible None values
