from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from kvk_connect.models.orm.base import Base


class VestigingenORM(Base):
    """Relatie tussen KvK-nummers en hun vestigingen.

    Relatie:
    - 1 record met vestigingsnummer='000000000000' = KvK zonder vestigingen (1:0)
    - N records met vestigingsnummer ingevuld = KvK met vestigingen (1:N)

    Gebruikt een sentinel waarde ('000000000000') i.p.v. NULL voor betere
    upsert performance met merge().
    """

    __tablename__ = "vestigingen"

    # Sentinel waarde voor ontbrekende vestigingsnummers
    SENTINEL_VESTIGINGSNUMMER = "000000000000"

    # Composite primary key
    kvk_nummer: Mapped[str] = mapped_column("kvkNummer", String(8), primary_key=True)
    vestigingsnummer: Mapped[str] = mapped_column("vestigingsnummer", String(12), primary_key=True)

    # Timestamp fields with defaults
    created_at: Mapped[datetime] = mapped_column(
        "created_at", DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )

    last_updated: Mapped[datetime] = mapped_column(
        "last_updated",
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
        index=True,
    )

    __table_args__ = (
        # Index voor filteren op sentinel waarden
        Index(
            "ix_kvkvestigingen_vestigingsnummer_filtered",
            vestigingsnummer,
            postgresql_where=vestigingsnummer != SENTINEL_VESTIGINGSNUMMER,
        ),
        # Composite index voor joins met vestigingsprofiel
        Index("ix_kvkvestigingen_vest_updated", vestigingsnummer, last_updated),
    )
