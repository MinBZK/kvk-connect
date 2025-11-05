from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import Date, DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from kvk_connect.models.orm.base import Base


class BasisProfielORM(Base):
    __tablename__ = "basisprofielen"

    # Primary key
    kvk_nummer: Mapped[str] = mapped_column("kvkNummer", String(8), primary_key=True)

    # Text fields
    naam: Mapped[str | None] = mapped_column("naam", Text)
    eerste_handelsnaam: Mapped[str | None] = mapped_column("eersteHandelsnaam", Text)
    websites: Mapped[str | None] = mapped_column("websites", Text)

    # String fields
    hoofdactiviteit: Mapped[str | None] = mapped_column("hoofdactiviteit", String(255))
    hoofdactiviteit_omschrijving: Mapped[str | None] = mapped_column("hoofdactiviteitOmschrijving", String(255))
    activiteit_overig: Mapped[str | None] = mapped_column("activiteitOverig", String(255))
    rechtsvorm: Mapped[str | None] = mapped_column("rechtsvorm", String(128))
    rechtsvorm_uitgebreid: Mapped[str | None] = mapped_column("rechtsvormUitgebreid", String(255))

    # Integer field
    totaal_werkzame_personen: Mapped[int | None] = mapped_column("totaalWerkzamePersonen", Integer)

    # Date fields
    registratie_datum_aanvang: Mapped[datetime | None] = mapped_column("RegistratieDatumAanvang", Date)
    registratie_datum_einde: Mapped[datetime | None] = mapped_column("RegistratieDatumEinde", Date)

    # Timestamp fields with defaults
    created_at: Mapped[datetime] = mapped_column(
        "created_at", DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )

    last_updated: Mapped[datetime] = mapped_column(
        "last_updated",
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
        index=True,  # Index voor last_updated filtering
    )

    __table_args__ = (
        # Composite index voor outdated checks
        Index("ix_basisprofiel_kvk_updated", kvk_nummer, last_updated),
    )
