from __future__ import annotations

from datetime import datetime

from sqlalchemy import Date, DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from kvk_connect.models.orm.base import Base


class BasisProfielHistorieORM(Base):
    __tablename__ = "basisprofielen_historie"

    # Primary key
    id: Mapped[int] = mapped_column("id", Integer, primary_key=True)

    # Identificatie (geen FK — historie blijft staan bij delete van basisprofiel)
    kvk_nummer: Mapped[str] = mapped_column("kvk_nummer", String(8), nullable=False)
    gewijzigd_op: Mapped[datetime] = mapped_column("gewijzigd_op", DateTime(timezone=True), nullable=False)
    gewijzigde_velden: Mapped[str | None] = mapped_column("gewijzigde_velden", Text)

    # Business fields (flat columns)
    naam: Mapped[str | None] = mapped_column("naam", Text)
    eerste_handelsnaam: Mapped[str | None] = mapped_column("eerste_handelsnaam", Text)
    handelsnamen: Mapped[str | None] = mapped_column("handelsnamen", Text)
    websites: Mapped[str | None] = mapped_column("websites", Text)
    ind_non_mailing: Mapped[str | None] = mapped_column("ind_non_mailing", String(8))
    hoofdactiviteit: Mapped[str | None] = mapped_column("hoofdactiviteit", String(255))
    hoofdactiviteit_omschrijving: Mapped[str | None] = mapped_column("hoofdactiviteit_omschrijving", String(255))
    activiteit_overig: Mapped[str | None] = mapped_column("activiteit_overig", String(255))
    rechtsvorm: Mapped[str | None] = mapped_column("rechtsvorm", String(128))
    rechtsvorm_uitgebreid: Mapped[str | None] = mapped_column("rechtsvorm_uitgebreid", String(255))
    totaal_werkzame_personen: Mapped[int | None] = mapped_column("totaal_werkzame_personen", Integer)
    formele_registratiedatum: Mapped[datetime | None] = mapped_column("formele_registratiedatum", Date)
    registratie_datum_aanvang: Mapped[datetime | None] = mapped_column("registratie_datum_aanvang", Date)
    registratie_datum_einde: Mapped[datetime | None] = mapped_column("registratie_datum_einde", Date)

    __table_args__ = (Index("ix_bp_historie_kvk_ts", kvk_nummer, gewijzigd_op),)
