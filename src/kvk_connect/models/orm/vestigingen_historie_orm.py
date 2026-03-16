from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from kvk_connect.models.orm.base import Base


class VestigingenHistorieORM(Base):
    __tablename__ = "vestigingen_historie"

    # Primary key
    id: Mapped[int] = mapped_column("id", Integer, primary_key=True)

    # Identificatie (geen FK — historie blijft staan bij delete van basisprofiel)
    kvk_nummer: Mapped[str] = mapped_column("kvk_nummer", String(8), nullable=False)
    vestigingsnummer: Mapped[str] = mapped_column("vestigingsnummer", String(12), nullable=False)
    event_type: Mapped[str] = mapped_column("event_type", String(16), nullable=False)
    gewijzigd_op: Mapped[datetime] = mapped_column("gewijzigd_op", DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_vestigingen_historie_kvk_ts", kvk_nummer, gewijzigd_op),)
