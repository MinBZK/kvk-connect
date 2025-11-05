from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from kvk_connect.models.orm.base import Base

"""
ORM model for the 'signalen' table.
"""


class SignaalORM(Base):
    __tablename__ = "signalen"

    # Primary key (UUID)
    id: Mapped[str] = mapped_column("id", String(36), primary_key=True)

    # Required fields with indexes
    timestamp: Mapped[datetime] = mapped_column(
        "timestamp",
        DateTime,
        index=True,  # Index voor timestamp filtering
    )
    kvknummer: Mapped[str] = mapped_column(
        "kvknummer",
        String(8),
        index=True,  # Index voor joins met basisprofiel
    )
    signaal_type: Mapped[str] = mapped_column("signaalType", String(100))

    # Optional field
    vestigingsnummer: Mapped[str | None] = mapped_column("vestigingsnummer", String(12))

    __table_args__ = (
        # Composite index voor outdated checks
        Index("ix_signaal_kvk_timestamp", kvknummer, timestamp),
    )
