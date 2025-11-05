from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import Date, DateTime, Float, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from kvk_connect.models.orm.base import Base


class VestigingsProfielORM(Base):
    __tablename__ = "vestigingsprofielen"

    # Primary key
    vestigingsnummer: Mapped[str] = mapped_column("vestigingsnummer", String(12), primary_key=True, index=True)

    # Correspondentie adres velden
    cor_adres_volledig: Mapped[str | None] = mapped_column("corAdresVolledig", String(500))
    cor_adres_postcode: Mapped[str | None] = mapped_column("corAdresPostcode", String(16))
    cor_adres_postbusnummer: Mapped[int | None] = mapped_column("corAdresPostbusnummer", Integer)
    cor_adres_plaats: Mapped[str | None] = mapped_column("corAdresPlaats", String(255))
    cor_adres_land: Mapped[str | None] = mapped_column("corAdresLand", String(100))

    # Bezoek adres velden
    bzk_adres_volledig: Mapped[str | None] = mapped_column("bzkAdresVolledig", String(500))
    bzk_adres_straatnaam: Mapped[str | None] = mapped_column("bzkAdresStraatnaam", String(255))
    bzk_adres_huisnummer: Mapped[int | None] = mapped_column("bzkAdresHuisnummer", Integer)
    bzk_adres_postcode: Mapped[str | None] = mapped_column("bzkAdresPostcode", String(16))
    bzk_adres_plaats: Mapped[str | None] = mapped_column("bzkAdresPlaats", String(255))
    bzk_adres_land: Mapped[str | None] = mapped_column("bzkAdresLand", String(100))

    # GPS coördinaten
    bzk_adres_gps_latitude: Mapped[float | None] = mapped_column("bzkAdresGpsLatitude", Float)
    bzk_adres_gps_longitude: Mapped[float | None] = mapped_column("bzkAdresGpsLongitude", Float)

    # Registratie datums
    registratie_datum_aanvang_vestiging: Mapped[datetime | None] = mapped_column(
        "RegistratieDatumAanvangVestiging", Date
    )
    registratie_datum_einde_vestiging: Mapped[datetime | None] = mapped_column("RegistratieDatumEindeVestiging", Date)

    # Timestamp velden met defaults
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
        # Index voor joins met kvkvestigingen
        Index("ix_vestigingsprofiel_vest_updated", vestigingsnummer, last_updated),
    )
