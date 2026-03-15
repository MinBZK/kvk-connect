from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import Date, DateTime, Float, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from kvk_connect.models.orm.base import Base


class VestigingsProfielORM(Base):
    __tablename__ = "vestigingsprofielen"

    # Primary key
    vestigingsnummer: Mapped[str] = mapped_column("vestigingsnummer", String(12), primary_key=True, index=True)

    # Identificatie
    kvk_nummer: Mapped[str | None] = mapped_column("kvkNummer", String(8))
    rsin: Mapped[str | None] = mapped_column("rsin", String(9))

    # Kenmerken
    ind_non_mailing: Mapped[str | None] = mapped_column("indNonMailing", String(8))
    ind_hoofdvestiging: Mapped[str | None] = mapped_column("indHoofdvestiging", String(8))
    ind_commerciele_vestiging: Mapped[str | None] = mapped_column("indCommercieleVestiging", String(8))

    # Namen
    statutaire_naam: Mapped[str | None] = mapped_column("statutaireNaam", Text)
    eerste_handelsnaam: Mapped[str | None] = mapped_column("eersteHandelsnaam", Text)
    handelsnamen: Mapped[str | None] = mapped_column("handelsnamen", Text)

    # Activiteiten
    hoofdactiviteit: Mapped[str | None] = mapped_column("hoofdactiviteit", String(255))
    hoofdactiviteit_omschrijving: Mapped[str | None] = mapped_column("hoofdactiviteitOmschrijving", String(255))
    activiteit_overig: Mapped[str | None] = mapped_column("activiteitOverig", String(255))

    # Werkzame personen
    voltijd_werkzame_personen: Mapped[int | None] = mapped_column("voltijdWerkzamePersonen", Integer)
    deeltijd_werkzame_personen: Mapped[int | None] = mapped_column("deeltijdWerkzamePersonen", Integer)
    totaal_werkzame_personen: Mapped[int | None] = mapped_column("totaalWerkzamePersonen", Integer)

    # Websites
    websites: Mapped[str | None] = mapped_column("websites", Text)

    # Correspondentie adres velden
    cor_adres_volledig: Mapped[str | None] = mapped_column("corAdresVolledig", String(500))
    cor_adres_straatnaam: Mapped[str | None] = mapped_column("corAdresStraatnaam", String(255))
    cor_adres_huisnummer: Mapped[int | None] = mapped_column("corAdresHuisnummer", Integer)
    cor_adres_postcode: Mapped[str | None] = mapped_column("corAdresPostcode", String(16))
    cor_adres_postbusnummer: Mapped[int | None] = mapped_column("corAdresPostbusnummer", Integer)
    cor_adres_plaats: Mapped[str | None] = mapped_column("corAdresPlaats", String(255))
    cor_adres_land: Mapped[str | None] = mapped_column("corAdresLand", String(100))
    cor_adres_gps_latitude: Mapped[float | None] = mapped_column("corAdresGpsLatitude", Float)
    cor_adres_gps_longitude: Mapped[float | None] = mapped_column("corAdresGpsLongitude", Float)

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
    formele_registratiedatum: Mapped[datetime | None] = mapped_column("formeleRegistratiedatum", Date)
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

    # Foutafhandeling
    niet_leverbaar_code: Mapped[str | None] = mapped_column(
        "niet_leverbaar_code", String(16), nullable=True, default=None
    )
    retry_after: Mapped[datetime | None] = mapped_column(
        "retry_after", DateTime(timezone=True), nullable=True, default=None
    )

    __table_args__ = (
        # Index voor joins met kvkvestigingen
        Index("ix_vestigingsprofiel_vest_updated", vestigingsnummer, last_updated),
    )
