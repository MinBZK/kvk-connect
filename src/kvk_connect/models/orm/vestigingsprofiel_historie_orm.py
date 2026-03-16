from __future__ import annotations

from datetime import datetime

from sqlalchemy import Date, DateTime, Float, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from kvk_connect.models.orm.base import Base


class VestigingsProfielHistorieORM(Base):
    __tablename__ = "vestigingsprofielen_historie"

    # Primary key
    id: Mapped[int] = mapped_column("id", Integer, primary_key=True)

    # Identificatie (geen FK — historie blijft staan bij delete van vestigingsprofiel)
    vestigingsnummer: Mapped[str] = mapped_column("vestigingsnummer", String(12), nullable=False)
    kvk_nummer: Mapped[str | None] = mapped_column("kvk_nummer", String(8))
    gewijzigd_op: Mapped[datetime] = mapped_column("gewijzigd_op", DateTime(timezone=True), nullable=False)
    gewijzigde_velden: Mapped[str | None] = mapped_column("gewijzigde_velden", Text)

    # Business fields (flat columns)
    rsin: Mapped[str | None] = mapped_column("rsin", String(9))
    ind_non_mailing: Mapped[str | None] = mapped_column("ind_non_mailing", String(8))
    ind_hoofdvestiging: Mapped[str | None] = mapped_column("ind_hoofdvestiging", String(8))
    ind_commerciele_vestiging: Mapped[str | None] = mapped_column("ind_commerciele_vestiging", String(8))
    statutaire_naam: Mapped[str | None] = mapped_column("statutaire_naam", Text)
    eerste_handelsnaam: Mapped[str | None] = mapped_column("eerste_handelsnaam", Text)
    handelsnamen: Mapped[str | None] = mapped_column("handelsnamen", Text)
    hoofdactiviteit: Mapped[str | None] = mapped_column("hoofdactiviteit", String(255))
    hoofdactiviteit_omschrijving: Mapped[str | None] = mapped_column("hoofdactiviteit_omschrijving", String(255))
    activiteit_overig: Mapped[str | None] = mapped_column("activiteit_overig", String(255))
    voltijd_werkzame_personen: Mapped[int | None] = mapped_column("voltijd_werkzame_personen", Integer)
    deeltijd_werkzame_personen: Mapped[int | None] = mapped_column("deeltijd_werkzame_personen", Integer)
    totaal_werkzame_personen: Mapped[int | None] = mapped_column("totaal_werkzame_personen", Integer)
    websites: Mapped[str | None] = mapped_column("websites", Text)

    # Correspondentie adres
    cor_adres_volledig: Mapped[str | None] = mapped_column("cor_adres_volledig", String(500))
    cor_adres_straatnaam: Mapped[str | None] = mapped_column("cor_adres_straatnaam", String(255))
    cor_adres_huisnummer: Mapped[int | None] = mapped_column("cor_adres_huisnummer", Integer)
    cor_adres_postcode: Mapped[str | None] = mapped_column("cor_adres_postcode", String(16))
    cor_adres_postbusnummer: Mapped[int | None] = mapped_column("cor_adres_postbusnummer", Integer)
    cor_adres_plaats: Mapped[str | None] = mapped_column("cor_adres_plaats", String(255))
    cor_adres_land: Mapped[str | None] = mapped_column("cor_adres_land", String(100))
    cor_adres_gps_latitude: Mapped[float | None] = mapped_column("cor_adres_gps_latitude", Float)
    cor_adres_gps_longitude: Mapped[float | None] = mapped_column("cor_adres_gps_longitude", Float)

    # Bezoek adres
    bzk_adres_volledig: Mapped[str | None] = mapped_column("bzk_adres_volledig", String(500))
    bzk_adres_straatnaam: Mapped[str | None] = mapped_column("bzk_adres_straatnaam", String(255))
    bzk_adres_huisnummer: Mapped[int | None] = mapped_column("bzk_adres_huisnummer", Integer)
    bzk_adres_postcode: Mapped[str | None] = mapped_column("bzk_adres_postcode", String(16))
    bzk_adres_plaats: Mapped[str | None] = mapped_column("bzk_adres_plaats", String(255))
    bzk_adres_land: Mapped[str | None] = mapped_column("bzk_adres_land", String(100))
    bzk_adres_gps_latitude: Mapped[float | None] = mapped_column("bzk_adres_gps_latitude", Float)
    bzk_adres_gps_longitude: Mapped[float | None] = mapped_column("bzk_adres_gps_longitude", Float)

    # Datums
    formele_registratiedatum: Mapped[datetime | None] = mapped_column("formele_registratiedatum", Date)
    registratie_datum_aanvang_vestiging: Mapped[datetime | None] = mapped_column(
        "registratie_datum_aanvang_vestiging", Date
    )
    registratie_datum_einde_vestiging: Mapped[datetime | None] = mapped_column(
        "registratie_datum_einde_vestiging", Date
    )

    __table_args__ = (
        Index("ix_vp_historie_vest_ts", vestigingsnummer, gewijzigd_op),
        Index("ix_vp_historie_kvk_ts", kvk_nummer, gewijzigd_op),
    )
