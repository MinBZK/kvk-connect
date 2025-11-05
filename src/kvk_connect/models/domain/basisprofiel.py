# ruff: noqa: D102
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class BasisProfielDomain:
    """Dit is ons domeinmodel van een KVK record.

    Alleen de velden die hier staan schrijven we weg naar CSV/SQL
    """

    kvk_nummer: str | None = None
    naam: str | None = None
    hoofdactiviteit: str | None = None
    hoofdactiviteit_omschrijving: str | None = None
    activiteit_overig: str | None = None
    rechtsvorm: str | None = None
    rechtsvorm_uitgebreid: str | None = None
    eerste_handelsnaam: str | None = None
    vestigingsnummer: str | None = None
    totaal_werkzame_personen: int | None = None
    websites: str | None = None
    registratie_datum_aanvang: str | None = None
    registratie_datum_einde: str | None = None
    adres_type: str | None = None
    postbusnummer: str | None = None
    adres_straatnaam: str | None = None
    adres_toevoeging: str | None = None
    adres_postcode: str | None = None
    adres_plaats: str | None = None
    gps_latitude: str | None = None
    gps_longitude: str | None = None

    @staticmethod
    def from_dict(d: dict[str, Any]) -> BasisProfielDomain:  # noqa: D102
        return BasisProfielDomain(
            kvk_nummer=d.get("kvkNummer"),
            naam=d.get("naam"),
            hoofdactiviteit=d.get("hoofdactiviteit"),
            hoofdactiviteit_omschrijving=d.get("hoofdactiviteitOmschrijving"),
            activiteit_overig=d.get("activiteitOverig"),
            rechtsvorm=d.get("rechtsvorm"),
            rechtsvorm_uitgebreid=d.get("rechtsvormUitgebreid"),
            eerste_handelsnaam=d.get("eersteHandelsnaam"),
            vestigingsnummer=d.get("vestigingsnummer"),
            totaal_werkzame_personen=d.get("totaalWerkzamePersonen"),
            websites=d.get("websites"),
            registratie_datum_aanvang=d.get("RegistratieDatumAanvang"),
            registratie_datum_einde=d.get("RegistratieDatumEinde"),
            adres_type=d.get("AdresType"),
            postbusnummer=d.get("Postbusnummer"),
            adres_straatnaam=d.get("AdresStraatnaam"),
            adres_toevoeging=d.get("AdresToevoeging"),
            adres_postcode=d.get("AdresPostcode"),
            adres_plaats=d.get("AdresPlaats"),
            gps_latitude=d.get("gpsLatitude"),
            gps_longitude=d.get("gpsLongitude"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Converteer domeinmodel naar dictionary."""
        return asdict(self)
