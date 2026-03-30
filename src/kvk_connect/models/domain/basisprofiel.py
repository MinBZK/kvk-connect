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
    ind_non_mailing: str | None = None
    formele_registratiedatum: str | None = None
    hoofdactiviteit: str | None = None
    hoofdactiviteit_omschrijving: str | None = None
    activiteit_overig: str | None = None
    rechtsvorm: str | None = None
    rechtsvorm_uitgebreid: str | None = None
    eerste_handelsnaam: str | None = None
    handelsnamen: str | None = None
    vestigingsnummer: str | None = None
    totaal_werkzame_personen: int | None = None
    websites: str | None = None
    registratie_datum_aanvang: str | None = None
    registratie_datum_einde: str | None = None
    niet_leverbaar_code: str | None = None

    @staticmethod
    def from_dict(d: dict[str, Any]) -> BasisProfielDomain:
        """Deserialize from a dictionary."""
        return BasisProfielDomain(
            kvk_nummer=d.get("kvkNummer"),
            naam=d.get("naam"),
            ind_non_mailing=d.get("indNonMailing"),
            formele_registratiedatum=d.get("formeleRegistratiedatum"),
            hoofdactiviteit=d.get("hoofdactiviteit"),
            hoofdactiviteit_omschrijving=d.get("hoofdactiviteitOmschrijving"),
            activiteit_overig=d.get("activiteitOverig"),
            rechtsvorm=d.get("rechtsvorm"),
            rechtsvorm_uitgebreid=d.get("rechtsvormUitgebreid"),
            eerste_handelsnaam=d.get("eersteHandelsnaam"),
            handelsnamen=d.get("handelsnamen"),
            vestigingsnummer=d.get("vestigingsnummer"),
            totaal_werkzame_personen=d.get("totaalWerkzamePersonen"),
            websites=d.get("websites"),
            registratie_datum_aanvang=d.get("RegistratieDatumAanvang"),
            registratie_datum_einde=d.get("RegistratieDatumEinde"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary."""
        """Converteer domeinmodel naar dictionary."""
        return asdict(self)
