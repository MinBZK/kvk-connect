# ruff: noqa: D102
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

"""
Naming conventions voor API models is appenden van *API
"""


@dataclass
class Link:
    rel: str = ""
    href: str = ""

    @staticmethod
    def from_dict(d: dict[str, Any] | None) -> Link | None:
        if not d:
            return None
        return Link(rel=d.get("rel", "") or "", href=d.get("href", "") or "")


@dataclass
class GeoData:
    addresseerbaar_object_id: str | None = None
    nummer_aanduiding_id: str | None = None
    gps_latitude: float | None = None
    gps_longitude: float | None = None
    rijksdriehoek_x: float | None = None
    rijksdriehoek_y: float | None = None
    rijksdriehoek_z: float | None = None

    @staticmethod
    def from_dict(d: dict[str, Any] | None) -> GeoData | None:
        if not d:
            return None
        return GeoData(
            addresseerbaar_object_id=d.get("addresseerbaarObjectId"),
            nummer_aanduiding_id=d.get("nummerAanduidingId"),
            gps_latitude=d.get("gpsLatitude"),
            gps_longitude=d.get("gpsLongitude"),
            rijksdriehoek_x=d.get("rijksdriehoekX"),
            rijksdriehoek_y=d.get("rijksdriehoekY"),
            rijksdriehoek_z=d.get("rijksdriehoekZ"),
        )


@dataclass
class Adres:
    type: str = ""
    ind_afgeschermd: str = ""
    volledig_adres: str = ""
    straatnaam: str = ""
    huisnummer: int | None = None
    huisletter: str | None = None
    postbusnummer: int | None = None
    postcode: str = ""
    plaats: str = ""
    land: str = ""
    geo_data: GeoData | None = None

    @staticmethod
    def from_dict(d: dict[str, Any] | None) -> Adres | None:
        if not d:
            return None
        return Adres(
            type=d.get("type", "") or "",
            ind_afgeschermd=d.get("indAfgeschermd", "") or "",
            volledig_adres=d.get("volledigAdres", "") or "",
            straatnaam=d.get("straatnaam", "") or "",
            huisnummer=d.get("huisnummer"),
            huisletter=d.get("huisletter"),
            postbusnummer=d.get("postbusnummer"),
            postcode=d.get("postcode", "") or "",
            plaats=d.get("plaats", "") or "",
            land=d.get("land", "") or "",
            geo_data=GeoData.from_dict(d.get("geoData")),
        )


@dataclass
class MaterieleRegistratie:
    datum_aanvang: str | None = None
    datum_einde: str | None = None

    @staticmethod
    def from_dict(d: dict[str, Any] | None) -> MaterieleRegistratie | None:
        if not d:
            return None
        return MaterieleRegistratie(datum_aanvang=d.get("datumAanvang"), datum_einde=d.get("datumEinde"))


@dataclass
class HandelNaam:
    naam: str = ""
    volgorde: int | None = None

    @staticmethod
    def from_dict(d: dict[str, Any] | None) -> HandelNaam | None:
        if not d:
            return None
        return HandelNaam(naam=d.get("naam", "") or "", volgorde=d.get("volgorde"))


@dataclass
class SBIActiviteit:
    sbi_code: str = ""
    sbi_omschrijving: str = ""
    ind_hoofdactiviteit: str = ""

    @staticmethod
    def from_dict(d: dict[str, Any] | None) -> SBIActiviteit | None:
        if not d:
            return None
        return SBIActiviteit(
            sbi_code=d.get("sbiCode", "") or "",
            sbi_omschrijving=d.get("sbiOmschrijving", "") or "",
            ind_hoofdactiviteit=d.get("indHoofdactiviteit", "") or "",
        )


@dataclass
class Hoofdvestiging:
    vestigingsnummer: str = ""
    kvk_nummer: str = ""
    formele_registratiedatum: str = ""
    materiele_registratie: MaterieleRegistratie | None = None
    eerste_handelsnaam: str = ""
    ind_hoofdvestiging: str = ""
    ind_commerciele_vestiging: str = ""
    totaal_werkzame_personen: int | None = None
    adressen: list[Adres] = field(default_factory=list)
    websites: list[str] = field(default_factory=list)
    links: list[Link] = field(default_factory=list)

    @staticmethod
    def from_dict(d: dict[str, Any] | None) -> Hoofdvestiging | None:  # noqa: D102
        if not d:
            return None
        return Hoofdvestiging(
            vestigingsnummer=d.get("vestigingsnummer", "") or "",
            kvk_nummer=d.get("kvkNummer", "") or "",
            formele_registratiedatum=d.get("formeleRegistratiedatum", "") or "",
            materiele_registratie=MaterieleRegistratie.from_dict(d.get("materieleRegistratie")),
            eerste_handelsnaam=d.get("eersteHandelsnaam", "") or "",
            ind_hoofdvestiging=d.get("indHoofdvestiging", "") or "",
            ind_commerciele_vestiging=d.get("indCommercieleVestiging", "") or "",
            totaal_werkzame_personen=d.get("totaalWerkzamePersonen"),
            adressen=[a for a in (Adres.from_dict(x) for x in d.get("adressen", [])) if a],
            websites=list(d.get("websites", []) or []),
            links=[link for link in (Link.from_dict(x) for x in d.get("links", [])) if link],
        )


@dataclass
class Eigenaar:
    rechtsvorm: str = ""
    uitgebreide_rechtsvorm: str = ""
    links: list[Link] = field(default_factory=list)

    @staticmethod
    def from_dict(d: dict[str, Any] | None) -> Eigenaar | None:  # noqa: D102
        if not d:
            return None
        return Eigenaar(
            rechtsvorm=d.get("rechtsvorm", "") or "",
            uitgebreide_rechtsvorm=d.get("uitgebreideRechtsvorm", "") or "",
            links=[link for link in (Link.from_dict(x) for x in d.get("links", [])) if link],
        )


@dataclass
class Embedded:
    hoofdvestiging: Hoofdvestiging | None = None
    eigenaar: Eigenaar | None = None

    @staticmethod
    def from_dict(d: dict[str, Any] | None) -> Embedded | None:  # noqa: D102
        if not d:
            return None
        return Embedded(
            hoofdvestiging=Hoofdvestiging.from_dict(d.get("hoofdvestiging")),
            eigenaar=Eigenaar.from_dict(d.get("eigenaar")),
        )


@dataclass
class BasisProfielAPI:
    kvk_nummer: str = ""
    ind_non_mailing: str = ""
    naam: str = ""
    formele_registratiedatum: str = ""
    materiele_registratie: MaterieleRegistratie | None = None
    totaal_werkzame_personen: int | None = None
    handelsnamen: list[HandelNaam] = field(default_factory=list)
    sbi_activiteiten: list[SBIActiviteit] = field(default_factory=list)
    links: list[Link] = field(default_factory=list)
    embedded: Embedded | None = None

    @staticmethod
    def from_dict(d: dict[str, Any]) -> BasisProfielAPI:  # noqa: D102
        return BasisProfielAPI(
            kvk_nummer=d.get("kvkNummer", "") or "",
            ind_non_mailing=d.get("indNonMailing", "") or "",
            naam=d.get("naam", "") or "",
            formele_registratiedatum=d.get("formeleRegistratiedatum", "") or "",
            materiele_registratie=MaterieleRegistratie.from_dict(d.get("materieleRegistratie")),
            totaal_werkzame_personen=d.get("totaalWerkzamePersonen"),
            handelsnamen=[h for h in (HandelNaam.from_dict(x) for x in d.get("handelsnamen", [])) if h],
            sbi_activiteiten=[a for a in (SBIActiviteit.from_dict(x) for x in d.get("sbiActiviteiten", [])) if a],
            links=[link for link in (Link.from_dict(x) for x in d.get("links", [])) if link],
            embedded=Embedded.from_dict(d.get("_embedded")),
        )

    @staticmethod
    def load_from_file(path: str, encoding: str = "utf-8") -> BasisProfielAPI:  # noqa: D102
        with open(path, encoding=encoding) as f:
            data = json.load(f)
        return BasisProfielAPI.from_dict(data)

    @staticmethod
    def load_from_json(json_str: str) -> BasisProfielAPI:  # noqa: D102
        data = json.loads(json_str)
        return BasisProfielAPI.from_dict(data)

    @staticmethod
    def load_from_dict(data: dict[str, Any]) -> BasisProfielAPI:  # noqa: D102
        return BasisProfielAPI.from_dict(data)

    def to_dict(self) -> dict[str, Any]:  # noqa: D102
        return asdict(self)
