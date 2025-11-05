# ruff: noqa: D102
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

from .basisprofiel_api import Adres, HandelNaam, Link, MaterieleRegistratie, SBIActiviteit


@dataclass
class VestigingsProfielAPI:
    vestigingsnummer: str = ""
    kvk_nummer: str = ""
    rsin: str = ""
    ind_non_mailing: str = ""
    formele_registratiedatum: str = ""
    materiele_registratie: MaterieleRegistratie | None = None
    statutaire_naam: str = ""
    eerste_handelsnaam: str = ""
    ind_hoofdvestiging: str = ""
    ind_commerciele_vestiging: str = ""
    voltijd_werkzame_personen: int | None = None
    totaal_werkzame_personen: int | None = None
    deeltijd_werkzame_personen: int | None = None
    handelsnamen: list[HandelNaam] = field(default_factory=list)
    adressen: list[Adres] = field(default_factory=list)
    websites: list[str] = field(default_factory=list)
    sbi_activiteiten: list[SBIActiviteit] = field(default_factory=list)
    links: list[Link] = field(default_factory=list)

    @staticmethod
    def from_dict(d: dict[str, Any]) -> VestigingsProfielAPI:  # noqa: D102
        return VestigingsProfielAPI(
            vestigingsnummer=d.get("vestigingsnummer", "") or "",
            kvk_nummer=d.get("kvkNummer", "") or "",
            rsin=d.get("rsin", "") or "",
            ind_non_mailing=d.get("indNonMailing", "") or "",
            formele_registratiedatum=d.get("formeleRegistratiedatum", "") or "",
            materiele_registratie=MaterieleRegistratie.from_dict(d.get("materieleRegistratie")),
            statutaire_naam=d.get("statutaireNaam", "") or "",
            eerste_handelsnaam=d.get("eersteHandelsnaam", "") or "",
            ind_hoofdvestiging=d.get("indHoofdvestiging", "") or "",
            ind_commerciele_vestiging=d.get("indCommercieleVestiging", "") or "",
            voltijd_werkzame_personen=d.get("voltijdWerkzamePersonen"),
            totaal_werkzame_personen=d.get("totaalWerkzamePersonen"),
            deeltijd_werkzame_personen=d.get("deeltijdWerkzamePersonen"),
            handelsnamen=[h for h in (HandelNaam.from_dict(x) for x in d.get("handelsnamen", [])) if h],
            adressen=[a for a in (Adres.from_dict(x) for x in d.get("adressen", [])) if a],
            websites=list(d.get("websites", []) or []),
            sbi_activiteiten=[s for s in (SBIActiviteit.from_dict(x) for x in d.get("sbiActiviteiten", [])) if s],
            links=[link for link in (Link.from_dict(x) for x in d.get("links", [])) if link],
        )

    @staticmethod
    def load_from_file(path: str, encoding: str = "utf-8") -> VestigingsProfielAPI:  # noqa: D102
        with open(path, encoding=encoding) as f:
            data = json.load(f)
        return VestigingsProfielAPI.from_dict(data)

    @staticmethod
    def load_from_json(json_str: str) -> VestigingsProfielAPI:  # noqa: D102
        data = json.loads(json_str)
        return VestigingsProfielAPI.from_dict(data)

    @staticmethod
    def load_from_dict(data: dict[str, Any]) -> VestigingsProfielAPI:  # noqa: D102
        return VestigingsProfielAPI.from_dict(data)

    def to_dict(self) -> dict[str, Any]:  # noqa: D102
        return asdict(self)
