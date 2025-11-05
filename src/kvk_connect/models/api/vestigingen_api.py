# ruff: noqa: D102
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

# Reuse Link from basisprofiel models
from .basisprofiel_api import Link


@dataclass
class Vestiging:
    vestigingsnummer: str = ""
    eerste_handelsnaam: str = ""
    ind_hoofdvestiging: str = ""
    ind_adres_afgeschermd: str = ""
    ind_commerciele_vestiging: str = ""
    volledig_adres: str = ""
    links: list[Link] = field(default_factory=list)

    @staticmethod
    def from_dict(d: dict[str, Any] | None) -> Vestiging | None:  # noqa: D102
        if not d:
            return None
        return Vestiging(
            vestigingsnummer=d.get("vestigingsnummer", "") or "",
            eerste_handelsnaam=d.get("eersteHandelsnaam", "") or "",
            ind_hoofdvestiging=d.get("indHoofdvestiging", "") or "",
            ind_adres_afgeschermd=d.get("indAdresAfgeschermd", "") or "",
            ind_commerciele_vestiging=d.get("indCommercieleVestiging", "") or "",
            volledig_adres=d.get("volledigAdres", "") or "",
            links=[link for link in (Link.from_dict(x) for x in d.get("links", [])) if link],
        )


@dataclass
class VestigingenAPI:
    kvk_nummer: str = ""
    aantal_commerciele_vestigingen: int | None = None
    aantal_niet_commerciele_vestigingen: int | None = None
    totaal_aantal_vestigingen: int | None = None
    vestigingen: list[Vestiging] = field(default_factory=list)
    links: list[Link] = field(default_factory=list)

    @staticmethod
    def from_dict(d: dict[str, Any]) -> VestigingenAPI:  # noqa: D102
        return VestigingenAPI(
            kvk_nummer=d.get("kvkNummer", "") or "",
            aantal_commerciele_vestigingen=d.get("aantalCommercieleVestigingen"),
            aantal_niet_commerciele_vestigingen=d.get("aantalNietCommercieleVestigingen"),
            totaal_aantal_vestigingen=d.get("totaalAantalVestigingen"),
            vestigingen=[v for v in (Vestiging.from_dict(x) for x in d.get("vestigingen", [])) if v],
            links=[link for link in (Link.from_dict(x) for x in d.get("links", [])) if link],
        )

    @staticmethod
    def load_from_file(path: str, encoding: str = "utf-8") -> VestigingenAPI:  # noqa: D102
        with open(path, encoding=encoding) as f:
            data = json.load(f)
        return VestigingenAPI.from_dict(data)

    @staticmethod
    def load_from_json(json_str: str) -> VestigingenAPI:  # noqa: D102
        data = json.loads(json_str)
        return VestigingenAPI.from_dict(data)

    @staticmethod
    def load_from_dict(data: dict[str, Any]) -> VestigingenAPI:  # noqa: D102
        return VestigingenAPI.from_dict(data)

    def to_dict(self) -> dict[str, Any]:  # noqa: D102
        return asdict(self)
