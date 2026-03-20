from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

from kvk_connect.models.domain.vestigingsadresdomain import VestigingsAdresDomain


@dataclass
class VestigingsAdressenDomain:
    """Domein model voor alle adressen behorende bij een KVVKRecord."""

    adressen: list[VestigingsAdresDomain] = field(default_factory=list)

    @staticmethod
    def from_list(data: list[Any]) -> VestigingsAdressenDomain:
        """Maak een VestigingsAdressen object van een lijst dicts of Vestigingsprofiel instanties."""
        profielen = [
            VestigingsAdresDomain.from_dict(item) if not isinstance(item, VestigingsAdresDomain) else item
            for item in (data or [])
        ]
        return VestigingsAdressenDomain(adressen=profielen)

    @staticmethod
    def load_from_file(path: str, encoding: str = "utf-8") -> VestigingsAdressenDomain:
        """Laad vanuit een JSON-bestand."""
        with open(path, encoding=encoding) as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("JSON must be a list of vestigingsprofielen.")
        return VestigingsAdressenDomain.from_list(data)

    @staticmethod
    def load_from_json(json_str: str) -> VestigingsAdressenDomain:
        """Deserialize from a JSON string."""
        data = json.loads(json_str)
        if not isinstance(data, list):
            raise ValueError("JSON must be a list of vestigingsprofielen.")
        return VestigingsAdressenDomain.from_list(data)

    def add_adres(self, adres: VestigingsAdresDomain) -> None:
        """Voeg een adres toe aan de lijst."""
        self.adressen.append(adres)

    def add_adressen(self, adres: VestigingsAdressenDomain) -> None:
        """Voeg meerdere adressen toe aan de lijst."""
        self.adressen.extend(adres.adressen)

    def to_dict(self) -> dict:
        """Serialize to a dictionary."""
        return asdict(self)
