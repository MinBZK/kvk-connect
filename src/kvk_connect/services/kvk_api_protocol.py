from __future__ import annotations

from typing import Protocol

from kvk_connect.models.api.basisprofiel_api import BasisProfielAPI
from kvk_connect.models.api.vestigingen_api import VestigingenAPI
from kvk_connect.models.api.vestigingsprofiel_api import VestigingsProfielAPI


class KVKApiClientProtocol(Protocol):
    def get_basisprofiel(self, kvk_nummer: str, geo_data: bool = True) -> BasisProfielAPI | None:
        """Haal basisprofiel op voor een KVK nummer."""
        ...

    def get_vestigingen(self, kvk_nummer: str) -> VestigingenAPI | None:
        """Haal vestigingen op voor een KVK nummer."""
        ...

    def get_vestigingsprofiel(self, vestigingsnummer: str, geo_data: bool = True) -> VestigingsProfielAPI | None:
        """Haal vestigingsprofiel op voor een vestigingsnummer."""
        ...
