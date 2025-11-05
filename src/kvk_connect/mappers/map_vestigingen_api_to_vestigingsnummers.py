from __future__ import annotations

from ..models.api.vestigingen_api import VestigingenAPI
from ..models.domain import KvKVestigingsNummersDomain


def map_vestigingen_api_to_vestigingsnummers(api_model: VestigingenAPI) -> KvKVestigingsNummersDomain:
    """Zet een VestigingenAPI (API-model) om naar ons domeinmodel KvKVestigingsnummers.

    Haalt de lijst van 'vestigingsnummer' waarden op in de originele volgorde.
    """
    nummers: list[str] = [v.vestigingsnummer for v in (api_model.vestigingen or []) if v and v.vestigingsnummer]

    return KvKVestigingsNummersDomain(kvk_nummer=api_model.kvk_nummer or "", vestigingsnummers=nummers)
