# ruff: noqa: D102
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class VestigingsAdresDomain:
    """Domein model voor een vestigingsadres van een KVK vestiging, behorende bij een KVK record."""

    kvk_nummer: str | None = None
    vestigingsnummer: str | None = None
    adres_type: str | None = None
    postbusnummer: str | None = None
    adres_straatnaam: str | None = None
    adres_toevoeging: str | None = None
    adres_postcode: str | None = None
    adres_plaats: str | None = None
    gps_latitude: str | None = None
    gps_longitude: str | None = None

    @staticmethod
    def from_dict(d: dict[str, Any]) -> VestigingsAdresDomain:  # noqa: D102
        def clean(val):
            return val  # if val not in ("", None) else None

        return VestigingsAdresDomain(
            kvk_nummer=clean(d.get("kvkNummer")),
            vestigingsnummer=clean(d.get("vestigingsnummer")),
            adres_type=clean(d.get("AdresType")),
            postbusnummer=clean(d.get("Postbusnummer")),
            adres_straatnaam=clean(d.get("AdresStraatnaam")),
            adres_toevoeging=clean(d.get("AdresToevoeging")),
            adres_postcode=clean(d.get("AdresPostcode")),
            adres_plaats=clean(d.get("AdresPlaats")),
            gps_latitude=clean(d.get("gpsLatitude")),
            gps_longitude=clean(d.get("gpsLongitude")),
        )

    @staticmethod
    def from_list(data: list[dict[str, Any]]) -> list[VestigingsAdresDomain]:  # noqa: D102
        return [VestigingsAdresDomain.from_dict(item) for item in data or []]

    @staticmethod
    def load_from_file(path: str, encoding: str = "utf-8") -> list[VestigingsAdresDomain]:  # noqa: D102
        with open(path, encoding=encoding) as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("JSON must be a list of vestigingsprofielen.")
        return VestigingsAdresDomain.from_list(data)

    @staticmethod
    def load_from_json(json_str: str) -> list[VestigingsAdresDomain]:  # noqa: D102
        data = json.loads(json_str)
        if not isinstance(data, list):
            raise ValueError("JSON must be a list of vestigingsprofielen.")
        return VestigingsAdresDomain.from_list(data)

    def to_dict(self) -> dict[str, Any]:  # noqa: D102
        return asdict(self)
