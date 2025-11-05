# ruff: noqa: D102
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class KvKVestigingsNummersDomain:
    """Domein model voor een lijst van alle vestigingsnummers (strings)."""

    kvk_nummer: str = ""
    vestigingsnummers: list[str] = field(default_factory=list)

    @staticmethod
    def from_dict(d: dict[str, Any]) -> KvKVestigingsNummersDomain:  # noqa: D102
        return KvKVestigingsNummersDomain(
            kvk_nummer=d.get("kvkNummer", "") or "", vestigingsnummers=d.get("vestigingen") or []
        )

    @staticmethod
    def load_from_json(json_str: str) -> KvKVestigingsNummersDomain:  # noqa: D102
        data = json.loads(json_str)
        return KvKVestigingsNummersDomain.from_dict(data)

    def to_dict(self) -> dict:  # noqa: D102
        return {"kvkNummer": self.kvk_nummer, "vestigingen": list(self.vestigingsnummers)}
