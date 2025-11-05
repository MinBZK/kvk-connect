# ruff: noqa: D102
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Contract:
    id: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Contract":
        return cls(id=data.get("id", ""))


@dataclass
class Abonnement:
    id: str
    contract: Contract
    start_datum: str
    actief: bool

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Abonnement":
        return cls(
            id=data.get("id", ""),
            contract=Contract.from_dict(data.get("contract", {})),
            start_datum=data.get("startDatum", ""),
            actief=data.get("actief", False),
        )


@dataclass
class AbonnementenAPI:
    klant_id: str
    abonnementen: list[Abonnement] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AbonnementenAPI":
        return cls(
            klant_id=data.get("klantId", ""),
            abonnementen=[Abonnement.from_dict(a) for a in data.get("abonnementen", [])],
        )
