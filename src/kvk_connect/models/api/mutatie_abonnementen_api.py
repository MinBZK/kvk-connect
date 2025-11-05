# ruff: noqa: D102
from dataclasses import dataclass


@dataclass
class Contract:
    id: str

    @staticmethod
    def from_dict(data: dict) -> "Contract":  # noqa: D102
        return Contract(id=data["id"])


@dataclass
class Abonnement:
    id: str
    contract: Contract
    start_datum: str
    actief: bool

    @staticmethod
    def from_dict(data: dict) -> "Abonnement":  # noqa: D102
        return Abonnement(
            id=data["id"],
            contract=Contract.from_dict(data["contract"]),
            start_datum=data["startDatum"],
            actief=data["actief"],
        )


@dataclass
class MutatieAbonnementenAPI:
    klant_id: str
    abonnementen: list[Abonnement]

    @staticmethod
    def from_dict(data: dict) -> "MutatieAbonnementenAPI":  # noqa: D102
        return MutatieAbonnementenAPI(
            klant_id=data["klantId"], abonnementen=[Abonnement.from_dict(a) for a in data["abonnementen"]]
        )
