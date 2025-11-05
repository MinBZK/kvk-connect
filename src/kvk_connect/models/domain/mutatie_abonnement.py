# ruff: noqa: D102
import json
from dataclasses import dataclass


@dataclass
class MutatieAbonnementDomain:
    abonnement_ids: list[str]

    @staticmethod
    def from_dict(data: list[str]) -> "MutatieAbonnementDomain":  # noqa: D102
        return MutatieAbonnementDomain(abonnement_ids=data)

    @staticmethod
    def from_json(json_str: str) -> "MutatieAbonnementDomain":  # noqa: D102
        data = json.loads(json_str)
        return MutatieAbonnementDomain.from_dict(data)

    def to_list(self) -> list[str]:  # noqa: D102
        return self.abonnement_ids
