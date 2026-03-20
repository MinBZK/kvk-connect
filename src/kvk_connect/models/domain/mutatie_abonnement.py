import json
from dataclasses import dataclass


@dataclass
class MutatieAbonnementDomain:
    abonnement_ids: list[str]

    @staticmethod
    def from_dict(data: list[str]) -> "MutatieAbonnementDomain":
        """Deserialize from a list."""
        return MutatieAbonnementDomain(abonnement_ids=data)

    @staticmethod
    def from_json(json_str: str) -> "MutatieAbonnementDomain":
        """Deserialize from a JSON string."""
        data = json.loads(json_str)
        return MutatieAbonnementDomain.from_dict(data)

    def to_list(self) -> list[str]:
        """Serialize to a list."""
        return self.abonnement_ids
