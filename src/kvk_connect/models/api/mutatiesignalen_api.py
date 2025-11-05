# ruff: noqa: D102
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class MutatieSignaal:
    id: str
    kvknummer: str
    signaal_type: str
    timestamp: datetime | None = None
    vestigingsnummer: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MutatieSignaal":  # noqa: D102
        ts = data.get("timestamp", "")
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) if ts else None
        return cls(
            id=data.get("id", ""),
            timestamp=dt,
            kvknummer=data.get("kvknummer", ""),
            signaal_type=data.get("signaalType", ""),
            vestigingsnummer=data.get("vestigingsnummer"),
        )


@dataclass
class MutatiesAPI:
    pagina: int
    aantal: int
    totaal: int
    totaal_paginas: int
    signalen: list[MutatieSignaal] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MutatiesAPI":  # noqa: D102
        return cls(
            pagina=data.get("pagina", 0),
            aantal=data.get("aantal", 0),
            totaal=data.get("totaal", 0),
            totaal_paginas=data.get("totaalPaginas", 0),
            signalen=[MutatieSignaal.from_dict(s) for s in data.get("signalen", [])],
        )
