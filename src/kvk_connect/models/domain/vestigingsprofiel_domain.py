# ruff: noqa: D102
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class VestigingsProfielDomain:
    """Dataclass voor vestigingsprofiel domeinmodel (gefilterde velden)."""

    vestigingsnummer: str | None = None

    cor_adres_volledig: str | None = None
    cor_adres_postcode: str | None = None
    cor_adres_postbusnummer: int | None = None
    cor_adres_plaats: str | None = None
    cor_adres_land: str | None = None

    bzk_adres_volledig: str | None = None
    bzk_adres_straatnaam: str | None = None
    bzk_adres_huisnummer: int | None = None
    bzk_adres_postcode: str | None = None
    bzk_adres_plaats: str | None = None
    bzk_adres_land: str | None = None
    bzk_adres_gps_latitude: str | None = None
    bzk_adres_gps_longitude: str | None = None

    registratie_datum_aanvang_vestiging: str | None = None
    registratie_datum_einde_vestiging: str | None = None

    @staticmethod
    def from_dict(d: dict[str, Any]) -> VestigingsProfielDomain:
        """Maak een VestigingsProfielDomain uit een dictionary."""
        if not d:
            return VestigingsProfielDomain()
        return VestigingsProfielDomain(
            vestigingsnummer=d.get("vestigingsnummer"),
            cor_adres_volledig=d.get("corAdresVolledig"),
            cor_adres_postcode=d.get("corAdresPostcode"),
            cor_adres_postbusnummer=d.get("corAdresPostbusnummer"),
            cor_adres_plaats=d.get("corAdresPlaats"),
            cor_adres_land=d.get("corAdresLand"),
            bzk_adres_volledig=d.get("bzkAdresVolledig"),
            bzk_adres_straatnaam=d.get("bzkAdresStraatnaam"),
            bzk_adres_huisnummer=d.get("bzkAdresHuisnummer"),
            bzk_adres_postcode=d.get("bzkAdresPostcode"),
            bzk_adres_plaats=d.get("bzkAdresPlaats"),
            bzk_adres_land=d.get("bzkAdresLand"),
            bzk_adres_gps_latitude=d.get("bzkAdresGpsLatitude"),
            bzk_adres_gps_longitude=d.get("bzkAdresGpsLongitude"),
            registratie_datum_aanvang_vestiging=d.get("RegistratieDatumAanvangVestiging"),
            registratie_datum_einde_vestiging=d.get("RegistratieDatumEindeVestiging"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Converteer domeinmodel naar dictionary."""
        return asdict(self)
