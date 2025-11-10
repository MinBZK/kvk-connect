from __future__ import annotations

from kvk_connect.models.api.vestigingsprofiel_api import VestigingsProfielAPI
from kvk_connect.models.domain.vestigingsprofiel_domain import VestigingsProfielDomain
from kvk_connect.utils.formatting import truncate_float
from kvk_connect.utils.tools import formatteer_datum


def map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api_model: VestigingsProfielAPI) -> VestigingsProfielDomain:
    """Zet een VestigingsProfielAPI (API-model) om naar een VestigingsProfielDomain.

    Extracteert correspondentieadres en bezoekadres uit de adressen lijst.
    """
    cor_adres = next((a for a in (api_model.adressen or []) if a.type == "correspondentieadres"), None)
    bzk_adres = next((a for a in (api_model.adressen or []) if a.type == "bezoekadres"), None)

    return VestigingsProfielDomain(
        vestigingsnummer=api_model.vestigingsnummer if api_model.vestigingsnummer else None,
        cor_adres_volledig=cor_adres.volledig_adres if cor_adres and cor_adres.volledig_adres else None,
        cor_adres_postcode=cor_adres.postcode if cor_adres and cor_adres.postcode else None,
        cor_adres_postbusnummer=getattr(cor_adres, "postbusnummer", None) if cor_adres else None,
        cor_adres_plaats=cor_adres.plaats if cor_adres and cor_adres.plaats else None,
        cor_adres_land=cor_adres.land if cor_adres and cor_adres.land else None,
        bzk_adres_volledig=bzk_adres.volledig_adres if bzk_adres and bzk_adres.volledig_adres else None,
        bzk_adres_straatnaam=bzk_adres.straatnaam if bzk_adres else None,
        bzk_adres_huisnummer=getattr(bzk_adres, "huisnummer", None) if bzk_adres else None,
        bzk_adres_postcode=bzk_adres.postcode if bzk_adres and bzk_adres.postcode else None,
        bzk_adres_plaats=bzk_adres.plaats if bzk_adres and bzk_adres.plaats else None,
        bzk_adres_land=bzk_adres.land if bzk_adres and bzk_adres.land else None,
        bzk_adres_gps_latitude=truncate_float(bzk_adres.geo_data.gps_latitude)
        if bzk_adres and bzk_adres.geo_data and bzk_adres.geo_data.gps_latitude
        else None,
        bzk_adres_gps_longitude=truncate_float(bzk_adres.geo_data.gps_longitude)
        if bzk_adres and bzk_adres.geo_data and bzk_adres.geo_data.gps_longitude
        else None,
        registratie_datum_aanvang_vestiging=formatteer_datum(str(api_model.materiele_registratie.datum_aanvang or ""))
        if api_model.materiele_registratie
        else None,
        registratie_datum_einde_vestiging=formatteer_datum(str(api_model.materiele_registratie.datum_einde or ""))
        if api_model.materiele_registratie
        else None,
    )
