from __future__ import annotations

from kvk_connect.models.api.vestigingsprofiel_api import VestigingsProfielAPI
from kvk_connect.models.domain.vestigingsprofiel_domain import VestigingsProfielDomain
from kvk_connect.utils.formatting import truncate_float
from kvk_connect.utils.sbi_utils import map_sbi_activiteiten
from kvk_connect.utils.tools import formatteer_datum


def _map_handelsnamen(handelsnamen: list) -> str | None:
    namen = sorted(handelsnamen or [], key=lambda h: (h.volgorde or 0, h.naam or ""))
    result = ", ".join(h.naam for h in namen if h.naam)
    return result or None


def map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api_model: VestigingsProfielAPI) -> VestigingsProfielDomain:
    """Zet een VestigingsProfielAPI (API-model) om naar een VestigingsProfielDomain.

    Extracteert correspondentieadres en bezoekadres uit de adressen lijst.
    """
    cor_adres = next((a for a in (api_model.adressen or []) if a.type == "correspondentieadres"), None)
    bzk_adres = next((a for a in (api_model.adressen or []) if a.type == "bezoekadres"), None)

    hoofd_act, hoofd_oms, overig_act = map_sbi_activiteiten(api_model.sbi_activiteiten)

    return VestigingsProfielDomain(
        vestigingsnummer=api_model.vestigingsnummer or None,
        kvk_nummer=api_model.kvk_nummer or None,
        rsin=api_model.rsin or None,
        ind_non_mailing=api_model.ind_non_mailing or None,
        formele_registratiedatum=formatteer_datum(api_model.formele_registratiedatum or "") or None,
        statutaire_naam=api_model.statutaire_naam or None,
        eerste_handelsnaam=api_model.eerste_handelsnaam or None,
        handelsnamen=_map_handelsnamen(api_model.handelsnamen),
        ind_hoofdvestiging=api_model.ind_hoofdvestiging or None,
        ind_commerciele_vestiging=api_model.ind_commerciele_vestiging or None,
        voltijd_werkzame_personen=api_model.voltijd_werkzame_personen,
        deeltijd_werkzame_personen=api_model.deeltijd_werkzame_personen,
        totaal_werkzame_personen=api_model.totaal_werkzame_personen,
        hoofdactiviteit=hoofd_act or None,
        hoofdactiviteit_omschrijving=hoofd_oms or None,
        activiteit_overig=overig_act or None,
        websites=", ".join(sorted(api_model.websites or [])) or None,
        cor_adres_volledig=cor_adres.volledig_adres if cor_adres and cor_adres.volledig_adres else None,
        cor_adres_straatnaam=cor_adres.straatnaam if cor_adres and cor_adres.straatnaam else None,
        cor_adres_huisnummer=cor_adres.huisnummer if cor_adres else None,
        cor_adres_postcode=cor_adres.postcode if cor_adres and cor_adres.postcode else None,
        cor_adres_postbusnummer=cor_adres.postbusnummer if cor_adres else None,
        cor_adres_plaats=cor_adres.plaats if cor_adres and cor_adres.plaats else None,
        cor_adres_land=cor_adres.land if cor_adres and cor_adres.land else None,
        cor_adres_gps_latitude=truncate_float(cor_adres.geo_data.gps_latitude)
        if cor_adres and cor_adres.geo_data and cor_adres.geo_data.gps_latitude
        else None,
        cor_adres_gps_longitude=truncate_float(cor_adres.geo_data.gps_longitude)
        if cor_adres and cor_adres.geo_data and cor_adres.geo_data.gps_longitude
        else None,
        bzk_adres_volledig=bzk_adres.volledig_adres if bzk_adres and bzk_adres.volledig_adres else None,
        bzk_adres_straatnaam=bzk_adres.straatnaam if bzk_adres else None,
        bzk_adres_huisnummer=bzk_adres.huisnummer if bzk_adres else None,
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
