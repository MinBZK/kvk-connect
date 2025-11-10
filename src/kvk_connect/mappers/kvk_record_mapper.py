from __future__ import annotations

from kvk_connect.models.api.basisprofiel_api import Adres, BasisProfielAPI, Hoofdvestiging, SBIActiviteit
from kvk_connect.models.domain import BasisProfielDomain
from kvk_connect.utils.formatting import truncate_float
from kvk_connect.utils.tools import clean_and_pad, formatteer_datum


def _select_adres(adressen: list[Adres] | None) -> Adres | None:
    if not adressen:
        return None
    # Prefer "bezoekadres", else fall back to "correspondentieadres", else first
    bezoek = next((a for a in adressen if a and a.type == "bezoekadres"), None)
    if bezoek:
        return bezoek
    corr = next((a for a in adressen if a and a.type == "correspondentieadres"), None)
    if corr:
        return corr

    # Return first address if available
    return adressen[0] if adressen else None


def _format_straatnaam(adres: Adres) -> str:
    if not adres or not adres.straatnaam:
        return ""
    parts = [adres.straatnaam]
    if adres.huisnummer is not None:
        parts.append(str(adres.huisnummer))
    # The parser used 'toevoegingAdres'; our model has 'huisletter'. Prefer huisletter if present.
    if adres.huisletter:
        parts[-1] = f"{parts[-1]}{adres.huisletter}" if parts else adres.huisletter
    return " ".join(parts)


def _map_sbi(api: BasisProfielAPI) -> tuple[str, str, str]:
    hoofd_code = ""
    hoofd_oms = ""
    overige_codes: list[str] = []

    activiteiten: list[SBIActiviteit] = api.sbi_activiteiten or []
    for act in activiteiten:
        if (act.ind_hoofdactiviteit or "").lower() == "ja":
            hoofd_code = clean_and_pad(act.sbi_code, 5) if act.sbi_code else ""
            hoofd_oms = act.sbi_omschrijving or ""
        else:
            if act.sbi_code:
                overige_codes.append(clean_and_pad(act.sbi_code, 5))

    return hoofd_code, hoofd_oms, ", ".join(overige_codes)


def _map_hoofdvestiging(hv: Hoofdvestiging | None, out: BasisProfielDomain) -> None:
    if not hv:
        return

    out.eerste_handelsnaam = hv.eerste_handelsnaam or ""
    out.vestigingsnummer = hv.vestigingsnummer or ""
    out.totaal_werkzame_personen = hv.totaal_werkzame_personen if hv.totaal_werkzame_personen is not None else None
    out.websites = ", ".join(hv.websites or [])

    # Adres
    adres = _select_adres(hv.adressen or [])
    if adres:
        out.adres_type = adres.type or ""
        # The Adres model has no 'postbusnummer' field; keep empty to match parser behavior when absent.
        out.postbusnummer = ""
        out.adres_straatnaam = _format_straatnaam(adres)
        out.adres_toevoeging = ""  # parser used 'toevoegingAdres', not present in model; keep empty
        out.adres_postcode = adres.postcode or ""
        out.adres_plaats = adres.plaats or ""
        if adres.geo_data:
            out.gps_latitude = truncate_float(adres.geo_data.gps_latitude)
            out.gps_longitude = truncate_float(adres.geo_data.gps_longitude)


def map_kvkbasisprofiel_api_to_kvkrecord(api: BasisProfielAPI) -> BasisProfielDomain:
    """Map a KVKRecordAPI model (Basisprofiel) to a KVKRecord (BasisprofielOutput).

    This function is mirroring the logic from parsers.kvkparser.parse_basisprofiel.
    """
    out = BasisProfielDomain()

    # Top-level
    out.kvk_nummer = api.kvk_nummer or ""
    out.naam = api.naam or ""

    # SBI activiteiten
    out.hoofdactiviteit, out.hoofdactiviteit_omschrijving, out.activiteit_overig = _map_sbi(api)

    # Eigenaar
    if api.embedded and api.embedded.eigenaar:
        out.rechtsvorm = api.embedded.eigenaar.rechtsvorm or ""
        out.rechtsvorm_uitgebreid = api.embedded.eigenaar.uitgebreide_rechtsvorm or ""

    # Registratie
    if api.materiele_registratie:
        out.registratie_datum_aanvang = formatteer_datum(str(api.materiele_registratie.datum_aanvang or ""))
        out.registratie_datum_einde = formatteer_datum(str(api.materiele_registratie.datum_einde or ""))

    # Hoofdvestiging
    hv = api.embedded.hoofdvestiging if api.embedded else None
    _map_hoofdvestiging(hv, out)

    return out
