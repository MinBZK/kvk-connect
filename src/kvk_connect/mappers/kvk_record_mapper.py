from __future__ import annotations

from kvk_connect.models.api.basisprofiel_api import BasisProfielAPI, Hoofdvestiging
from kvk_connect.models.domain import BasisProfielDomain
from kvk_connect.utils.sbi_utils import map_sbi_activiteiten
from kvk_connect.utils.tools import formatteer_datum


def _map_hoofdvestiging(hv: Hoofdvestiging | None, out: BasisProfielDomain) -> None:
    if not hv:
        return

    out.eerste_handelsnaam = hv.eerste_handelsnaam or ""
    out.vestigingsnummer = hv.vestigingsnummer or ""
    out.totaal_werkzame_personen = hv.totaal_werkzame_personen if hv.totaal_werkzame_personen is not None else None
    out.websites = ", ".join(hv.websites or [])


def _map_handelsnamen(handelsnamen: list) -> str | None:
    namen = sorted(handelsnamen or [], key=lambda h: (h.volgorde or 0, h.naam or ""))
    result = ", ".join(h.naam for h in namen if h.naam)
    return result or None


def map_kvkbasisprofiel_api_to_kvkrecord(api: BasisProfielAPI) -> BasisProfielDomain:
    """Map a KVKRecordAPI model (Basisprofiel) to a KVKRecord (BasisprofielOutput).

    This function is mirroring the logic from parsers.kvkparser.parse_basisprofiel.
    """
    out = BasisProfielDomain()

    # Top-level
    out.kvk_nummer = api.kvk_nummer or ""
    out.naam = api.naam or ""
    out.ind_non_mailing = api.ind_non_mailing or None
    out.formele_registratiedatum = formatteer_datum(api.formele_registratiedatum or "") or None
    out.handelsnamen = _map_handelsnamen(api.handelsnamen)

    # SBI activiteiten
    out.hoofdactiviteit, out.hoofdactiviteit_omschrijving, out.activiteit_overig = map_sbi_activiteiten(
        api.sbi_activiteiten
    )

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
