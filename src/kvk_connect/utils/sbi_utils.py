from __future__ import annotations

from kvk_connect.models.api.basisprofiel_api import SBIActiviteit
from kvk_connect.utils.tools import clean_and_pad


def map_sbi_activiteiten(activiteiten: list[SBIActiviteit] | None) -> tuple[str, str, str]:
    """Extraheer hoofdactiviteit en overige SBI-activiteiten.

    Returns:
        Tuple van (hoofdactiviteit_code, hoofdactiviteit_omschrijving, activiteit_overig)
        waarbij activiteit_overig een gesorteerde komma-gescheiden string is.
    """
    hoofd_code = ""
    hoofd_oms = ""
    overige_codes: list[str] = []

    for act in activiteiten or []:
        if (act.ind_hoofdactiviteit or "").lower() == "ja":
            hoofd_code = clean_and_pad(act.sbi_code, 5) if act.sbi_code else ""
            hoofd_oms = act.sbi_omschrijving or ""
        else:
            if act.sbi_code:
                overige_codes.append(clean_and_pad(act.sbi_code, 5))

    return hoofd_code, hoofd_oms, ", ".join(sorted(overige_codes))
