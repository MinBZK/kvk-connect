"""Utility functions voor het bijhouden van veldwijzigingen in de historietabellen."""

from __future__ import annotations

_BASISPROFIEL_BUSINESS_FIELDS: frozenset[str] = frozenset(
    [
        "naam",
        "eerste_handelsnaam",
        "handelsnamen",
        "websites",
        "ind_non_mailing",
        "hoofdactiviteit",
        "hoofdactiviteit_omschrijving",
        "activiteit_overig",
        "rechtsvorm",
        "rechtsvorm_uitgebreid",
        "totaal_werkzame_personen",
        "formele_registratiedatum",
        "registratie_datum_aanvang",
        "registratie_datum_einde",
    ]
)

_VESTIGINGSPROFIEL_BUSINESS_FIELDS: frozenset[str] = frozenset(
    [
        "rsin",
        "ind_non_mailing",
        "ind_hoofdvestiging",
        "ind_commerciele_vestiging",
        "statutaire_naam",
        "eerste_handelsnaam",
        "handelsnamen",
        "hoofdactiviteit",
        "hoofdactiviteit_omschrijving",
        "activiteit_overig",
        "voltijd_werkzame_personen",
        "deeltijd_werkzame_personen",
        "totaal_werkzame_personen",
        "websites",
        "cor_adres_volledig",
        "cor_adres_straatnaam",
        "cor_adres_huisnummer",
        "cor_adres_postcode",
        "cor_adres_postbusnummer",
        "cor_adres_plaats",
        "cor_adres_land",
        "cor_adres_gps_latitude",
        "cor_adres_gps_longitude",
        "bzk_adres_volledig",
        "bzk_adres_straatnaam",
        "bzk_adres_huisnummer",
        "bzk_adres_postcode",
        "bzk_adres_plaats",
        "bzk_adres_land",
        "bzk_adres_gps_latitude",
        "bzk_adres_gps_longitude",
        "formele_registratiedatum",
        "registratie_datum_aanvang_vestiging",
        "registratie_datum_einde_vestiging",
    ]
)


def compute_changed_fields(existing_orm: object | None, new_orm: object, business_fields: frozenset[str]) -> list[str]:
    """Retourneer gesorteerde lijst van veldnamen waar old != new.

    Bij nieuw record (existing_orm is None): alle non-None velden van new_orm.
    Lege lijst = geen wijziging — geen historierij schrijven.
    """
    if existing_orm is None:
        return sorted(f for f in business_fields if getattr(new_orm, f) is not None)
    return sorted(f for f in business_fields if getattr(existing_orm, f) != getattr(new_orm, f))
