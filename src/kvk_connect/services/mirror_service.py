from __future__ import annotations

import logging
from kvk_connect.db.mcp_onbekend_vraag_writer import McpOnbekendVraagWriter
from kvk_connect.db.mirror_reader import KVKMirrorReader
from kvk_connect.models.domain.basisprofiel import BasisProfielDomain
from kvk_connect.models.domain.vestigingsprofiel_domain import VestigingsProfielDomain
from kvk_connect.models.orm.basisprofiel_historie_orm import BasisProfielHistorieORM
from kvk_connect.models.orm.vestigingsprofiel_historie_orm import VestigingsProfielHistorieORM

logger = logging.getLogger(__name__)

_MAX_NAAM_RESULTATEN = 100
_MAX_SBI_RESULTATEN = 500
_MAX_BATCH_NUMMERS = 200
_DOORSTARTER_WINDOW_DAGEN = 7


class KVKMirrorService:
    """Use case-laag voor de MCP server.

    Bevat alle business logic: is_actief-berekening, indNonMailing-filtering,
    data_quality-opbouw en assemblage van gecombineerde responses.
    """

    def __init__(self, reader: KVKMirrorReader, writer: McpOnbekendVraagWriter) -> None:
        self.reader = reader
        self.writer = writer

    # ------------------------------------------------------------------
    # Business rules
    # ------------------------------------------------------------------

    @staticmethod
    def _is_actief_basisprofiel(domain: BasisProfielDomain) -> bool:
        """NULL-safe actief-check voor basisprofielen."""
        return domain.registratie_datum_einde is None and domain.niet_leverbaar_code != "IPD0005"

    @staticmethod
    def _is_actief_vestigingsprofiel(domain: VestigingsProfielDomain) -> bool:
        """Actief-check voor vestigingsprofielen (einde datum afwezig = actief)."""
        return domain.registratie_datum_einde_vestiging is None and domain.niet_leverbaar_code != "IPD0005"

    @staticmethod
    def _data_quality(
        sentinel_excluded_count: int = 0, coverage_warnings: list[str] | None = None, query_limited: bool = False
    ) -> dict:
        return {
            "sentinel_excluded_count": sentinel_excluded_count,
            "coverage_warnings": coverage_warnings or [],
            "query_limited": query_limited,
        }

    @staticmethod
    def _non_mailing_warning() -> str:
        return "indNonMailing=Ja records zijn meegenomen. Gebruik deze data met inachtneming van privacyregels."

    # ------------------------------------------------------------------
    # Laag 1: exacte lookups
    # ------------------------------------------------------------------

    def get_bedrijf(self, kvk_nummer: str, include_non_mailing: bool = False) -> dict:
        """Basisprofiel met is_actief en vestiging_count."""
        bp = self.reader.get_basisprofiel(kvk_nummer)
        if bp is None:
            return {"status": "niet_gevonden", "kvk_nummer": kvk_nummer, "data_quality": self._data_quality()}

        warnings: list[str] = []
        if bp.ind_non_mailing == "Ja" and not include_non_mailing:
            return {
                "status": "niet_gevonden",
                "kvk_nummer": kvk_nummer,
                "reden": "indNonMailing",
                "data_quality": self._data_quality(),
            }
        if bp.ind_non_mailing == "Ja":
            warnings.append(self._non_mailing_warning())

        vestigingsnummers = self.reader.get_vestigingsnummers(kvk_nummer)

        return {
            "status": "ok",
            "data": {
                "kvk_nummer": bp.kvk_nummer,
                "naam": bp.naam,
                "eerste_handelsnaam": bp.eerste_handelsnaam,
                "rechtsvorm": bp.rechtsvorm,
                "rechtsvorm_uitgebreid": bp.rechtsvorm_uitgebreid,
                "sbi_code": bp.hoofdactiviteit,
                "sbi_omschrijving": bp.hoofdactiviteit_omschrijving,
                "activiteit_overig": bp.activiteit_overig,
                "is_actief": self._is_actief_basisprofiel(bp),
                "datum_einde": bp.registratie_datum_einde,
                "datum_aanvang": bp.registratie_datum_aanvang,
                "formele_registratiedatum": bp.formele_registratiedatum,
                "totaal_werkzame_personen": bp.totaal_werkzame_personen,
                "websites": bp.websites,
                "vestiging_count": len(vestigingsnummers),
                "ind_non_mailing": bp.ind_non_mailing,
            },
            "data_quality": self._data_quality(coverage_warnings=warnings),
        }

    def get_vestiging(self, vestigingsnummer: str) -> dict:
        """Vestigingsprofiel met is_actief en adresgegevens."""
        vp = self.reader.get_vestigingsprofiel(vestigingsnummer)
        if vp is None:
            return {
                "status": "niet_gevonden",
                "vestigingsnummer": vestigingsnummer,
                "data_quality": self._data_quality(),
            }

        return {
            "status": "ok",
            "data": {
                "vestigingsnummer": vp.vestigingsnummer,
                "kvk_nummer": vp.kvk_nummer,
                "rsin": vp.rsin,
                "naam": vp.eerste_handelsnaam or vp.statutaire_naam,
                "statutaire_naam": vp.statutaire_naam,
                "is_hoofdvestiging": vp.ind_hoofdvestiging == "Ja",
                "is_commercieel": vp.ind_commerciele_vestiging == "Ja",
                "sbi_code": vp.hoofdactiviteit,
                "sbi_omschrijving": vp.hoofdactiviteit_omschrijving,
                "is_actief": self._is_actief_vestigingsprofiel(vp),
                "datum_einde": vp.registratie_datum_einde_vestiging,
                "adres": {
                    "volledig": vp.cor_adres_volledig,
                    "postcode": vp.cor_adres_postcode,
                    "plaats": vp.cor_adres_plaats,
                    "straatnaam": vp.cor_adres_straatnaam,
                    "huisnummer": vp.cor_adres_huisnummer,
                    "land": vp.cor_adres_land,
                    "gps_lat": vp.cor_adres_gps_latitude,
                    "gps_lon": vp.cor_adres_gps_longitude,
                },
                "totaal_werkzame_personen": vp.totaal_werkzame_personen,
                "websites": vp.websites,
            },
            "data_quality": self._data_quality(),
        }

    def list_vestigingen(self, kvk_nummer: str, include_non_mailing: bool = False) -> dict:
        """Alle vestigingen van een bedrijf met is_actief, SBI en adres."""
        vestigingsnummers = self.reader.get_vestigingsnummers(kvk_nummer)
        profielen = self.reader.get_vestigingsprofielen(vestigingsnummers)

        warnings: list[str] = []
        resultaten = []
        for vp in profielen:
            if vp.ind_non_mailing == "Ja" and not include_non_mailing:
                continue
            if vp.ind_non_mailing == "Ja" and include_non_mailing and self._non_mailing_warning() not in warnings:
                warnings.append(self._non_mailing_warning())
            resultaten.append(
                {
                    "vestigingsnummer": vp.vestigingsnummer,
                    "is_hoofdvestiging": vp.ind_hoofdvestiging == "Ja",
                    "is_actief": self._is_actief_vestigingsprofiel(vp),
                    "sbi_code": vp.hoofdactiviteit,
                    "sbi_omschrijving": vp.hoofdactiviteit_omschrijving,
                    "plaats": vp.cor_adres_plaats,
                    "adres_volledig": vp.cor_adres_volledig,
                }
            )

        return {
            "status": "ok",
            "kvk_nummer": kvk_nummer,
            "vestigingen": resultaten,
            "data_quality": self._data_quality(coverage_warnings=warnings),
        }

    def get_alles(self, kvk_nummer: str, include_non_mailing: bool = False) -> dict:
        """Basisprofiel + alle vestigingsprofielen gecombineerd."""
        bedrijf = self.get_bedrijf(kvk_nummer, include_non_mailing=include_non_mailing)
        if bedrijf["status"] == "niet_gevonden":
            return bedrijf

        vestigingen = self.list_vestigingen(kvk_nummer, include_non_mailing=include_non_mailing)

        warnings = bedrijf["data_quality"]["coverage_warnings"] + vestigingen["data_quality"]["coverage_warnings"]
        return {
            "status": "ok",
            "basisprofiel": bedrijf["data"],
            "vestigingen": vestigingen["vestigingen"],
            "data_quality": self._data_quality(coverage_warnings=list(dict.fromkeys(warnings))),
        }

    def check_doorstarter(self, kvk_nummer: str) -> dict:
        """Zoekt actieve opvolger op hetzelfde vestigingsnummer (doorstarter-detectie)."""
        ref_bp = self.reader.get_basisprofiel(kvk_nummer)
        if ref_bp is None:
            return {"status": "niet_gevonden", "kvk_nummer": kvk_nummer, "data_quality": self._data_quality()}

        ref_einde_str = ref_bp.registratie_datum_einde
        vestigingsnummers = self.reader.get_vestigingsnummers(kvk_nummer)

        coverage_warnings = [
            "Doorstarter-detectie is onvolledig: bedrijven zonder geldig vestigingsnummer zijn niet meegenomen."
        ]

        if not vestigingsnummers or ref_einde_str is None:
            return {
                "status": "ok",
                "kvk_nummer": kvk_nummer,
                "doorstarters": [],
                "data_quality": self._data_quality(coverage_warnings=coverage_warnings),
            }

        from datetime import date

        ref_einde = date.fromisoformat(ref_einde_str)
        kandidaten_paren = self.reader.get_kvk_nummers_op_vestigingsnummers(vestigingsnummers, kvk_nummer)
        unieke_kvk = {k for k, _ in kandidaten_paren}

        doorstarters = []
        for kandidaat_kvk in unieke_kvk:
            bp = self.reader.get_basisprofiel(kandidaat_kvk)
            if bp is None or bp.registratie_datum_aanvang is None:
                continue
            if not self._is_actief_basisprofiel(bp):
                continue
            aanvang = date.fromisoformat(bp.registratie_datum_aanvang)
            delta = abs((aanvang - ref_einde).days)
            if delta <= _DOORSTARTER_WINDOW_DAGEN:
                gedeeld = [v for k, v in kandidaten_paren if k == kandidaat_kvk]
                doorstarters.append(
                    {
                        "kvk_nummer": bp.kvk_nummer,
                        "naam": bp.naam,
                        "datum_aanvang": bp.registratie_datum_aanvang,
                        "gedeeld_vestigingsnummer": gedeeld[0] if gedeeld else None,
                    }
                )

        return {
            "status": "ok",
            "kvk_nummer": kvk_nummer,
            "doorstarters": doorstarters,
            "data_quality": self._data_quality(coverage_warnings=coverage_warnings),
        }

    # ------------------------------------------------------------------
    # Laag 2: analytisch
    # ------------------------------------------------------------------

    def zoek_op_naam_prefix(self, naam_prefix: str, limit: int = 25) -> dict:
        """Zoekt bedrijven op naam-prefix, max 100 resultaten."""
        effective_limit = min(limit, _MAX_NAAM_RESULTATEN)
        resultaten = self.reader.zoek_op_naam_prefix(naam_prefix, effective_limit)
        limited = len(resultaten) == effective_limit

        return {
            "status": "ok",
            "resultaten": [
                {
                    "kvk_nummer": bp.kvk_nummer,
                    "naam": bp.naam,
                    "sbi_code": bp.hoofdactiviteit,
                    "is_actief": self._is_actief_basisprofiel(bp),
                }
                for bp in resultaten
            ],
            "data_quality": self._data_quality(query_limited=limited),
        }

    def filter_op_sbi(self, sbi_prefix: str, gemeente: str | None = None, limit: int = 100) -> dict:
        """Actieve vestigingen gefilterd op SBI-sector en optioneel gemeente."""
        effective_limit = min(limit, _MAX_SBI_RESULTATEN)
        profielen = self.reader.filter_op_sbi(sbi_prefix, gemeente, effective_limit)
        limited = len(profielen) == effective_limit

        return {
            "status": "ok",
            "resultaten": [
                {
                    "vestigingsnummer": vp.vestigingsnummer,
                    "kvk_nummer": vp.kvk_nummer,
                    "naam": vp.eerste_handelsnaam or vp.statutaire_naam,
                    "sbi_code": vp.hoofdactiviteit,
                    "sbi_omschrijving": vp.hoofdactiviteit_omschrijving,
                    "is_actief": self._is_actief_vestigingsprofiel(vp),
                    "adres_volledig": vp.cor_adres_volledig,
                    "plaats": vp.cor_adres_plaats,
                }
                for vp in profielen
            ],
            "data_quality": self._data_quality(query_limited=limited),
        }

    def check_actiefstatus_batch(self, kvk_nummers: list[str]) -> dict:
        """Actiefstatus voor een lijst kvk_nummers, max 200."""
        if len(kvk_nummers) > _MAX_BATCH_NUMMERS:
            raise ValueError(
                "check_actiefstatus_batch: maximaal %d nummers per aanroep, ontvangen %d."
                % (_MAX_BATCH_NUMMERS, len(kvk_nummers))
            )
        profielen = self.reader.check_actiefstatus_batch(kvk_nummers)
        gevonden = {bp.kvk_nummer: bp for bp in profielen}

        return {
            "status": "ok",
            "resultaten": [
                {
                    "kvk_nummer": kvk,
                    "is_actief": self._is_actief_basisprofiel(gevonden[kvk]) if kvk in gevonden else None,
                    "datum_einde": gevonden[kvk].registratie_datum_einde if kvk in gevonden else None,
                    "niet_leverbaar_code": gevonden[kvk].niet_leverbaar_code if kvk in gevonden else None,
                    "gevonden": kvk in gevonden,
                }
                for kvk in kvk_nummers
            ],
            "data_quality": self._data_quality(),
        }

    # ------------------------------------------------------------------
    # Laag 3: onbekende vragen + historie
    # ------------------------------------------------------------------

    def report_onbekende_vraag(self, vraag: str) -> dict:
        """Registreert een niet-ondersteunde vraag en retourneert een standaard afwijzing."""
        self.writer.add(vraag)
        logger.info("Onbekende vraag geregistreerd: %s", vraag[:80])
        return {
            "status": "niet_ondersteund",
            "bericht": "Deze vraag valt buiten de mogelijkheden van het huidige systeem. "
            "De vraag is geregistreerd voor toekomstige uitbreiding.",
            "data_quality": self._data_quality(coverage_warnings=["Vraag geregistreerd als niet-ondersteund"]),
        }

    def get_basisprofiel_historie(self, kvk_nummer: str) -> dict:
        """Wijzigingsgeschiedenis van een basisprofiel, meest recent eerst."""
        records = self.reader.get_basisprofiel_historie(kvk_nummer)
        return {
            "status": "ok",
            "kvk_nummer": kvk_nummer,
            "historie": [self._basisprofiel_historie_to_dict(r) for r in records],
            "data_quality": self._data_quality(),
        }

    def get_vestigingsprofiel_historie(self, vestigingsnummer: str) -> dict:
        """Wijzigingsgeschiedenis van een vestigingsprofiel, meest recent eerst."""
        records = self.reader.get_vestigingsprofiel_historie(vestigingsnummer)
        return {
            "status": "ok",
            "vestigingsnummer": vestigingsnummer,
            "historie": [self._vestigingsprofiel_historie_to_dict(r) for r in records],
            "data_quality": self._data_quality(),
        }

    # ------------------------------------------------------------------
    # Private formatters voor historie ORM-objecten
    # ------------------------------------------------------------------

    @staticmethod
    def _basisprofiel_historie_to_dict(r: BasisProfielHistorieORM) -> dict:
        return {
            "gewijzigd_op": r.gewijzigd_op.isoformat(),
            "gewijzigde_velden": r.gewijzigde_velden,
            "naam": r.naam,
            "eerste_handelsnaam": r.eerste_handelsnaam,
            "rechtsvorm": r.rechtsvorm,
            "hoofdactiviteit": r.hoofdactiviteit,
            "hoofdactiviteit_omschrijving": r.hoofdactiviteit_omschrijving,
            "totaal_werkzame_personen": r.totaal_werkzame_personen,
        }

    @staticmethod
    def _vestigingsprofiel_historie_to_dict(r: VestigingsProfielHistorieORM) -> dict:
        return {
            "gewijzigd_op": r.gewijzigd_op.isoformat(),
            "gewijzigde_velden": r.gewijzigde_velden,
            "eerste_handelsnaam": r.eerste_handelsnaam,
            "statutaire_naam": r.statutaire_naam,
            "hoofdactiviteit": r.hoofdactiviteit,
            "cor_adres_volledig": r.cor_adres_volledig,
            "cor_adres_postcode": r.cor_adres_postcode,
            "cor_adres_plaats": r.cor_adres_plaats,
            "totaal_werkzame_personen": r.totaal_werkzame_personen,
        }
