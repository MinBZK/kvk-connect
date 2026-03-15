"""Tests for map_vestigingsprofiel_api_to_vestigingsprofiel_domain."""

from __future__ import annotations

import pytest

from kvk_connect.mappers.map_vestigingsprofiel_api_to_vestigingsprofiel_domain import (
    map_vestigingsprofiel_api_to_vestigingsprofiel_domain,
)
from kvk_connect.models.api.vestigingsprofiel_api import VestigingsProfielAPI
from kvk_connect.models.domain.vestigingsprofiel_domain import VestigingsProfielDomain


class TestMapVestigingsProfielMapper:
    """One test per mapped field, using the shared fixture."""

    def test_maps_vestigingsnummer(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.vestigingsnummer == "000000000001"

    def test_maps_kvk_nummer(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.kvk_nummer == "12345678"

    def test_maps_rsin(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.rsin == "123456789"

    def test_maps_ind_non_mailing(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.ind_non_mailing == "Nee"

    def test_maps_formele_registratiedatum(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.formele_registratiedatum == "01-01-2020"

    def test_maps_statutaire_naam(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.statutaire_naam == "Test B.V."

    def test_maps_eerste_handelsnaam(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.eerste_handelsnaam == "Test Company"

    def test_maps_ind_hoofdvestiging(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.ind_hoofdvestiging == "Ja"

    def test_maps_ind_commerciele_vestiging(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.ind_commerciele_vestiging == "Ja"

    def test_maps_werkzame_personen(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.voltijd_werkzame_personen == 5
        assert domain.deeltijd_werkzame_personen == 5
        assert domain.totaal_werkzame_personen == 10

    def test_maps_handelsnamen_sorted(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.handelsnamen == "Test Company, Test Services"

    def test_maps_sbi_hoofdactiviteit(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.hoofdactiviteit == "62010"
        assert domain.hoofdactiviteit_omschrijving == "Computer programming activities"

    def test_maps_sbi_activiteit_overig_sorted(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.activiteit_overig == "62020, 62090"

    def test_maps_websites_sorted(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.websites == "https://example.com"

    def test_maps_cor_adres_volledig(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.cor_adres_volledig == "Postbus 100 1012AB Amsterdam"

    def test_maps_cor_adres_postbusnummer(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.cor_adres_postbusnummer == 100
        assert domain.cor_adres_postcode == "1012AB"
        assert domain.cor_adres_plaats == "Amsterdam"
        assert domain.cor_adres_land == "Nederland"

    def test_maps_cor_adres_straatnaam_none_for_postbus(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        """Correspondentieadres is a postbus — straatnaam and huisnummer are absent."""
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.cor_adres_straatnaam is None
        assert domain.cor_adres_huisnummer is None

    def test_maps_cor_adres_gps_none_when_zero(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        """GPS 0.0 is treated as absent and mapped to None."""
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.cor_adres_gps_latitude is None
        assert domain.cor_adres_gps_longitude is None

    def test_maps_bzk_adres(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.bzk_adres_volledig == "Straatnaam 1 1012AB Amsterdam"
        assert domain.bzk_adres_straatnaam == "Straatnaam"
        assert domain.bzk_adres_huisnummer == 1
        assert domain.bzk_adres_postcode == "1012AB"
        assert domain.bzk_adres_plaats == "Amsterdam"
        assert domain.bzk_adres_land == "Nederland"

    def test_maps_bzk_adres_gps(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.bzk_adres_gps_latitude == "52,36760"
        assert domain.bzk_adres_gps_longitude == "4,90409"

    def test_maps_registratie_datums(self, mock_kvk_vestigingsprofiel_response: dict) -> None:
        api = VestigingsProfielAPI.from_dict(mock_kvk_vestigingsprofiel_response)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)
        assert domain.registratie_datum_aanvang_vestiging == "01-01-2020"
        assert domain.registratie_datum_einde_vestiging == "31-12-2025"

    def test_maps_none_when_optional_fields_missing(self) -> None:
        """Minimal API payload — all optional fields map to None."""
        minimal = {"vestigingsnummer": "000000000099"}
        api = VestigingsProfielAPI.from_dict(minimal)
        domain = map_vestigingsprofiel_api_to_vestigingsprofiel_domain(api)

        assert isinstance(domain, VestigingsProfielDomain)
        assert domain.vestigingsnummer == "000000000099"
        assert domain.kvk_nummer is None
        assert domain.rsin is None
        assert domain.statutaire_naam is None
        assert domain.handelsnamen is None
        assert domain.cor_adres_volledig is None
        assert domain.bzk_adres_volledig is None
        assert domain.registratie_datum_aanvang_vestiging is None
