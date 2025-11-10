"""Tests for Basisprofiel domain model."""

from __future__ import annotations

import logging

import pytest

from kvk_connect.mappers.kvk_record_mapper import map_kvkbasisprofiel_api_to_kvkrecord
from kvk_connect.models.api.basisprofiel_api import BasisProfielAPI
from kvk_connect.models.domain.basisprofiel import BasisProfielDomain

logger = logging.getLogger(__name__)


class TestBasisProfielDomain:
    """Test suite for BasisProfielDomain."""

    def test_create_basisprofiel_from_dict(
        self, mock_kvk_basisprofiel_response: dict
    ) -> None:
        """Test creating domain model from dict."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        assert domain.kvk_nummer == "12345678"
        assert domain.naam == "Blooming Tandartsen"
        assert domain.rechtsvorm == "Eenmanszaak"

    def test_basisprofiel_to_dict(
        self, mock_kvk_basisprofiel_response: dict
    ) -> None:
        """Test converting domain model to dict."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)
        result_dict = domain.to_dict()

        assert result_dict["kvk_nummer"] == "12345678"
        assert result_dict["naam"] == "Blooming Tandartsen"

    def test_basisprofiel_with_null_fields(self) -> None:
        """Test handling optional null fields."""
        minimal_data = {
            "kvkNummer": "87654321",
            "naam": None,
            "rechtsvorm": "Eenmanszaak",
        }

        domain = BasisProfielDomain.from_dict(minimal_data)

        assert domain.kvk_nummer == "87654321"
        assert domain.naam is None
        assert domain.rechtsvorm == "Eenmanszaak"

    def test_basisprofiel_all_fields(self) -> None:
        """Test with all fields populated."""
        complete_data = {
            "kvkNummer": "12345678",
            "naam": "Test Company B.V.",
            "hoofdactiviteit": "62010",
            "hoofdactiviteitOmschrijving": "IT-consultancy",
            "activiteitOverig": "Software development",
            "rechtsvorm": "Besloten vennootschap",
            "rechtsvormUitgebreid": "B.V.",
            "eersteHandelsnaam": "Test Company",
            "vestigingsnummer": "000000000001",
            "totaalWerkzamePersonen": 10,
            "websites": "https://test.nl",
            "RegistratieDatumAanvang": "2020-01-15",
            "RegistratieDatumEinde": None,
            "AdresType": "bezoekadres",
            "Postbusnummer": None,
            "AdresStraatnaam": "Hoofdstraat",
            "AdresToevoeging": "1",
            "AdresPostcode": "1234 AB",
            "AdresPlaats": "Amsterdam",
            "gpsLatitude": "52.3676",
            "gpsLongitude": "4.9041",
        }

        domain = BasisProfielDomain.from_dict(complete_data)

        assert domain.kvk_nummer == "12345678"
        assert domain.totaal_werkzame_personen == 10
        assert domain.gps_latitude == "52.3676"
        assert domain.adres_straatnaam == "Hoofdstraat"

    def test_basisprofiel_empty_dict(self) -> None:
        """Test creating from empty dict."""
        domain = BasisProfielDomain.from_dict({})

        assert domain.kvk_nummer is None
        assert domain.naam is None
        assert domain.totaal_werkzame_personen is None
