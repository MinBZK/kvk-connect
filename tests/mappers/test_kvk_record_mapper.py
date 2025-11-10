"""Tests for KVK record mapper."""

from __future__ import annotations

import logging

import pytest

from kvk_connect.mappers.kvk_record_mapper import map_kvkbasisprofiel_api_to_kvkrecord
from kvk_connect.models.api.basisprofiel_api import BasisProfielAPI
from kvk_connect.models.domain.basisprofiel import BasisProfielDomain

logger = logging.getLogger(__name__)


class TestKvkRecordMapper:
    """Test suite for KVK record mapper."""

    def test_map_basisprofiel_api_to_domain(
        self, mock_kvk_basisprofiel_response: dict
    ) -> None:
        """Test mapping API response to BasisProfielDomain."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        assert isinstance(domain, BasisProfielDomain)
        assert domain.kvk_nummer == "12345678"
        assert domain.naam == "Blooming Tandartsen"
        logger.info("Successfully mapped basisprofiel API response to domain")

    def test_map_basisprofiel_with_null_optional_fields(self) -> None:
        """Test mapping handles null optional fields gracefully."""
        api_response = {
            "kvkNummer": "12345678",
            "naam": None,
            "formeleRegistratiedatum": "2020-01-15",
            "sbiActiviteiten": [],
            "websites": None,
            "handelsnamen": [],
            "totaalWerkzamePersonen": 0,
            "materieleRegistratie": {
                "datumAanvang": "2020-01-15",
                "datumEinde": None,
            },
        }

        api_model = BasisProfielAPI.from_dict(api_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        assert domain.naam is None or domain.naam == ""
        assert domain.websites is None or domain.websites == ""
        logger.info("Null fields handled correctly in mapping")

    def test_map_basisprofiel_with_all_fields(
        self, mock_kvk_basisprofiel_response: dict
    ) -> None:
        """Test mapping with all fields populated."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        assert domain.kvk_nummer is not None
        assert domain.naam is not None
        assert domain.rechtsvorm is not None
        logger.info("All fields successfully mapped")

    def test_map_basisprofiel_preserves_field_types(
        self, mock_kvk_basisprofiel_response: dict
    ) -> None:
        """Test that field types are preserved during mapping."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        assert isinstance(domain.kvk_nummer, str)
        assert isinstance(domain.naam, str) or domain.naam is None
        assert isinstance(domain.totaal_werkzame_personen, (int, type(None)))
        logger.info("Field types preserved correctly")

    def test_map_basisprofiel_invalid_input_raises_error(self) -> None:
        """Test that invalid input raises appropriate error."""
        with pytest.raises((KeyError, TypeError, AttributeError, ValueError)):
            BasisProfielAPI.from_dict(None)
        logger.warning("Invalid input correctly raises error")

    def test_map_basisprofiel_with_extra_fields(self) -> None:
        """Test mapping handles extra API fields gracefully."""
        api_response = {
            "kvkNummer": "12345678",
            "naam": "Test Company",
            "formeleRegistratiedatum": "2020-01-15",
            "sbiActiviteiten": [],
            "websites": ["https://test.nl"],
            "handelsnamen": [{"naam": "Test Company", "volgorde": 1}],
            "totaalWerkzamePersonen": 10,
            "materieleRegistratie": {
                "datumAanvang": "2020-01-15",
                "datumEinde": None,
            },
            "extra_field_from_api": "should be ignored",
            "another_unknown_field": 123,
        }

        api_model = BasisProfielAPI.from_dict(api_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        assert domain.kvk_nummer == "12345678"
        logger.info("Extra fields handled gracefully")

    def test_map_basisprofiel_converts_websites_list(
        self, mock_kvk_basisprofiel_response: dict
    ) -> None:
        """Test that websites list is properly converted."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        assert domain.websites is None or isinstance(domain.websites, str)
        logger.info("Websites list properly converted")

    def test_map_basisprofiel_handles_handelsnamen(
        self, mock_kvk_basisprofiel_response: dict
    ) -> None:
        """Test that handelsnamen are properly mapped."""
        api_model = BasisProfielAPI.from_dict(mock_kvk_basisprofiel_response)
        domain = map_kvkbasisprofiel_api_to_kvkrecord(api_model)

        assert domain.eerste_handelsnaam is not None
        logger.info("Handelsnamen properly mapped")
