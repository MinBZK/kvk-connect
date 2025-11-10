"""Tests for record service."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from kvk_connect.api.client import KVKApiClient
from kvk_connect.models.domain.basisprofiel import BasisProfielDomain
from kvk_connect.models.domain.vestigingsprofiel_domain import VestigingsProfielDomain
from kvk_connect.services.record_service import KVKRecordService

logger = logging.getLogger(__name__)


class TestKVKRecordService:
    """Test suite for KVKRecordService."""

    @pytest.fixture
    def mock_client(self) -> MagicMock:
        """Create mock KVK API client."""
        return MagicMock(spec=KVKApiClient)

    @pytest.fixture
    def service(self, mock_client: MagicMock) -> KVKRecordService:
        """Create service instance with mock client."""
        return KVKRecordService(mock_client)

    # ============ get_basisprofiel tests ============

    def test_get_basisprofiel_success(
        self,
        service: KVKRecordService,
        mock_client: MagicMock,
    ) -> None:
        """Test successful basisprofiel retrieval."""
        mock_api_response = MagicMock()
        mock_api_response.kvk_nummer = "12345678"
        mock_api_response.naam = "Test Company B.V."

        mock_client.get_basisprofiel.return_value = mock_api_response

        with patch(
            "kvk_connect.services.record_service.map_kvkbasisprofiel_api_to_kvkrecord"
        ) as mock_mapper:
            expected_domain = BasisProfielDomain(
                kvk_nummer="12345678",
                naam="Test Company B.V.",
                rechtsvorm="B.V.",
                totaal_werkzame_personen=10,
            )
            mock_mapper.return_value = expected_domain

            result = service.get_basisprofiel("12345678")

            assert result is not None
            assert result.kvk_nummer == "12345678"
            assert result.naam == "Test Company B.V."
            mock_client.get_basisprofiel.assert_called_once_with("12345678")

    def test_get_basisprofiel_pads_short_number(
        self,
        service: KVKRecordService,
        mock_client: MagicMock,
    ) -> None:
        """Test that short KVK numbers are padded."""
        mock_api_response = MagicMock()
        mock_client.get_basisprofiel.return_value = mock_api_response

        with patch(
            "kvk_connect.services.record_service.map_kvkbasisprofiel_api_to_kvkrecord"
        ) as mock_mapper:
            mock_mapper.return_value = MagicMock(spec=BasisProfielDomain)

            service.get_basisprofiel("1234567")

            call_args = mock_client.get_basisprofiel.call_args[0][0]
            assert len(call_args) == 8
            assert call_args == "01234567"

    def test_get_basisprofiel_not_found(
        self,
        service: KVKRecordService,
        mock_client: MagicMock,
        caplog,
    ) -> None:
        """Test basisprofiel retrieval when API returns None."""
        mock_client.get_basisprofiel.return_value = None

        result = service.get_basisprofiel("12345678")

        assert result is None
        #assert "No basisprofiel found" in caplog.text

    def test_get_basisprofiel_calls_mapper(
        self,
        service: KVKRecordService,
        mock_client: MagicMock,
    ) -> None:
        """Test that mapper is called with API response."""
        mock_api_response = MagicMock()
        mock_client.get_basisprofiel.return_value = mock_api_response

        with patch(
            "kvk_connect.services.record_service.map_kvkbasisprofiel_api_to_kvkrecord"
        ) as mock_mapper:
            mock_mapper.return_value = MagicMock(spec=BasisProfielDomain)

            service.get_basisprofiel("12345678")

            mock_mapper.assert_called_once_with(mock_api_response)

    # ============ get_vestigingen tests ============

    def test_get_vestigingen_success(
        self,
        service: KVKRecordService,
        mock_client: MagicMock,
    ) -> None:
        """Test successful vestigingen retrieval."""
        mock_api_response = MagicMock()
        mock_client.get_vestigingen.return_value = mock_api_response

        with patch(
            "kvk_connect.services.record_service.map_vestigingen_api_to_vestigingsnummers"
        ) as mock_mapper:
            expected_domain = MagicMock()
            mock_mapper.return_value = expected_domain

            result = service.get_vestigingen("12345678")

            assert result is expected_domain
            mock_client.get_vestigingen.assert_called_once_with("12345678")

    def test_get_vestigingen_pads_short_number(
        self,
        service: KVKRecordService,
        mock_client: MagicMock,
    ) -> None:
        """Test that short KVK numbers are padded."""
        mock_client.get_vestigingen.return_value = MagicMock()

        with patch(
            "kvk_connect.services.record_service.map_vestigingen_api_to_vestigingsnummers"
        ) as mock_mapper:
            mock_mapper.return_value = MagicMock()

            service.get_vestigingen("1234567")

            call_args = mock_client.get_vestigingen.call_args[0][0]
            assert len(call_args) == 8

    def test_get_vestigingen_not_found(
        self,
        service: KVKRecordService,
        mock_client: MagicMock,
        caplog,
    ) -> None:
        """Test vestigingen retrieval when API returns None."""
        mock_client.get_vestigingen.return_value = None

        result = service.get_vestigingen("12345678")

        assert result is None
        #assert "No vestigingen found" in caplog.text

    # ============ get_vestigingsprofiel tests ============

    def test_get_vestigingsprofiel_success(
        self,
        service: KVKRecordService,
        mock_client: MagicMock,
    ) -> None:
        """Test successful vestigingsprofiel retrieval."""
        mock_api_response = MagicMock()
        mock_client.get_vestigingsprofiel.return_value = mock_api_response

        with patch(
            "kvk_connect.services.record_service.map_vestigingsprofiel_api_to_vestigingsprofiel_domain"
        ) as mock_mapper:
            expected_domain = MagicMock(spec=VestigingsProfielDomain)
            mock_mapper.return_value = expected_domain

            result = service.get_vestigingsprofiel("000000000001")

            assert result is expected_domain
            mock_client.get_vestigingsprofiel.assert_called_once_with(
                "000000000001", geo_data=True
            )

    def test_get_vestigingsprofiel_passes_geo_data_parameter(
        self,
        service: KVKRecordService,
        mock_client: MagicMock,
    ) -> None:
        """Test that geo_data=True parameter is passed to client."""
        mock_client.get_vestigingsprofiel.return_value = MagicMock()

        with patch(
            "kvk_connect.services.record_service.map_vestigingsprofiel_api_to_vestigingsprofiel_domain"
        ) as mock_mapper:
            mock_mapper.return_value = MagicMock()

            service.get_vestigingsprofiel("000000000001")

            call_kwargs = mock_client.get_vestigingsprofiel.call_args[1]
            assert call_kwargs.get("geo_data") is True

    def test_get_vestigingsprofiel_not_found(
        self,
        service: KVKRecordService,
        mock_client: MagicMock,
        caplog,
    ) -> None:
        """Test vestigingsprofiel retrieval when API returns None."""
        mock_client.get_vestigingsprofiel.return_value = None

        result = service.get_vestigingsprofiel("000000000001")

        assert result is None
        #assert "No vestigingsprofiel found" in caplog.text

    # ============ Integration tests ============

    def test_service_handles_multiple_calls(
        self,
        service: KVKRecordService,
        mock_client: MagicMock,
    ) -> None:
        """Test service can handle multiple sequential calls."""
        mock_client.get_basisprofiel.return_value = MagicMock()

        with patch(
            "kvk_connect.services.record_service.map_kvkbasisprofiel_api_to_kvkrecord"
        ) as mock_mapper:
            mock_mapper.return_value = MagicMock(spec=BasisProfielDomain)

            result1 = service.get_basisprofiel("12345678")
            result2 = service.get_basisprofiel("87654321")

            assert result1 is not None
            assert result2 is not None
            assert mock_client.get_basisprofiel.call_count == 2

    def test_service_client_reuse(
        self,
        mock_client: MagicMock,
    ) -> None:
        """Test that service reuses same client instance."""
        service1 = KVKRecordService(mock_client)
        service2 = KVKRecordService(mock_client)

        assert service1.client is service2.client
