"""Tests for KVK API client."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest
import requests

from kvk_connect.api.client import KVKApiClient
#from kvk_connect.exceptions import KVKApiError, RateLimitError

logger = logging.getLogger(__name__)


class TestKVKApiClient:
    """Test suite for KVKApiClient."""

    def test_get_basisprofiel_success(
        self, mock_kvk_basisprofiel_response: dict
    ) -> None:
        """Test successful basisprofiel retrieval."""
        client = KVKApiClient(api_key="test-key")

        with patch.object(
            client.session, "get"
        ) as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = mock_kvk_basisprofiel_response
            mock_response.status_code = 200
            mock_get.return_value = mock_response

            result = client.get_basisprofiel("12345678")

            assert result.kvk_nummer == "12345678"
            assert result.naam == "Blooming Tandartsen"
            mock_get.assert_called_once()


    def test_get_vestigingsprofiel_success(
        self, mock_kvk_vestigingsprofiel_response: dict
    ) -> None:
        """Test successful vestigingsprofiel retrieval."""
        client = KVKApiClient(api_key="test-key")

        with patch.object(client.session, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = mock_kvk_vestigingsprofiel_response
            mock_response.status_code = 200
            mock_get.return_value = mock_response

            result = client.get_vestigingsprofiel("000000000001")

            assert result.vestigingsnummer == "000038976579"
            mock_get.assert_called_once()

    def test_get_vestigingen_success(
        self, mock_kvk_vestigingen_response: dict
    ) -> None:
        """Test successful vestigingen retrieval."""
        client = KVKApiClient(api_key="test-key")

        with patch.object(client.session, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = mock_kvk_vestigingen_response
            mock_response.status_code = 200
            mock_get.return_value = mock_response

            result = client.get_vestigingen("12345678")

            assert result.vestigingen is not None
            assert len(result.vestigingen) == 38
            mock_get.assert_called_once()

    def test_session_lazy_loading(self) -> None:
        """Test session is lazily initialized."""
        client = KVKApiClient(api_key="test-key")

        # Session should be initialized on first access
        session1 = client.session
        session2 = client.session

        assert session1 is session2  # Same session object

    def test_api_key_in_headers(self) -> None:
        """Test API key is correctly added to request headers."""
        api_key = "secret-key-123"
        client = KVKApiClient(api_key=api_key)

        auth_header = client.session.headers.get("apikey")
        assert auth_header == f"{api_key}"
