"""Tests for KVK API client."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest
import requests

from kvk_connect.api.client import KVKApiClient
from kvk_connect.exceptions import KVKPermanentError, KVKTemporaryError

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
            assert result.naam == "Test B.V."
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

            assert result.vestigingsnummer == "000000000001"
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
            assert len(result.vestigingen) == 5
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

    # --- _extract_kvk_fout_codes ---

    def test_extract_fout_codes_with_single_code(self) -> None:
        """Test extracting a single fout code from response JSON."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"fout": [{"code": "IPD0005", "omschrijving": "Product niet leverbaar"}]}

        codes = KVKApiClient._extract_kvk_fout_codes(mock_resp)

        assert codes == [("IPD0005", "Product niet leverbaar")]

    def test_extract_fout_codes_empty_fout_list(self) -> None:
        """Test that empty fout list returns empty result."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"fout": []}

        codes = KVKApiClient._extract_kvk_fout_codes(mock_resp)

        assert codes == []

    def test_extract_fout_codes_no_fout_key(self) -> None:
        """Test that missing fout key returns empty result."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {}

        codes = KVKApiClient._extract_kvk_fout_codes(mock_resp)

        assert codes == []

    def test_extract_fout_codes_invalid_json(self) -> None:
        """Test that JSON decode error returns empty result."""
        mock_resp = MagicMock()
        mock_resp.json.side_effect = ValueError("invalid json")

        codes = KVKApiClient._extract_kvk_fout_codes(mock_resp)

        assert codes == []

    # --- _raise_for_kvk_fout ---

    def test_raise_for_kvk_fout_raises_permanent_error(self) -> None:
        """Test that non-temporary codes raise KVKPermanentError."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"fout": [{"code": "IPD0005", "omschrijving": "niet leverbaar"}]}

        with pytest.raises(KVKPermanentError) as exc_info:
            KVKApiClient._raise_for_kvk_fout("12345678", mock_resp)

        assert exc_info.value.kvk_nummer == "12345678"
        assert exc_info.value.code == "IPD0005"

    def test_raise_for_kvk_fout_raises_temporary_on_ipd1002(self) -> None:
        """Test that IPD1002 raises KVKTemporaryError."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"fout": [{"code": "IPD1002", "omschrijving": "in behandeling"}]}

        with pytest.raises(KVKTemporaryError) as exc_info:
            KVKApiClient._raise_for_kvk_fout("12345678", mock_resp)

        assert exc_info.value.code == "IPD1002"

    def test_raise_for_kvk_fout_raises_temporary_on_ipd1003(self) -> None:
        """Test that IPD1003 raises KVKTemporaryError."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"fout": [{"code": "IPD1003", "omschrijving": "probeer over 5 min"}]}

        with pytest.raises(KVKTemporaryError) as exc_info:
            KVKApiClient._raise_for_kvk_fout("12345678", mock_resp)

        assert exc_info.value.code == "IPD1003"

    def test_raise_for_kvk_fout_does_not_raise_without_codes(self) -> None:
        """Test that a response without fout codes does not raise."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {}

        # Should return None silently — no raise
        result = KVKApiClient._raise_for_kvk_fout("12345678", mock_resp)
        assert result is None

    # --- get_basisprofiel_raw error scenarios ---

    def test_get_basisprofiel_raises_permanent_error_on_ipd0005(self) -> None:
        """Test get_basisprofiel_raw raises KVKPermanentError for IPD0005."""
        client = KVKApiClient(api_key="test-key")

        with patch.object(client.session, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = {"fout": [{"code": "IPD0005", "omschrijving": "niet leverbaar"}]}
            mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
            mock_get.return_value = mock_response

            with pytest.raises(KVKPermanentError) as exc_info:
                client.get_basisprofiel_raw("12345678")

        assert exc_info.value.kvk_nummer == "12345678"
        assert exc_info.value.code == "IPD0005"

    def test_get_basisprofiel_raises_temporary_error_on_ipd1002(self) -> None:
        """Test get_basisprofiel_raw raises KVKTemporaryError for IPD1002."""
        client = KVKApiClient(api_key="test-key")

        with patch.object(client.session, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = {"fout": [{"code": "IPD1002", "omschrijving": "in behandeling"}]}
            mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
            mock_get.return_value = mock_response

            with pytest.raises(KVKTemporaryError) as exc_info:
                client.get_basisprofiel_raw("12345678")

        assert exc_info.value.code == "IPD1002"

    def test_get_basisprofiel_returns_none_on_unknown_http_error(self) -> None:
        """Test get_basisprofiel_raw returns None when no IPD code is present."""
        client = KVKApiClient(api_key="test-key")

        with patch.object(client.session, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = {}  # No fout codes
            mock_response.raise_for_status.side_effect = requests.HTTPError("500 Internal Server Error")
            mock_get.return_value = mock_response

            result = client.get_basisprofiel_raw("12345678")

        assert result is None
