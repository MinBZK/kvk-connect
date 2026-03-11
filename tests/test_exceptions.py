"""Tests for KVK exception classes."""

# ruff: noqa: D103
from __future__ import annotations

from kvk_connect.exceptions import KVKPermanentError, KVKTemporaryError


class TestKVKPermanentError:
    def test_attributes(self) -> None:
        exc = KVKPermanentError("12345678", "IPD0005", "Product niet leverbaar")
        assert exc.kvk_nummer == "12345678"
        assert exc.code == "IPD0005"
        assert exc.omschrijving == "Product niet leverbaar"

    def test_str_contains_kvk_and_code(self) -> None:
        exc = KVKPermanentError("12345678", "IPD0005", "Product niet leverbaar")
        msg = str(exc)
        assert "12345678" in msg
        assert "IPD0005" in msg

    def test_is_exception(self) -> None:
        exc = KVKPermanentError("12345678", "IPD0005", "omschrijving")
        assert isinstance(exc, Exception)


class TestKVKTemporaryError:
    def test_attributes(self) -> None:
        exc = KVKTemporaryError("87654321", "IPD1002", "Tijdelijk in behandeling")
        assert exc.kvk_nummer == "87654321"
        assert exc.code == "IPD1002"
        assert exc.omschrijving == "Tijdelijk in behandeling"

    def test_str_contains_kvk_and_code(self) -> None:
        exc = KVKTemporaryError("87654321", "IPD1003", "Probeer over 5 minuten")
        msg = str(exc)
        assert "87654321" in msg
        assert "IPD1003" in msg

    def test_is_exception(self) -> None:
        exc = KVKTemporaryError("87654321", "IPD1002", "omschrijving")
        assert isinstance(exc, Exception)
