"""Tests for KVK number formatting and date utilities."""

from __future__ import annotations

import pytest

from kvk_connect.utils.tools import clean_and_pad, formatteer_datum, parse_kvk_datum


class TestCleanAndPad:
    """Test suite for clean_and_pad utility function."""

    def test_clean_and_pad_with_valid_kvk_number(self) -> None:
        """Test padding valid KVK number."""
        result = clean_and_pad("12345678")
        assert result == "12345678"
        assert len(result) == 8

    def test_clean_and_pad_with_short_number(self) -> None:
        """Test padding short number with leading zeros."""
        result = clean_and_pad("1234")
        assert result == "00001234"
        assert len(result) == 8

    def test_clean_and_pad_with_formatted_kvk(self) -> None:
        """Test stripping non-digits from formatted KVK."""
        result = clean_and_pad("12.345.678")
        assert result == "12345678"

    def test_clean_and_pad_with_spaces(self) -> None:
        """Test stripping spaces."""
        result = clean_and_pad("12 345 678")
        assert result == "12345678"

    def test_clean_and_pad_custom_fill_length(self) -> None:
        """Test custom padding length."""
        result = clean_and_pad("1234", fill=12)
        assert result == "000000001234"
        assert len(result) == 12

    def test_clean_and_pad_with_none_raises_error(self) -> None:
        """Test None input raises ValueError."""
        with pytest.raises(ValueError, match="non-empty string"):
            clean_and_pad(None)  # type: ignore

    def test_clean_and_pad_with_empty_string_raises_error(self) -> None:
        """Test empty string raises ValueError."""
        with pytest.raises(ValueError, match="non-empty string"):
            clean_and_pad("")

    def test_clean_and_pad_with_no_digits_raises_error(self) -> None:
        """Test string with no digits raises ValueError."""
        with pytest.raises(ValueError, match="No digits found"):
            clean_and_pad("abc-def")

    def test_clean_and_pad_with_mixed_input(self) -> None:
        """Test string with mixed alphanumeric."""
        result = clean_and_pad("abc123def456")
        assert result == "00123456"


class TestFormatteerDatum:
    """Test suite for formatteer_datum utility function."""

    def test_formatteer_datum_valid_format(self) -> None:
        """Test formatting valid YYYYMMDD date."""
        result = formatteer_datum("20200115")
        assert result == "15-01-2020"

    def test_formatteer_datum_with_none_string(self) -> None:
        """Test 'None' string returns None."""
        result = formatteer_datum("None")
        assert result is None

    def test_formatteer_datum_with_empty_string(self) -> None:
        """Test 'None' string returns None."""
        result = formatteer_datum("")
        assert result is None

    def test_formatteer_datum_with_none_value(self) -> None:
        """Test None value returns None."""
        result = formatteer_datum(None)
        assert result is None

    def test_formatteer_datum_invalid_format_returns_input(self) -> None:
        """Test invalid format returns original input."""
        result = formatteer_datum("not-a-date")
        assert result == "not-a-date"

    def test_formatteer_datum_invalid_date_returns_input(self) -> None:
        """Test invalid date (e.g., month 13) returns input."""
        result = formatteer_datum("20201301")  # Month 13 doesn't exist
        assert result == "20201301"

    def test_formatteer_datum_leading_zeros(self) -> None:
        """Test date with leading zeros formats correctly."""
        result = formatteer_datum("20010101")
        assert result == "01-01-2001"

    def test_formatteer_datum_end_of_month(self) -> None:
        """Test end-of-month dates."""
        result = formatteer_datum("20200131")
        assert result == "31-01-2020"

    def test_formatteer_datum_leap_year(self) -> None:
        """Test leap year February 29."""
        result = formatteer_datum("20200229")
        assert result == "29-02-2020"

    def test_formatteer_datum_no_day(self) -> None:
        result = formatteer_datum("19881100")
        assert result == "01-11-1988"

    def test_formatteer_datum_no_month_no_day(self) -> None:
        result = formatteer_datum("19880000")
        assert result == "01-01-1988"

    def test_formatteer_datum_all_zero(self) -> None:
        result = formatteer_datum("00000000")
        assert result == None

class TestParseKVKDatum:
    """Test suite for parse_kvk_datum utility function."""

    def test_parse_kvk_datum_ddmmyyyy_format(self) -> None:
        """Test parsing DD-MM-YYYY format."""
        from datetime import date
        result = parse_kvk_datum("15-01-2020")
        assert result == date(2020, 1, 15)

    def test_parse_kvk_datum_yyyymmdd_format(self) -> None:
        """Test parsing YYYYMMDD format."""
        from datetime import date
        result = parse_kvk_datum("20200115")
        assert result == date(2020, 1, 15)

    def test_parse_kvk_datum_yyyymm00_format(self) -> None:
        """Test parsing YYYYMM00 format (sets day to 1)."""
        from datetime import date
        result = parse_kvk_datum("20200100")
        assert result == date(2020, 1, 1)

    def test_parse_kvk_datum_yyyy0000_format(self) -> None:
        """Test parsing YYYY0000 format (sets month and day to 1)."""
        from datetime import date
        result = parse_kvk_datum("20200000")
        assert result == date(2020, 1, 1)

    def test_parse_kvk_datum_with_none(self) -> None:
        """Test None input returns None."""
        result = parse_kvk_datum(None)
        assert result is None

    def test_parse_kvk_datum_empty_string(self) -> None:
        """Test empty string returns None."""
        result = parse_kvk_datum("")
        assert result is None

    def test_parse_kvk_datum_invalid_format(self) -> None:
        """Test invalid format returns None."""
        result = parse_kvk_datum("invalid-date")
        assert result is None

    def test_parse_kvk_datum_invalid_month(self) -> None:
        """Test invalid month returns None."""
        result = parse_kvk_datum("20201301")
        assert result is None

    def test_parse_kvk_datum_invalid_day(self) -> None:
        """Test invalid day returns None."""
        result = parse_kvk_datum("20200232")  # Feb 32 doesn't exist
        assert result is None

    def test_parse_kvk_datum_string_none(self) -> None:
        """Test literal string 'None' returns None."""
        result = parse_kvk_datum("None")
        assert result is None

    def test_parse_kvk_datum_whitespace(self) -> None:
        """Test string with whitespace is handled."""
        from datetime import date
        result = parse_kvk_datum("  20200115  ")
        assert result == date(2020, 1, 15)
