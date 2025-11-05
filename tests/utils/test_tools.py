import unittest
from datetime import date

from kvk_connect.utils.tools import parse_kvk_datum


class TestParseKvkDatum(unittest.TestCase):
    def test_parse_valid_datum(self):
        """Test parsing van een geldige datum string."""
        result = parse_kvk_datum("15-03-2024")
        expected = date(2024, 3, 15)
        assert result == expected

    def test_parse_none_string(self):
        """Test parsing van de string 'None'."""
        result = parse_kvk_datum("None")
        assert result is None

    def test_parse_empty_string(self):
        """Test parsing van een lege string."""
        result = parse_kvk_datum("")
        assert result is None

    def test_parse_whitespace_string(self):
        """Test parsing van een string met alleen spaties."""
        result = parse_kvk_datum("   ")
        assert result is None

    def test_parse_none_value(self):
        """Test parsing van None waarde."""
        result = parse_kvk_datum(None)
        assert result is None

    def test_parse_invalid_format(self):
        """Test parsing van ongeldige datum format (YYYY-MM-DD)."""
        result = parse_kvk_datum("2024-03-15")
        assert result is None

    def test_parse_invalid_datum(self):
        """Test parsing van ongeldige datum (32e dag)."""
        result = parse_kvk_datum("32-13-2024")
        assert result is None

    def test_parse_datum_with_whitespace(self):
        """Test parsing van datum met whitespace aan begin/einde."""
        result = parse_kvk_datum("  15-03-2024  ")
        expected = date(2024, 3, 15)
        assert result == expected

    def test_parse_yyyymmdd_format_valid(self):
        """Test parsing van YYYYMMDD formaat met geldige datum."""
        result = parse_kvk_datum("19700315")
        expected = date(1970, 3, 15)
        assert result == expected

    def test_parse_yyyymm00_format(self):
        """Test parsing van YYYYMM00 formaat (dag onbekend)."""
        result = parse_kvk_datum("19700000")
        expected = date(1970, 1, 1)
        assert result == expected

    def test_parse_yyyy0m00_format(self):
        """Test parsing van YYYY0M00 formaat (maand met leading zero, dag onbekend)."""
        result = parse_kvk_datum("19180000")
        expected = date(1918, 1, 1)
        assert result == expected

    def test_parse_yyyymm00_with_month(self):
        """Test parsing van YYYYMM00 formaat met specifieke maand."""
        result = parse_kvk_datum("18720500")
        expected = date(1872, 5, 1)
        assert result == expected

    def test_parse_yyyymmdd_invalid_month(self):
        """Test parsing van YYYYMMDD formaat met ongeldige maand (13)."""
        result = parse_kvk_datum("19701315")
        assert result is None

    def test_parse_yyyymmdd_invalid_day(self):
        """Test parsing van YYYYMMDD formaat met ongeldige dag (32)."""
        result = parse_kvk_datum("19700332")
        assert result is None

    def test_parse_yyyymmdd_too_short(self):
        """Test parsing van te korte string (7 cijfers)."""
        result = parse_kvk_datum("1970032")
        assert result is None

    def test_parse_yyyymmdd_too_long(self):
        """Test parsing van te lange string (9 cijfers)."""
        result = parse_kvk_datum("197003150")
        assert result is None


if __name__ == "__main__":
    unittest.main()
