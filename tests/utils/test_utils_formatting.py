# from utils.formatting import truncate_float
import unittest

from kvk_connect.utils import truncate_float


class TestUtils(unittest.TestCase):
    def test_truncate_float_basic(self):
        assert truncate_float(52.1234567) == "52,12345"

    def test_truncate_float_zero(self):
        assert truncate_float(0.0) == ""

    def test_truncate_float_none(self):
        assert truncate_float(None) == ""
