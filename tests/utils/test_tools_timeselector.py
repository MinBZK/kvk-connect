import unittest
from datetime import UTC, datetime

from kvk_connect.utils.tools import get_timeselector


def utc(y, m, d, h=0, mi=0, s=0):
    return datetime(y, m, d, h, mi, s, tzinfo=UTC)


def to_pairs(chunks):
    return [(c["from"], c["to"]) for c in chunks]


class TestGetTimeSelector(unittest.TestCase):
    def test_before_repo_range(self):
        # Entirely before repo: st <= rf
        sf, st = utc(2024, 1, 1), utc(2024, 1, 10)
        out = get_timeselector(sf, st)
        assert to_pairs(out) == [(utc(2024, 1, 1), utc(2024, 1, 8)), (utc(2024, 1, 8), utc(2024, 1, 10))]

    def test_incorrect_range(self):
        sf, st = utc(2024, 1, 12), utc(2024, 1, 10)
        out = get_timeselector(sf, st)
        assert out == []

    def test_zero_length_selection_returns_empty(self):
        sel = utc(2024, 1, 1)
        out = get_timeselector(sel, sel)
        assert out == []


if __name__ == "__main__":
    unittest.main()
