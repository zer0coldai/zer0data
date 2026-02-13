"""Tests for interval constants and validation."""


from zer0data_ingestor.constants import VALID_INTERVALS, Interval, is_valid_interval


class TestValidIntervals:
    """Tests for VALID_INTERVALS constant."""

    def test_valid_intervals_is_a_list(self):
        """VALID_INTERVALS should be a sequence type."""
        assert isinstance(VALID_INTERVALS, (list, tuple))

    def test_valid_intervals_contains_all_expected_intervals(self):
        """VALID_INTERVALS should contain all expected interval values."""
        expected = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]
        assert set(VALID_INTERVALS) == set(expected)

    def test_valid_intervals_has_correct_length(self):
        """VALID_INTERVALS should have 12 elements."""
        assert len(VALID_INTERVALS) == 12

    def test_valid_intervals_is_immutable(self):
        """VALID_INTERVALS should be a tuple for immutability."""
        # The constant should be a tuple to prevent accidental modification
        assert isinstance(VALID_INTERVALS, tuple)


class TestIntervalClass:
    """Tests for Interval class."""

    def test_interval_m1(self):
        """Interval.M1 should be '1m'."""
        assert Interval.M1 == "1m"

    def test_interval_m3(self):
        """Interval.M3 should be '3m'."""
        assert Interval.M3 == "3m"

    def test_interval_m5(self):
        """Interval.M5 should be '5m'."""
        assert Interval.M5 == "5m"

    def test_interval_m15(self):
        """Interval.M15 should be '15m'."""
        assert Interval.M15 == "15m"

    def test_interval_m30(self):
        """Interval.M30 should be '30m'."""
        assert Interval.M30 == "30m"

    def test_interval_h1(self):
        """Interval.H1 should be '1h'."""
        assert Interval.H1 == "1h"

    def test_interval_h2(self):
        """Interval.H2 should be '2h'."""
        assert Interval.H2 == "2h"

    def test_interval_h4(self):
        """Interval.H4 should be '4h'."""
        assert Interval.H4 == "4h"

    def test_interval_h6(self):
        """Interval.H6 should be '6h'."""
        assert Interval.H6 == "6h"

    def test_interval_h8(self):
        """Interval.H8 should be '8h'."""
        assert Interval.H8 == "8h"

    def test_interval_h12(self):
        """Interval.H12 should be '12h'."""
        assert Interval.H12 == "12h"

    def test_interval_d1(self):
        """Interval.D1 should be '1d'."""
        assert Interval.D1 == "1d"

    def test_interval_values_match_valid_intervals(self):
        """All Interval class values should be in VALID_INTERVALS."""
        interval_values = [
            Interval.M1, Interval.M3, Interval.M5, Interval.M15, Interval.M30,
            Interval.H1, Interval.H2, Interval.H4, Interval.H6, Interval.H8,
            Interval.H12, Interval.D1
        ]
        for value in interval_values:
            assert value in VALID_INTERVALS


class TestIsValidInterval:
    """Tests for is_valid_interval function."""

    def test_valid_interval_returns_true(self):
        """is_valid_interval should return True for valid intervals."""
        for interval in VALID_INTERVALS:
            assert is_valid_interval(interval) is True

    def test_invalid_interval_returns_false(self):
        """is_valid_interval should return False for invalid intervals."""
        invalid_intervals = ["2m", "10m", "3h", "1w", "invalid", "", "1M"]
        for interval in invalid_intervals:
            assert is_valid_interval(interval) is False

    def test_is_valid_interval_with_interval_class(self):
        """is_valid_interval should work with Interval class attributes."""
        assert is_valid_interval(Interval.M1) is True
        assert is_valid_interval(Interval.H1) is True
        assert is_valid_interval(Interval.D1) is True

    def test_is_valid_interval_with_none(self):
        """is_valid_interval should handle None input."""
        assert is_valid_interval(None) is False

    def test_is_valid_interval_with_case_sensitivity(self):
        """is_valid_interval should be case sensitive."""
        assert is_valid_interval("1M") is False
        assert is_valid_interval("1H") is False
        assert is_valid_interval("1D") is False
