"""Tests for constants module."""

import pytest

from zer0data_ingestor.constants import (
    INTERVAL_MS,
    VALID_INTERVALS,
    Interval,
    interval_to_ms,
    is_valid_interval,
)


class TestIsValidInterval:
    def test_valid_intervals(self):
        for iv in VALID_INTERVALS:
            assert is_valid_interval(iv) is True

    def test_invalid_intervals(self):
        assert is_valid_interval("2m") is False
        assert is_valid_interval("3h") is False
        assert is_valid_interval("") is False
        assert is_valid_interval(None) is False

    def test_interval_class_constants(self):
        assert is_valid_interval(Interval.M1) is True
        assert is_valid_interval(Interval.H1) is True
        assert is_valid_interval(Interval.D1) is True


class TestIntervalToMs:
    def test_basic_mapping(self):
        assert interval_to_ms("1m") == 60_000
        assert interval_to_ms("1h") == 3_600_000
        assert interval_to_ms("1d") == 86_400_000

    def test_all_intervals_mapped(self):
        for iv in VALID_INTERVALS:
            assert iv in INTERVAL_MS
            assert interval_to_ms(iv) == INTERVAL_MS[iv]

    def test_invalid_interval_raises(self):
        with pytest.raises(ValueError, match="Invalid interval"):
            interval_to_ms("2m")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            interval_to_ms("")
