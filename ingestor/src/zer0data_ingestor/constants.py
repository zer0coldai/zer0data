"""Interval constants and validation for multi-interval k-line data."""

from typing import Optional


VALID_INTERVALS = (
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
)
"""Valid k-line interval values supported by the system."""


class Interval:
    """Interval constants for type-safe interval specification.

    Usage:
        from zer0data_ingestor.constants import Interval

        # Use the constant directly
        interval = Interval.H1

        # Pass to functions
        is_valid = is_valid_interval(Interval.H1)
    """

    M1 = "1m"
    M3 = "3m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H2 = "2h"
    H4 = "4h"
    H6 = "6h"
    H8 = "8h"
    H12 = "12h"
    D1 = "1d"


def is_valid_interval(interval: Optional[str]) -> bool:
    """Check if an interval string is valid.

    Args:
        interval: The interval string to validate (e.g., "1m", "1h", "1d")

    Returns:
        True if the interval is valid, False otherwise

    Examples:
        >>> is_valid_interval("1m")
        True
        >>> is_valid_interval("2m")
        False
        >>> is_valid_interval(None)
        False
        >>> is_valid_interval(Interval.H1)
        True
    """
    if interval is None:
        return False
    return interval in VALID_INTERVALS
