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


# Mapping from interval string to duration in milliseconds.
INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}


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


def interval_to_ms(interval: str) -> int:
    """Convert a valid interval string to milliseconds.

    Args:
        interval: A valid interval string (e.g., "1m", "1h", "1d")

    Returns:
        The interval duration in milliseconds.

    Raises:
        ValueError: If the interval is not valid.

    Examples:
        >>> interval_to_ms("1m")
        60000
        >>> interval_to_ms("1h")
        3600000
        >>> interval_to_ms("1d")
        86400000
    """
    if interval not in INTERVAL_MS:
        raise ValueError(
            f"Invalid interval '{interval}'. Must be one of: {', '.join(VALID_INTERVALS)}"
        )
    return INTERVAL_MS[interval]
