"""zer0data ingestion service."""

from zer0data_ingestor.constants import VALID_INTERVALS, Interval, is_valid_interval

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "VALID_INTERVALS",
    "Interval",
    "is_valid_interval",
]
