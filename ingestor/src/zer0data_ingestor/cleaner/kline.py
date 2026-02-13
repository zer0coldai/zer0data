"""Kline data cleaner."""

from dataclasses import dataclass, field
from typing import List
from zer0data_ingestor.writer.clickhouse import KlineRecord


@dataclass
class CleaningStats:
    """Statistics for data cleaning operations."""
    duplicates_removed: int = 0
    gaps_filled: int = 0
    invalid_records_removed: int = 0
    validation_errors: List[str] = field(default_factory=list)


@dataclass
class CleanResult:
    """Result of cleaning operation."""
    cleaned_records: List[KlineRecord]
    stats: CleaningStats


class KlineCleaner:
    """Cleaner for kline data."""

    def clean(self, records: List[KlineRecord]) -> CleanResult:
        """Clean kline records by removing duplicates.

        Args:
            records: List of kline records to clean

        Returns:
            CleanResult with cleaned records and statistics
        """
        stats = CleaningStats()

        # Remove duplicates, keep first occurrence
        seen_times = set()
        cleaned = []
        for record in records:
            if record.open_time in seen_times:
                stats.duplicates_removed += 1
            else:
                seen_times.add(record.open_time)
                cleaned.append(record)

        return CleanResult(cleaned_records=cleaned, stats=stats)
