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

    def _validate_record(self, record: KlineRecord) -> tuple[bool, list[str]]:
        """Validate a kline record.

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []

        # Check for negative prices
        if record.open_price <= 0 or record.high_price <= 0 or record.low_price <= 0 or record.close_price <= 0:
            errors.append(f"non-positive price at {record.open_time}")

        # Check OHLC logic: high >= max(open, close) and low <= min(open, close)
        if record.high_price < max(record.open_price, record.close_price):
            errors.append(f"high < max(open, close) at {record.open_time}")

        if record.low_price > min(record.open_price, record.close_price):
            errors.append(f"low > min(open, close) at {record.open_time}")

        if record.high_price < record.low_price:
            errors.append(f"high < low at {record.open_time}")

        # Check volume
        if record.volume < 0:
            errors.append(f"negative volume at {record.open_time}")

        return (len(errors) == 0, errors)

    def clean(self, records: List[KlineRecord]) -> CleanResult:
        """Clean kline records by removing duplicates and invalid records.

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

        # Validate records
        validated = []
        for record in cleaned:
            is_valid, errors = self._validate_record(record)
            if is_valid:
                validated.append(record)
            else:
                stats.invalid_records_removed += 1
                stats.validation_errors.extend(errors)

        return CleanResult(cleaned_records=validated, stats=stats)
