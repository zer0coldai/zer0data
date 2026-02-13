"""Main ingestion logic for kline data."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from zer0data_ingestor.cleaner.kline import KlineCleaner
from zer0data_ingestor.config import IngestorConfig
from zer0data_ingestor.parser import KlineParser
from zer0data_ingestor.writer.clickhouse import ClickHouseWriter, KlineRecord

logger = logging.getLogger(__name__)


@dataclass
class IngestStats:
    """Statistics for ingestion operations."""

    symbols_processed: int = 0
    dates_processed: int = 0
    records_written: int = 0
    files_processed: int = 0
    duplicates_removed: int = 0
    gaps_filled: int = 0
    invalid_records_removed: int = 0
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class KlineIngestor:
    """Main ingestor for parsing and writing kline data."""

    def __init__(self, config: IngestorConfig, data_dir: str = "./data/download"):
        """Initialize the ingestor.

        Args:
            config: IngestorConfig instance with database settings
            data_dir: Directory containing downloaded zip files (overrides config.data_dir)
        """
        self.config = config
        self.data_dir = Path(data_dir)
        self.parser = KlineParser()
        self._cleaners_by_interval: Dict[str, KlineCleaner] = {}
        self.writer = ClickHouseWriter(
            host=config.clickhouse.host,
            port=config.clickhouse.port,
            database=config.clickhouse.database,
            username=config.clickhouse.username or "default",
            password=config.clickhouse.password or "",
            batch_size=config.batch_size,
        )
        self._closed = False

    def ingest_from_directory(
        self,
        source: str,
        symbols: Optional[List[str]] = None,
        pattern: str = "**/*.zip"
    ) -> IngestStats:
        """Ingest kline data from a directory of zip files.

        Args:
            source: Path to directory containing zip files
            symbols: Optional list of symbols to filter
            pattern: Glob pattern for matching files (default: "*.zip")

        Returns:
            IngestStats with ingestion statistics
        """
        if self._closed:
            raise RuntimeError("Ingestor has been closed")

        stats = IngestStats()
        symbols_seen = set()

        try:
            # Buffer records by (symbol, interval) and process incrementally.
            records_by_key: Dict[Tuple[str, str], List[KlineRecord]] = {}
            carry_over_by_key: Dict[Tuple[str, str], KlineRecord] = {}
            keys_seen = set()

            # Parse all matching files in the directory
            for symbol, interval, record in self.parser.parse_directory(
                source,
                symbols,
                pattern=pattern,
            ):
                key = (symbol, interval)
                if key not in records_by_key:
                    records_by_key[key] = []
                records_by_key[key].append(record)
                keys_seen.add(key)
                symbols_seen.add(symbol)

                if len(records_by_key[key]) >= self.config.batch_size:
                    self._clean_and_write_key_chunk(
                        key=key,
                        records_by_key=records_by_key,
                        carry_over_by_key=carry_over_by_key,
                        stats=stats,
                        final=False,
                    )

            # Final flush for all keys and any pending carry-over record.
            for key in keys_seen:
                self._clean_and_write_key_chunk(
                    key=key,
                    records_by_key=records_by_key,
                    carry_over_by_key=carry_over_by_key,
                    stats=stats,
                    final=True,
                )

            # Track the directory as processed
            stats.files_processed = 1
            stats.symbols_processed = len(symbols_seen)

        except Exception as e:
            error_msg = f"Error processing directory {source}: {e}"
            stats.errors.append(error_msg)
            logger.error(error_msg)

        # Flush any remaining records
        self.writer.flush()

        # Log overall stats
        logger.info(
            f"Ingestion complete: {stats.records_written} records written, "
            f"{stats.duplicates_removed} duplicates removed, "
            f"{stats.gaps_filled} gaps filled, "
            f"{stats.invalid_records_removed} invalid records removed"
        )

        return stats

    def _clean_and_write_key_chunk(
        self,
        key: Tuple[str, str],
        records_by_key: Dict[Tuple[str, str], List[KlineRecord]],
        carry_over_by_key: Dict[Tuple[str, str], KlineRecord],
        stats: IngestStats,
        final: bool,
    ) -> None:
        """Clean and write one (symbol, interval) chunk.

        For non-final chunks we keep the last cleaned record as carry-over so
        the next chunk can preserve continuity for dedup/gap-filling logic.
        """
        symbol, interval = key
        records = records_by_key.get(key, [])

        if not records:
            if final and key in carry_over_by_key:
                self.writer.insert_many([carry_over_by_key.pop(key)])
                stats.records_written += 1
            return

        if key in carry_over_by_key:
            records = [carry_over_by_key.pop(key)] + records

        cleaner = self._get_cleaner(interval)
        clean_result = cleaner.clean(records)

        stats.duplicates_removed += clean_result.stats.duplicates_removed
        stats.gaps_filled += clean_result.stats.gaps_filled
        stats.invalid_records_removed += clean_result.stats.invalid_records_removed

        if (
            clean_result.stats.duplicates_removed > 0
            or clean_result.stats.gaps_filled > 0
            or clean_result.stats.invalid_records_removed > 0
        ):
            logger.info(
                f"Symbol {symbol} ({interval}): removed {clean_result.stats.duplicates_removed} duplicates, "
                f"filled {clean_result.stats.gaps_filled} gaps, "
                f"removed {clean_result.stats.invalid_records_removed} invalid records"
            )

        cleaned_records = clean_result.cleaned_records
        to_write: List[KlineRecord]
        if final:
            to_write = cleaned_records
        elif len(cleaned_records) <= 1:
            to_write = []
            if cleaned_records:
                carry_over_by_key[key] = cleaned_records[0]
        else:
            carry_over_by_key[key] = cleaned_records[-1]
            to_write = cleaned_records[:-1]

        if to_write:
            self.writer.insert_many(to_write)
            stats.records_written += len(to_write)

        records_by_key[key] = []

    def _get_cleaner(self, interval: str) -> KlineCleaner:
        """Get cleaner instance for a specific interval."""
        if interval not in self._cleaners_by_interval:
            self._cleaners_by_interval[interval] = KlineCleaner(
                interval_ms=self._interval_to_ms(interval)
            )
        return self._cleaners_by_interval[interval]

    def _interval_to_ms(self, interval: str) -> int:
        """Convert kline interval string (e.g., 1m, 1h) to milliseconds."""
        if not interval or len(interval) < 2:
            return self.config.cleaner_interval_ms

        value_part = interval[:-1]
        unit = interval[-1]
        if not value_part.isdigit():
            return self.config.cleaner_interval_ms

        value = int(value_part)
        if unit == "m":
            return value * 60_000
        if unit == "h":
            return value * 3_600_000
        if unit == "d":
            return value * 86_400_000

        # Fallback for unsupported units to preserve compatibility.
        return self.config.cleaner_interval_ms

    def close(self) -> None:
        """Close the ingestor and cleanup resources."""
        if not self._closed:
            self.writer.close()
            self._closed = True

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
