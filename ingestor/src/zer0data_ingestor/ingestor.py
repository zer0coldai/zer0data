"""Main ingestion logic for kline data."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

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
        self.cleaner = KlineCleaner()
        self.writer = ClickHouseWriter(
            host=config.clickhouse.host,
            port=config.clickhouse.port,
            database=config.clickhouse.database,
            username=config.clickhouse.username or "default",
            password=config.clickhouse.password or "",
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
            # Group records by symbol for cleaning
            records_by_symbol: Dict[str, List[KlineRecord]] = {}

            # Parse all matching files in the directory
            for symbol, record in self.parser.parse_directory(source, symbols, pattern):
                if symbol not in records_by_symbol:
                    records_by_symbol[symbol] = []
                records_by_symbol[symbol].append(record)
                symbols_seen.add(symbol)

            # Clean and write records for each symbol
            for symbol, records in records_by_symbol.items():
                # Apply cleaning
                clean_result = self.cleaner.clean(records)

                # Track cleaning stats
                stats.duplicates_removed += clean_result.stats.duplicates_removed
                stats.gaps_filled += clean_result.stats.gaps_filled
                stats.invalid_records_removed += clean_result.stats.invalid_records_removed

                # Log cleaning stats for this symbol
                if (clean_result.stats.duplicates_removed > 0 or
                    clean_result.stats.gaps_filled > 0 or
                    clean_result.stats.invalid_records_removed > 0):
                    logger.info(
                        f"Symbol {symbol}: removed {clean_result.stats.duplicates_removed} duplicates, "
                        f"filled {clean_result.stats.gaps_filled} gaps, "
                        f"removed {clean_result.stats.invalid_records_removed} invalid records"
                    )

                # Write cleaned records
                for cleaned_record in clean_result.cleaned_records:
                    self.writer.insert(cleaned_record)
                    stats.records_written += 1

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
