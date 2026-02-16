"""Main ingestion logic for kline data — DataFrame edition."""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from zer0data_ingestor.cleaner.kline import KlineCleaner
from zer0data_ingestor.config import IngestorConfig
from zer0data_ingestor.constants import interval_to_ms
from zer0data_ingestor.parser import KlineParser
from zer0data_ingestor.writer.clickhouse import ClickHouseWriter

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
    errors: List[str] = field(default_factory=list)


class KlineIngestor:
    """Main ingestor for parsing and writing kline data."""

    def __init__(self, config: IngestorConfig):
        """Initialize the ingestor.

        Args:
            config: IngestorConfig instance with database settings.
        """
        self.config = config
        self.parser = KlineParser()
        self._cleaners: Dict[str, KlineCleaner] = {}
        self.writer = ClickHouseWriter(
            host=config.clickhouse.host,
            port=config.clickhouse.port,
            database=config.clickhouse.database,
            username=config.clickhouse.username or "default",
            password=config.clickhouse.password or "",
        )
        self._closed = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest_from_directory(
        self,
        source: str,
        symbols: Optional[List[str]] = None,
        pattern: str = "**/*.zip",
        force: bool = False,
    ) -> IngestStats:
        """Ingest kline data from a directory of zip files.

        Args:
            source: Path to directory containing zip files.
            symbols: Optional list of symbols to filter.
            pattern: Glob pattern for matching files.
            force: If True, re-import data even if it already exists.

        Returns:
            IngestStats with ingestion statistics.
        """
        if self._closed:
            raise RuntimeError("Ingestor has been closed")

        stats = IngestStats()
        symbols_seen: set[str] = set()
        files_skipped = 0

        try:
            for symbol, interval, df, file_path in self.parser.parse_directory_with_path(
                source,
                symbols,
                pattern=pattern,
            ):
                stats.files_processed += 1
                symbols_seen.add(symbol)

                # Build a human-readable time range for the log.
                _ts_start = pd.Timestamp(int(df["open_time"].iloc[0]), unit="ms")
                _ts_end = pd.Timestamp(int(df["open_time"].iloc[-1]), unit="ms")
                _range = f"{_ts_start:%Y-%m-%d} ~ {_ts_end:%Y-%m-%d}"

                # Check if data already exists (incremental import)
                if not force:
                    from zer0data_ingestor.parser.zip_parser import extract_date_from_filename
                    date_str = extract_date_from_filename(file_path)
                    if date_str:
                        # Determine if this is a monthly or daily file
                        # Monthly files have date_str ending in -01 and the filename has only YYYY-MM
                        file_name = Path(file_path).stem
                        parts = file_name.split("-")

                        # Check if it's a monthly file (SYMBOL-INTERVAL-YYYY-MM)
                        is_monthly = len(parts) == 4 and date_str.endswith("-01")

                        if is_monthly:
                            # Extract year and month from date_str
                            date_obj = pd.to_datetime(date_str)
                            if self.writer.has_data_for_month(symbol, interval, date_obj.year, date_obj.month):
                                logger.info(
                                    "[%d] Skipping %s %s %s (data already exists)",
                                    stats.files_processed, symbol, interval, date_str[:7],
                                )
                                files_skipped += 1
                                continue
                        else:
                            # Daily file
                            if self.writer.has_data_for_date(symbol, interval, date_str):
                                logger.info(
                                    "[%d] Skipping %s %s %s (data already exists)",
                                    stats.files_processed, symbol, interval, date_str,
                                )
                                files_skipped += 1
                                continue

                logger.info(
                    "[%d] Processing %s %s  %s  (%d rows)",
                    stats.files_processed, symbol, interval, _range, len(df),
                )

                cleaner = self._get_cleaner(interval)
                clean_result = cleaner.clean(df)

                stats.duplicates_removed += clean_result.stats.duplicates_removed
                stats.gaps_filled += clean_result.stats.gaps_filled
                stats.invalid_records_removed += clean_result.stats.invalid_records_removed

                if (
                    clean_result.stats.duplicates_removed > 0
                    or clean_result.stats.gaps_filled > 0
                    or clean_result.stats.invalid_records_removed > 0
                ):
                    logger.info(
                        "Symbol %s (%s): removed %d duplicates, "
                        "filled %d gaps, removed %d invalid records",
                        symbol,
                        interval,
                        clean_result.stats.duplicates_removed,
                        clean_result.stats.gaps_filled,
                        clean_result.stats.invalid_records_removed,
                    )

                cleaned_df = clean_result.cleaned_df
                if not cleaned_df.empty:
                    self.writer.write_df(cleaned_df, interval)
                    stats.records_written += len(cleaned_df)
                    logger.info(
                        "[%d] Written %d rows for %s %s  %s",
                        stats.files_processed, len(cleaned_df), symbol, interval, _range,
                    )

        except Exception as e:
            error_msg = f"Error processing directory {source}: {e}"
            stats.errors.append(error_msg)
            logger.error(error_msg)

        stats.symbols_processed = len(symbols_seen)

        logger.info(
            "Ingestion complete: %d files processed, %d files skipped, "
            "%d records written, "
            "%d duplicates removed, "
            "%d gaps filled, "
            "%d invalid records removed",
            stats.files_processed,
            files_skipped,
            stats.records_written,
            stats.duplicates_removed,
            stats.gaps_filled,
            stats.invalid_records_removed,
        )

        return stats

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the ingestor and cleanup resources."""
        if not self._closed:
            self.writer.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_cleaner(self, interval: str) -> KlineCleaner:
        """Get (or create) cleaner instance for a specific interval."""
        if interval not in self._cleaners:
            self._cleaners[interval] = KlineCleaner(
                interval_ms=interval_to_ms(interval)
            )
        return self._cleaners[interval]
