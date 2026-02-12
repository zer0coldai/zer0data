"""Main ingestion logic for kline data."""

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from zer0data_ingestor.config import IngestorConfig
from zer0data_ingestor.parser import KlineParser
from zer0data_ingestor.writer.clickhouse import ClickHouseWriter, KlineRecord


@dataclass
class IngestStats:
    """Statistics for ingestion operations."""

    symbols_processed: int = 0
    dates_processed: int = 0
    records_written: int = 0
    files_processed: int = 0
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
        pattern: str = "*.zip"
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

        try:
            # Parse all matching files in the directory
            for symbol, record in self.parser.parse_directory(source, symbols, pattern):
                self.writer.insert(record)
                stats.records_written += 1

            # Track the directory as processed
            stats.files_processed = 1

        except Exception as e:
            error_msg = f"Error processing directory {source}: {e}"
            stats.errors.append(error_msg)

        # Flush any remaining records
        self.writer.flush()

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
