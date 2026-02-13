"""Kline data parser for Binance zip files."""

import csv
import zipfile
from io import TextIOWrapper
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

from zer0data_ingestor.writer.clickhouse import KlineRecord


def extract_interval_from_filename(filename: str) -> str:
    """Extract interval from a Binance kline filename.

    Binance filename format: SYMBOL-INTERVAL-DATE.zip
    Example: BTCUSDT-1h-2024-01-01.zip -> 1h

    Args:
        filename: The filename or path to extract interval from

    Returns:
        The extracted interval string (e.g., "1m", "1h", "1d")
        Returns "1m" as default if interval cannot be extracted

    Examples:
        >>> extract_interval_from_filename("BTCUSDT-1h-2024-01-01.zip")
        "1h"
        >>> extract_interval_from_filename("ETHUSDT-1d-2024-01-01.zip")
        "1d"
        >>> extract_interval_from_filename("/path/to/BTCUSDT-5m-2024-01-01.zip")
        "5m"
        >>> extract_interval_from_filename("invalid.zip")
        "1m"
    """
    # Get just the filename from path
    name = Path(filename).stem

    # Split by "-" and extract the interval part (second element)
    parts = name.split("-")
    if len(parts) >= 2:
        return parts[1]

    # Default to 1m if we can't extract interval
    return "1m"


class KlineParser:
    """Parser for Binance kline data from zip files."""

    def parse_file(
        self, zip_path: str, symbol: str, interval: Optional[str] = None
    ) -> Iterator[KlineRecord]:
        """Parse a single zip file containing kline CSV data.

        Binance CSV format has 12 columns:
        0: Open time
        1: Open price
        2: High price
        3: Low price
        4: Close price
        5: Volume
        6: Close time
        7: Quote asset volume
        8: Number of trades
        9: Taker buy base asset volume
        10: Taker buy quote asset volume
        11: Ignore

        Args:
            zip_path: Path to the zip file
            symbol: Trading symbol (e.g., "BTCUSDT")
            interval: Optional interval override. If not provided, interval will be
                extracted from the filename (e.g., "BTCUSDT-1h-2024-01-01.zip" -> "1h")

        Yields:
            KlineRecord objects

        Raises:
            FileNotFoundError: If zip file does not exist
            ValueError: If zip file is corrupted or invalid
        """
        path = Path(zip_path)
        if not path.exists():
            raise FileNotFoundError(f"Zip file not found: {zip_path}")

        # Extract interval from filename if not provided
        if interval is None:
            interval = extract_interval_from_filename(zip_path)

        try:
            with zipfile.ZipFile(zip_path, mode="r") as zf:
                # Get the first CSV file in the archive
                csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
                if not csv_files:
                    return

                # Read and parse the CSV data
                with zf.open(csv_files[0]) as csv_file:
                    # Stream rows directly from the compressed file to avoid loading
                    # the whole CSV into memory.
                    text_stream = TextIOWrapper(csv_file, encoding="utf-8", newline="")
                    csv_reader = csv.reader(text_stream)

                    for row in csv_reader:
                        if not row or len(row) < 12:
                            continue
                        if row[0] == "open_time":
                            continue

                        try:
                            yield KlineRecord(
                                symbol=symbol,
                                open_time=int(row[0]),
                                close_time=int(row[6]),
                                open_price=float(row[1]),
                                high_price=float(row[2]),
                                low_price=float(row[3]),
                                close_price=float(row[4]),
                                volume=float(row[5]),
                                quote_volume=float(row[7]),
                                trades_count=int(row[8]),
                                taker_buy_volume=float(row[9]),
                                taker_buy_quote_volume=float(row[10]),
                                interval=interval,
                            )
                        except (ValueError, IndexError) as e:
                            raise ValueError(f"Invalid CSV format in row {row}: {e}") from e

        except zipfile.BadZipFile as e:
            raise ValueError(f"Invalid zip file: {zip_path}") from e
        except Exception as e:
            if isinstance(e, (FileNotFoundError, ValueError)):
                raise
            raise ValueError(f"Error parsing zip file: {e}") from e

    def parse_directory(
        self,
        dir_path: str,
        symbols: Optional[List[str]] = None,
        intervals: Optional[List[str]] = None,
        pattern: str = "**/*.zip"
    ) -> Iterator[Tuple[str, KlineRecord]]:
        """Parse all zip files in a directory.

        Args:
            dir_path: Path to the directory containing zip files
            symbols: Optional list of symbols to filter. If None, parse all files.
            intervals: Optional list of intervals to filter (e.g., ["1m", "1h"]).
                If None, parse all files.
            pattern: Glob pattern for matching files (default: "**/*.zip")

        Yields:
            Tuples of (symbol, KlineRecord)

        Raises:
            FileNotFoundError: If directory does not exist
        """
        dir_p = Path(dir_path)
        if not dir_p.exists():
            raise FileNotFoundError(f"Directory not found: {dir_path}")

        if not dir_p.is_dir():
            raise ValueError(f"Path is not a directory: {dir_path}")

        # Find all zip files matching the pattern
        zip_files = sorted(dir_p.glob(pattern))
        symbol_filter = set(symbols) if symbols is not None else None
        interval_filter = set(intervals) if intervals is not None else None

        for zip_path in zip_files:
            # Extract symbol from filename if not provided
            # Filename format: BTCUSDT-1m-2024-01-01.zip
            file_symbol = zip_path.stem.partition("-")[0]

            # Skip if symbols filter is provided and file doesn't match
            if symbol_filter is not None:
                if file_symbol not in symbol_filter:
                    continue
                symbol = file_symbol
            else:
                symbol = file_symbol if file_symbol else "UNKNOWN"

            # Extract interval from filename for filtering
            file_interval = extract_interval_from_filename(zip_path)

            # Skip if intervals filter is provided and file doesn't match
            if interval_filter is not None:
                if file_interval not in interval_filter:
                    continue

            # Parse the zip file and yield records
            try:
                for record in self.parse_file(str(zip_path), symbol):
                    yield (symbol, record)
            except (FileNotFoundError, ValueError):
                # Skip files that can't be parsed
                continue
