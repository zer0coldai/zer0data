"""Kline data parser for Binance zip files."""

import csv
import zipfile
from io import TextIOWrapper
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

from zer0data_ingestor.writer.clickhouse import KlineRecord


class KlineParser:
    """Parser for Binance kline data from zip files."""

    def parse_file(self, zip_path: str, symbol: str) -> Iterator[KlineRecord]:
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

        Yields:
            KlineRecord objects

        Raises:
            FileNotFoundError: If zip file does not exist
            ValueError: If zip file is corrupted or invalid
        """
        path = Path(zip_path)
        if not path.exists():
            raise FileNotFoundError(f"Zip file not found: {zip_path}")

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
        pattern: str = "**/*.zip"
    ) -> Iterator[Tuple[str, KlineRecord]]:
        """Parse all zip files in a directory.

        Args:
            dir_path: Path to the directory containing zip files
            symbols: Optional list of symbols to filter. If None, parse all files.
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

        for zip_path in zip_files:
            # Extract symbol from filename if not provided
            # Filename format: BTCUSDT-1m-2024-01-01.zip
            filename = zip_path.stem  # Remove .zip extension
            parts = filename.split("-")
            file_symbol = parts[0] if parts else None

            # Skip if symbols filter is provided and file doesn't match
            if symbols is not None:
                if file_symbol not in symbols:
                    continue
                symbol = file_symbol
            else:
                symbol = file_symbol if file_symbol else "UNKNOWN"

            # Parse the zip file and yield records
            try:
                for record in self.parse_file(str(zip_path), symbol):
                    yield (symbol, record)
            except (FileNotFoundError, ValueError):
                # Skip files that can't be parsed
                continue
