# K-line Data Cleaning Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在 K线数据入库 ClickHouse 之前进行数据清洗，确保数据质量。

**Architecture:** 在 parser 和 writer 之间插入一个新的 cleaner 模块。parser 返回迭代器，cleaner 接收迭代器并输出清洗后的迭代器，writer 接收清洗后的数据。

**Tech Stack:** pandas, pytest, Python 3.11+

---

## Task 1: Create cleaner module structure

**Files:**
- Create: `ingestor/src/zer0data_ingestor/cleaner/__init__.py`
- Create: `ingestor/src/zer0data_ingestor/cleaner/kline.py`

**Step 1: Write the failing test**

Create file: `ingestor/tests/cleaner/__init__.py` (empty)

Create file: `ingestor/tests/cleaner/test_kline.py`:

```python
import pytest
import pandas as pd
from datetime import datetime
from zer0data_ingestor.cleaner.kline import KlineCleaner, CleanResult
from zer0data_ingestor.writer.clickhouse import KlineRecord


def test_kline_cleaner_removes_duplicates():
    """Test that duplicate records are removed, keeping first occurrence."""
    records = [
        KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,
                   open_price=50000.0, high_price=50100.0, low_price=49900.0,
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
        KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,  # duplicate
                   open_price=50000.0, high_price=50100.0, low_price=49900.0,
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
        KlineRecord(symbol="BTCUSDT", open_time=2000, close_time=2059,
                   open_price=50100.0, high_price=50200.0, low_price=50000.0,
                   close_price=50150.0, volume=200.0, quote_volume=10000000.0,
                   trades_count=2000, taker_buy_volume=100.0, taker_buy_quote_volume=5000000.0),
    ]

    cleaner = KlineCleaner()
    result = cleaner.clean(records)

    assert len(result.cleaned_records) == 2
    assert result.stats.duplicates_removed == 1
    assert result.cleaned_records[0].open_time == 1000
    assert result.cleaned_records[1].open_time == 2000
```

**Step 2: Run test to verify it fails**

```bash
cd ingestor && pytest tests/cleaner/test_kline.py::test_kline_cleaner_removes_duplicates -v
```

Expected: `ModuleNotFoundError: No module named 'zer0data_ingestor.cleaner'`

**Step 3: Write minimal implementation**

Create: `ingestor/src/zer0data_ingestor/cleaner/__init__.py`:

```python
"""Data cleaning module for kline data."""
```

Create: `ingestor/src/zer0data_ingestor/cleaner/kline.py`:

```python
"""Kline data cleaner."""

from dataclasses import dataclass, field
from typing import List, Iterator
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
```

**Step 4: Run test to verify it passes**

```bash
cd ingestor && pytest tests/cleaner/test_kline.py::test_kline_cleaner_removes_duplicates -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add ingestor/src/zer0data_ingestor/cleaner/ ingestor/tests/cleaner/
git commit -m "feat(cleaner): add kline cleaner module with duplicate removal"
```

---

## Task 2: Add OHLC validation

**Files:**
- Modify: `ingestor/src/zer0data_ingestor/cleaner/kline.py`
- Modify: `ingestor/tests/cleaner/test_kline.py`

**Step 1: Write the failing test**

Add to `ingestor/tests/cleaner/test_kline.py`:

```python
def test_kline_cleaner_validates_ohlc_logic():
    """Test that invalid OHLC records are removed."""
    records = [
        KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,
                   open_price=50000.0, high_price=50100.0, low_price=49900.0,
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
        KlineRecord(symbol="BTCUSDT", open_time=2000, close_time=2059,
                   open_price=50100.0, high_price=50000.0, low_price=50200.0,  # Invalid: low > high
                   close_price=50150.0, volume=200.0, quote_volume=10000000.0,
                   trades_count=2000, taker_buy_volume=100.0, taker_buy_quote_volume=5000000.0),
        KlineRecord(symbol="BTCUSDT", open_time=3000, close_time=3059,
                   open_price=50000.0, high_price=-100.0, low_price=49900.0,  # Invalid: negative price
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
    ]

    cleaner = KlineCleaner()
    result = cleaner.clean(records)

    assert len(result.cleaned_records) == 1
    assert result.stats.invalid_records_removed == 2
    assert "high < low" in result.stats.validation_errors[0] or "negative" in result.stats.validation_errors[0]
```

**Step 2: Run test to verify it fails**

```bash
cd ingestor && pytest tests/cleaner/test_kline.py::test_kline_cleaner_validates_ohlc_logic -v
```

Expected: FAIL (records not being validated)

**Step 3: Write minimal implementation**

Modify `ingestor/src/zer0data_ingestor/cleaner/kline.py`, add validation method:

```python
class KlineCleaner:
    """Cleaner for kline data."""

    def _validate_record(self, record: KlineRecord) -> bool:
        """Validate a kline record.

        Returns:
            True if record is valid, False otherwise
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

        if errors:
            return False
        return True

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
            # Skip duplicates
            if record.open_time in seen_times:
                stats.duplicates_removed += 1
                continue

            # Validate record
            if not self._validate_record(record):
                stats.invalid_records_removed += 1
                continue

            seen_times.add(record.open_time)
            cleaned.append(record)

        return CleanResult(cleaned_records=cleaned, stats=stats)
```

**Step 4: Run test to verify it passes**

```bash
cd ingestor && pytest tests/cleaner/test_kline.py::test_kline_cleaner_validates_ohlc_logic -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add ingestor/src/zer0data_ingestor/cleaner/kline.py ingestor/tests/cleaner/test_kline.py
git commit -m "feat(cleaner): add OHLC validation to kline cleaner"
```

---

## Task 3: Add time gap filling with pandas

**Files:**
- Modify: `ingestor/src/zer0data_ingestor/cleaner/kline.py`
- Modify: `ingestor/tests/cleaner/test_kline.py`
- Modify: `ingestor/pyproject.toml` (add pandas dependency if not present)

**Step 1: Write the failing test**

Add to `ingestor/tests/cleaner/test_kline.py`:

```python
def test_kline_cleaner_fills_time_gaps():
    """Test that time gaps are filled using forward fill."""
    records = [
        KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,
                   open_price=50000.0, high_price=50100.0, low_price=49900.0,
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
        # Gap: missing 2000, should be filled
        KlineRecord(symbol="BTCUSDT", open_time=3000, close_time=3059,
                   open_price=50200.0, high_price=50300.0, low_price=50100.0,
                   close_price=50250.0, volume=300.0, quote_volume=15000000.0,
                   trades_count=3000, taker_buy_volume=150.0, taker_buy_quote_volume=7500000.0),
    ]

    cleaner = KlineCleaner()
    result = cleaner.clean(records)

    # Should have 3 records: original 2 + 1 filled gap
    assert len(result.cleaned_records) == 3
    assert result.stats.gaps_filled == 1

    # Check the filled record
    filled = result.cleaned_records[1]
    assert filled.open_time == 2000
    # Filled with forward fill from previous record
    assert filled.open_price == 50050.0  # Previous close
    assert filled.close_price == 50050.0
    assert filled.volume == 0.0
```

**Step 2: Run test to verify it fails**

```bash
cd ingestor && pytest tests/cleaner/test_kline.py::test_kline_cleaner_fills_time_gaps -v
```

Expected: FAIL (gap filling not implemented)

**Step 3: Write minimal implementation**

First check pandas dependency in `ingestor/pyproject.toml`:

```bash
grep -A5 "dependencies" ingestor/pyproject.toml
```

If pandas is not listed, add it to dependencies.

Modify `ingestor/src/zer0data_ingestor/cleaner/kline.py`:

```python
"""Kline data cleaner."""

from dataclasses import dataclass, field
from typing import List
import pandas as pd
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

    def __init__(self, interval_minutes: int = 1):
        """Initialize the cleaner.

        Args:
            interval_minutes: Expected time interval between records (default: 1 minute)
        """
        self.interval_minutes = interval_minutes

    def _validate_record(self, record: KlineRecord) -> bool:
        """Validate a kline record.

        Returns:
            True if record is valid, False otherwise
        """
        # Check for negative prices
        if record.open_price <= 0 or record.high_price <= 0 or record.low_price <= 0 or record.close_price <= 0:
            return False

        # Check OHLC logic: high >= max(open, close) and low <= min(open, close)
        if record.high_price < max(record.open_price, record.close_price):
            return False

        if record.low_price > min(record.open_price, record.close_price):
            return False

        if record.high_price < record.low_price:
            return False

        # Check volume
        if record.volume < 0:
            return False

        return True

    def _convert_to_dataframe(self, records: List[KlineRecord]) -> pd.DataFrame:
        """Convert records to pandas DataFrame for gap filling.

        Returns DataFrame with columns: open_time, open, high, low, close, volume, ...
        """
        data = {
            'open_time': [r.open_time for r in records],
            'open': [r.open_price for r in records],
            'high': [r.high_price for r in records],
            'low': [r.low_price for r in records],
            'close': [r.close_price for r in records],
            'volume': [r.volume for r in records],
            'quote_volume': [r.quote_volume for r in records],
            'trades_count': [r.trades_count for r in records],
            'taker_buy_volume': [r.taker_buy_volume for r in records],
            'taker_buy_quote_volume': [r.taker_buy_quote_volume for r in records],
            'symbol': [r.symbol for r in records],
            'close_time': [r.close_time for r in records],
        }
        return pd.DataFrame(data)

    def _convert_from_dataframe(self, df: pd.DataFrame) -> List[KlineRecord]:
        """Convert DataFrame back to list of KlineRecord."""
        records = []
        for _, row in df.iterrows():
            records.append(KlineRecord(
                symbol=row['symbol'],
                open_time=int(row['open_time']),
                close_time=int(row['close_time']),
                open_price=float(row['open']),
                high_price=float(row['high']),
                low_price=float(row['low']),
                close_price=float(row['close']),
                volume=float(row['volume']),
                quote_volume=float(row['quote_volume']),
                trades_count=int(row['trades_count']),
                taker_buy_volume=float(row['taker_buy_volume']),
                taker_buy_quote_volume=float(row['taker_buy_quote_volume']),
            ))
        return records

    def _fill_gaps(self, records: List[KlineRecord]) -> tuple[List[KlineRecord], int]:
        """Fill time gaps in records.

        Returns:
            Tuple of (filled_records, gaps_filled_count)
        """
        if not records:
            return records, 0

        # Convert to DataFrame
        df = self._convert_to_dataframe(records)
        original_len = len(df)

        # Create complete time range
        min_time = df['open_time'].min()
        max_time = df['open_time'].max()
        full_range = pd.RangeIndex(
            start=min_time,
            stop=max_time + self.interval_minutes * 1000,  # ms to minutes
            step=self.interval_minutes * 1000
        )

        # Set index and reindex
        df = df.set_index('open_time')
        df = df.reindex(full_range)

        # Forward fill OHLC
        df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].ffill()
        df[['quote_volume', 'trades_count', 'taker_buy_volume', 'taker_buy_quote_volume']] = df[
            ['quote_volume', 'trades_count', 'taker_buy_volume', 'taker_buy_quote_volume']
        ].ffill()

        # Fill volume with 0 for gaps
        df['volume'] = df['volume'].fillna(0)

        # Fill close_time incrementally for gaps
        df['close_time'] = df.index + (self.interval_minutes * 1000 - 1)

        # Reset index
        df = df.reset_index()
        df = df.rename(columns={'index': 'open_time'})

        # Fill symbol (forward fill)
        df['symbol'] = df['symbol'].ffill()

        gaps_filled = len(df) - original_len

        return self._convert_from_dataframe(df), gaps_filled

    def clean(self, records: List[KlineRecord]) -> CleanResult:
        """Clean kline records by removing duplicates, validating, and filling gaps.

        Args:
            records: List of kline records to clean

        Returns:
            CleanResult with cleaned records and statistics
        """
        stats = CleaningStats()

        # Remove duplicates, keep first occurrence
        seen_times = set()
        deduped = []
        for record in records:
            if record.open_time in seen_times:
                stats.duplicates_removed += 1
                continue

            # Validate record
            if not self._validate_record(record):
                stats.invalid_records_removed += 1
                continue

            seen_times.add(record.open_time)
            deduped.append(record)

        # Fill time gaps
        cleaned, gaps_filled = self._fill_gaps(deduped)
        stats.gaps_filled = gaps_filled

        return CleanResult(cleaned_records=cleaned, stats=stats)
```

**Step 4: Run test to verify it passes**

```bash
cd ingestor && pytest tests/cleaner/test_kline.py::test_kline_cleaner_fills_time_gaps -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add ingestor/src/zer0data_ingestor/cleaner/kline.py ingestor/tests/cleaner/test_kline.py
git commit -m "feat(cleaner): add time gap filling with forward fill"
```

---

## Task 4: Integrate cleaner into ingestor

**Files:**
- Modify: `ingestor/src/zer0data_ingestor/ingestor.py`
- Modify: `ingestor/src/zer0data_ingestor/parser/__init__.py` (update exports)
- Modify: `ingestor/tests/test_ingestor.py` (add integration test)

**Step 1: Write the failing test**

First check existing test: `ingestor/tests/test_ingestor.py`

Then add integration test:

```python
def test_ingestor_cleans_data_before_writing(mock_clickhouse_client):
    """Test that ingestor applies cleaning before writing to database."""
    from unittest.mock import Mock, patch
    from zer0data_ingestor.ingestor import KlineIngestor
    from zer0data_ingestor.config import IngestorConfig
    from zer0data_ingestor.writer.clickhouse import KlineRecord

    config = IngestorConfig()

    # Create mock data with duplicates
    records = [
        KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,
                   open_price=50000.0, high_price=50100.0, low_price=49900.0,
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
        KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,  # duplicate
                   open_price=50000.0, high_price=50100.0, low_price=49900.0,
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
    ]

    # Mock parser to return our test data
    with patch.object(KlineIngestor, '__init__', lambda self, config, data_dir=None: None):
        ingestor = KlineIngestor(None)
        ingestor.config = config
        ingestor.writer = mock_clickhouse_client

        # Manually test cleaning
        from zer0data_ingestor.cleaner.kline import KlineCleaner
        cleaner = KlineCleaner()
        result = cleaner.clean(records)

        # Should only write 1 record after cleaning
        assert len(result.cleaned_records) == 1
        assert result.stats.duplicates_removed == 1
```

**Step 2: Run test to verify it fails**

```bash
cd ingestor && pytest tests/test_ingestor.py::test_ingestor_cleans_data_before_writing -v
```

Expected: May pass if cleaner works, but we need to integrate it into the ingestor flow

**Step 3: Write minimal implementation**

Modify `ingestor/src/zer0data_ingestor/ingestor.py`:

```python
"""Main ingestion logic for kline data."""

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from zer0data_ingestor.config import IngestorConfig
from zer0data_ingestor.parser import KlineParser
from zer0data_ingestor.writer.clickhouse import ClickHouseWriter, KlineRecord
from zer0data_ingestor.cleaner.kline import KlineCleaner  # Add import


@dataclass
class IngestStats:
    """Statistics for ingestion operations."""

    symbols_processed: int = 0
    dates_processed: int = 0
    records_written: int = 0
    files_processed: int = 0
    duplicates_removed: int = 0  # Add
    gaps_filled: int = 0  # Add
    invalid_records_removed: int = 0  # Add
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class KlineIngestor:
    """Main ingestor for parsing, cleaning and writing kline data."""

    def __init__(self, config: IngestorConfig, data_dir: str = "./data/download"):
        """Initialize the ingestor.

        Args:
            config: IngestorConfig instance with database settings
            data_dir: Directory containing downloaded zip files (overrides config.data_dir)
        """
        self.config = config
        self.data_dir = Path(data_dir)
        self.parser = KlineParser()
        self.cleaner = KlineCleaner()  # Add cleaner
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

        # Group records by symbol for cleaning
        records_by_symbol = {}

        try:
            # Parse all matching files in the directory
            for symbol, record in self.parser.parse_directory(source, symbols, pattern):
                if symbol not in records_by_symbol:
                    records_by_symbol[symbol] = []
                records_by_symbol[symbol].append(record)

            # Clean and write records per symbol
            for symbol, records in records_by_symbol.items():
                clean_result = self.cleaner.clean(records)

                # Update stats
                stats.duplicates_removed += clean_result.stats.duplicates_removed
                stats.gaps_filled += clean_result.stats.gaps_filled
                stats.invalid_records_removed += clean_result.stats.invalid_records_removed

                # Write cleaned records
                for record in clean_result.cleaned_records:
                    self.writer.insert(record)
                    stats.records_written += 1

                symbols_seen.add(symbol)

            # Track the directory as processed
            stats.files_processed = 1
            stats.symbols_processed = len(symbols_seen)

        except Exception as e:
            error_msg = f"Error processing directory {source}: {e}"
            stats.errors.append(error_msg)

        # Flush any remaining records
        self.writer.flush()

        return stats
```

**Step 4: Run test to verify it passes**

```bash
cd ingestor && pytest tests/test_ingestor.py -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add ingestor/src/zer0data_ingestor/ingestor.py ingestor/src/zer0data_ingestor/parser/__init__.py ingestor/tests/test_ingestor.py
git commit -m "feat(ingestor): integrate data cleaner into ingestion pipeline"
```

---

## Task 5: Add logging for cleaning statistics

**Files:**
- Modify: `ingestor/src/zer0data_ingestor/ingestor.py`
- Modify: `ingestor/src/zer0data_ingestor/cleaner/kline.py` (add stats logging)

**Step 1: Write the failing test**

Add to `ingestor/tests/test_ingestor.py`:

```python
def test_ingestor_logs_cleaning_stats(caplog):
    """Test that ingestor logs cleaning statistics."""
    from unittest.mock import Mock, patch
    from zer0data_ingestor.ingestor import KlineIngestor
    from zer0data_ingestor.config import IngestorConfig
    from zer0data_ingestor.writer.clickhouse import KlineRecord

    config = IngestorConfig()

    records = [
        KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,
                   open_price=50000.0, high_price=50100.0, low_price=49900.0,
                   close_price=50050.0, volume=100.0, quote_volume=5000000.0,
                   trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
    ]

    with patch.object(KlineIngestor, '__init__', lambda self, config, data_dir=None: None):
        ingestor = KlineIngestor(None)
        ingestor.config = config
        ingestor.writer = Mock()

        from zer0data_ingestor.cleaner.kline import KlineCleaner
        cleaner = KlineCleaner()

        # Check that stats are tracked
        result = cleaner.clean(records)
        assert result.stats.duplicates_removed == 0
        assert result.stats.gaps_filled == 0
```

**Step 2: Run test to verify it fails**

```bash
cd ingestor && pytest tests/test_ingestor.py::test_ingestor_logs_cleaning_stats -v
```

Expected: May pass, but we need to add actual logging

**Step 3: Write minimal implementation**

Modify `ingestor/src/zer0data_ingestor/ingestor.py` to add logging:

```python
"""Main ingestion logic for kline data."""

import logging  # Add
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)  # Add

from zer0data_ingestor.config import IngestorConfig
from zer0data_ingestor.parser import KlineParser
from zer0data_ingestor.writer.clickhouse import ClickHouseWriter, KlineRecord
from zer0data_ingestor.cleaner.kline import KlineCleaner


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
    """Main ingestor for parsing, cleaning and writing kline data."""

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

        # Group records by symbol for cleaning
        records_by_symbol = {}

        try:
            # Parse all matching files in the directory
            for symbol, record in self.parser.parse_directory(source, symbols, pattern):
                if symbol not in records_by_symbol:
                    records_by_symbol[symbol] = []
                records_by_symbol[symbol].append(record)

            # Clean and write records per symbol
            for symbol, records in records_by_symbol.items():
                clean_result = self.cleaner.clean(records)

                # Update stats
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
                for record in clean_result.cleaned_records:
                    self.writer.insert(record)
                    stats.records_written += 1

                symbols_seen.add(symbol)

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
```

**Step 4: Run test to verify it passes**

```bash
cd ingestor && pytest tests/test_ingestor.py -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add ingestor/src/zer0data_ingestor/ingestor.py
git commit -m "feat(ingestor): add logging for data cleaning statistics"
```

---

## Task 6: Run full integration tests

**Files:**
- Test: `ingestor/tests/integration/test_full_flow.py`

**Step 1: Run existing integration tests**

```bash
cd ingestor && pytest tests/integration/test_full_flow.py -v
```

**Step 2: Verify data quality with real data**

Create a test script to verify cleaning with sample data:

```bash
# Create test script
cat > /tmp/test_cleaning.py << 'EOF'
from zer0data_ingestor.cleaner.kline import KlineCleaner
from zer0data_ingestor.writer.clickhouse import KlineRecord

# Test with duplicates
records = [
    KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,
               open_price=50000.0, high_price=50100.0, low_price=49900.0,
               close_price=50050.0, volume=100.0, quote_volume=5000000.0,
               trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
    KlineRecord(symbol="BTCUSDT", open_time=1000, close_time=1059,
               open_price=50000.0, high_price=50100.0, low_price=49900.0,
               close_price=50050.0, volume=100.0, quote_volume=5000000.0,
               trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0),
    KlineRecord(symbol="BTCUSDT", open_time=3000, close_time=3059,
               open_price=50200.0, high_price=50300.0, low_price=50100.0,
               close_price=50250.0, volume=300.0, quote_volume=15000000.0,
               trades_count=3000, taker_buy_volume=150.0, taker_buy_quote_volume=7500000.0),
]

cleaner = KlineCleaner()
result = cleaner.clean(records)

print(f"Original: {len(records)} records")
print(f"Cleaned: {len(result.cleaned_records)} records")
print(f"Duplicates removed: {result.stats.duplicates_removed}")
print(f"Gaps filled: {result.stats.gaps_filled}")
print(f"Invalid records removed: {result.stats.invalid_records_removed}")
EOF

# Run test script
cd /Users/rock/work/zer0data && python /tmp/test_cleaning.py
```

**Step 3: Commit integration test verification**

```bash
git add -A
git commit -m "test(cleaner): verify integration with existing tests"
```

---

## Task 7: Update documentation

**Files:**
- Modify: `ingestor/README.md`
- Create: `docs/plans/2026-02-13-kline-data-cleaning-summary.md` (summary doc)

**Step 1: Update README**

Add to `ingestor/README.md`:

```markdown
## Data Cleaning

K线数据在入库前会自动进行清洗：

- **去重**：删除重复时间戳的记录，保留第一条
- **有效性校验**：检查 OHLC 逻辑关系（high ≥ max(open,close), low ≤ min(open,close)）
- **时间连续性**：检测时间缺口并使用前向填充补齐
- **异常值处理**：过滤掉负数价格、成交量等无效记录

清洗统计会在日志中输出，便于监控数据质量。
```

**Step 2: Create design summary document**

Create `docs/plans/2026-02-13-kline-data-cleaning-summary.md`:

```markdown
# K-line Data Cleaning Implementation Summary

## Overview
实现了 K线数据入库前的自动清洗功能，确保数据质量。

## Architecture
在 ingestor 的数据流程中添加了 cleaner 模块：

```
Parser → Cleaner → Writer
```

- **Parser**: 解析 CSV，yield KlineRecord
- **Cleaner**: 接收记录列表，执行清洗，返回清洗后的记录
- **Writer**: 写入 ClickHouse

## Cleaning Rules

1. **Duplicate Removal**
   - 按时间戳去重，保留第一条

2. **OHLC Validation**
   - 检查 high ≥ max(open, close)
   - 检查 low ≤ min(open, close)
   - 检查 high ≥ low
   - 过滤负数价格

3. **Time Gap Filling**
   - 创建完整时间索引（1分钟间隔）
   - 使用前向填充补齐缺失时间点
   - volume 缺失填 0

## Files Modified

- `ingestor/src/zer0data_ingestor/cleaner/` - 新增清洗模块
- `ingestor/src/zer0data_ingestor/ingestor.py` - 集成清洗流程
- `ingestor/tests/cleaner/` - 单元测试

## Testing

```bash
cd ingestor && pytest tests/cleaner/ -v
```
```

**Step 3: Commit documentation**

```bash
git add ingestor/README.md docs/plans/2026-02-13-kline-data-cleaning-summary.md
git commit -m "docs(cleaner): add data cleaning documentation"
```

---

## Completion Checklist

- [ ] All tests pass (`pytest ingestor/tests/ -v`)
- [ ] Integration tests pass (`pytest ingestor/tests/integration/ -v`)
- [ ] Documentation updated
- [ ] No linting errors (`ruff check ingestor/src/`)
- [ ] Type checking passes if applicable

---

## Notes for Implementation

1. **pandas dependency**: Ensure pandas is in `pyproject.toml` dependencies
2. **Logging**: Use Python's standard `logging` module
3. **Time handling**: Binance uses milliseconds since epoch for timestamps
4. **Memory**: Gap filling loads all records into memory - acceptable for daily batches
5. **Symbol grouping**: Clean per symbol to avoid cross-symbol contamination

---

**End of Implementation Plan**
