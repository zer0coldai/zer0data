# Multi-Interval K-Line Data Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在现有1分钟K线数据基础上，扩展支持12个K线周期（1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d）

**Architecture:** 每个周期独立表存储，从公共数据项目直接下载各周期原始数据，按周期串行处理入库。Parser从文件名提取interval信息，Writer根据interval动态选择目标表。

**Tech Stack:** Python 3.11+, ClickHouse, pytest, Poetry, click

---

## Task 1: 添加 interval 常量定义

**Files:**
- Create: `ingestor/src/zer0data_ingestor/constants.py`
- Modify: `ingestor/src/zer0data_ingestor/__init__.py`
- Test: `ingestor/tests/test_constants.py`

**Step 1: Write the failing test**

Create `ingestor/tests/test_constants.py`:

```python
"""Tests for interval constants."""

from zer0data_ingestor.constants import VALID_INTERVALS, Interval


def test_valid_intervals_contains_all_periods():
    """Test that VALID_INTERVALS contains all 12 expected periods."""
    expected = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]
    assert VALID_INTERVALS == expected


def test_interval_constants():
    """Test Interval class has all period constants."""
    assert Interval.M1 == "1m"
    assert Interval.M3 == "3m"
    assert Interval.M5 == "5m"
    assert Interval.M15 == "15m"
    assert Interval.M30 == "30m"
    assert Interval.H1 == "1h"
    assert Interval.H2 == "2h"
    assert Interval.H4 == "4h"
    assert Interval.H6 == "6h"
    assert Interval.H8 == "8h"
    assert Interval.H12 == "12h"
    assert Interval.D1 == "1d"


def test_is_valid_interval():
    """Test interval validation function."""
    from zer0data_ingestor.constants import is_valid_interval

    assert is_valid_interval("1m") is True
    assert is_valid_interval("1h") is True
    assert is_valid_interval("1d") is True
    assert is_valid_interval("2d") is False
    assert is_valid_interval("invalid") is False
```

**Step 2: Run test to verify it fails**

```bash
cd ingestor && pytest tests/test_constants.py -v
```

Expected: `ModuleNotFoundError: No module named 'zer0data_ingestor.constants'`

**Step 3: Write minimal implementation**

Create `ingestor/src/zer0data_ingestor/constants.py`:

```python
"""Constants for kline intervals."""

from typing import List

# All supported kline intervals
VALID_INTERVALS: List[str] = [
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d"
]


class Interval:
    """Interval constants for type-safe usage."""

    M1 = "1m"
    M3 = "3m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H2 = "2h"
    H4 = "4h"
    H6 = "6h"
    H8 = "8h"
    H12 = "12h"
    D1 = "1d"


def is_valid_interval(interval: str) -> bool:
    """Check if interval is valid.

    Args:
        interval: Interval string (e.g., "1m", "1h", "1d")

    Returns:
        True if interval is valid, False otherwise
    """
    return interval in VALID_INTERVALS
```

**Step 4: Update __init__.py**

Modify `ingestor/src/zer0data_ingestor/__init__.py`:

```python
"""Zer0data ingestor for kline data."""

from zer0data_ingestor.constants import VALID_INTERVALS, Interval, is_valid_interval

__all__ = ["VALID_INTERVALS", "Interval", "is_valid_interval"]
```

**Step 5: Run test to verify it passes**

```bash
cd ingestor && pytest tests/test_constants.py -v
```

Expected: PASS

**Step 6: Commit**

```bash
git add ingestor/src/zer0data_ingestor/constants.py
git add ingestor/src/zer0data_ingestor/__init__.py
git add ingestor/tests/test_constants.py
git commit -m "feat(intervals): add interval constants and validation"
```

---

## Task 2: KlineRecord 添加 interval 字段

**Files:**
- Modify: `ingestor/src/zer0data_ingestor/writer/clickhouse.py`
- Test: `ingestor/tests/writer/test_clickhouse.py`

**Step 1: Write the failing test**

Add to `ingestor/tests/writer/test_clickhouse.py`:

```python
def test_kline_record_has_interval_field():
    """Test that KlineRecord has interval field."""
    from zer0data_ingestor.writer.clickhouse import KlineRecord

    record = KlineRecord(
        symbol="BTCUSDT",
        interval="1h",
        open_time=1704067200000,
        close_time=1704070799999,
        open_price=42000.0,
        high_price=42100.0,
        low_price=41900.0,
        close_price=42050.0,
        volume=1000.5,
        quote_volume=42050000.0,
        trades_count=1500,
        taker_buy_volume=500.25,
        taker_buy_quote_volume=21000000.0,
    )

    assert record.interval == "1h"
    assert record.symbol == "BTCUSDT"


def test_kline_record_defaults_to_1m_interval():
    """Test that KlineRecord defaults to 1m interval."""
    from zer0data_ingestor.writer.clickhouse import KlineRecord

    record = KlineRecord(
        symbol="BTCUSDT",
        open_time=1704067200000,
        close_time=1704067259999,
        open_price=42000.0,
        high_price=42100.0,
        low_price=41900.0,
        close_price=42050.0,
        volume=1000.5,
        quote_volume=42050000.0,
        trades_count=1500,
        taker_buy_volume=500.25,
        taker_buy_quote_volume=21000000.0,
    )

    assert record.interval == "1m"
```

**Step 2: Run test to verify it fails**

```bash
cd ingestor && pytest tests/writer/test_clickhouse.py::test_kline_record_has_interval_field -v
```

Expected: FAIL - `TypeError: KlineRecord() got an unexpected keyword argument 'interval'`

**Step 3: Modify KlineRecord**

Modify `ingestor/src/zer0data_ingestor/writer/clickhouse.py`:

```python
@dataclass
class KlineRecord:
    """Kline data record."""

    symbol: str
    interval: str = "1m"  # Add interval field with default
    open_time: int
    close_time: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    quote_volume: float
    trades_count: int
    taker_buy_volume: float
    taker_buy_quote_volume: float
```

**Step 4: Run test to verify it passes**

```bash
cd ingestor && pytest tests/writer/test_clickhouse.py::test_kline_record_has_interval_field tests/writer/test_clickhouse.py::test_kline_record_defaults_to_1m_interval -v
```

Expected: PASS

**Step 5: Update existing tests**

Update existing KlineRecord instantiations in tests to include interval parameter:

```bash
cd ingestor && grep -r "KlineRecord(" tests/ -l
```

For each file, add `interval="1m"` parameter to KlineRecord calls (or use default).

**Step 6: Run all tests to verify**

```bash
cd ingestor && pytest tests/ -v
```

Expected: All tests pass

**Step 7: Commit**

```bash
git add ingestor/src/zer0data_ingestor/writer/clickhouse.py
git add ingestor/tests/
git commit -m "feat(record): add interval field to KlineRecord"
```

---

## Task 3: Parser 从文件名提取 interval

**Files:**
- Modify: `ingestor/src/zer0data_ingestor/parser/zip_parser.py`
- Test: `ingestor/tests/parser/test_zip_parser.py`

**Step 1: Write the failing test**

Add to `ingestor/tests/parser/test_zip_parser.py`:

```python
def test_extract_interval_from_filename():
    """Test extracting interval from filename."""
    from zer0data_ingestor.parser.zip_parser import extract_interval_from_filename
    from pathlib import Path

    # Standard format: BTCUSDT-1h-2024-01-01.zip
    assert extract_interval_from_filename(Path("BTCUSDT-1h-2024-01-01.zip")) == "1h"
    assert extract_interval_from_filename(Path("ETHUSDT-5m-2024-01-01.zip")) == "5m"
    assert extract_interval_from_filename(Path("BTCUSDT-1d-2024-01-01.zip")) == "1d"

    # Edge cases
    assert extract_interval_from_filename(Path("invalid.zip")) is None
    assert extract_interval_from_filename(Path("BTCUSDT-2024-01-01.zip")) is None


def test_parse_file_includes_interval():
    """Test that parse_file sets interval from filename."""
    import tempfile
    import zipfile
    from pathlib import Path
    from zer0data_ingestor.parser.zip_parser import KlineParser

    csv_data = b"1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0\n"

    with tempfile.NamedTemporaryFile(suffix="-1h-2024-01-01.zip", delete=False, dir="/tmp") as f:
        zip_path = Path(f.name)
        with zipfile.ZipFile(f, 'w') as zf:
            zf.writestr("BTCUSDT-1h-2024-01-01.csv", csv_data)

    try:
        parser = KlineParser()
        records = list(parser.parse_file(str(zip_path), "BTCUSDT"))

        assert len(records) == 1
        assert records[0].interval == "1h"
    finally:
        zip_path.unlink()
```

**Step 2: Run test to verify it fails**

```bash
cd ingestor && pytest tests/parser/test_zip_parser.py::test_extract_interval_from_filename -v
```

Expected: FAIL - `function not defined`

**Step 3: Implement extract_interval_from_filename**

Modify `ingestor/src/zer0data_ingestor/parser/zip_parser.py`:

```python
"""Kline data parser for exchange zip files."""

from pathlib import Path
from typing import Iterator, List, Optional

from zer0data_ingestor.writer.clickhouse import KlineRecord


def extract_interval_from_filename(filepath: Path) -> Optional[str]:
    """Extract interval from filename.

    Args:
        filepath: Path to zip file (e.g., BTCUSDT-1h-2024-01-01.zip)

    Returns:
        Interval string (e.g., "1h", "5m") or None if not found
    """
    parts = filepath.stem.split('-')
    if len(parts) >= 3:
        return parts[1]
    return None


class KlineParser:
    """Parser for exchange kline data from zip files."""

    def parse_file(self, zip_path: str, symbol: str) -> Iterator[KlineRecord]:
        """Parse a single zip file and yield kline records.

        Args:
            zip_path: Path to the zip file
            symbol: Trading symbol (e.g., "BTCUSDT")

        Yields:
            KlineRecord objects
        """
        if not Path(zip_path).exists():
            raise FileNotFoundError(f"Zip file not found: {zip_path}")

        # Extract interval from filename
        zip_path_obj = Path(zip_path)
        interval = extract_interval_from_filename(zip_path_obj)
        if not interval:
            interval = "1m"  # Default to 1m if not found

        try:
            import zipfile
            with zipfile.ZipFile(zip_path, 'r') as zf:
                csv_files = [n for n in zf.namelist() if n.endswith('.csv')]
                if not csv_files:
                    return

                with zf.open(csv_files[0]) as csv_file:
                    content = csv_file.read().decode('utf-8')
                    for line in content.strip().split('\n'):
                        if not line:
                            continue
                        parts = line.split(',')
                        if len(parts) < 12:
                            continue

                        try:
                            yield KlineRecord(
                                symbol=symbol,
                                interval=interval,  # Add interval
                                open_time=int(parts[0]),
                                close_time=int(parts[6]),
                                open_price=float(parts[1]),
                                high_price=float(parts[2]),
                                low_price=float(parts[3]),
                                close_price=float(parts[4]),
                                volume=float(parts[5]),
                                quote_volume=float(parts[7]),
                                trades_count=int(parts[8]),
                                taker_buy_volume=float(parts[9]),
                                taker_buy_quote_volume=float(parts[10]),
                            )
                        except (ValueError, IndexError):
                            continue

        except zipfile.BadZipFile as e:
            raise ValueError(f"Invalid zip file: {e}") from e

    def parse_directory(
        self,
        dir_path: str,
        symbols: List[str] = None,
        intervals: List[str] = None,  # Add intervals filter
        pattern: str = "*.zip"
    ) -> Iterator[tuple[str, str, KlineRecord]]:  # Return (symbol, interval, record)
        """Parse all matching zip files in a directory.

        Args:
            dir_path: Path to the directory containing zip files
            symbols: Optional list of symbols to filter. If None, parses all.
            intervals: Optional list of intervals to filter. If None, parses all.
            pattern: Glob pattern for matching files (default: "*.zip")

        Yields:
            Tuples of (symbol, interval, KlineRecord)
        """
        from glob import glob

        dir_path_obj = Path(dir_path)
        if not dir_path_obj.exists():
            raise FileNotFoundError(f"Directory not found: {dir_path}")

        for zip_file in glob(str(dir_path_obj / pattern)):
            filename = Path(zip_file).stem
            parts = filename.split('-')
            if len(parts) >= 3:
                symbol = parts[0]
                interval = parts[1]

                # Filter by symbols
                if symbols and symbol not in symbols:
                    continue

                # Filter by intervals
                if intervals and interval not in intervals:
                    continue

                for record in self.parse_file(zip_file, symbol):
                    yield (symbol, interval, record)
```

**Step 4: Run test to verify it passes**

```bash
cd ingestor && pytest tests/parser/test_zip_parser.py::test_extract_interval_from_filename tests/parser/test_zip_parser.py::test_parse_file_includes_interval -v
```

Expected: PASS

**Step 5: Update parse_directory return type**

Modify `ingestor/src/zer0data_ingestor/parser/__init__.py`:

```python
"""Parser module for kline data."""

from zer0data_ingestor.parser.kline import parse_klines_csv
from zer0data_ingestor.parser.zip_parser import KlineParser, extract_interval_from_filename

__all__ = ["parse_klines_csv", "KlineParser", "extract_interval_from_filename"]
```

**Step 6: Update dependent tests**

Update tests that use `parse_directory` to handle new return type `(symbol, interval, record)`:

```bash
cd ingestor && grep -r "parse_directory" tests/ -l
```

Update `tests/parser/test_zip_parser.py::test_parse_directory_with_symbols_filter`:
```python
def test_parse_directory_with_symbols_filter():
    # ... setup code ...

    for symbol, interval, record in parser.parse_directory(tmp_dir, symbols=["BTCUSDT"]):
        assert symbol == "BTCUSDT"
        assert isinstance(interval, str)
        assert isinstance(record, KlineRecord)
        found = True

    assert found
```

**Step 7: Run all tests to verify**

```bash
cd ingestor && pytest tests/ -v
```

Expected: All tests pass

**Step 8: Commit**

```bash
git add ingestor/src/zer0data_ingestor/parser/
git add ingestor/tests/parser/test_zip_parser.py
git commit -m "feat(parser): extract interval from filename and add intervals filter"
```

---

## Task 4: Writer 支持动态表名

**Files:**
- Modify: `ingestor/src/zer0data_ingestor/writer/clickhouse.py`
- Test: `ingestor/tests/writer/test_clickhouse.py`

**Step 1: Write the failing test**

Add to `ingestor/tests/writer/test_clickhouse.py`:

```python
def test_get_table_name_for_interval():
    """Test _get_table_name returns correct table name."""
    from zer0data_ingestor.writer.clickhouse import ClickHouseWriter

    writer = ClickHouseWriter(
        host="localhost",
        port=8123,
        database="test",
    )

    assert writer._get_table_name("1m") == "klines_1m"
    assert writer._get_table_name("1h") == "klines_1h"
    assert writer._get_table_name("1d") == "klines_1d"


def test_insert_uses_interval_field():
    """Test that insert uses interval from record to select table."""
    from unittest.mock import Mock, patch
    from zer0data_ingestor.writer.clickhouse import ClickHouseWriter, KlineRecord

    with patch.object(ClickHouseWriter, '__init__', lambda self, host, port, database, username=None, password=None: None):
        writer = ClickHouseWriter(None, None, None)
        writer.client = Mock()
        writer.client.command = Mock()

        record = KlineRecord(
            symbol="BTCUSDT",
            interval="1h",
            open_time=1704067200000,
            close_time=1704070799999,
            open_price=42000.0,
            high_price=42100.0,
            low_price=41900.0,
            close_price=42050.0,
            volume=1000.5,
            quote_volume=42050000.0,
            trades_count=1500,
            taker_buy_volume=500.25,
            taker_buy_quote_volume=21000000.0,
        )

        # Track which table was used
        tables_used = []

        def mock_command(cmd, parameters=None):
            if "INSERT" in cmd:
                # Extract table name from INSERT INTO statement
                import re
                match = re.search(r'INTO\s+(\w+)', cmd)
                if match:
                    tables_used.append(match.group(1))

        writer.client.command.side_effect = mock_command
        writer._raw_insert = Mock(side_effect=writer.client.command)

        writer.insert(record)

        assert len(tables_used) == 1
        assert tables_used[0] == "klines_1h"
```

**Step 2: Run test to verify it fails**

```bash
cd ingestor && pytest tests/writer/test_clickhouse.py::test_get_table_name_for_interval -v
```

Expected: FAIL - `method not found`

**Step 3: Implement _get_table_name and update insert**

Modify `ingestor/src/zer0data_ingestor/writer/clickhouse.py`:

```python
class ClickHouseWriter:
    """ClickHouse writer for kline data."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str = None,
        password: str = None,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.username = username or "default"
        self.password = password or ""
        self.table_prefix = "klines_"
        self._batch: List[KlineRecord] = []
        self._batch_size = 10000

    def _get_table_name(self, interval: str) -> str:
        """Get table name for interval.

        Args:
            interval: Interval string (e.g., "1m", "1h", "1d")

        Returns:
            Table name (e.g., "klines_1m", "klines_1h")
        """
        return f"{self.table_prefix}{interval}"

    def insert(self, record: KlineRecord) -> None:
        """Insert a single record.

        Args:
            record: KlineRecord to insert
        """
        self._batch.append(record)
        if len(self._batch) >= self._batch_size:
            self.flush()

    def flush(self) -> None:
        """Flush batched records to ClickHouse."""
        if not self._batch:
            return

        # Group by interval
        by_interval = {}
        for record in self._batch:
            interval = record.interval
            if interval not in by_interval:
                by_interval[interval] = []
            by_interval[interval].append(record)

        # Insert each interval group
        for interval, records in by_interval.items():
            table = self._get_table_name(interval)
            self._raw_insert_batch(table, records)

        self._batch.clear()

    def _raw_insert_batch(self, table: str, records: List[KlineRecord]) -> None:
        """Raw batch insert to specific table.

        Args:
            table: Table name to insert into
            records: List of records to insert
        """
        # Convert records to Native format
        data = []
        for r in records:
            data.append({
                'symbol': r.symbol,
                'open_time': r.open_time,
                'close_time': r.close_time,
                'open_price': r.open_price,
                'high_price': r.high_price,
                'low_price': r.low_price,
                'close_price': r.close_price,
                'volume': r.volume,
                'quote_volume': r.quote_volume,
                'trades_count': r.trades_count,
                'taker_buy_volume': r.taker_buy_volume,
                'taker_buy_quote_volume': r.taker_buy_quote_volume,
            })

        # Import clickhouse connect
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
        )

        client.insert(table, data, column_names=[
            'symbol', 'open_time', 'close_time',
            'open_price', 'high_price', 'low_price', 'close_price',
            'volume', 'quote_volume', 'trades_count',
            'taker_buy_volume', 'taker_buy_quote_volume',
        ])
```

**Step 4: Run test to verify it passes**

```bash
cd ingestor && pytest tests/writer/test_clickhouse.py::test_get_table_name_for_interval tests/writer/test_clickhouse.py::test_insert_uses_interval_field -v
```

Expected: PASS

**Step 5: Run all tests to verify**

```bash
cd ingestor && pytest tests/ -v
```

Expected: All tests pass

**Step 6: Commit**

```bash
git add ingestor/src/zer0data_ingestor/writer/clickhouse.py
git add ingestor/tests/writer/test_clickhouse.py
git commit -m "feat(writer): support dynamic table names based on interval"
```

---

## Task 5: Writer 自动创建表

**Files:**
- Modify: `ingestor/src/zer0data_ingestor/writer/clickhouse.py`
- Test: `ingestor/tests/writer/test_clickhouse.py`

**Step 1: Write the failing test**

Add to `ingestor/tests/writer/test_clickhouse.py`:

```python
def test_ensure_table_exists():
    """Test _ensure_table_exists creates table if not exists."""
    from unittest.mock import Mock, patch, call
    from zer0data_ingestor.writer.clickhouse import ClickHouseWriter
    from zer0data_ingestor.constants import VALID_INTERVALS

    with patch.object(ClickHouseWriter, '__init__', lambda self, host, port, database, username=None, password=None: None):
        writer = ClickHouseWriter(None, None, None)
        writer.client = Mock()

        # Mock _table_exists to return False
        writer._table_exists = Mock(return_value=False)

        # Mock _create_table
        writer._create_table = Mock()

        # Call _ensure_table_exists
        writer._ensure_table_exists("1h")

        # Verify _create_table was called
        writer._create_table.assert_called_once_with("1h")

        # Test with invalid interval
        try:
            writer._ensure_table_exists("2d")
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "Invalid interval" in str(e)


def test_table_exists():
    """Test _table_exists checks if table exists."""
    from unittest.mock import Mock, patch
    from zer0data_ingestor.writer.clickhouse import ClickHouseWriter

    with patch.object(ClickHouseWriter, '__init__', lambda self, host, port, database, username=None, password=None: None):
        writer = ClickHouseWriter(None, None, None)
        writer.client = Mock()

        # Mock EXISTS query to return True
        writer.client.query = Mock(return_value="1")

        exists = writer._table_exists("klines_1h")

        assert exists is True
        writer.client.query.assert_called_once()
```

**Step 2: Run test to verify it fails**

```bash
cd ingestor && pytest tests/writer/test_clickhouse.py::test_ensure_table_exists -v
```

Expected: FAIL - `method not found`

**Step 3: Implement table management methods**

Modify `ingestor/src/zer0data_ingestor/writer/clickhouse.py`:

```python
from zer0data_ingestor.constants import VALID_INTERVALS, is_valid_interval


class ClickHouseWriter:
    """ClickHouse writer for kline data."""

    # ... existing __init__ and other methods ...

    def _ensure_table_exists(self, interval: str) -> None:
        """Ensure table exists for interval, create if not.

        Args:
            interval: Interval string (e.g., "1m", "1h", "1d")

        Raises:
            ValueError: If interval is invalid
        """
        if not is_valid_interval(interval):
            raise ValueError(f"Invalid interval: {interval}")

        table = self._get_table_name(interval)
        if not self._table_exists(table):
            self._create_table(interval)

    def _table_exists(self, table: str) -> bool:
        """Check if table exists.

        Args:
            table: Table name to check

        Returns:
            True if table exists, False otherwise
        """
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
        )

        result = client.query(f"EXISTS TABLE {table}")
        return result == "1"

    def _create_table(self, interval: str) -> None:
        """Create table for interval.

        Args:
            interval: Interval string (e.g., "1m", "1h", "1d")
        """
        import clickhouse_connect

        table = self._get_table_name(interval)

        # Determine partition strategy
        if interval == "1d":
            partition_by = "toYYYY(open_time)"
        else:
            partition_by = "toYYYYMM(open_time)"

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                symbol String,
                open_time DateTime,
                close_time DateTime,
                open_price Decimal64(18, 8),
                high_price Decimal64(18, 8),
                low_price Decimal64(18, 8),
                close_price Decimal64(18, 8),
                volume Decimal64(18, 8),
                quote_volume Decimal64(18, 8),
                trades_count UInt32,
                taker_buy_volume Decimal64(18, 8),
                taker_buy_quote_volume Decimal64(18, 8)
            ) ENGINE = MergeTree()
            PARTITION BY {partition_by}
            ORDER BY (symbol, open_time)
            SETTINGS index_granularity = 8192
        """

        client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
        )

        client.command(create_sql)
```

**Step 4: Update insert to ensure table exists**

Modify `flush` method in `ClickHouseWriter`:

```python
def flush(self) -> None:
    """Flush batched records to ClickHouse."""
    if not self._batch:
        return

    # Group by interval
    by_interval = {}
    for record in self._batch:
        interval = record.interval
        if interval not in by_interval:
            by_interval[interval] = []
        by_interval[interval].append(record)

    # Insert each interval group
    for interval, records in by_interval.items():
        self._ensure_table_exists(interval)  # Add this line
        table = self._get_table_name(interval)
        self._raw_insert_batch(table, records)

    self._batch.clear()
```

**Step 5: Run test to verify it passes**

```bash
cd ingestor && pytest tests/writer/test_clickhouse.py::test_ensure_table_exists tests/writer/test_clickhouse.py::test_table_exists -v
```

Expected: PASS

**Step 6: Run all tests to verify**

```bash
cd ingestor && pytest tests/ -v
```

Expected: All tests pass

**Step 7: Commit**

```bash
git add ingestor/src/zer0data_ingestor/writer/clickhouse.py
git add ingestor/tests/writer/test_clickhouse.py
git commit -m "feat(writer): auto-create tables for intervals"
```

---

## Task 6: Ingestor 支持多周期处理

**Files:**
- Modify: `ingestor/src/zer0data_ingestor/ingestor.py`
- Test: `ingestor/tests/test_ingestor.py`

**Step 1: Write the failing test**

Add to `ingestor/tests/test_ingestor.py`:

```python
def test_ingest_from_directory_supports_intervals_filter():
    """Test that ingest_from_directory can filter by intervals."""
    from zer0data_ingestor.ingestor import KlineIngestor
    from zer0data_ingestor.config import IngestorConfig
    from zer0data_ingestor.writer.clickhouse import KlineRecord

    config = IngestorConfig()

    # Create mock records with different intervals
    records_1m = KlineRecord(
        symbol="BTCUSDT", interval="1m",
        open_time=1000, close_time=1059,
        open_price=50000.0, high_price=50100.0, low_price=49900.0,
        close_price=50050.0, volume=100.0, quote_volume=5000000.0,
        trades_count=1000, taker_buy_volume=50.0, taker_buy_quote_volume=2500000.0,
    )

    # Mock parser to return records
    from unittest.mock import Mock, patch

    with patch.object(KlineIngestor, '__init__', lambda self, config, data_dir=None: None):
        ingestor = KlineIngestor(None)
        ingestor.config = config
        ingestor.parser = Mock()
        ingestor.cleaner = Mock()
        ingestor.writer = Mock()

        # Setup parser to yield records
        ingestor.parser.parse_directory = Mock(return_value=[
            ("BTCUSDT", "1m", records_1m),
        ])

        # Setup cleaner to return cleaned records
        ingestor.cleaner.clean = Mock(return_value=Mock(
            cleaned_records=[records_1m],
            stats=Mock(duplicates_removed=0, gaps_filled=0, invalid_records_removed=0)
        ))

        # Call ingest_from_directory with intervals filter
        stats = ingestor.ingest_from_directory(
            source="/tmp/test",
            symbols=["BTCUSDT"],
            intervals=["1m", "1h"],  # Only these intervals
        )

        # Verify parser was called with intervals filter
        ingestor.parser.parse_directory.assert_called_once_with(
            "/tmp/test", ["BTCUSDT"], "*.zip", ["1m", "1h"]
        )
```

**Step 2: Run test to verify it fails**

```bash
cd ingestor && pytest tests/test_ingestor.py::test_ingest_from_directory_supports_intervals_filter -v
```

Expected: FAIL - `ingest_from_directory missing intervals parameter`

**Step 3: Update Ingestor**

Modify `ingestor/src/zer0data_ingestor/ingestor.py`:

```python
from typing import List, Optional
from pathlib import Path

from zer0data_ingestor.config import IngestorConfig
from zer0data_ingestor.parser import KlineParser
from zer0data_ingestor.writer.clickhouse import ClickHouseWriter, KlineRecord
from zer0data_ingestor.cleaner.kline import KlineCleaner


class IngestStats:
    """Statistics for ingestion operations."""

    def __init__(self):
        self.symbols_processed = 0
        self.dates_processed = 0
        self.records_written = 0
        self.files_processed = 0
        self.duplicates_removed = 0
        self.gaps_filled = 0
        self.invalid_records_removed = 0
        self.intervals_processed = 0  # Add
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
        intervals: Optional[List[str]] = None,  # Add intervals parameter
        pattern: str = "**/*.zip"
    ) -> IngestStats:
        """Ingest kline data from a directory of zip files.

        Args:
            source: Path to directory containing zip files
            symbols: Optional list of symbols to filter
            intervals: Optional list of intervals to filter (e.g., ["1m", "1h"])
            pattern: Glob pattern for matching files (default: "**/*.zip")

        Returns:
            IngestStats with ingestion statistics
        """
        if self._closed:
            raise RuntimeError("Ingestor has been closed")

        stats = IngestStats()
        symbols_seen = set()
        intervals_seen = set()

        # Group records by (symbol, interval) for cleaning
        records_by_key = {}

        try:
            # Parse all matching files in the directory
            for symbol, interval, record in self.parser.parse_directory(source, symbols, intervals, pattern):
                key = (symbol, interval)
                if key not in records_by_key:
                    records_by_key[key] = []
                records_by_key[key].append(record)

            # Clean and write records per (symbol, interval)
            for (symbol, interval), records in records_by_key.items():
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
                intervals_seen.add(interval)

            # Track the directory as processed
            stats.files_processed = 1
            stats.symbols_processed = len(symbols_seen)
            stats.intervals_processed = len(intervals_seen)

        except Exception as e:
            error_msg = f"Error processing directory {source}: {e}"
            stats.errors.append(error_msg)
            raise

        # Flush any remaining records
        self.writer.flush()

        return stats
```

**Step 4: Run test to verify it passes**

```bash
cd ingestor && pytest tests/test_ingestor.py::test_ingest_from_directory_supports_intervals_filter -v
```

Expected: PASS

**Step 5: Update existing tests**

Update tests that call `ingest_from_directory` to handle new return type with `intervals_processed` field.

**Step 6: Run all tests to verify**

```bash
cd ingestor && pytest tests/ -v
```

Expected: All tests pass

**Step 7: Commit**

```bash
git add ingestor/src/zer0data_ingestor/ingestor.py
git add ingestor/tests/test_ingestor.py
git commit -m "feat(ingestor): add intervals filter support"
```

---

## Task 7: CLI 添加 intervals 参数

**Files:**
- Modify: `ingestor/src/zer0data_ingestor/cli.py`
- Test: `ingestor/tests/test_cli.py`

**Step 1: Write the failing test**

Add to `ingestor/tests/test_cli.py`:

```python
def test_ingest_from_dir_with_intervals():
    """Test ingest-from-dir command with intervals filter."""
    from click.testing import CliRunner
    from unittest.mock import Mock, patch

    runner = CliRunner()

    with patch('zer0data_ingestor.cli.KlineIngestor') as mock_ingestor_cls:
        mock_ingestor = Mock()
        mock_ingestor.ingest_from_directory = Mock(return_value=Mock(
            symbols_processed=1,
            intervals_processed=2,
            records_written=100,
            duplicates_removed=0,
            gaps_filled=0,
            invalid_records_removed=0,
            errors=[]
        ))
        mock_ingestor.__enter__ = Mock(return_value=mock_ingestor)
        mock_ingestor.__exit__ = Mock(return_value=None)
        mock_ingestor_cls.return_value = mock_ingestor

        result = runner.invoke(cli, [
            'ingest-from-dir',
            '--source', '/tmp/test',
            '--intervals', '1h',
            '--intervals', '1d'
        ])

        assert result.exit_code == 0
        mock_ingestor.ingest_from_directory.assert_called_once()
        call_args = mock_ingestor.ingest_from_directory.call_args

        # Verify intervals parameter was passed
        assert '1h' in call_args[1]['intervals']
        assert '1d' in call_args[1]['intervals']
```

**Step 2: Run test to verify it fails**

```bash
cd ingestor && pytest tests/test_cli.py::test_ingest_from_dir_with_intervals -v
```

Expected: FAIL - `no such option: --intervals`

**Step 3: Update CLI**

Modify `ingestor/src/zer0data_ingestor/cli.py`:

```python
@cli.command()
@click.option(
    "--source",
    "-s",
    required=True,
    type=click.Path(exists=True),
    help="Directory containing downloaded zip files",
)
@click.option(
    "--symbols",
    multiple=True,
    help="Specific symbols to ingest (e.g., BTCUSDT). Can be specified multiple times.",
)
@click.option(
    "--intervals",
    multiple=True,
    help="Specific intervals to ingest (e.g., 1h, 1d). Can be specified multiple times.",
)
@click.option(
    "--pattern",
    default="*.zip",
    help="File pattern to match (default: *.zip)",
)
@click.pass_context
def ingest_from_dir(
    ctx: click.Context,
    source: str,
    symbols: tuple,
    intervals: tuple,  # Add intervals parameter
    pattern: str,
) -> None:
    """Ingest kline data from a directory of downloaded zip files.

    Download data first using public-data scripts, then ingest with this command.

    Examples:

        # Ingest all files from directory
        zer0data-ingestor ingest-from-dir --source ./data/download

        # Ingest specific intervals only
        zer0data-ingestor ingest-from-dir --source ./data/download --intervals 1h --intervals 1d

        # Ingest specific symbols and intervals
        zer0data-ingestor ingest-from-dir --source ./data/download --symbols BTCUSDT --intervals 1h
    """
    config = ctx.obj["config"]

    source_path = Path(source)
    if not source_path.is_dir():
        raise click.ClickException(f"Source is not a directory: {source}")

    symbol_list = list(symbols) if symbols else None
    interval_list = list(intervals) if intervals else None  # Convert to list or None

    click.echo(f"Ingesting from: {source}")
    if symbol_list:
        click.echo(f"Symbols: {', '.join(symbol_list)}")
    else:
        click.echo("Symbols: ALL")
    if interval_list:
        click.echo(f"Intervals: {', '.join(interval_list)}")
    else:
        click.echo("Intervals: ALL")
    click.echo(f"ClickHouse: {config['clickhouse_host']}:{config['clickhouse_port']}/{config['clickhouse_db']}")

    try:
        ingestor_config = IngestorConfig(
            clickhouse_host=config['clickhouse_host'],
            clickhouse_port=config['clickhouse_port'],
            clickhouse_database=config['clickhouse_db'],
            clickhouse_user=config['clickhouse_user'],
            clickhouse_password=config['clickhouse_password'],
        )

        with KlineIngestor(ingestor_config) as ingestor:
            stats = ingestor.ingest_from_directory(
                source,
                symbols=symbol_list,
                intervals=interval_list,  # Pass intervals parameter
                pattern=pattern
            )

        click.echo(f"\nIngestion complete:")
        click.echo(f"  Symbols processed: {stats.symbols_processed}")
        click.echo(f"  Intervals processed: {stats.intervals_processed}")  # Add
        click.echo(f"  Records written: {stats.records_written}")

        if stats.duplicates_removed > 0 or stats.gaps_filled > 0 or stats.invalid_records_removed > 0:
            click.echo(f"\nData cleaning:")
            click.echo(f"  Duplicates removed: {stats.duplicates_removed}")
            click.echo(f"  Gaps filled: {stats.gaps_filled}")
            click.echo(f"  Invalid records removed: {stats.invalid_records_removed}")

        if stats.errors:
            click.echo(f"\nErrors ({len(stats.errors)}):")
            for error in stats.errors[:5]:
                click.echo(f"  - {error}")
            if len(stats.errors) > 5:
                click.echo(f"  ... and {len(stats.errors) - 5} more")

    except Exception as e:
        raise click.ClickException(f"Ingestion failed: {e}")
```

**Step 4: Run test to verify it passes**

```bash
cd ingestor && pytest tests/test_cli.py::test_ingest_from_dir_with_intervals -v
```

Expected: PASS

**Step 5: Run all tests to verify**

```bash
cd ingestor && pytest tests/ -v
```

Expected: All tests pass

**Step 6: Commit**

```bash
git add ingestor/src/zer0data_ingestor/cli.py
git add ingestor/tests/test_cli.py
git commit -m "feat(cli): add --intervals filter option"
```

---

## Task 8: SDK 添加 interval 参数

**Files:**
- Modify: `sdk/src/zer0data/kline.py`
- Test: `sdk/tests/test_kline.py`

**Step 1: Write the failing test**

Add to `sdk/tests/test_kline.py`:

```python
def test_query_with_interval():
    """Test that query accepts interval parameter."""
    from unittest.mock import Mock, patch
    from zer0data.kline import KlineQuery
    from datetime import datetime

    with patch('zer0data.kline.clickhouse_connect') as mock_ch:
        mock_client = Mock()
        mock_client.query = Mock(return_value=Mock(
            to_arrow=lambda: Mock(
                to_pandas=lambda: Mock(
                    to_dict=lambda: {
                        'symbol': ['BTCUSDT'],
                        'open_time': [datetime(2024, 1, 1, 0, 0)],
                        'close_price': [42000.0]
                    }
                )
            )
        ))
        mock_ch.get_client.return_value = mock_client

        query = KlineQuery(
            host="localhost",
            port=8123,
            database="test",
        )

        df = query.query(
            symbols=["BTCUSDT"],
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 2),
            interval="1h"  # Specify interval
        )

        # Verify the query uses correct table
        assert mock_client.query.called
        sql = mock_client.query.call_args[0][0]
        assert "klines_1h" in sql


def test_query_defaults_to_1m_interval():
    """Test that query defaults to 1m interval."""
    from unittest.mock import Mock, patch
    from zer0data.kline import KlineQuery
    from datetime import datetime

    with patch('zer0data.kline.clickhouse_connect') as mock_ch:
        mock_client = Mock()
        mock_client.query = Mock(return_value=Mock(
            to_arrow=lambda: Mock(
                to_pandas=lambda: Mock(
                    to_dict=lambda: {
                        'symbol': ['BTCUSDT'],
                        'open_time': [datetime(2024, 1, 1, 0, 0)],
                        'close_price': [42000.0]
                    }
                )
            )
        ))
        mock_ch.get_client.return_value = mock_client

        query = KlineQuery(
            host="localhost",
            port=8123,
            database="test",
        )

        df = query.query(
            symbols=["BTCUSDT"],
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 2),
            # No interval specified
        )

        # Verify the query uses default 1m table
        assert mock_client.query.called
        sql = mock_client.query.call_args[0][0]
        assert "klines_1m" in sql
```

**Step 2: Run test to verify it fails**

```bash
cd sdk && pytest tests/test_kline.py::test_query_with_interval -v
```

Expected: FAIL - `query() missing interval parameter`

**Step 3: Update KlineQuery**

Modify `sdk/src/zer0data/kline.py`:

```python
from datetime import datetime
from typing import List, Optional
import polars as pl
import clickhouse_connect


class KlineQuery:
    """Kline query interface."""

    def __init__(self, host: str, port: int, database: str, username: str = None, password: str = None):
        """Initialize KlineQuery.

        Args:
            host: ClickHouse host
            port: ClickHouse HTTP port
            database: Database name
            username: ClickHouse username
            password: ClickHouse password
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username or "default"
        self.password = password or ""
        self.table_prefix = "klines_"

    def _get_table_name(self, interval: str) -> str:
        """Get table name for interval.

        Args:
            interval: Interval string (e.g., "1m", "1h", "1d")

        Returns:
            Table name (e.g., "klines_1m", "klines_1h")
        """
        return f"{self.table_prefix}{interval}"

    def query(
        self,
        symbols: List[str],
        start: datetime,
        end: datetime,
        interval: str = "1m",  # Add interval parameter with default
        limit: int = None,
    ) -> pl.DataFrame:
        """Query kline data, supports multiple intervals.

        Args:
            symbols: List of trading symbols
            start: Start datetime
            end: End datetime
            interval: Kline interval (1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d)
            limit: Maximum number of records to return

        Returns:
            Polars DataFrame with results
        """
        table = self._get_table_name(interval)

        # Format symbols for SQL IN clause
        symbols_str = ", ".join(f"'{s}'" for s in symbols)

        # Build SQL query
        sql = f"""
            SELECT
                symbol,
                open_time,
                close_time,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                quote_volume,
                trades_count,
                taker_buy_volume,
                taker_buy_quote_volume
            FROM {table}
            WHERE symbol IN ({symbols_str})
              AND open_time >= '{start.strftime('%Y-%m-%d %H:%M:%S')}'
              AND open_time < '{end.strftime('%Y-%m-%d %H:%M:%S')}'
            ORDER BY symbol, open_time
        """

        if limit:
            sql += f" LIMIT {limit}"

        # Execute query
        client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
        )

        result = client.query(sql)

        # Convert to Polars DataFrame
        import pyarrow as pa

        arrow_table = pa.ipc.open_stream(result.get_arrow())
        return pl.from_arrow(arrow_table)
```

**Step 4: Run test to verify it passes**

```bash
cd sdk && pytest tests/test_kline.py::test_query_with_interval tests/test_kline.py::test_query_defaults_to_1m_interval -v
```

Expected: PASS

**Step 5: Update Client class**

Modify `sdk/src/zer0data/client.py` to propagate interval parameter:

```python
class Client:
    """Zer0data client for querying kline data."""

    def __init__(self, host: str = "localhost", port: int = 8123, database: str = "zer0data", username: str = None, password: str = None):
        """Initialize client.

        Args:
            host: ClickHouse host
            port: ClickHouse HTTP port
            database: Database name
            username: ClickHouse username
            password: ClickHouse password
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self._kline = None

    @property
    def kline(self) -> "KlineQuery":
        """Get kline query interface."""
        if self._kline is None:
            from zer0data.kline import KlineQuery
            self._kline = KlineQuery(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
            )
        return self._kline
```

**Step 6: Run all tests to verify**

```bash
cd sdk && pytest tests/ -v
```

Expected: All tests pass

**Step 7: Commit**

```bash
git add sdk/src/zer0data/kline.py sdk/src/zer0data/client.py
git add sdk/tests/test_kline.py
git commit -m "feat(sdk): add interval parameter to kline query"
```

---

## Task 9: SDK 添加 Interval 常量类

**Files:**
- Create: `sdk/src/zer0data/interval.py`
- Modify: `sdk/src/zer0data/__init__.py`
- Test: `sdk/tests/test_interval.py`

**Step 1: Write the failing test**

Create `sdk/tests/test_interval.py`:

```python
"""Tests for Interval constants."""

from zer0data import Interval


def test_interval_constants():
    """Test Interval class has all period constants."""
    assert Interval.M1 == "1m"
    assert Interval.M3 == "3m"
    assert Interval.M5 == "5m"
    assert Interval.M15 == "15m"
    assert Interval.M30 == "30m"
    assert Interval.H1 == "1h"
    assert Interval.H2 == "2h"
    assert Interval.H4 == "4h"
    assert Interval.H6 == "6h"
    assert Interval.H8 == "8h"
    assert Interval.H12 == "12h"
    assert Interval.D1 == "1d"
```

**Step 2: Run test to verify it fails**

```bash
cd sdk && pytest tests/test_interval.py -v
```

Expected: FAIL - `No module named 'zer0data.interval'`

**Step 3: Create Interval class**

Create `sdk/src/zer0data/interval.py`:

```python
"""Interval constants for type-safe kline queries."""


class Interval:
    """Kline interval constants.

    Usage:
        from zer0data import Client, Interval

        client = Client()
        df = client.kline.query(
            symbols=['BTCUSDT'],
            interval=Interval.H1,
            ...
        )
    """

    M1 = "1m"
    M3 = "3m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H2 = "2h"
    H4 = "4h"
    H6 = "6h"
    H8 = "8h"
    H12 = "12h"
    D1 = "1d"
```

**Step 4: Update __init__.py**

Modify `sdk/src/zer0data/__init__.py`:

```python
"""Zer0data SDK for querying kline data."""

from zer0data.client import Client
from zer0data.interval import Interval

__all__ = ["Client", "Interval"]
```

**Step 5: Run test to verify it passes**

```bash
cd sdk && pytest tests/test_interval.py -v
```

Expected: PASS

**Step 6: Commit**

```bash
git add sdk/src/zer0data/interval.py sdk/src/zer0data/__init__.py sdk/tests/test_interval.py
git commit -m "feat(sdk): add Interval constants for type-safe queries"
```

---

## Task 10: 更新 Docker Compose 配置

**Files:**
- Modify: `docker/clickhouse/init.sql`

**Step 1: Add table creation scripts**

Modify `docker/clickhouse/init.sql`:

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS zer0data;

-- Switch to zer0data database
USE zer0data;

-- Create klines tables for all intervals
-- 1m
CREATE TABLE IF NOT EXISTS klines_1m (
    symbol String,
    open_time DateTime,
    close_time DateTime,
    open_price Decimal64(18, 8),
    high_price Decimal64(18, 8),
    low_price Decimal64(18, 8),
    close_price Decimal64(18, 8),
    volume Decimal64(18, 8),
    quote_volume Decimal64(18, 8),
    trades_count UInt32,
    taker_buy_volume Decimal64(18, 8),
    taker_buy_quote_volume Decimal64(18, 8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 3m
CREATE TABLE IF NOT EXISTS klines_3m (
    symbol String,
    open_time DateTime,
    close_time DateTime,
    open_price Decimal64(18, 8),
    high_price Decimal64(18, 8),
    low_price Decimal64(18, 8),
    close_price Decimal64(18, 8),
    volume Decimal64(18, 8),
    quote_volume Decimal64(18, 8),
    trades_count UInt32,
    taker_buy_volume Decimal64(18, 8),
    taker_buy_quote_volume Decimal64(18, 8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 5m
CREATE TABLE IF NOT EXISTS klines_5m (
    symbol String,
    open_time DateTime,
    close_time DateTime,
    open_price Decimal64(18, 8),
    high_price Decimal64(18, 8),
    low_price Decimal64(18, 8),
    close_price Decimal64(18, 8),
    volume Decimal64(18, 8),
    quote_volume Decimal64(18, 8),
    trades_count UInt32,
    taker_buy_volume Decimal64(18, 8),
    taker_buy_quote_volume Decimal64(18, 8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 15m
CREATE TABLE IF NOT EXISTS klines_15m (
    symbol String,
    open_time DateTime,
    close_time DateTime,
    open_price Decimal64(18, 8),
    high_price Decimal64(18, 8),
    low_price Decimal64(18, 8),
    close_price Decimal64(18, 8),
    volume Decimal64(18, 8),
    quote_volume Decimal64(18, 8),
    trades_count UInt32,
    taker_buy_volume Decimal64(18, 8),
    taker_buy_quote_volume Decimal64(18, 8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 30m
CREATE TABLE IF NOT EXISTS klines_30m (
    symbol String,
    open_time DateTime,
    close_time DateTime,
    open_price Decimal64(18, 8),
    high_price Decimal64(18, 8),
    low_price Decimal64(18, 8),
    close_price Decimal64(18, 8),
    volume Decimal64(18, 8),
    quote_volume Decimal64(18, 8),
    trades_count UInt32,
    taker_buy_volume Decimal64(18, 8),
    taker_buy_quote_volume Decimal64(18, 8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 1h
CREATE TABLE IF NOT EXISTS klines_1h (
    symbol String,
    open_time DateTime,
    close_time DateTime,
    open_price Decimal64(18, 8),
    high_price Decimal64(18, 8),
    low_price Decimal64(18, 8),
    close_price Decimal64(18, 8),
    volume Decimal64(18, 8),
    quote_volume Decimal64(18, 8),
    trades_count UInt32,
    taker_buy_volume Decimal64(18, 8),
    taker_buy_quote_volume Decimal64(18, 8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 2h
CREATE TABLE IF NOT EXISTS klines_2h (
    symbol String,
    open_time DateTime,
    close_time DateTime,
    open_price Decimal64(18, 8),
    high_price Decimal64(18, 8),
    low_price Decimal64(18, 8),
    close_price Decimal64(18, 8),
    volume Decimal64(18, 8),
    quote_volume Decimal64(18, 8),
    trades_count UInt32,
    taker_buy_volume Decimal64(18, 8),
    taker_buy_quote_volume Decimal64(18, 8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 4h
CREATE TABLE IF NOT EXISTS klines_4h (
    symbol String,
    open_time DateTime,
    close_time DateTime,
    open_price Decimal64(18, 8),
    high_price Decimal64(18, 8),
    low_price Decimal64(18, 8),
    close_price Decimal64(18, 8),
    volume Decimal64(18, 8),
    quote_volume Decimal64(18, 8),
    trades_count UInt32,
    taker_buy_volume Decimal64(18, 8),
    taker_buy_quote_volume Decimal64(18, 8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 6h
CREATE TABLE IF NOT EXISTS klines_6h (
    symbol String,
    open_time DateTime,
    close_time DateTime,
    open_price Decimal64(18, 8),
    high_price Decimal64(18, 8),
    low_price Decimal64(18, 8),
    close_price Decimal64(18, 8),
    volume Decimal64(18, 8),
    quote_volume Decimal64(18, 8),
    trades_count UInt32,
    taker_buy_volume Decimal64(18, 8),
    taker_buy_quote_volume Decimal64(18, 8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 8h
CREATE TABLE IF NOT EXISTS klines_8h (
    symbol String,
    open_time DateTime,
    close_time DateTime,
    open_price Decimal64(18, 8),
    high_price Decimal64(18, 8),
    low_price Decimal64(18, 8),
    close_price Decimal64(18, 8),
    volume Decimal64(18, 8),
    quote_volume Decimal64(18, 8),
    trades_count UInt32,
    taker_buy_volume Decimal64(18, 8),
    taker_buy_quote_volume Decimal64(18, 8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 12h
CREATE TABLE IF NOT EXISTS klines_12h (
    symbol String,
    open_time DateTime,
    close_time DateTime,
    open_price Decimal64(18, 8),
    high_price Decimal64(18, 8),
    low_price Decimal64(18, 8),
    close_price Decimal64(18, 8),
    volume Decimal64(18, 8),
    quote_volume Decimal64(18, 8),
    trades_count UInt32,
    taker_buy_volume Decimal64(18, 8),
    taker_buy_quote_volume Decimal64(18, 8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 1d (use yearly partition for daily data)
CREATE TABLE IF NOT EXISTS klines_1d (
    symbol String,
    open_time DateTime,
    close_time DateTime,
    open_price Decimal64(18, 8),
    high_price Decimal64(18, 8),
    low_price Decimal64(18, 8),
    close_price Decimal64(18, 8),
    volume Decimal64(18, 8),
    quote_volume Decimal64(18, 8),
    trades_count UInt32,
    taker_buy_volume Decimal64(18, 8),
    taker_buy_quote_volume Decimal64(18, 8)
) ENGINE = MergeTree()
PARTITION BY toYYYY(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;
```

**Step 2: Verify SQL syntax**

```bash
cat docker/clickhouse/init.sql | head -50
```

**Step 3: Commit**

```bash
git add docker/clickhouse/init.sql
git commit -m "feat(docker): add all interval tables to init.sql"
```

---

## Task 11: 更新 README 文档

**Files:**
- Modify: `README.md`

**Step 1: Update README with multi-interval examples**

Modify `README.md`, add section after existing quick start:

```markdown
## 多周期K线数据

支持12个K线周期：1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d

### 下载特定周期数据

```bash
# 进入公共数据项目目录
cd binance-public-data/python

# 下载1小时K线数据
STORE_DIRECTORY=../../data/download \
./download-kline.py \
  --type futures \
  --symbols BTCUSDT \
  --interval 1h \
  --date 2024-01-01
```

### 入库特定周期

```bash
# 只入库 1h 和 1d 数据
zer0data-ingestor ingest-from-dir \
  --source ./data/download \
  --intervals 1h \
  --intervals 1d
```

### SDK 查询多周期

```python
from zer0data import Client, Interval
from datetime import datetime, timedelta

client = Client()

# 查询1小时K线
df_1h = client.kline.query(
    symbols=['BTCUSDT'],
    interval=Interval.H1,  # 或使用字符串 "1h"
    start=datetime.now() - timedelta(days=7),
    end=datetime.now()
)

# 查询1日K线
df_1d = client.kline.query(
    symbols=['BTCUSDT'],
    interval="1d",
    start=datetime.now() - timedelta(days=90),
    end=datetime.now()
)
```
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add multi-interval examples to README"
```

---

## 完成清单

- [ ] Task 1: 添加 interval 常量定义
- [ ] Task 2: KlineRecord 添加 interval 字段
- [ ] Task 3: Parser 从文件名提取 interval
- [ ] Task 4: Writer 支持动态表名
- [ ] Task 5: Writer 自动创建表
- [ ] Task 6: Ingestor 支持多周期处理
- [ ] Task 7: CLI 添加 intervals 参数
- [ ] Task 8: SDK 添加 interval 参数
- [ ] Task 9: SDK 添加 Interval 常量类
- [ ] Task 10: 更新 Docker Compose 配置
- [ ] Task 11: 更新 README 文档

## 文件变更汇总

| 文件 | 变更类型 |
|------|----------|
| `ingestor/src/zer0data_ingestor/constants.py` | 新建 |
| `ingestor/src/zer0data_ingestor/parser/zip_parser.py` | 修改 |
| `ingestor/src/zer0data_ingestor/writer/clickhouse.py` | 修改 |
| `ingestor/src/zer0data_ingestor/ingestor.py` | 修改 |
| `ingestor/src/zer0data_ingestor/cli.py` | 修改 |
| `sdk/src/zer0data/interval.py` | 新建 |
| `sdk/src/zer0data/kline.py` | 修改 |
| `docker/clickhouse/init.sql` | 修改 |
| `README.md` | 修改 |
| `ingestor/tests/*.py` | 多个测试文件 |
| `sdk/tests/*.py` | 多个测试文件 |
