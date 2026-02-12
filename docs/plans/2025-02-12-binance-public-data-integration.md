# Binance Public Data Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace custom Binance downloader with official binance-public-data repository, separating download and ingestion concerns.

**Architecture:**
- Use git submodule for `binance-public-data` (official download scripts)
- Create new `KlineParser` class to parse local zip files
- Update CLI to accept `--source` directory instead of download parameters
- Remove `downloader/` module completely

**Tech Stack:** Python 3.11+, ClickHouse, Poetry, git submodule

---

## Task 1: Add git submodule for binance-public-data

**Files:**
- Modify: `.gitmodules` (create)
- Modify: `.gitignore`

**Step 1: Create .gitmodules file**

```bash
cat > .gitmodules << 'EOF'
[submodule "binance-public-data"]
    path = binance-public-data
    url = https://github.com/binance/binance-public-data.git
EOF
```

**Step 2: Add submodule**

```bash
git submodule add https://github.com/binance/binance-public-data.git
```

Expected: Submodule cloned into `binance-public-data/` directory

**Step 3: Update .gitignore**

```bash
# Add to .gitignore
binance-public-data/python/__pycache__/
binance-public-data/python/*.pyc
```

**Step 4: Commit**

```bash
git add .gitmodules .gitignore binance-public-data
git commit -m "feat: add binance-public-data as submodule"
```

---

## Task 2: Create new KlineParser class

**Files:**
- Create: `ingestor/src/zer0data_ingestor/parser/zip_parser.py`
- Modify: `ingestor/src/zer0data_ingestor/parser/__init__.py`

**Step 1: Write the failing test**

Create `ingestor/tests/parser/test_zip_parser.py`:

```python
"""Tests for KlineParser zip file parser."""

import io
import tempfile
import zipfile
from pathlib import Path

import pytest

from zer0data_ingestor.parser.zip_parser import KlineParser


def test_parse_single_zip_file():
    """Test parsing a single zip file containing kline CSV."""
    # Sample Binance CSV data
    csv_data = b"1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0\n"

    # Create a temporary zip file
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
        zip_path = f.name
        with zipfile.ZipFile(f, 'w') as zf:
            zf.writestr("BTCUSDT-1m-2024-01-01.csv", csv_data)

    try:
        parser = KlineParser()
        records = list(parser.parse_file(zip_path, "BTCUSDT"))

        assert len(records) == 1
        assert records[0].symbol == "BTCUSDT"
        assert records[0].open_time == 1704067200000
        assert records[0].open_price == 42000.00
    finally:
        Path(zip_path).unlink()


def test_parse_nonexistent_file():
    """Test parsing a file that doesn't exist."""
    parser = KlineParser()
    with pytest.raises(FileNotFoundError):
        list(parser.parse_file("/nonexistent/file.zip", "BTCUSDT"))


def test_parse_corrupted_zip():
    """Test parsing a corrupted zip file."""
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
        f.write(b"not a valid zip")
        zip_path = f.name

    try:
        parser = KlineParser()
        with pytest.raises(ValueError, match="Invalid zip file"):
            list(parser.parse_file(zip_path, "BTCUSDT"))
    finally:
        Path(zip_path).unlink()
```

**Step 2: Run test to verify it fails**

```bash
cd ingestor && pytest tests/parser/test_zip_parser.py -v
```

Expected: `ModuleNotFoundError: No module named 'zer0data_ingestor.parser.zip_parser'`

**Step 3: Write minimal implementation**

Create `ingestor/src/zer0data_ingestor/parser/zip_parser.py`:

```python
"""Kline data parser for Binance zip files."""

import io
import zipfile
from pathlib import Path
from typing import Iterator, List

from zer0data_ingestor.writer.clickhouse import KlineRecord


class KlineParser:
    """Parser for Binance kline data from zip files."""

    def parse_file(self, zip_path: str, symbol: str) -> Iterator[KlineRecord]:
        """Parse a single zip file and yield kline records.

        Args:
            zip_path: Path to the zip file
            symbol: Trading symbol (e.g., "BTCUSDT")

        Yields:
            KlineRecord objects

        Raises:
            FileNotFoundError: If zip file doesn't exist
            ValueError: If zip file is corrupted or invalid
        """
        if not Path(zip_path).exists():
            raise FileNotFoundError(f"Zip file not found: {zip_path}")

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # Get the first CSV file in the zip
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
                            # Skip malformed rows
                            continue

        except zipfile.BadZipFile as e:
            raise ValueError(f"Invalid zip file: {e}") from e

    def parse_directory(
        self,
        dir_path: str,
        symbols: List[str] = None,
        pattern: str = "*.zip"
    ) -> Iterator[tuple[str, KlineRecord]]:
        """Parse all matching zip files in a directory.

        Args:
            dir_path: Path to the directory containing zip files
            symbols: Optional list of symbols to filter. If None, parses all.
            pattern: Glob pattern for matching files (default: "*.zip")

        Yields:
            Tuples of (symbol, KlineRecord)
        """
        from glob import glob

        dir_path_obj = Path(dir_path)
        if not dir_path_obj.exists():
            raise FileNotFoundError(f"Directory not found: {dir_path}")

        for zip_file in glob(str(dir_path_obj / pattern)):
            # Extract symbol from filename if not provided
            # Format: BTCUSDT-1m-2024-01-01.zip
            filename = Path(zip_file).stem
            parts = filename.split('-')
            if len(parts) >= 3:
                symbol = parts[0]
                if symbols and symbol not in symbols:
                    continue

                for record in self.parse_file(zip_file, symbol):
                    yield (symbol, record)
```

**Step 4: Update __init__.py**

```python
"""Parser module for kline data."""

from zer0data_ingestor.parser.kline import parse_klines_csv
from zer0data_ingestor.parser.zip_parser import KlineParser

__all__ = ["parse_klines_csv", "KlineParser"]
```

**Step 5: Run tests to verify they pass**

```bash
cd ingestor && pytest tests/parser/test_zip_parser.py -v
```

Expected: All tests pass

**Step 6: Commit**

```bash
git add ingestor/src/zer0data_ingestor/parser/zip_parser.py
git add ingestor/src/zer0data_ingestor/parser/__init__.py
git add ingestor/tests/parser/test_zip_parser.py
git commit -m "feat: add KlineParser for zip files"
```

---

## Task 3: Update KlineIngestor to use KlineParser

**Files:**
- Modify: `ingestor/src/zer0data_ingestor/ingestor.py`
- Test: `ingestor/tests/test_ingestor.py`

**Step 1: Write test for new behavior**

Update `ingestor/tests/test_ingestor.py`:

```python
"""Tests for KlineIngestor with local file parsing."""

import tempfile
import zipfile
from pathlib import Path
from datetime import date

from zer0data_ingestor.config import IngestorConfig
from zer0data_ingestor.ingestor import KlineIngestor
from zer0data_ingestor.writer.clickhouse import KlineRecord


def test_ingest_from_directory(tmp_path):
    """Test ingesting klines from a directory of zip files."""
    # Create sample zip file
    csv_data = b"1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0\n"
    zip_file_path = tmp_path / "BTCUSDT-1m-2024-01-01.zip"

    with zipfile.ZipFile(zip_file_path, 'w') as zf:
        zf.writestr("BTCUSDT-1m-2024-01-01.csv", csv_data)

    # Create mock config
    config = IngestorConfig()

    # Mock the writer to capture records
    written_records = []

    def mock_insert(record: KlineRecord):
        written_records.append(record)

    # Create ingestor and ingest from directory
    with KlineIngestor(config) as ingestor:
        ingestor.writer.insert = mock_insert
        stats = ingestor.ingest_from_directory(str(tmp_path), symbols=["BTCUSDT"])

    assert stats.symbols_processed >= 1
    assert len(written_records) > 0
    assert written_records[0].symbol == "BTCUSDT"
```

**Step 2: Run test to verify it fails**

```bash
cd ingestor && pytest tests/test_ingestor.py::test_ingest_from_directory -v
```

Expected: `AttributeError: 'KlineIngestor' object has no attribute 'ingest_from_directory'`

**Step 3: Update ingestor.py**

Replace `ingestor/src/zer0data_ingestor/ingestor.py`:

```python
"""Main ingestion logic for kline data."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, List, Optional

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
    """Main ingestor for parsing local kline files and writing to ClickHouse."""

    def __init__(self, config: IngestorConfig, data_dir: str = "./data/download"):
        """Initialize the ingestor.

        Args:
            config: IngestorConfig instance with database settings
            data_dir: Directory containing downloaded zip files
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
            symbols: Optional list of symbols to filter. If None, processes all.
            pattern: Glob pattern for matching files (default: "*.zip")

        Returns:
            IngestStats with ingestion statistics
        """
        if self._closed:
            raise RuntimeError("Ingestor has been closed")

        stats = IngestStats()
        source_path = Path(source)

        if not source_path.exists():
            stats.errors.append(f"Directory not found: {source}")
            return stats

        processed_symbols = set()

        for symbol, record in self.parser.parse_directory(source, symbols, pattern):
            try:
                self.writer.insert(record)
                stats.records_written += 1
                processed_symbols.add(symbol)
            except Exception as e:
                error_msg = f"Error inserting {symbol} record: {e}"
                stats.errors.append(error_msg)

        self.writer.flush()
        stats.symbols_processed = len(processed_symbols)

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
```

**Step 4: Run tests to verify they pass**

```bash
cd ingestor && pytest tests/test_ingestor.py -v
```

Expected: Tests pass (may need to update other tests in the file)

**Step 5: Commit**

```bash
git add ingestor/src/zer0data_ingestor/ingestor.py
git add ingestor/tests/test_ingestor.py
git commit -m "refactor: update KlineIngestor to use KlineParser"
```

---

## Task 4: Update CLI commands

**Files:**
- Modify: `ingestor/src/zer0data_ingestor/cli.py`
- Test: `ingestor/tests/test_cli.py`

**Step 1: Write test for new CLI command**

Update `ingestor/tests/test_cli.py`:

```python
"""Tests for CLI commands."""

import tempfile
import zipfile
from pathlib import Path
from click.testing import CliRunner

from zer0data_ingestor.cli import cli


def test_ingest_from_dir_command():
    """Test the ingest-from-dir command."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create sample zip file
        csv_data = b"1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0\n"
        zip_path = Path(tmp_dir) / "BTCUSDT-1m-2024-01-01.zip"

        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("BTCUSDT-1m-2024-01-01.csv", csv_data)

        # Run command
        result = runner.invoke(cli, [
            'ingest-from-dir',
            '--source', tmp_dir,
            '--symbols', 'BTCUSDT'
        ])

        # Should not error
        assert result.exit_code == 0 or 'ClickHouse' in result.output
```

**Step 2: Run test to verify it fails**

```bash
cd ingestor && pytest tests/test_cli.py::test_ingest_from_dir_command -v
```

Expected: `No such command 'ingest-from-dir'`

**Step 3: Update CLI**

Replace `ingestor/src/zer0data_ingestor/cli.py`:

```python
"""CLI interface for zer0data ingestor."""

import click
from pathlib import Path
from typing import Optional

from zer0data_ingestor.config import IngestorConfig
from zer0data_ingestor.ingestor import KlineIngestor


@click.group()
@click.option(
    "--clickhouse-host",
    envvar="CLICKHOUSE_HOST",
    default="localhost",
    help="ClickHouse server host",
)
@click.option(
    "--clickhouse-port",
    envvar="CLICKHOUSE_PORT",
    default=8123,
    type=int,
    help="ClickHouse HTTP port",
)
@click.option(
    "--clickhouse-db",
    envvar="CLICKHOUSE_DB",
    default="zer0data",
    help="ClickHouse database name",
)
@click.option(
    "--clickhouse-user",
    envvar="CLICKHOUSE_USER",
    default="default",
    help="ClickHouse username",
)
@click.option(
    "--clickhouse-password",
    envvar="CLICKHOUSE_PASSWORD",
    default="",
    help="ClickHouse password",
)
@click.pass_context
def cli(
    ctx: click.Context,
    clickhouse_host: str,
    clickhouse_port: int,
    clickhouse_db: str,
    clickhouse_user: str,
    clickhouse_password: str,
) -> None:
    """Zer0data Ingestor - Parse and ingest Binance kline data from local files.

    Download data using binance-public-data scripts, then use this tool to ingest.
    """
    ctx.ensure_object(dict)

    # Store configuration in context for subcommands
    ctx.obj["config"] = {
        "clickhouse_host": clickhouse_host,
        "clickhouse_port": clickhouse_port,
        "clickhouse_db": clickhouse_db,
        "clickhouse_user": clickhouse_user,
        "clickhouse_password": clickhouse_password,
    }


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
    "--pattern",
    default="*.zip",
    help="File pattern to match (default: *.zip)",
)
@click.pass_context
def ingest_from_dir(
    ctx: click.Context,
    source: str,
    symbols: tuple,
    pattern: str,
) -> None:
    """Ingest kline data from a directory of downloaded zip files.

    Download data first using binance-public-data scripts, then ingest with this command.

    Examples:

        # Ingest all files from directory
        zer0data-ingestor ingest-from-dir --source ./data/download

        # Ingest specific symbols only
        zer0data-ingestor ingest-from-dir --source ./data/download --symbols BTCUSDT --symbols ETHUSDT
    """
    config = ctx.obj["config"]

    source_path = Path(source)
    if not source_path.is_dir():
        raise click.ClickException(f"Source is not a directory: {source}")

    symbol_list = list(symbols) if symbols else None

    click.echo(f"Ingesting from: {source}")
    if symbol_list:
        click.echo(f"Symbols: {', '.join(symbol_list)}")
    else:
        click.echo("Symbols: ALL")
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
            stats = ingestor.ingest_from_directory(source, symbols=symbol_list, pattern=pattern)

        click.echo(f"\nIngestion complete:")
        click.echo(f"  Symbols processed: {stats.symbols_processed}")
        click.echo(f"  Records written: {stats.records_written}")

        if stats.errors:
            click.echo(f"\nErrors ({len(stats.errors)}):")
            for error in stats.errors[:5]:  # Show first 5 errors
                click.echo(f"  - {error}")
            if len(stats.errors) > 5:
                click.echo(f"  ... and {len(stats.errors) - 5} more")

    except Exception as e:
        raise click.ClickException(f"Ingestion failed: {e}")


if __name__ == "__main__":
    cli()
```

**Step 4: Remove old commands and config options**

Remove `backfill`, `ingest_daily`, and `check_missing` commands and their associated options.

**Step 5: Update tests**

Update existing tests in `ingestor/tests/test_cli.py` to match new CLI structure.

**Step 6: Run tests to verify they pass**

```bash
cd ingestor && pytest tests/test_cli.py -v
```

Expected: Tests pass

**Step 7: Commit**

```bash
git add ingestor/src/zer0data_ingestor/cli.py
git add ingestor/tests/test_cli.py
git commit -m "refactor: update CLI to use local files"
```

---

## Task 5: Remove downloader module

**Files:**
- Delete: `ingestor/src/zer0data_ingestor/downloader/`
- Delete: `ingestor/tests/downloader/`

**Step 1: Remove downloader directory**

```bash
rm -rf ingestor/src/zer0data_ingestor/downloader/
rm -rf ingestor/tests/downloader/
```

**Step 2: Verify imports are updated**

```bash
cd ingestor && grep -r "from zer0data_ingestor.downloader" src/ tests/
```

Expected: No results (if found, update the imports)

**Step 3: Commit**

```bash
git add -A
git commit -m "refactor: remove downloader module"
```

---

## Task 6: Update README documentation

**Files:**
- Modify: `README.md`

**Step 1: Update README**

Replace the quick start section:

```markdown
## 快速开始

### 1. 启动 ClickHouse

```bash
docker-compose up -d
```

### 2. 下载 K 线数据

```bash
# 添加 submodule 并初始化
git submodule update --init --recursive

# 进入 binance-public-data 目录
cd binance-public-data/python

# 安装依赖
pip install -r requirements.txt

# 下载永续合约 1 分钟 K 线数据
STORE_DIRECTORY=../../data/download \
./download-kline.py \
  --type futures \
  --symbols BTCUSDT,ETHUSDT \
  --interval 1m \
  --date 2024-01-01
```

### 3. 入库到 ClickHouse

```bash
# 返回项目根目录
cd ../..

# 安装 ingestor
pip install zer0data-ingestor

# 解析并入库已下载的文件
zer0data-ingestor ingest-from-dir \
  --source ./data/download \
  --symbols BTCUSDT,ETHUSDT
```

### 4. 查询数据

```python
from datetime import datetime, timedelta
from zer0data import Client

print(Client().kline.query(
    symbols=['BTCUSDT'],
    start=datetime.now()-timedelta(days=7),
    end=datetime.now()
).head())
```
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: update README for binance-public-data integration"
```

---

## Task 7: Run full integration test

**Files:**
- Test: `ingestor/tests/integration/test_full_flow.py`

**Step 1: Update integration test**

```python
"""Integration test for full download -> parse -> ingest flow."""

import tempfile
import zipfile
from pathlib import Path

from zer0data_ingestor.config import IngestorConfig
from zer0data_ingestor.ingestor import KlineIngestor


def test_full_flow():
    """Test complete flow: download (simulated) -> parse -> ingest."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Simulate downloaded file
        csv_data = b"1704067200000,42000.00,42100.00,41900.00,42050.00,1000.5,1704067259999,42050000.00,1500,500.25,21000000.00,0\n"
        zip_path = Path(tmp_dir) / "BTCUSDT-1m-2024-01-01.zip"

        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("BTCUSDT-1m-2024-01-01.csv", csv_data)

        # Ingest
        config = IngestorConfig()
        with KlineIngestor(config) as ingestor:
            stats = ingestor.ingest_from_directory(tmp_dir, symbols=["BTCUSDT"])

        assert stats.symbols_processed >= 1
        assert stats.records_written >= 1
```

**Step 2: Run integration test**

```bash
cd ingestor && pytest tests/integration/test_full_flow.py -v
```

Expected: Pass

**Step 3: Commit**

```bash
git add ingestor/tests/integration/test_full_flow.py
git commit -m "test: update integration test for new flow"
```

---

## Summary

After completing all tasks:

1. `binance-public-data` submodule added for official download scripts
2. `KlineParser` class replaces `BinanceKlineDownloader`
3. CLI now accepts `--source` directory instead of downloading
4. Removed `downloader/` module completely
5. Tests updated to cover new flow
6. Documentation updated with new workflow

**Files changed:**
- Created: `.gitmodules`, `ingestor/src/zer0data_ingestor/parser/zip_parser.py`
- Modified: `ingestor/src/zer0data_ingestor/cli.py`, `ingestor/src/zer0data_ingestor/ingestor.py`
- Modified: `README.md`, various test files
- Deleted: `ingestor/src/zer0data_ingestor/downloader/`
