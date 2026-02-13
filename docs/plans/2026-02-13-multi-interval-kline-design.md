# Multi-Interval K-Line Data Design

**项目名称**: zer0data 多周期K线数据支持
**创建日期**: 2026-02-13
**状态**: 设计阶段

## 1. 概述

在现有1分钟K线数据基础上，扩展支持12个K线周期：**1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d**

### 核心决策

| 决策点 | 选择 | 理由 |
|--------|------|------|
| 数据来源 | 公共数据项目直接下载各周期原始数据 | 官方数据质量有保证，无需维护聚合逻辑 |
| 表结构 | 每个周期独立表 | 查询性能最优，管理简单 |
| 下载流程 | 按周期串行处理 | 资源占用低，便于监控调试 |
| SDK接口 | 统一接口 + interval 参数 | 简洁易用，向后兼容 |
| 历史数据 | 只存储新数据（从首次运行开始） | 快速上线，避免历史聚合复杂度 |
| 增量更新 | 统一调度器处理所有周期 | 配置简单，一次调度全量更新 |

## 2. 整体架构

```
公共数据项目下载 → ZIP解析 → 数据清洗 → ClickHouse入库
                ↓
         按周期串行处理 (1m → 3m → 5m → ... → 1d)
                ↓
    klines_1m, klines_3m, ..., klines_1d (12张独立表)
```

### 核心改动点

1. **Parser**: 支持解析不同周期的ZIP文件，从文件名提取interval信息
2. **Cleaner**: 复用现有清洗逻辑，每个周期独立清洗
3. **Writer**: 根据interval字段动态选择目标表写入
4. **SDK**: `Client.kline.query()` 增加 `interval` 参数
5. **CLI**: `ingest-from-dir` 支持 `--intervals` 参数过滤

### 周期到表名映射

| interval | 表名 | 分区策略 |
|-----------|------|----------|
| 1m | klines_1m | 按月分区 |
| 3m | klines_3m | 按月分区 |
| 5m | klines_5m | 按月分区 |
| 15m | klines_15m | 按月分区 |
| 30m | klines_30m | 按月分区 |
| 1h | klines_1h | 按月分区 |
| 2h | klines_2h | 按月分区 |
| 4h | klines_4h | 按月分区 |
| 6h | klines_6h | 按月分区 |
| 8h | klines_8h | 按月分区 |
| 12h | klines_12h | 按月分区 |
| 1d | klines_1d | 按年分区 |

## 3. ClickHouse表结构

每个周期对应一张独立的表，命名格式 `klines_{interval}`。表结构与现有的 `klines_1m` 保持一致。

### 表结构示例（1h）

```sql
CREATE TABLE klines_1h (
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
    taker_buy_quote_volume Decimal64(18, 8),
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;
```

### 表创建自动化

Writer模块在首次写入某周期数据时自动创建对应表。

## 4. Parser模块改造

### KlineRecord 添加 interval 字段

```python
@dataclass
class KlineRecord:
    symbol: str
    interval: str  # 新增：1m, 5m, 1h 等
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

### 文件名解析

```python
def extract_interval_from_filename(filename: str) -> str:
    """从 BTCUSDT-1h-2024-01-01.zip 提取 1h"""
    parts = Path(filename).stem.split('-')
    if len(parts) >= 3:
        return parts[1]
    return None
```

### parse_directory 支持周期过滤

```python
def parse_directory(
    self,
    dir_path: str,
    symbols: List[str] = None,
    intervals: List[str] = None,  # 新增
    pattern: str = "*.zip"
) -> Iterator[tuple[str, str, KlineRecord]]:  # 返回 (symbol, interval, record)
```

## 5. Writer模块改造

### 动态表名

```python
class ClickHouseWriter:
    def __init__(self, host, port, database, ...):
        self.client = ...
        self.table_prefix = "klines_"

    def _get_table_name(self, interval: str) -> str:
        """根据 interval 获取表名"""
        return f"{self.table_prefix}{interval}"

    def insert(self, record: KlineRecord) -> None:
        """根据 record.interval 选择表"""
        table = self._get_table_name(record.interval)
        self._raw_insert(table, record)
```

### 批量插入优化

```python
def insert_batch(self, records: List[KlineRecord]) -> None:
    """按 interval 分组批量插入"""
    by_interval = defaultdict(list)
    for r in records:
        by_interval[r.interval].append(r)

    for interval, recs in by_interval.items():
        table = self._get_table_name(interval)
        self._raw_insert_batch(table, recs)
```

### 表自动创建

```python
def _ensure_table_exists(self, interval: str) -> None:
    """检查表是否存在，不存在则创建"""
    if interval not in VALID_INTERVALS:
        raise ValueError(f"Invalid interval: {interval}")

    table = self._get_table_name(interval)
    if not self._table_exists(table):
        self._create_table(interval)
```

## 6. CLI命令改造

### ingest-from-dir 命令扩展

```python
@cli.command()
@click.option("--source", "-s", required=True, type=click.Path(exists=True))
@click.option("--symbols", multiple=True, help="过滤交易对")
@click.option("--intervals", multiple=True, help="过滤周期，可多次指定")
@click.option("--pattern", default="*.zip", help="文件匹配模式")
@click.pass_context
def ingest_from_dir(ctx, source, symbols, intervals, pattern):
    """从目录入库K线数据，支持多周期。

    Examples:
        # 入库所有周期
        zer0data-ingestor ingest-from-dir --source ./data/download

        # 只入库 1h 和 1d
        zer0data-ingestor ingest-from-dir --source ./data/download --intervals 1h --intervals 1d

        # 指定交易对和周期
        zer0data-ingestor ingest-from-dir --source ./data/download --symbols BTCUSDT --intervals 1h
    """
```

### 输出示例

```
Ingesting from: ./data/download
Symbols: ALL
Intervals: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d
ClickHouse: localhost:8123/zer0data

Processing interval 1m... 1500 records written ✓
Processing interval 3m... 500 records written ✓
Processing interval 5m... 300 records written ✓
...
Processing interval 1d... 12 records written ✓

Ingestion complete:
  Intervals processed: 12
  Total records written: 5000
```

## 7. SDK查询接口改造

### KlineQuery 接口

```python
class KlineQuery:
    def query(
        self,
        symbols: List[str],
        start: datetime,
        end: datetime,
        interval: str = "1m",  # 新增参数，默认1分钟
        limit: int = None,
    ) -> pl.DataFrame:
        """查询K线数据，支持多周期。

        Args:
            symbols: 交易对列表
            start: 开始时间
            end: 结束时间
            interval: K线周期 (1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d)
            limit: 返回记录数限制

        Returns:
            Polars DataFrame
        """
        table = f"klines_{interval}"
        sql = f"""
            SELECT * FROM {table}
            WHERE symbol IN ({symbols})
              AND open_time >= {start}
              AND open_time < {end}
            ORDER BY symbol, open_time
        """
        if limit:
            sql += f" LIMIT {limit}"

        return self._execute(sql)
```

### 使用示例

```python
from zer0data import Client
from datetime import datetime, timedelta

client = Client()

# 查询1小时K线
df_1h = client.kline.query(
    symbols=['BTCUSDT'],
    interval='1h',
    start=datetime.now() - timedelta(days=7),
    end=datetime.now()
)

# 查询1日K线
df_1d = client.kline.query(
    symbols=['BTCUSDT'],
    interval='1d',
    start=datetime.now() - timedelta(days=90),
    end=datetime.now()
)
```

### Interval常量（可选）

```python
class Interval:
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

# 使用
df = client.kline.query(symbols=['BTCUSDT'], interval=Interval.H1, ...)
```

## 8. 数据完整性与错误处理

### 支持的周期验证

```python
VALID_INTERVALS = [
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d"
]
```

### 下载失败处理

```python
# 某个周期失败时，记录错误但继续处理其他周期
errors = {}
for interval in intervals:
    try:
        stats = ingestor.ingest_from_directory(source, intervals=[interval])
        results[interval] = stats
    except Exception as e:
        errors[f"{interval}"] = str(e)
        logger.error(f"Failed to ingest {interval}: {e}")
```

### 数据缺失检测（可选）

```python
def check_missing_data(intervals: List[str], date: date) -> Dict[str, bool]:
    """检查指定日期各周期是否有数据"""
    result = {}
    for interval in intervals:
        table = f"klines_{interval}"
        count = query(f"SELECT count(*) FROM {table} WHERE toDate(open_time) = '{date}'")
        result[interval] = count > 0
    return result
```

## 9. 实施清单

### 阶段1：基础改造
- [ ] 修改 `KlineRecord` 添加 `interval` 字段
- [ ] 修改 `KlineParser` 支持从文件名提取interval
- [ ] 修改 `ClickHouseWriter` 支持动态表名
- [ ] 添加表自动创建逻辑

### 阶段2：CLI和SDK
- [ ] 修改 `ingest-from-dir` 命令支持 `--intervals` 参数
- [ ] 修改 `Client.kline.query()` 支持 `interval` 参数
- [ ] 更新相关测试

### 阶段3：部署和验证
- [ ] 更新Docker镜像
- [ ] 创建各周期的ClickHouse表
- [ ] 测试端到端流程
- [ ] 更新文档

## 10. 文件变更清单

| 文件 | 变更类型 | 描述 |
|------|----------|------|
| `ingestor/src/zer0data_ingestor/writer/clickhouse.py` | 修改 | KlineRecord添加interval字段，Writer支持动态表名 |
| `ingestor/src/zer0data_ingestor/parser/zip_parser.py` | 修改 | 解析文件名提取interval |
| `ingestor/src/zer0data_ingestor/cli.py` | 修改 | ingest-from-dir添加--intervals参数 |
| `sdk/src/zer0data/kline.py` | 修改 | query方法添加interval参数 |
| `docker/clickhouse/init.sql` | 修改 | 添加多周期表DDL |
| `docs/plans/2026-02-13-multi-interval-kline-implementation.md` | 新建 | 实施计划 |

## 11. 后续扩展

- [ ] 支持秒级K线 (1s)
- [ ] 数据聚合回填历史数据
- [ ] 周期转换API（从1m聚合到5m等）
- [ ] WebUI支持周期选择
