# zer0data 设计文档

**项目名称**: 币安永续合约数据平台
**创建日期**: 2025-02-10
**状态**: 设计阶段

## 1. 项目概述

zer0data 是一个币安永续合约数据平台，用于采集、存储和提供 1 分钟 K 线数据。

### 核心功能
- 从 binance-public-data 项目下载历史 K 线数据
- 存储到 ClickHouse 数据库
- 提供 Python SDK 供用户查询数据
- 支持定时每日增量入库
- 支持按需回补历史数据

### 技术栈
| 组件 | 技术 |
|------|------|
| 数据存储 | ClickHouse |
| 数据采集 | Python (binance-public-data) |
| SDK | Python + Polars |
| 部署 | Docker Compose |
| 调度 | APScheduler |

---

## 2. 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                    zer0data 平台                              │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│   │ 用户代码    │───→│ Python SDK  │───→│ ClickHouse  │     │
│   │             │    │ (直接查询)   │    │             │     │
│   └─────────────┘    └─────────────┘    └─────────────┘     │
│                              ▲                               │
│                              │                               │
│   ┌──────────────────────────┴─────────────────────────┐    │
│   │           Python Ingestion Service                   │    │
│   │   - binance-public-data 下载                        │    │
│   │   - ClickHouse 批量写入                             │    │
│   └──────────────────────────────────────────────────────┘    │
│                              ▲                               │
│                              │                               │
│   ┌──────────────────────────┴─────────────────────────┐    │
│   │              Scheduler (定时任务)                    │    │
│   └──────────────────────────────────────────────────────┘    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. ClickHouse 数据模型

### 表结构

```sql
CREATE TABLE klines_1m (
    symbol String,                      -- 交易对，如 BTCUSDT
    open_time DateTime,                 -- K线开盘时间
    close_time DateTime,                -- K线收盘时间

    open_price Decimal64(18, 8),        -- 开盘价
    high_price Decimal64(18, 8),        -- 最高价
    low_price Decimal64(18, 8),         -- 最低价
    close_price Decimal64(18, 8),       -- 收盘价

    volume Decimal64(18, 8),            -- 成交量(基础币)
    quote_volume Decimal64(18, 8),      -- 成交额(USDT)

    trades_count UInt32,                -- 成交笔数
    taker_buy_volume Decimal64(18, 8),  -- 主动买入成交量
    taker_buy_quote_volume Decimal64(18, 8), -- 主动买入成交额
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)       -- 按月分区
ORDER BY (symbol, open_time)           -- 主排序键
SETTINGS index_granularity = 8192;
```

### 设计说明

- **分区策略**: 按月分区，便于管理和查询优化
- **排序键**: (symbol, open_time) 支持最常见的查询模式
- **数据类型**: Decimal64(18,8) 支持加密货币的高精度价格

---

## 4. Python 数据采集服务

### 项目结构

```
ingestor/
├── pyproject.toml
├── Dockerfile
├── src/
│   └── zer0data_ingestor/
│       ├── __init__.py
│       ├── cli.py            # 命令行入口
│       ├── config.py         # 配置管理
│       ├── downloader/       # 数据下载模块
│       │   ├── __init__.py
│       │   └── binance.py    # binance-public-data 封装
│       ├── parser/           # 数据解析模块
│       │   ├── __init__.py
│       │   └── kline.py      # K线 CSV 解析
│       ├── writer/           # ClickHouse 写入模块
│       │   ├── __init__.py
│       │   └── clickhouse.py
│       └── scheduler.py      # 调度器
└── tests/
```

### 核心模块

#### Downloader (binance.py)

```python
class BinanceKlineDownloader:
    """封装 binance-public-data 项目功能"""

    def download_daily_klines(
        self,
        symbol: str,
        date: date,
        interval: str = "1m"
    ) -> Path:
        """下载指定日期的K线zip文件，返回解压后路径"""

    def list_available_symbols(self) -> list[str]:
        """获取所有可用的永续合约交易对"""

    def list_available_dates(
        self,
        symbol: str,
        start: date,
        end: date
    ) -> list[date]:
        """列出实际存在数据的日期（跳过缺失日期）"""
```

#### Parser (kline.py)

```python
def parse_klines_csv(file_path: Path) -> Iterator[KlineRecord]:
    """流式解析CSV，避免大文件内存问题"""
```

#### Writer (clickhouse.py)

```python
class ClickHouseWriter:
    def batch_insert(
        self,
        records: list[KlineRecord],
        batch_size: int = 10000
    ) -> None:
        """批量插入，使用 ClickHouse 原生批量插入协议"""
```

### CLI 接口

```bash
# 按日期范围回补
zer0data-ingestor backfill \
    --symbols BTCUSDT,ETHUSDT \
    --start-date 2020-01-01 \
    --end-date 2024-12-31

# 回补所有合约
zer0data-ingestor backfill --all-symbols \
    --start-date 2020-01-01 --end-date 2024-12-31

# 每日增量入库
zer0data-ingestor ingest-daily

# 检查缺失数据
zer0data-ingestor check-missing \
    --symbols BTCUSDT --start 2023-01-01 --end 2024-12-31

# 启动调度器
zer0data-ingestor scheduler --cron "0 1 * * *"
```

---

## 5. Python SDK

### 项目结构

```
sdk/
├── pyproject.toml
├── README.md
├── src/
│   └── zer0data/
│       ├── __init__.py
│       ├── client.py          # 主客户端
│       ├── kline.py           # K线查询
│       └── models.py          # 数据模型
└── examples/
    └── basic.py
```

### 核心接口

```python
from zer0data import Client

# 创建客户端
with Client() as client:
    # 查询K线，返回 Polars DataFrame
    df = client.kline.query(
        symbols=["BTCUSDT", "ETHUSDT"],
        start=datetime.now() - timedelta(days=7),
        end=datetime.now(),
    )

    # 使用 Polars 进行数据分析
    btc_df = df.filter(pl.col("symbol") == "BTCUSDT")
    print(btc_df.select(["open_time", "close_price", "volume"]))
```

### 返回格式

优先使用 **Polars DataFrame**，支持 Arrow 作为备选：
- Polars: 高性能内存数据分析
- Arrow: 与 pandas 生态兼容

---

## 6. Docker Compose 部署

### docker-compose.yml

```yaml
version: '3.8'

services:
  clickhouse:
    image: clickhouse/clickhouse-server:24
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - ./docker/clickhouse/init.sql:/docker-entrypoint-initdb.d/init.sql
    environment:
      CLICKHOUSE_DB: zer0data

  ingestor:
    build: ./ingestor
    volumes:
      - ./data/download:/app/data
    environment:
      CLICKHOUSE_HOST: clickhouse
      CLICKHOUSE_PORT: 8123
    depends_on:
      - clickhouse

  scheduler:
    build: ./ingestor
    command: python -m zer0data_ingestor scheduler
    environment:
      SCHEDULE_CRON: "0 1 * * *"
      TZ: Asia/Shanghai
    depends_on:
      - clickhouse
    restart: unless-stopped

volumes:
  clickhouse_data:
```

### 常用命令

```bash
# 启动服务
docker-compose up -d

# 查看日志
docker-compose logs -f ingestor

# 执行回补
docker-compose exec ingestor zer0data-ingestor backfill \
    --all-symbols --start-date 2020-01-01 --end-date 2024-12-31

# 进入 ClickHouse
docker-compose exec clickhouse clickhouse-client
```

---

## 7. 项目目录结构

```
zer0data/
├── docker-compose.yml
├── .env.example
├── docs/
│   └── plans/
│       └── 2025-02-10-zer0data-design.md
├── sdk/                    # Python SDK
│   ├── pyproject.toml
│   └── src/zer0data/
├── ingestor/               # 数据采集服务
│   ├── pyproject.toml
│   └── src/zer0data_ingestor/
├── docker/
│   └── clickhouse/
│       └── init.sql
└── data/
    └── download/           # 下载缓存
```

---

## 8. 数据采集策略

### 数据来源
- GitHub: https://github.com/binance/binance-public-data
- 数据类型: 1 分钟 K 线 (klines/daily)
- 合约范围: 所有永续合约（历史 + 活跃）

### 回补策略
- **按需逐步回补**: 支持指定日期范围，灵活控制
- **并发下载**: 支持多线程并发下载
- **断点续传**: 记录已下载文件，支持中断恢复

### 调度策略
- **每日增量**: 默认每天凌晨 1 点执行，入库前一天数据
- **手动触发**: 支持 CLI 手动触发每日入库
- **缺失检测**: 支持检测并补全缺失数据

---

## 9. 后续扩展

### 可能的扩展方向
- [ ] 支持其他周期 K 线 (5m, 15m, 1h, 4h, 1d)
- [ ] 支持逐笔成交 (aggTrade) 数据
- [ ] 支持资金费率 (fundingRate) 数据
- [ ] 提供 Web UI 查询界面
- [ ] 数据导出功能 (CSV, Parquet)

---

## 10. 依赖清单

### Python 依赖
```
# ingestor
clickhouse-connect
requests
python-dateutil
apscheduler
click

# sdk
clickhouse-connect
polars
pyarrow
```

### 系统依赖
- Docker & Docker Compose
- Python 3.11+
- ClickHouse 24+
