# zer0data-ingestor

币安数据采集服务。

## 架构

全程使用 pandas DataFrame 作为数据载体：

```
ZIP 文件 → pd.read_csv() → DataFrame 清洗 → client.insert_df() → ClickHouse
```

- **Parser**: 从 Binance 公共数据 ZIP 文件读取 CSV，输出 DataFrame
- **Cleaner**: 去重、OHLC 校验、时间缺口补齐（价格前向填充，量填 0）
- **Writer**: 通过 `insert_df()` 直接写入 ClickHouse

## Data Cleaning

K线数据在入库前会自动进行清洗：

- **去重**：删除重复时间戳的记录，保留第一条
- **有效性校验**：检查 OHLC 逻辑关系（high ≥ max(open,close), low ≤ min(open,close)）
- **时间连续性**：检测时间缺口，价格列前向填充，量/笔数列填 0
- **异常值处理**：过滤掉负数价格、成交量等无效记录

Interval 自动从文件名提取（如 `BTCUSDT-1h-2024-01-01.zip` → `1h`）。

清洗统计会在日志中输出，便于监控数据质量。

## 安装

```bash
# 使用 uv（从根目录 pyproject.toml 安装）
cd /path/to/zer0data && uv sync
```

## 使用

```bash
# 从目录导入所有数据
zer0data-ingestor ingest-from-dir --source ./data/download

# 仅导入指定交易对
zer0data-ingestor ingest-from-dir --source ./data/download --symbols BTCUSDT --symbols ETHUSDT

# 指定 ClickHouse 连接
zer0data-ingestor --clickhouse-host 10.0.0.1 --clickhouse-port 8123 ingest-from-dir --source ./data/download
```

## 环境变量

| 变量名 | 说明 | 默认值 |
|-------|------|--------|
| `CLICKHOUSE_HOST` | ClickHouse 服务器地址 | `localhost` |
| `CLICKHOUSE_PORT` | ClickHouse HTTP 端口 | `8123` |
| `CLICKHOUSE_DB` | 数据库名 | `zer0data` |
| `CLICKHOUSE_USER` | 用户名 | `default` |
| `CLICKHOUSE_PASSWORD` | 密码 | (空) |
