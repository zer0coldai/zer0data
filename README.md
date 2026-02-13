# zer0data

币安永续合约数据平台 - 采集、存储和提供多周期 K 线数据（如 1m/1h/1d）。

## 功能特性

- 多周期 K 线数据，覆盖全部历史永续合约
- 每日定时增量入库
- 按需逐步回补历史数据
- Python SDK 直接查询 ClickHouse

## 快速开始

### 1. 创建数据目录

```bash
mkdir -p data/clickhouse data/download
```

### 2. 启动 ClickHouse

```bash
docker compose -f docker/clickhouse/compose.yml up -d
```

### 3. 构建服务镜像

```bash
docker compose -f docker/downloader/compose.yml build
docker compose -f docker/ingestor/compose.yml build
```

## 环境管理（推荐统一流程）

本项目建议统一使用 `uv + .venv`（本地）与 Docker（运行服务）。

```bash
# 1) 本地同步依赖（与项目锁文件一致）
uv sync --python 3.11 --extra dev

# 2) 仅在本地需要时运行 SDK 脚本
PYTHONPATH=sdk/src .venv/bin/python sdk/tests/run_kline_query_2025_1h.py

# 3) 需要完全隔离时，使用 Docker 运行同一脚本
docker compose -f docker/ingestor/compose.yml run --rm --entrypoint sh ingestor -lc \
  'PYTHONPATH=/app/sdk/src python /app/sdk/tests/run_kline_query_2025_1h.py'
```

### 4. 下载 K 线数据

```bash
docker compose -f docker/downloader/compose.yml run --rm downloader \
  -t um -s BTCUSDT -i 1m -d 2024-01-01
```

`download-kline.py` 使用短参数：`-t`(市场类型：`spot`/`um`/`cm`)、`-s`(交易对)、`-i`(周期)、`-d`(日期)。

下载所有 USD-M 合约的 1m 月度数据（不传 `-s` 即全合约）：

```bash
docker compose -f docker/downloader/compose.yml run --rm downloader \
  -t um -i 1m -skip-daily 1
```

### 5. 入库到 ClickHouse（支持多周期）

```bash
docker compose -f docker/ingestor/compose.yml run --rm ingestor \
  ingest-from-dir --source /data --pattern "**/*.zip"
```

按交易对过滤：

```bash
docker compose -f docker/ingestor/compose.yml run --rm ingestor \
  ingest-from-dir --source /data --symbols BTCUSDT --symbols ETHUSDT
```

按周期过滤（通过文件名 pattern）：

```bash
# 仅导入 1h
docker compose -f docker/ingestor/compose.yml run --rm ingestor \
  ingest-from-dir --source /data --pattern "**/*-1h-*.zip"

# 仅导入 1d
docker compose -f docker/ingestor/compose.yml run --rm ingestor \
  ingest-from-dir --source /data --pattern "**/*-1d-*.zip"
```

默认情况下，ingestor 会根据文件周期（如 `1m`/`1h`/`1d`）自动适配清洗间隔。
如需手动覆盖，可显式指定 `--cleaner-interval-ms`。

```bash
# 可选：手动指定清洗间隔（1h = 3600000ms）
docker compose -f docker/ingestor/compose.yml run --rm ingestor \
  --cleaner-interval-ms 3600000 \
  ingest-from-dir --source /data --pattern "**/*-1h-*.zip"
```

### 6. 查询数据

```python
from zer0data import Client
from datetime import datetime

df = Client().kline.query(
    symbols=['BTCUSDT'],
    start=datetime(2024, 1, 1),
    end=datetime(2024, 1, 2)
)
print(df)
```

详细文档请参考 [Docker 部署指南](docker/README.md)

## 项目结构

```
zer0data/
├── sdk/                    # Python SDK
├── ingestor/               # 数据采集服务
├── docker/                 # Docker 部署配置
│   ├── clickhouse/         # ClickHouse 服务
│   ├── downloader/         # 数据下载服务
│   └── ingestor/           # 数据入库服务
└── docs/
    ├── plans/              # 设计文档和实施计划
    └── ...
```

## 文档

- [设计文档](docs/plans/2025-02-10-zer0data-design.md)
- [实施计划](docs/plans/2025-02-10-zer0data-implementation.md)

## License

MIT
