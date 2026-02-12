# zer0data

币安永续合约数据平台 - 采集、存储和提供 1 分钟 K 线数据。

## 功能特性

- 1 分钟 K 线数据，覆盖全部历史永续合约
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

### 4. 下载 K 线数据

```bash
docker compose -f docker/downloader/compose.yml run --rm downloader \
  -t um -s BTCUSDT -i 1m -d 2024-01-01
```

`download-kline.py` 使用短参数：`-t`(市场类型：`spot`/`um`/`cm`)、`-s`(交易对)、`-i`(周期)、`-d`(日期)。

### 5. 入库到 ClickHouse

```bash
docker compose -f docker/ingestor/compose.yml run --rm ingestor \
  ingest-from-dir --source /data --symbols BTCUSDT
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
