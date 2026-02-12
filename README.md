# zer0data

币安永续合约数据平台 - 采集、存储和提供 1 分钟 K 线数据。

## 功能特性

- 1 分钟 K 线数据，覆盖全部历史永续合约
- 每日定时增量入库
- 按需逐步回补历史数据
- Python SDK 直接查询 ClickHouse

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

## Docker 部署

### 1. 创建数据目录

```bash
sudo mkdir -p /data/clickhouse /data/download
sudo chown $USER:$USER /data/clickhouse /data/download
```

### 2. 启动 ClickHouse

```bash
docker compose -f docker/clickhouse/compose.yml up -d
```

### 3. 构建镜像

```bash
docker compose -f docker/downloader/compose.yml build
docker compose -f docker/ingestor/compose.yml build
```

### 4. 下载数据

```bash
docker compose -f docker/downloader/compose.yml run --rm downloader \
  --type futures --symbols BTCUSDT --interval 1m --date 2024-01-01
```

### 5. 入库

```bash
docker compose -f docker/ingestor/compose.yml run --rm ingestor \
  ingest-from-dir --source /data --symbols BTCUSDT
```

详细文档请参考 [Docker 部署指南](docker/README.md)

## 项目结构

```
zer0data/
├── sdk/                    # Python SDK
├── ingestor/               # 数据采集服务
├── docker-compose.yml      # ClickHouse 部署
└── docs/
    ├── plans/              # 设计文档和实施计划
    └── ...
```

## 文档

- [设计文档](docs/plans/2025-02-10-zer0data-design.md)
- [实施计划](docs/plans/2025-02-10-zer0data-implementation.md)

## License

MIT
