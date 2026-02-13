# Docker Deployment

## Prerequisites

- Docker
- Docker Compose
- uv (for local development)

## Initial Setup

### 1. Create data directories

```bash
mkdir -p data/clickhouse data/download
```

### 2. Start ClickHouse

```bash
docker compose -f clickhouse/compose.yml up -d
```

### 3. Verify ClickHouse is running

```bash
docker compose -f clickhouse/compose.yml ps
```

## Building Images

```bash
# Build downloader image
docker compose -f downloader/compose.yml build

# Build ingestor image
docker compose -f ingestor/compose.yml build
```

## Daily Operations

### Download Data

```bash
docker compose -f downloader/compose.yml run --rm downloader \
  -t um -s BTCUSDT -i 1m -d 2024-01-01
```

`download-kline.py` 参数为 `-t/-s/-i/-d`，其中永续合约对应 `-t um`（USD-M futures）。

下载所有 USD-M 合约的 1m 月度数据（不传 `-s` 即全合约）：

```bash
docker compose -f downloader/compose.yml run --rm downloader \
  -t um -i 1m -skip-daily 1
```

### Ingest to Database

```bash
docker compose -f ingestor/compose.yml run --rm ingestor \
  ingest-from-dir --source /data --symbols BTCUSDT
```

## Troubleshooting

### Permission denied on /data

```bash
sudo chown -R $USER:$USER /data
```

### ClickHouse connection failed

Check if ClickHouse is running:
```bash
docker compose -f clickhouse/compose.yml ps
```

Check logs:
```bash
docker compose -f clickhouse/compose.yml logs -f
```

### Package Management

This project uses **uv** as the package manager for unified dependency management across Docker and local development.

### View container logs

# Downloader logs
docker logs zer0data-downloader

# Ingestor logs
docker logs zer0data-ingestor
```
