# Docker Deployment

## Prerequisites

- Docker
- Docker Compose

## Initial Setup

### 1. Create data directories

```bash
sudo mkdir -p /data/clickhouse /data/download
sudo chown $USER:$USER /data/clickhouse /data/download
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
  --type futures --symbols BTCUSDT --interval 1m --date 2024-01-01
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

### View container logs

```bash
# ClickHouse logs
docker logs zer0data-clickhouse

# Downloader logs
docker logs zer0data-downloader

# Ingestor logs
docker logs zer0data-ingestor
```