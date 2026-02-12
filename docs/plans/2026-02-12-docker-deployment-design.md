# Docker Deployment Design

## Overview

Containerize the entire zer0data service stack for local development and testing, with ClickHouse as a separate database service.

## Architecture

### Directory Structure

```
/data/                           # Data root directory (host)
├── clickhouse/                   # ClickHouse data files
└── download/                     # Downloaded raw data

zer0data/
├── docker/
│   ├── clickhouse/
│   │   ├── compose.yml
│   │   └── init.sql
│   ├── downloader/
│   │   ├── compose.yml
│   │   └── Dockerfile
│   └── ingestor/
│       ├── compose.yml
│       └── Dockerfile
├── downloader/                   # downloader service source
├── ingestor/                     # ingestor service source
└── sdk/
```

### Services

| Service | Image | Ports | Data Directory |
|---------|-------|-------|-----------------|
| clickhouse | clickhouse/clickhouse-server:24 | 8123, 9000 | `/data/clickhouse` |
| downloader | zer0data-downloader:latest | - | `/data/download` |
| ingestor | zer0data-ingestor:latest | - | `/data/download` |

### Data Flow

```
[downloader container]
    └── downloads to /data (actual: /data/download)
           ↓
[ingestor container]
    └── reads from /data (actual: /data/download)
           ↓
[ClickHouse container]
    └── stores data at /data/clickhouse
        └── HTTP: host:8123
```

## Dockerfiles

### downloader/Dockerfile

```dockerfile
FROM python:3.13-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies
COPY requirements.txt .
RUN uv pip install --no-cache -r requirements.txt

# Copy code
COPY . .

# Data directory
VOLUME ["/data"]

# Default command
ENTRYPOINT ["python", "download-kline.py"]
CMD ["--help"]
```

### ingestor/Dockerfile

```dockerfile
FROM python:3.13-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies
COPY pyproject.toml ./
RUN uv pip install --no-cache . --no-dev

# Copy code
COPY . .

# Data directory
VOLUME ["/data"]

# Default command
ENTRYPOINT ["zer0data-ingestor"]
CMD ["--help"]
```

## Docker Compose Files

### docker/clickhouse/compose.yml

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:24
    container_name: zer0data-clickhouse
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - /data/clickhouse:/data/clickhouse
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql
    environment:
      CLICKHOUSE_DB: zer0data
    command: >
      clickhouse server
      --path=/data/clickhouse
      --listen_host=0.0.0.0
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 10s
      timeout: 5s
      retries: 5
    logging:
      driver: "json-file"
      options:
        max-size: "100m"
        max-file: "5"
```

### docker/downloader/compose.yml

```yaml
services:
  downloader:
    build:
      context: ../../binance-public-data/python
      dockerfile: ../../../docker/downloader/Dockerfile
    container_name: zer0data-downloader
    volumes:
      - /data/download:/data
    environment:
      STORE_DIRECTORY: /data
    logging:
      driver: "json-file"
      options:
        max-size: "100m"
        max-file: "5"
```

### docker/ingestor/compose.yml

```yaml
services:
  ingestor:
    build:
      context: ../../ingestor
      dockerfile: ../docker/ingestor/Dockerfile
    container_name: zer0data-ingestor
    volumes:
      - /data/download:/data
    environment:
      CLICKHOUSE_HOST: host.docker.internal
      CLICKHOUSE_PORT: 8123
      CLICKHOUSE_DB: zer0data
    logging:
      driver: "json-file"
      options:
        max-size: "100m"
        max-file: "5"
```

## Usage

### Initial Setup

```bash
# 1. Create data directories
sudo mkdir -p /data/clickhouse /data/download
sudo chown $USER:$USER /data/clickhouse /data/download

# 2. Build images
docker compose -f docker/downloader/compose.yml build
docker compose -f docker/ingestor/compose.yml build

# 3. Start ClickHouse
docker compose -f docker/clickhouse/compose.yml up -d
```

### Daily Operations

```bash
# Download data
docker compose -f docker/downloader/compose.yml run --rm downloader \
  --type futures --symbols BTCUSDT --interval 1m --date 2024-01-01

# Ingest to database
docker compose -f docker/ingestor/compose.yml run --rm ingestor \
  ingest-from-dir --source /data --symbols BTCUSDT
```

### Networking

Services use `host.docker.internal` to connect to ClickHouse on the host, avoiding complex container networking.

## Data Directory Layout

```
/data/
├── clickhouse/
│   ├── data/
│   ├── metadata/
│   └── logs/
└── download/
    └── futures/um/
        └── 1m/
            └── BTCUSDT/
                └── BTCUSDT-1m-2024-01-01.zip
```

## Error Handling

| Issue | Solution |
|-------|----------|
| `/data` permission denied | `sudo chown -R $USER:$USER /data` |
| ClickHouse connection failed | Check port 8123 availability |
| Corrupted data files | Delete and re-download |

## Health Checks

| Service | Check Method |
|---------|--------------|
| ClickHouse | `clickhouse-client --query "SELECT 1"` |
| downloader | Exit code (one-time task) |
| ingestor | Exit code (one-time task) |
