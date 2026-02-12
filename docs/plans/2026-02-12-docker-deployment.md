# Docker Deployment Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Containerize the entire zer0data service stack (ClickHouse, downloader, ingestor) for local development and testing.

**Architecture:** Three separate services with independent Docker images. ClickHouse runs independently with data stored at `/data/clickhouse` on host. downloader and ingestor containers share `/data/download` with host for data files. Services use `host.docker.internal` to connect to ClickHouse.

**Tech Stack:** Docker, Docker Compose, Python 3.13, uv package manager, ClickHouse 24

---

## Task 1: Create ClickHouse Docker Compose Configuration

**Files:**
- Create: `docker/clickhouse/compose.yml`

**Step 1: Create docker/clickhouse directory**

Run: `mkdir -p docker/clickhouse`
Expected: Directory created

**Step 2: Create compose.yml file**

```bash
cat > docker/clickhouse/compose.yml << 'EOF'
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
EOF
```

Expected: File created at `docker/clickhouse/compose.yml`

**Step 3: Verify YAML syntax**

Run: `docker compose -f docker/clickhouse/compose.yml config`
Expected: No syntax errors, output shows parsed configuration

**Step 4: Copy init.sql to docker/clickhouse directory**

Run: `cp docker/clickhouse/init.sql docker/clickhouse/init.sql` (if it exists in original docker/ directory)
Or verify the file exists at the correct location

**Step 5: Commit**

```bash
git add docker/clickhouse/
git commit -m "feat: add ClickHouse docker compose configuration"
```

---

## Task 2: Create Downloader Dockerfile

**Files:**
- Create: `docker/downloader/Dockerfile`

**Step 1: Create docker/downloader directory**

Run: `mkdir -p docker/downloader`
Expected: Directory created

**Step 2: Create Dockerfile**

```bash
cat > docker/downloader/Dockerfile << 'EOF'
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
EOF
```

Expected: File created at `docker/downloader/Dockerfile`

**Step 3: Verify Dockerfile syntax**

Run: `docker build -f docker/downloader/Dockerfile --check .`
Expected: No syntax errors

**Step 4: Commit**

```bash
git add docker/downloader/
git commit -m "feat: add downloader Dockerfile"
```

---

## Task 3: Create Downloader Docker Compose Configuration

**Files:**
- Create: `docker/downloader/compose.yml`

**Step 1: Create compose.yml file**

```bash
cat > docker/downloader/compose.yml << 'EOF'
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
EOF
```

Expected: File created at `docker/downloader/compose.yml`

**Step 2: Verify YAML syntax**

Run: `docker compose -f docker/downloader/compose.yml config`
Expected: No syntax errors

**Step 3: Commit**

```bash
git add docker/downloader/compose.yml
git commit -m "feat: add downloader docker compose configuration"
```

---

## Task 4: Create Ingestor Dockerfile

**Files:**
- Create: `docker/ingestor/Dockerfile`

**Step 1: Create docker/ingestor directory**

Run: `mkdir -p docker/ingestor`
Expected: Directory created

**Step 2: Create Dockerfile**

```bash
cat > docker/ingestor/Dockerfile << 'EOF'
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
EOF
```

Expected: File created at `docker/ingestor/Dockerfile`

**Step 3: Verify Dockerfile syntax**

Run: `docker build -f docker/ingestor/Dockerfile --check ingestor`
Expected: No syntax errors

**Step 4: Commit**

```bash
git add docker/ingestor/
git commit -m "feat: add ingestor Dockerfile"
```

---

## Task 5: Create Ingestor Docker Compose Configuration

**Files:**
- Create: `docker/ingestor/compose.yml`

**Step 1: Create compose.yml file**

```bash
cat > docker/ingestor/compose.yml << 'EOF'
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
EOF
```

Expected: File created at `docker/ingestor/compose.yml`

**Step 2: Verify YAML syntax**

Run: `docker compose -f docker/ingestor/compose.yml config`
Expected: No syntax errors

**Step 3: Commit**

```bash
git add docker/ingestor/compose.yml
git commit -m "feat: add ingestor docker compose configuration"
```

---

## Task 6: Build Downloader Image

**Files:**
- Test: `docker/downloader/compose.yml`, `docker/downloader/Dockerfile`

**Step 1: Build downloader image**

Run: `docker compose -f docker/downloader/compose.yml build`
Expected: Build completes successfully, image `zer0data-downloader` created

**Step 2: Verify image was created**

Run: `docker images | grep zer0data-downloader`
Expected: Output shows zer0data-downloader image

**Step 3: Test image with help command**

Run: `docker compose -f docker/downloader/compose.yml run --rm downloader --help`
Expected: Output shows download-kline.py help message

**Step 4: Commit any changes**

```bash
git add -A
git commit -m "test: verify downloader image builds successfully"
```

---

## Task 7: Build Ingestor Image

**Files:**
- Test: `docker/ingestor/compose.yml`, `docker/ingestor/Dockerfile`

**Step 1: Build ingestor image**

Run: `docker compose -f docker/ingestor/compose.yml build`
Expected: Build completes successfully, image `ingestor` created

**Step 2: Verify image was created**

Run: `docker images | grep ingestor`
Expected: Output shows ingestor image

**Step 3: Test image with help command**

Run: `docker compose -f docker/ingestor/compose.yml run --rm ingestor --help`
Expected: Output shows zer0data-ingestor help message

**Step 4: Commit any changes**

```bash
git add -A
git commit -m "test: verify ingestor image builds successfully"
```

---

## Task 8: Create Quick Start Documentation

**Files:**
- Create: `docker/README.md`

**Step 1: Create README**

```bash
cat > docker/README.md << 'EOF'
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
EOF
```

Expected: File created at `docker/README.md`

**Step 2: Commit**

```bash
git add docker/README.md
git commit -m "docs: add Docker deployment quick start guide"
```

---

## Task 9: Update Main README

**Files:**
- Modify: `README.md`

**Step 1: Add Docker deployment section to README.md**

Add after the existing "快速开始" section:

```markdown
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
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add Docker deployment instructions to main README"
```

---

## Task 10: Integration Test (End-to-End)

**Files:**
- All docker configurations

**Step 1: Create test data directories**

Run: `sudo mkdir -p /data/clickhouse /data/download && sudo chown $USER:$USER /data/clickhouse /data/download`
Expected: Directories created with correct permissions

**Step 2: Start ClickHouse**

Run: `docker compose -f docker/clickhouse/compose.yml up -d`
Expected: Container starts successfully

**Step 3: Wait for ClickHouse to be healthy**

Run: `docker compose -f docker/clickhouse/compose.yml ps`
Expected: Status shows "healthy"

**Step 4: Test downloader (dry run with help)**

Run: `docker compose -f docker/downloader/compose.yml run --rm downloader --help`
Expected: Help output displayed

**Step 5: Test ingestor (dry run with help)**

Run: `docker compose -f docker/ingestor/compose.yml run --rm ingestor --help`
Expected: Help output displayed

**Step 6: Stop ClickHouse**

Run: `docker compose -f docker/clickhouse/compose.yml down`
Expected: Container stopped and removed

**Step 7: Commit final test notes**

```bash
git add -A
git commit -m "test: complete Docker deployment integration test"
```

---

## Completion Checklist

- [ ] ClickHouse compose.yml created and tested
- [ ] Downloader Dockerfile created
- [ ] Downloader compose.yml created
- [ ] Ingestor Dockerfile created
- [ ] Ingestor compose.yml created
- [ ] All images build successfully
- [ ] Documentation updated
- [ ] Integration test passes
