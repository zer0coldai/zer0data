# CoinMetrics Docker 导入 README

本文档用于在 `zer0data` 项目中，通过 Docker 验证 CoinMetrics 因子数据导入 ClickHouse。

## 1. 前置条件

- 本地已安装 Docker / Docker Compose
- 项目根目录存在 `.env`
- `.env` 中已配置远程 ClickHouse 连接信息（示例）：

```env
CLICKHOUSE_HOST=192.168.31.30
CLICKHOUSE_PORT=8123
CLICKHOUSE_DB=zer0data
CLICKHOUSE_USER=zer0cold
CLICKHOUSE_PASSWORD=your_password
```

## 2. 关键配置说明

`docker/ingestor/compose.yml` 已移除硬编码 `CLICKHOUSE_HOST/PORT/DB`，默认从 `.env` 注入。

可用以下命令确认容器最终拿到的配置：

```bash
docker compose -f docker/ingestor/compose.yml run --rm ingestor env | grep ^CLICKHOUSE_
```

## 3. 构建并运行

### 3.1 构建 ingestor 镜像

```bash
docker compose -f docker/ingestor/compose.yml build
```

### 3.2 小范围导入验证（BTC/ETH）

```bash
docker compose -f docker/ingestor/compose.yml run --rm ingestor \
  ingest-source coinmetrics \
  --symbols btc --symbols eth \
  --head 2 --tail 2 \
  --max-partitions-per-insert-block 1000
```

说明：
- `--max-partitions-per-insert-block 1000` 用于避免跨多年数据写入时触发 ClickHouse 默认 100 分区上限。
- 当前日志已简化为关键节点；CSV 前后行明细默认只在 `DEBUG` 下输出。

### 3.3 全量导入

```bash
docker compose -f docker/ingestor/compose.yml run --rm ingestor \
  ingest-source coinmetrics \
  --head 1 --tail 1 \
  --max-partitions-per-insert-block 1000
```

## 4. 导入后验证

### 4.1 验证有无写入

```bash
docker exec -it zer0data-clickhouse clickhouse-client -q "
SELECT symbol, count() AS rows
FROM zer0data.factors
WHERE source='coinmetrics' AND symbol IN ('btc','eth')
GROUP BY symbol
ORDER BY symbol;
"
```

### 4.2 验证时间范围

```bash
docker exec -it zer0data-clickhouse clickhouse-client -q "
SELECT
  min(datetime) AS min_dt,
  max(datetime) AS max_dt,
  count() AS rows
FROM zer0data.factors
WHERE source='coinmetrics' AND symbol='btc';
"
```

## 5. 常见问题

### 5.1 `Connection refused`

通常是 ClickHouse 地址不可达或端口未开放。先检查：

```bash
docker compose -f docker/ingestor/compose.yml run --rm ingestor env | grep ^CLICKHOUSE_
```

确保容器内看到的 `CLICKHOUSE_HOST/PORT` 与目标实例一致。

### 5.2 `TOO_MANY_PARTS` / `max_partitions_per_insert_block`

原因：历史数据跨度过大，单次 insert 涉及分区数超过 ClickHouse 默认限制（100）。

处理：调高参数，例如：

```bash
--max-partitions-per-insert-block 1000
```

若仍报错，可临时提高到 `2000` 再测试。

### 5.3 需要更详细日志

当前 CLI 默认 `INFO` 为关键节点。要看 CSV 头尾明细，请使用 `DEBUG`（本地运行模块时）：

```bash
PYTHONPATH=ingestor/src python -m zer0data_ingestor.cli \
  ingest-source coinmetrics --symbols btc --dry-run
```
