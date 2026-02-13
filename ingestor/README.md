# zer0data-ingestor

币安数据采集服务。

## Data Cleaning

K线数据在入库前会自动进行清洗：

- **去重**：删除重复时间戳的记录，保留第一条
- **有效性校验**：检查 OHLC 逻辑关系（high ≥ max(open,close), low ≤ min(open,close)）
- **时间连续性**：检测时间缺口并使用前向填充补齐
- **异常值处理**：过滤掉负数价格、成交量等无效记录

清洗统计会在日志中输出，便于监控数据质量。

## 安装

\`\`\`bash
# 使用 uv（从根目录 pyproject.toml 安装）
cd /path/to/zer0data && uv sync
# 或在模块内运行
uv sync
\`\`\`

## 使用

\`\`\`bash
# 回补数据
zer0data-ingestor backfill --symbols BTCUSDT --start-date 2023-01-01 --end-date 2023-12-31

# 每日增量
zer0data-ingestor ingest-daily

# 启动调度器
zer0data-ingestor scheduler
\`\`\`
