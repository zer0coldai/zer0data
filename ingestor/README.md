# zer0data-ingestor

币安数据采集服务。

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
