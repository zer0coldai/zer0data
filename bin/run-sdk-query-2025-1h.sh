#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

docker compose -f docker/ingestor/compose.yml build
docker compose -f docker/ingestor/compose.yml run --rm --entrypoint sh ingestor -lc \
  'PYTHONPATH=/app/sdk/src python /app/sdk/tests/run_kline_query_2025_1h.py'
