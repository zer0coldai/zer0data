# zer0data SDK

Binance perpetual futures data SDK for efficient data access and analysis.

## Installation

```bash
# 使用 uv（从根目录 pyproject.toml 安装）
cd /path/to/zer0data && uv sync
# 或在模块内运行
uv sync
```

## Usage

```python
from zer0data import Client

# Connect to ClickHouse
client = Client(
    host="localhost",
    port=8123,
    database="zer0data",
    username="default",
    password=""
)

# Fetch historical trades
trades = client.get_trades(
    symbol="BTCUSDT",
    start_date="2024-01-01",
    end_date="2024-01-31"
)

# Fetch liquidations
liquidations = client.get_liquidations(
    symbol="BTCUSDT",
    start_date="2024-01-01",
    end_date="2024-01-31"
)

# Get aggregated funding rates
funding = client.get_funding_rates(
    symbol="BTCUSDT",
    interval="1h"
)

# Query with custom SQL
result = client.query("SELECT * FROM trades WHERE symbol = 'BTCUSDT' LIMIT 100")
```

## Development

```bash
# Install development dependencies
poetry install --with dev

# Run tests
poetry run pytest

# Format code
poetry run black .
poetry run ruff check .
```

## License

MIT
