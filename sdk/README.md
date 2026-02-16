# zer0data SDK

SDK for querying `zer0data` ClickHouse tables.

## Install (from git subdirectory)

```bash
pip install "git+<YOUR_GIT_URL>.git@<BRANCH_OR_TAG>#subdirectory=sdk"
```

Example:

```bash
pip install "git+ssh://git@github.com/<org>/<repo>.git@main#subdirectory=sdk"
```

## Quick Start

```python
from zer0data import Client

client = Client.from_env()
df = client.get_klines(
    symbols=["BTCUSDT", "ETHUSDT"],
    interval="1h",
    start="2025-01-01T00:00:00Z",
    end="2025-01-02T00:00:00Z",
    limit=5000,
)
print(df.head())
client.close()
```

Environment variables:

- `ZER0DATA_CLICKHOUSE_HOST` (default: `localhost`)
- `ZER0DATA_CLICKHOUSE_PORT` (default: `8123`)
- `ZER0DATA_CLICKHOUSE_DATABASE` (default: `zer0data`)
- `ZER0DATA_CLICKHOUSE_USERNAME` (default: `default`)
- `ZER0DATA_CLICKHOUSE_PASSWORD` (default: empty)

## API

- `Client.get_klines(...)`: direct kline query entrypoint, returns `polars.DataFrame`
- `Client.get_symbols(market="um")`: query latest symbol metadata from `raw_exchange_info`, returns `polars.DataFrame`
- `Client.kline.query(...)`: lower-level service call
- `Client.kline.query_stream(...)`: batch stream query for large ranges

## Development

```bash
cd sdk
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
PYTHONPATH=src pytest -q tests
```

## Documentation (MkDocs)

```bash
cd sdk
pip install -r requirements-docs.txt
mkdocs serve
```

- MkDocs config: `sdk/mkdocs.yml`
- Docs content: `sdk/docs/`

### Docker preview/build

```bash
# Preview on http://127.0.0.1:8000
docker compose -f sdk/docker-compose.docs.yml up docs-serve

# Build static site to sdk/site/
docker compose -f sdk/docker-compose.docs.yml run --rm docs-build
```
