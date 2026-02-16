# 客户端 API

## `Client.from_env()`

从环境变量创建客户端。

```python
from zer0data import Client

client = Client.from_env()
```

## `Client(...)`

创建客户端，参数可覆盖环境变量。

```python
from zer0data import Client

client = Client(host="127.0.0.1", port=8123, database="zer0data")
```

## `Client.get_klines(...)`

直接查询 Kline 数据，返回 `polars.DataFrame`。

```python
df = client.get_klines(
    symbols=["BTCUSDT"],
    interval="1h",
    start="2025-01-01T00:00:00Z",
    end="2025-01-02T00:00:00Z",
    limit=1000,
)
```

参数说明：

- `symbols`: `str | list[str]`
- `interval`: 例如 `1m`、`1h`、`1d`
- `start`: `str | int | datetime | None`
- `end`: `str | int | datetime | None`
- `limit`: `int | None`

## `Client.get_symbols(...)`

查询最新一版 `exchangeInfo` 的 symbols 元数据（来自 `raw_exchange_info`），返回 `polars.DataFrame`。

```python
df = client.get_symbols(market="um")

usdt_df = client.get_symbols(market="um", quote_asset="USDT")

tradable_usdt_df = client.get_symbols(
    market="um",
    quote_asset="USDT",
    exclude_stable_base=True,
)
```

参数说明：

- `market`: `str`，可选 `spot` / `um` / `cm`，默认 `um`
- `quote_asset`: `str | None`，例如 `USDT`；不传时返回该市场全部 symbols
- `exclude_stable_base`: `bool`，默认 `False`；设为 `True` 时过滤稳定币 `baseAsset`

返回字段：

- `symbol`
- `quoteAsset`
- `onboardDate`
- `deliveryDate`
- `underlyingType`
- `status`

## 关闭连接

```python
client.close()
```
