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

## `Client.get_factors(...)`

查询因子数据（来自 `zer0data.factors` 表），返回 `polars.DataFrame`。

```python
# 长格式查询（默认）
df = client.get_factors(
    symbols=["BTCUSDT", "ETHUSDT"],
    factor_names=["price_usd", "volume"],
    start="2024-01-01",
    end="2024-01-02",
)

# 宽格式查询
df_wide = client.get_factors(
    symbols="BTCUSDT",
    factor_names="price_usd",
    format="wide"
)
```

参数说明：

- `symbols`: `str | list[str]` - 交易对（必填）
- `factor_names`: `str | list[str]` - 因子名称（必填）
- `start`: `str | int | datetime | None` - 开始时间
- `end`: `str | int | datetime | None` - 结束时间
- `format`: `str` - 输出格式，`"long"` 或 `"wide"`，默认 `"long"`

**数据格式：**

- **长格式**：每行一个 `(symbol, datetime, factor_name, factor_value)` 组合
- **宽格式**：每行一个 `(symbol, datetime)` 时间点，多个因子作为列

## `Client.write_factors(...)`

写入 long 格式因子数据到 `zer0data.factors`，返回写入行数 `int`。
默认会跳过 `factor_value` 中的 `NaN`、`inf`、`-inf` 和非数字值。

```python
import polars as pl

rows = pl.DataFrame(
    {
        "symbol": ["BTCUSDT", "ETHUSDT"],
        "datetime": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
        "factor_name": ["price_usd", "price_usd"],
        "factor_value": [42500.5, 2250.75],
    }
)

written = client.write_factors(rows, source="sdk")
print(written)  # 2
```

参数说明：

- `data`: `polars.DataFrame | pandas.DataFrame` - 必须包含 `symbol`, `datetime`, `factor_name`, `factor_value`
- `source`: `str` - 数据来源标记，默认 `"sdk"`

## 关闭连接

```python
client.close()
```
