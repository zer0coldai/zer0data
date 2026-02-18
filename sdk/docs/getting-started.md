# 快速开始

## 1. 设置环境变量

```bash
export ZER0DATA_CLICKHOUSE_HOST=127.0.0.1
export ZER0DATA_CLICKHOUSE_PORT=8123
export ZER0DATA_CLICKHOUSE_DATABASE=zer0data
export ZER0DATA_CLICKHOUSE_USERNAME=default
export ZER0DATA_CLICKHOUSE_PASSWORD=
```

## 2. 调用 SDK

### 查询 Kline 数据

```python
from zer0data import Client

client = Client.from_env()
df = client.get_klines(
    symbols=["BTCUSDT", "ETHUSDT"],
    interval="1h",
    start="2025-01-01T00:00:00Z",
    end="2025-01-02T00:00:00Z",
    limit=1000,
)
print(df.head())
client.close()
```

### 查询因子数据

```python
from zer0data import Client

client = Client.from_env()

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

print(df.head())
client.close()
```

## 3. 最小连通性检查

```python
from zer0data import Client

client = Client.from_env()
df = client.get_klines(symbols=["BTCUSDT"], interval="1h", limit=5)
print(df.shape)
client.close()
```
