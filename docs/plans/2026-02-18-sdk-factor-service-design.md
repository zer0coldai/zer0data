# SDK FactorService 设计文档

## 概述

在 zer0data SDK 中新增 `FactorService` 服务类，用于从 ClickHouse 查询已入库的 CoinMetrics 因子数据，支持所有可用因子类型。

## 设计目标

- 提供从 ClickHouse `zer0data.factors` 表查询因子数据的能力
- 支持长格式和宽格式两种数据输出格式
- 提供简洁的 API 入口，与现有 `KlineService` 风格保持一致
- 必须指定因子名称进行查询

## 架构设计

### 文件结构

```
sdk/src/zer0data/
├── __init__.py          # 导出 FactorService
├── client.py            # 添加 _factors 属性和 get_factors() 方法
├── kline.py             # 现有 KlineService（参考）
├── symbols.py           # 现有 SymbolService（参考）
└── factor.py            # 新建 FactorService
```

### 类关系

```
Client
├── _client: ClickHouse 客户端
├── _kline: KlineService (懒加载)
├── _symbols: SymbolService (懒加载)
├── _factors: FactorService (懒加载)
└── get_factors() -> factors.query() (快捷入口)
```

## FactorService API

### 核心方法

```python
class FactorService:
    def query(
        self,
        symbols: Union[str, list[str]],
        factor_names: Union[str, list[str]],
        start: Optional[Union[str, int, datetime]] = None,
        end: Optional[Union[str, int, datetime]] = None,
        format: str = "long",  # "long" or "wide"
    ) -> pl.DataFrame:
        """
        查询因子数据

        Args:
            symbols: 交易对（如 'BTCUSDT' 或 ['BTCUSDT', 'ETHUSDT']）
            factor_names: 因子名称（如 'price_usd' 或 ['price_usd', 'volume']）- 必填
            start: 开始时间（ISO 格式字符串或 Unix 时间戳毫秒）
            end: 结束时间（ISO 格式字符串或 Unix 时间戳毫秒）
            format: 返回格式，"long" 或 "wide"，默认 "long"

        Returns:
            Polars DataFrame 包含因子数据

        Raises:
            ValueError: 如果 symbols 或 factor_names 为空，或 format 无效
        """
```

### Client 快捷入口

```python
class Client:
    @property
    def factors(self) -> FactorService:
        """获取因子服务（懒加载）"""
        if self._factors is None:
            self._factors = FactorService(self._client, self.config.database)
        return self._factors

    def get_factors(
        self,
        symbols: Union[str, list[str]],
        factor_names: Union[str, list[str]],
        start: Optional[Union[str, int, datetime]] = None,
        end: Optional[Union[str, int, datetime]] = None,
        format: str = "long",
    ) -> pl.DataFrame:
        """直接查询因子数据的快捷入口"""
        return self.factors.query(
            symbols=symbols,
            factor_names=factor_names,
            start=start,
            end=end,
            format=format,
        )
```

## 数据格式

### 长格式 (long) - 默认

每行包含一个 (symbol, datetime, factor_name, factor_value) 组合：

```
┌───────────┬─────────────────────┬─────────────┬──────────────┐
│ symbol    │ datetime            │ factor_name │ factor_value │
├───────────┼─────────────────────┼─────────────┼──────────────┤
│ BTCUSDT   │ 2024-01-01 00:00:00 │ price_usd   │ 42500.50     │
│ BTCUSDT   │ 2024-01-01 00:00:00 │ volume      │ 1234567.89   │
│ ETHUSDT   │ 2024-01-01 00:00:00 │ price_usd   │ 2250.75      │
└───────────┴─────────────────────┴─────────────┴──────────────┘
```

### 宽格式 (wide)

每行一个时间点，多个因子作为列：

```
┌───────────┬─────────────────────┬───────────┬────────┐
│ symbol    │ datetime            │ price_usd │ volume │
├───────────┼─────────────────────┼───────────┼────────┤
│ BTCUSDT   │ 2024-01-01 00:00:00 │ 42500.50  │ 1.23e6 │
│ ETHUSDT   │ 2024-01-01 00:00:00 │ 2250.75   │ 987654 │
└───────────┴─────────────────────┴───────────┴────────┘
```

宽格式通过 Polars 的 `pivot()` 实现：
```python
df.pivot(
    index=['symbol', 'datetime'],
    columns='factor_name',
    values='factor_value'
)
```

## ClickHouse 查询

### 表结构

`zer0data.factors` 表：
```
symbol LowCardinality(String)
datetime DateTime64(3, 'UTC')
factor_name LowCardinality(String)
factor_value Float64
source LowCardinality(String) DEFAULT 'coinmetrics'
update_time DateTime64(3, 'UTC')
```

### SQL 查询

```sql
SELECT
    symbol,
    datetime,
    factor_name,
    factor_value
FROM zer0data.factors
WHERE symbol IN ('BTCUSDT', 'ETHUSDT')
  AND factor_name IN ('price_usd', 'volume')
  AND datetime >= '2024-01-01 00:00:00'
  AND datetime <= '2024-01-02 00:00:00'
ORDER BY symbol, datetime, factor_name
```

## 验证和错误处理

### 输入验证

1. **symbols 验证**：
   - 支持 `str` 或 `list[str]`
   - 必须非空
   - 自动去重

2. **factor_names 验证**：
   - 支持 `str` 或 `list[str]`
   - **必须非空**（与 Kline 不同，因子名称是必填的）
   - 自动去重

3. **format 验证**：
   - 只接受 `"long"` 或 `"wide"`
   - 其他值抛出 `ValueError`

4. **时间戳解析**：
   - 复用 `KlineService._parse_timestamp()` 方法
   - 支持 ISO 格式字符串、Unix 时间戳（毫秒）、`datetime` 对象

### 私有方法

```python
def _normalize_symbols(self, symbols: Union[str, list[str]]) -> list[str]:
    """标准化 symbols 输入，确保返回非空列表并去重"""

def _normalize_factor_names(self, factor_names: Union[str, list[str]]) -> list[str]:
    """标准化 factor_names 输入，确保返回非空列表（必填）并去重"""

def _parse_timestamp(self, timestamp: Union[str, int, datetime]) -> int:
    """解析时间戳为 Unix 毫秒时间戳（复用 KlineService 逻辑）"""

def _build_where_clause(
    self,
    symbols: list[str],
    factor_names: list[str],
    start: Optional[Union[str, int, datetime]],
    end: Optional[Union[str, int, datetime]],
) -> str:
    """构建 SQL WHERE 子句"""

def _validate_format(self, format: str) -> str:
    """验证并标准化 format 参数"""
```

## 使用示例

```python
from zer0data import Client

client = Client()

# 长格式查询（默认）
df_long = client.get_factors(
    symbols=['BTCUSDT', 'ETHUSDT'],
    factor_names=['price_usd', 'volume'],
    start='2024-01-01',
    end='2024-01-02',
)

# 宽格式查询
df_wide = client.get_factors(
    symbols='BTCUSDT',
    factor_names='price_usd',
    format='wide'
)

# 使用 Service 属性
df = client.factors.query(
    symbols='ETHUSDT',
    factor_names=['price_usd', 'market_cap'],
    start='2024-01-01',
)
```

## 测试策略

### 单元测试

1. **查询功能测试**：
   - `test_factor_query_long_format()` - 长格式查询
   - `test_factor_query_wide_format()` - 宽格式查询
   - `test_factor_query_single_symbol_factor()` - 单个交易对和因子
   - `test_factor_query_multiple_symbols_factors()` - 多个交易对和因子
   - `test_factor_query_with_time_range()` - 时间范围过滤

2. **验证方法测试**：
   - `test_factor_normalize_symbols()` - symbols 标准化
   - `test_factor_normalize_factor_names()` - factor_names 标准化
   - `test_factor_validate_format()` - format 参数验证
   - `test_factor_build_where_clause()` - SQL WHERE 子句构建

3. **错误处理测试**：
   - `test_factor_query_empty_symbols_raises_error()` - 空 symbols 报错
   - `test_factor_query_empty_factor_names_raises_error()` - 空 factor_names 报错
   - `test_factor_query_invalid_format_raises_error()` - 无效 format 报错

### 集成测试

- `test_client_get_factors_delegates_to_factor_service()` - 测试委托
- `test_client_get_factors_passes_all_params()` - 测试参数传递
- `test_client_factors_property_creates_service()` - 测试懒加载

## 实现计划

1. 创建 `sdk/src/zer0data/factor.py` - 实现 `FactorService`
2. 修改 `sdk/src/zer0data/client.py` - 添加 `_factors` 属性和 `get_factors()` 方法
3. 修改 `sdk/src/zer0data/__init__.py` - 导出 `FactorService`
4. 添加测试用例到 `sdk/tests/test_client.py`
5. 运行测试验证功能
