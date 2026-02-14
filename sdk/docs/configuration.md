# 配置说明

`Client.from_env()` 和 `Client()` 默认从环境变量读取 ClickHouse 配置。

## 环境变量

- `ZER0DATA_CLICKHOUSE_HOST`（默认：`localhost`）
- `ZER0DATA_CLICKHOUSE_PORT`（默认：`8123`）
- `ZER0DATA_CLICKHOUSE_DATABASE`（默认：`zer0data`）
- `ZER0DATA_CLICKHOUSE_USERNAME`（默认：`default`）
- `ZER0DATA_CLICKHOUSE_PASSWORD`（默认：空）

## 参数优先级

1. 显式传入 `Client(...)` 参数
2. 环境变量 `ZER0DATA_CLICKHOUSE_*`
3. 内置默认值

## 推荐方式

- 服务/CI 场景：用环境变量 + `Client.from_env()`
- 临时调试：显式传参覆盖
