# zer0data SDK 独立安装与使用指南

## 📋 概述

本文档用于指导在任意 Python 项目中独立安装并使用 `zer0data` SDK。  
SDK 默认通过环境变量读取 ClickHouse 连接配置，安装后可直接调用：

- `Client.from_env()`
- `Client.get_klines(...)`

## 🏗️ 前置条件

### 环境要求

- Python `3.11+`
- 可访问的 ClickHouse 服务（HTTP 接口，默认端口 `8123`）
- 对目标仓库有读取权限（SSH 或 HTTPS）

### 推荐准备

- 建议在虚拟环境中安装
- 建议先验证本地 `git` 与 `pip` 可用

## ⚙️ 环境变量配置

SDK 支持以下环境变量（未配置时会使用默认值）：

- `ZER0DATA_CLICKHOUSE_HOST`（默认：`localhost`）
- `ZER0DATA_CLICKHOUSE_PORT`（默认：`8123`）
- `ZER0DATA_CLICKHOUSE_DATABASE`（默认：`zer0data`）
- `ZER0DATA_CLICKHOUSE_USERNAME`（默认：`default`）
- `ZER0DATA_CLICKHOUSE_PASSWORD`（默认：空字符串）

示例：

```bash
export ZER0DATA_CLICKHOUSE_HOST=127.0.0.1
export ZER0DATA_CLICKHOUSE_PORT=8123
export ZER0DATA_CLICKHOUSE_DATABASE=zer0data
export ZER0DATA_CLICKHOUSE_USERNAME=default
export ZER0DATA_CLICKHOUSE_PASSWORD=
```

## 📦 安装方式

### 方式 1：从指定提交安装（推荐用于稳定复现）

```bash
pip install "git+ssh://git@github.com/zer0coldai/zer0data.git@bd74466#subdirectory=sdk"
```

### 方式 2：从主分支安装（推荐用于跟进最新）

```bash
pip install "git+ssh://git@github.com/zer0coldai/zer0data.git@main#subdirectory=sdk"
```

### 方式 3：HTTPS 安装（无 SSH Key 场景）

```bash
pip install "git+https://github.com/zer0coldai/zer0data.git@main#subdirectory=sdk"
```

## 🚀 快速使用

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

## ✅ 安装验证

### 1. 导入验证

```bash
python -c "from zer0data import Client; print(Client.from_env)"
```

预期：输出包含 `<bound method Client.from_env ...>`，无异常。

### 2. 连通性验证

```bash
python - <<'PY'
from zer0data import Client

client = Client.from_env()
df = client.get_klines(symbols=["BTCUSDT"], interval="1h", limit=5)
print(df.shape)
client.close()
PY
```

预期：返回数据形状（如 `(5, 12)`）或在无数据时返回空表结构，不应出现导入/连接配置错误。

## 🐛 常见问题

### 问题 1：`ModuleNotFoundError: No module named 'zer0data'`

- 检查是否在当前 Python 环境安装成功
- 使用 `python -m pip show zer0data-sdk` 确认包存在
- 确认 `pip` 与 `python` 指向同一环境

### 问题 2：ClickHouse 连接失败

- 检查 `ZER0DATA_CLICKHOUSE_*` 是否配置正确
- 检查 ClickHouse 是否启动并监听 HTTP 端口
- 本机 Docker 场景确认端口映射与防火墙设置

### 问题 3：Git 地址安装失败

- SSH 失败时改用 HTTPS 地址
- 检查仓库权限和网络连通性
- 锁定到具体提交安装，避免分支变更影响

## ✅ 操作检查清单

- [ ] 已创建并激活 Python 虚拟环境
- [ ] 已设置 `ZER0DATA_CLICKHOUSE_*` 环境变量
- [ ] 已执行 `pip install ...#subdirectory=sdk`
- [ ] 已通过导入验证命令
- [ ] 已通过最小查询脚本验证
- [ ] 已记录本次安装使用的仓库地址和提交号
