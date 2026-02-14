# 常见问题

## `ModuleNotFoundError: No module named 'zer0data'`

- 确认安装命令包含 `#subdirectory=sdk`
- 确认 `pip` 和 `python` 指向同一环境
- 执行：`python -m pip show zer0data-sdk`

## ClickHouse 连接失败

- 检查 `ZER0DATA_CLICKHOUSE_*` 环境变量
- 检查 ClickHouse 是否监听 `8123` 端口
- 检查容器端口映射和防火墙

## 安装时 Git 鉴权失败

- SSH 不通时改用 HTTPS 地址
- 确认仓库权限正常
