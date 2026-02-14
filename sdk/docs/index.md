# zer0data SDK 文档

`zer0data` 是一个面向 ClickHouse 的行情数据 SDK，当前提供 Kline 查询能力。

## 你可以在这里找到

- 安装方式（从 Git 子目录直接安装）
- 环境变量配置规范
- `Client.from_env()` 的推荐调用方式
- `Client.get_klines(...)` 查询示例
- 常见问题排查

## 快速入口

- [安装指南](installation.md)
- [快速开始](getting-started.md)
- [配置说明](configuration.md)
- [客户端 API](api/client.md)

## 设计原则

- SDK 可独立安装：`pip install ...#subdirectory=sdk`
- 默认走环境变量配置，避免在业务代码重复写连接参数
- 提供稳定、简洁、可脚本化的调用接口
