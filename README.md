# zer0data

币安永续合约数据平台 - 采集、存储和提供 1 分钟 K 线数据。

## 功能特性

- 1 分钟 K 线数据，覆盖全部历史永续合约
- 每日定时增量入库
- 按需逐步回补历史数据
- Python SDK 直接查询 ClickHouse

## 快速开始

## 项目结构

```
zer0data/
├── sdk/                    # Python SDK
├── ingestor/               # 数据采集服务
├── docker-compose.yml      # ClickHouse 部署
└── docs/
    ├── plans/              # 设计文档和实施计划
    └── ...
```

## 文档

- [设计文档](docs/plans/2025-02-10-zer0data-design.md)
- [实施计划](docs/plans/2025-02-10-zer0data-implementation.md)

## License

MIT
