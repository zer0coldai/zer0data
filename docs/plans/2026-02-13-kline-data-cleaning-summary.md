# K-line Data Cleaning Implementation Summary

## Overview
实现了 K线数据入库前的自动清洗功能，确保数据质量。

## Architecture
在 ingestor 的数据流程中添加了 cleaner 模块：

```
Parser → Cleaner → Writer
```

- **Parser**: 解析 CSV，yield KlineRecord
- **Cleaner**: 接收记录列表，执行清洗，返回清洗后的记录
- **Writer**: 写入 ClickHouse

## Cleaning Rules

1. **Duplicate Removal**
   - 按时间戳去重，保留第一条

2. **OHLC Validation**
   - 检查 high ≥ max(open, close)
   - 检查 low ≤ min(open, close)
   - 检查 high ≥ low
   - 过滤负数价格

3. **Time Gap Filling**
   - 创建完整时间索引（1分钟间隔）
   - 使用前向填充补齐缺失时间点
   - volume 缺失填 0

## Files Modified

- `ingestor/src/zer0data_ingestor/cleaner/` - 新增清洗模块
- `ingestor/src/zer0data_ingestor/ingestor.py` - 集成清洗流程
- `ingestor/tests/cleaner/` - 单元测试

## Testing

```bash
cd ingestor && pytest tests/cleaner/ -v
```
