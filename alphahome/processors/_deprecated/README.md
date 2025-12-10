# Deprecated processors tasks/components

本目录用于归档已迁出的旧任务/组件，避免误删历史实现：

- `tasks/stock/stock_adjdaily_processor.py`
- `tasks/stock/v2/stock_adjusted_price_v2.py`
- `pit/manager.py` 及相关占位

这些文件不再在 `processors` 中导出或注册，不参与当前测试与生产流程。如需参考历史实现，可在此查阅；如需恢复，请明确评估兼容性与调用方。

