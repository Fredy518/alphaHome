# fundpos 0.4.0 AlphaHome 内聚发布

发布日期：2026-09-14

本版本把全部生产计算代码、模型协议、数据库迁移、固定基金范围、证据种子和测试迁入
AlphaHome 的 `packages/fundpos`。生产运行不再读取原“公募基金行业仓位测算”项目。

主要变化：

- 独立 Python 3.12 子包和锁文件保留求解器环境隔离；
- 运行快照和补充证据统一写入 AlphaHome 的 `logs/fundpos-engine`；
- Git 标签冻结 AlphaHome 内的引擎、编排器和调度入口，允许仓库其他模块独立演进；
- 引擎清单使用冻结发布修订，不因 AlphaHome 其他模块提交而改变逻辑运行身份；
- AlphaDB `fundpos` schema 成为唯一维护和消费层，生产任务不生成 Excel、HTML 或 JSON
  业务报表，入库后直接以冻结 Parquet 对数据库逐行勾稽；
- 删除 Excel 导出实现及 `openpyxl` 生产依赖；
- 版本化证据种子可幂等初始化，可变证据继续以文件哈希和输入指纹审计。

构建哈希与真实数据切换验证在正式构建后写入 AlphaHome 生产接入记录及隔离运行时
`release.json`。
