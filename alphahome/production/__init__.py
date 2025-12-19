"""
生产脚本包装模块

当前采用轻量级设计：通过 subprocess 调用脚本，保持脚本原样。

未来改造指南：
1. 将脚本内的核心逻辑抽出为可导入的函数/类
2. 脚本本身改为薄包装，仅调用这些函数
3. 在此模块注册包装的导入入口
4. `ah prod` 改为直接调用这些入口，避免 subprocess 开销
5. 这样能更好地处理异常、日志和状态传递

示例改造过程：

# 原始脚本结构
scripts/production/data_updaters/tushare/data_collection_smart_update_production.py
  - async main()
  - class DataCollectionProductionUpdater

# 改造后的结构
alphahome/production/updaters/__init__.py
  - from .data_collection import run_data_collection_update

alphahome/production/updaters/data_collection.py
  - async def run(config) -> bool
  - class DataCollectionUpdater  # 核心逻辑

scripts/production/data_updaters/tushare/data_collection_smart_update_production.py
  - 保留为薄包装，调用：
    from alphahome.production.updaters import run_data_collection_update
    return asyncio.run(run_data_collection_update(...))

alphahome/cli/commands/prod.py
  - 直接导入并调用包内模块，避免 subprocess
"""

# 当前实现：使用 subprocess 透传
# 这样的好处：
# 1. 脚本可以原样保留，无需改造
# 2. 快速启用统一 CLI
# 3. 避免脚本内 sys.exit() 污染父进程
# 缺点：
# 1. 进程开销
# 2. 无法实时获取子进程的返回值和异常
# 3. 日志不统一

# 未来改造时，逐步迁移到包内模块调用
