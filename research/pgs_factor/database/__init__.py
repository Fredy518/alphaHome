"""
PGS因子模块数据库管理层
=====================

数据库管理层负责数据库模式、表结构和数据迁移的管理。

组件说明：
- db_manager: PGS因子数据库管理器（保持向后兼容）
- schema_manager: 数据库模式管理器（待实现）
- migration_tools: 数据迁移工具（待实现）

职责边界：
- 专注于数据库结构管理，不处理业务逻辑
- 提供数据库操作的基础设施
- 确保数据库模式的一致性和完整性
"""

# 保持向后兼容，导出现有的db_manager
from .db_manager import PGSFactorDBManager

__all__ = [
    'PGSFactorDBManager'
]
