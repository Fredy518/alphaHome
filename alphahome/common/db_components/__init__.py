"""
数据库管理器组件模块

该模块包含了数据库管理器的各个功能组件，通过Mixin模式实现功能分离和代码重用。

== 架构整合完成 ==
🎉 架构已完全整合为统一的 v2.0 版本！

当前架构 (v2.0):
- DatabaseOperationsMixin: 整合的数据库操作组件（包含所有SQL和数据操作功能）
- SchemaManagementMixin: 表结构管理
- UtilityMixin: 实用工具
- DBManagerCore: 核心连接管理
- TableNameResolver: 表名解析

架构优势:
- 简化的继承链，提高性能
- 统一的数据库操作接口
- 更好的代码组织和维护性
- 减少组件间依赖复杂性
"""

# 主要组件
from .database_operations_mixin import DatabaseOperationsMixin
from .db_manager_core import DBManagerCore
from .schema_management_mixin import SchemaManagementMixin
from .table_name_resolver import TableNameResolver
from .utility_mixin import UtilityMixin

__all__ = [
    # == 核心组件 ==
    "DBManagerCore",
    "TableNameResolver",
    
    # == 主要组件 ==
    "DatabaseOperationsMixin",  # 整合的数据库操作组件
    "SchemaManagementMixin",    # 表结构管理
    "UtilityMixin",             # 实用工具
]

# 架构信息
__architecture_info__ = {
    "version": "2.0",
    "status": "UNIFIED",
    "primary_component": "DatabaseOperationsMixin",
    "components_count": 5,
    "migration_date": "2024-12-18",
    "performance_improvement": "简化继承链，减少方法解析复杂性",
    "note": "架构已完全整合，移除了冗余组件，提供更好的性能和维护性。"
}

# 使用指南
__usage_guide__ = {
    "新项目": """
    from alphahome.common.db_components import DatabaseOperationsMixin, DBManagerCore
    
    class MyDBManager(DatabaseOperationsMixin, DBManagerCore):
        pass
    """,
    
    "标准使用": """
    from alphahome.common.db_manager import create_async_manager
    
    db = create_async_manager(connection_string)
    await db.connect()
    result = await db.fetch("SELECT * FROM table")
    """,
    
    "高级数据操作": """
    # 所有功能都在 DatabaseOperationsMixin 中
    await db.copy_from_dataframe(df, target_table)
    await db.upsert(df, target_table, conflict_columns=['id'])
    """
}
