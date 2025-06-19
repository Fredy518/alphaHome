"""
重构后的数据库管理器 - 使用整合架构

该文件通过整合的DatabaseOperationsMixin实现简洁、高效的数据库管理器。

== 版本说明 ==
v2.0: 使用整合的 DatabaseOperationsMixin，简化架构（当前版本）
v1.0: 已完全迁移，不再保留Legacy支持
"""

from .db_components import (
    DatabaseOperationsMixin,  # v2.0 整合组件
    DBManagerCore,
    SchemaManagementMixin,
    UtilityMixin,
)


# ============================================================================
# v2.0 版本 - 唯一版本（整合架构）
# ============================================================================

class DBManager(
    DatabaseOperationsMixin,  # 整合的数据库操作功能
    SchemaManagementMixin,    # 表结构管理功能
    UtilityMixin,             # 实用工具功能
    DBManagerCore,            # 核心连接管理功能
):
    """数据库连接管理器 v2.0 - 整合架构版本
    
    架构特点：
    --------
    1. **简化继承链**: 使用整合的 DatabaseOperationsMixin
    2. **功能完整**: 包含所有数据库操作功能
    3. **性能优化**: 减少方法解析路径，提高执行效率
    4. **易于维护**: 清晰的组件层次结构
    
    组件构成：
    --------
    - DBManagerCore: 核心连接管理和模式切换
    - DatabaseOperationsMixin: 整合的数据库操作功能
      ↳ 包含所有基础SQL操作（execute, fetch, executemany等）
      ↳ 包含所有高级数据操作（copy_from_dataframe, upsert等）
    - SchemaManagementMixin: 表结构管理（table_exists, create_table等）
    - UtilityMixin: 实用工具（get_latest_date, test_connection等）
    
    使用方式：
    --------
    支持两种工作模式：
    - async: 使用 asyncpg，适用于异步环境（如 fetchers）
    - sync: 使用 psycopg2，适用于同步环境（如 Backtrader）
    
    Example:
        >>> db = DBManager(connection_string, mode="async")
        >>> await db.connect()
        >>> result = await db.fetch("SELECT * FROM stocks")
        >>> await db.copy_from_dataframe(df, target_table)
    """
    
    def __init__(self, connection_string: str, mode: str = "async", **kwargs):
        """初始化 DBManager 实例
        
        Args:
            connection_string (str): PostgreSQL数据库连接字符串
            mode (str): 工作模式 ('async' | 'sync')
            **kwargs: 额外的配置参数
        """
        super().__init__(connection_string, mode=mode, **kwargs)  # type: ignore


# ============================================================================
# 工厂函数
# ============================================================================

def create_async_manager(connection_string: str) -> DBManager:
    """创建异步模式的数据库管理器
    
    Args:
        connection_string (str): 数据库连接字符串
        
    Returns:
        DBManager: 异步模式的数据库管理器实例
        
    Example:
        >>> db = create_async_manager("postgresql://user:pass@localhost/db")
        >>> await db.connect()
    """
    return DBManager(connection_string, mode="async")


def create_sync_manager(connection_string: str) -> DBManager:
    """创建同步模式的数据库管理器
    
    专为 Backtrader 等同步环境设计，使用 psycopg2 提供真正的同步操作
    
    Args:
        connection_string (str): 数据库连接字符串
        
    Returns:
        DBManager: 同步模式的数据库管理器实例
        
    Example:
        >>> db = create_sync_manager("postgresql://user:pass@localhost/db")
        >>> result = db.fetch_sync("SELECT * FROM stocks")
    """
    return DBManager(connection_string, mode="sync")


# ============================================================================
# 向后兼容别名
# ============================================================================

def SyncDBManager(connection_string: str):
    """向后兼容别名"""
    return create_sync_manager(connection_string)


# v2.0 别名（保持API一致性）
DBManagerV2 = DBManager
create_async_manager_v2 = create_async_manager
create_sync_manager_v2 = create_sync_manager


# ============================================================================
# 迁移完成信息
# ============================================================================

__migration_status__ = "COMPLETED"
__current_version__ = "2.0"
__architecture__ = "Unified"

__notes__ = """
架构已完全整合！

特点：
- 使用 DatabaseOperationsMixin 整合所有数据库操作
- 移除了Legacy组件，简化架构
- 保持100% API兼容性
- 提供更好的性能和维护性

使用方法：
from alphahome.common.db_manager import create_async_manager
db = create_async_manager(connection_string)
"""
