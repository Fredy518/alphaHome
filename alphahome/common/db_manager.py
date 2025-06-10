"""
重构后的数据库管理器 - 使用Mix-in模式实现功能分离

该文件将原本1000+行的代码重构为模块化设计，通过多重继承组合各个功能模块。
"""

from .db_components import (
    DBManagerCore,
    SQLOperationsMixin,
    DataOperationsMixin,
    SchemaManagementMixin,
    UtilityMixin
)


class DBManager(
    DataOperationsMixin,      # 复杂数据操作功能
    SchemaManagementMixin,    # 表结构管理功能  
    UtilityMixin,             # 实用工具功能
    SQLOperationsMixin,       # 基础SQL操作功能
    DBManagerCore             # 核心连接管理功能
):
    """数据库连接管理器 - 支持异步和同步双模式操作
    
    通过Mix-in模式组合多个功能模块：
    - DBManagerCore: 核心连接管理和模式切换
    - SQLOperationsMixin: 基础SQL操作（execute, fetch等）
    - DataOperationsMixin: 复杂数据操作（copy_from_dataframe, upsert等）
    - SchemaManagementMixin: 表结构管理（table_exists, create_table等）
    - UtilityMixin: 实用工具（get_latest_date, test_connection等）
    
    支持两种工作模式：
    - async: 使用 asyncpg，适用于异步环境（如 fetchers）
    - sync: 使用 psycopg2，适用于同步环境（如 Backtrader）
    """
    pass  # 所有功能都通过Mix-in继承获得


# === 工厂函数 ===

def create_async_manager(connection_string: str) -> DBManager:
    """创建异步模式的数据库管理器
    
    Args:
        connection_string (str): 数据库连接字符串
        
    Returns:
        DBManager: 异步模式的数据库管理器实例
    """
    return DBManager(connection_string, mode='async')


def create_sync_manager(connection_string: str) -> DBManager:
    """创建同步模式的数据库管理器
    
    专为 Backtrader 等同步环境设计，使用 psycopg2 提供真正的同步操作
    
    Args:
        connection_string (str): 数据库连接字符串
        
    Returns:
        DBManager: 同步模式的数据库管理器实例
    """
    return DBManager(connection_string, mode='sync')


# === 向后兼容别名 ===

def SyncDBManager(connection_string: str):
    """向后兼容别名，推荐使用 create_sync_manager"""
    return create_sync_manager(connection_string) 