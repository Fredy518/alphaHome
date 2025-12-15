"""
物化视图系统的数据库表定义和初始化

定义了物化视图系统所需的所有数据库表结构。
"""

from typing import Optional
import logging

logger = logging.getLogger(__name__)


# 物化视图元数据表的 SQL 定义
CREATE_MATERIALIZED_VIEWS_METADATA_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS materialized_views.materialized_views_metadata (
    view_name VARCHAR(255) PRIMARY KEY,
    view_schema VARCHAR(255) NOT NULL DEFAULT 'materialized_views',
    source_tables TEXT,
    refresh_strategy VARCHAR(50) DEFAULT 'full',
    last_refresh_time TIMESTAMP,
    refresh_status VARCHAR(50),
    row_count INTEGER,
    refresh_duration_seconds FLOAT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
"""

# 物化视图元数据表的索引
CREATE_MATERIALIZED_VIEWS_METADATA_INDEXES_SQL = [
    """
    CREATE INDEX IF NOT EXISTS idx_materialized_views_metadata_refresh_status 
    ON materialized_views.materialized_views_metadata(refresh_status);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_materialized_views_metadata_last_refresh_time 
    ON materialized_views.materialized_views_metadata(last_refresh_time DESC);
    """,
]

# 数据质量检查结果表的 SQL 定义
CREATE_MATERIALIZED_VIEWS_QUALITY_CHECKS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS materialized_views.materialized_views_quality_checks (
    id SERIAL PRIMARY KEY,
    view_name VARCHAR(255) NOT NULL,
    check_name VARCHAR(255) NOT NULL,
    check_status VARCHAR(50) NOT NULL,
    check_message TEXT,
    check_details JSONB,
    checked_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (view_name) REFERENCES materialized_views.materialized_views_metadata(view_name) ON DELETE CASCADE
);
"""

# 数据质量检查结果表的索引
CREATE_MATERIALIZED_VIEWS_QUALITY_CHECKS_INDEXES_SQL = [
    """
    CREATE INDEX IF NOT EXISTS idx_materialized_views_quality_checks_view_name 
    ON materialized_views.materialized_views_quality_checks(view_name);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_materialized_views_quality_checks_check_status 
    ON materialized_views.materialized_views_quality_checks(check_status);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_materialized_views_quality_checks_checked_at 
    ON materialized_views.materialized_views_quality_checks(checked_at DESC);
    """,
]

# 物化视图告警表的 SQL 定义
CREATE_MATERIALIZED_VIEWS_ALERTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS materialized_views.materialized_views_alerts (
    id SERIAL PRIMARY KEY,
    view_name VARCHAR(255),
    alert_type VARCHAR(100),
    severity VARCHAR(50),
    message TEXT,
    details JSONB,
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by VARCHAR(255),
    acknowledged_at TIMESTAMP,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
"""

# 物化视图告警表的索引
CREATE_MATERIALIZED_VIEWS_ALERTS_INDEXES_SQL = [
    """
    CREATE INDEX IF NOT EXISTS idx_materialized_views_alerts_view_name_created
    ON materialized_views.materialized_views_alerts (view_name, created_at DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_materialized_views_alerts_severity_created
    ON materialized_views.materialized_views_alerts (severity, created_at DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_materialized_views_alerts_acknowledged
    ON materialized_views.materialized_views_alerts (acknowledged, created_at DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_materialized_views_alerts_alert_type
    ON materialized_views.materialized_views_alerts (alert_type, created_at DESC);
    """,
]


async def initialize_materialized_views_schema(db_manager) -> None:
    """
    初始化物化视图系统的数据库表
    
    创建以下表：
    1. materialized_views_metadata - 物化视图元数据表
    2. materialized_views_quality_checks - 数据质量检查结果表
    3. materialized_views_alerts - 告警记录表
    
    Args:
        db_manager: 数据库管理器实例
    
    Returns:
        None
    
    Raises:
        Exception: 如果表创建失败
    """
    try:
        # 确保 materialized_views schema 存在
        await db_manager.ensure_schema_exists('materialized_views')
        
        # 创建物化视图元数据表
        logger.info("创建 materialized_views_metadata 表...")
        await db_manager.execute(CREATE_MATERIALIZED_VIEWS_METADATA_TABLE_SQL)
        logger.info("materialized_views_metadata 表创建成功或已存在")
        
        # 创建物化视图元数据表的索引
        for index_sql in CREATE_MATERIALIZED_VIEWS_METADATA_INDEXES_SQL:
            logger.info(f"创建索引: {index_sql.strip()[:50]}...")
            await db_manager.execute(index_sql)
        logger.info("materialized_views_metadata 表的索引创建成功")
        
        # 创建数据质量检查结果表
        logger.info("创建 materialized_views_quality_checks 表...")
        await db_manager.execute(CREATE_MATERIALIZED_VIEWS_QUALITY_CHECKS_TABLE_SQL)
        logger.info("materialized_views_quality_checks 表创建成功或已存在")
        
        # 创建数据质量检查结果表的索引
        for index_sql in CREATE_MATERIALIZED_VIEWS_QUALITY_CHECKS_INDEXES_SQL:
            logger.info(f"创建索引: {index_sql.strip()[:50]}...")
            await db_manager.execute(index_sql)
        logger.info("materialized_views_quality_checks 表的索引创建成功")

        # 创建告警表
        logger.info("创建 materialized_views_alerts 表...")
        await db_manager.execute(CREATE_MATERIALIZED_VIEWS_ALERTS_TABLE_SQL)
        logger.info("materialized_views_alerts 表创建成功或已存在")

        # 创建告警表索引
        for index_sql in CREATE_MATERIALIZED_VIEWS_ALERTS_INDEXES_SQL:
            logger.info(f"创建索引: {index_sql.strip()[:50]}...")
            await db_manager.execute(index_sql)
        logger.info("materialized_views_alerts 表的索引创建成功")
        
        logger.info("物化视图系统的数据库表初始化完成")
    
    except Exception as e:
        logger.error(f"初始化物化视图系统的数据库表失败: {e}", exc_info=True)
        raise


async def check_materialized_views_schema_exists(db_manager) -> bool:
    """
    检查物化视图系统的数据库表是否存在
    
    Args:
        db_manager: 数据库管理器实例
    
    Returns:
        bool: 如果所有表都存在则返回 True，否则返回 False
    """
    try:
        # 检查 materialized_views_metadata 表
        metadata_exists = await db_manager.table_exists(
            'materialized_views.materialized_views_metadata'
        )
        
        # 检查 materialized_views_quality_checks 表
        quality_checks_exists = await db_manager.table_exists(
            'materialized_views.materialized_views_quality_checks'
        )

        # 检查 materialized_views_alerts 表
        alerts_exists = await db_manager.table_exists(
            'materialized_views.materialized_views_alerts'
        )
        
        return metadata_exists and quality_checks_exists and alerts_exists
    
    except Exception as e:
        logger.error(f"检查物化视图系统的数据库表失败: {e}", exc_info=True)
        return False
