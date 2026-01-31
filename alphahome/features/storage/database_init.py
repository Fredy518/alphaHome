"""
Features 数据库初始化模块

负责创建 features schema 以及元数据表:
- features.mv_metadata: 物化视图元数据表
- features.mv_refresh_log: 物化视图刷新日志表

强制迁移约定: 仅允许 schema="features"
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ==============================================================================
# 元数据表 DDL
# ==============================================================================

CREATE_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS features;
"""

CREATE_MV_METADATA_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS features.mv_metadata (
    view_name VARCHAR(128) PRIMARY KEY,
    schema_name VARCHAR(64) NOT NULL DEFAULT 'features',
    description TEXT,
    source_tables TEXT[],
    refresh_strategy VARCHAR(32) NOT NULL DEFAULT 'full',
    quality_checks JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    version INTEGER NOT NULL DEFAULT 1,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT mv_metadata_schema_check CHECK (schema_name = 'features')
);

COMMENT ON TABLE features.mv_metadata IS '物化视图元数据表';
COMMENT ON COLUMN features.mv_metadata.view_name IS '物化视图名称';
COMMENT ON COLUMN features.mv_metadata.schema_name IS '所属 schema（强制为 features）';
COMMENT ON COLUMN features.mv_metadata.description IS '视图描述';
COMMENT ON COLUMN features.mv_metadata.source_tables IS '数据来源表列表';
COMMENT ON COLUMN features.mv_metadata.refresh_strategy IS '刷新策略: full / concurrent';
COMMENT ON COLUMN features.mv_metadata.quality_checks IS '质量检查配置 (JSONB)';
COMMENT ON COLUMN features.mv_metadata.created_at IS '创建时间';
COMMENT ON COLUMN features.mv_metadata.updated_at IS '最后更新时间';
COMMENT ON COLUMN features.mv_metadata.version IS '元数据版本号';
COMMENT ON COLUMN features.mv_metadata.is_active IS '是否启用';
"""

CREATE_MV_REFRESH_LOG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS features.mv_refresh_log (
    id BIGSERIAL PRIMARY KEY,
    view_name VARCHAR(128) NOT NULL,
    schema_name VARCHAR(64) NOT NULL DEFAULT 'features',
    refresh_strategy VARCHAR(32) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE,
    duration_seconds DECIMAL(10, 3),
    success BOOLEAN NOT NULL DEFAULT FALSE,
    error_message TEXT,
    row_count BIGINT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT mv_refresh_log_schema_check CHECK (schema_name = 'features')
);

CREATE INDEX IF NOT EXISTS idx_mv_refresh_log_view_name
    ON features.mv_refresh_log (view_name);
CREATE INDEX IF NOT EXISTS idx_mv_refresh_log_started_at
    ON features.mv_refresh_log (started_at DESC);

COMMENT ON TABLE features.mv_refresh_log IS '物化视图刷新日志表';
COMMENT ON COLUMN features.mv_refresh_log.view_name IS '物化视图名称';
COMMENT ON COLUMN features.mv_refresh_log.schema_name IS '所属 schema（强制为 features）';
COMMENT ON COLUMN features.mv_refresh_log.refresh_strategy IS '刷新策略: full / concurrent';
COMMENT ON COLUMN features.mv_refresh_log.started_at IS '刷新开始时间';
COMMENT ON COLUMN features.mv_refresh_log.finished_at IS '刷新结束时间';
COMMENT ON COLUMN features.mv_refresh_log.duration_seconds IS '刷新耗时（秒）';
COMMENT ON COLUMN features.mv_refresh_log.success IS '是否成功';
COMMENT ON COLUMN features.mv_refresh_log.error_message IS '错误信息';
COMMENT ON COLUMN features.mv_refresh_log.row_count IS '刷新后行数';
"""


class FeaturesDatabaseInit:
    """
    Features 数据库初始化器

    负责创建 features schema 和元数据表。
    强制迁移约定: 仅支持 schema="features"。
    """

    ALLOWED_SCHEMA = "features"

    def __init__(self, db_manager=None, schema: str = "features"):
        """
        初始化 FeaturesDatabaseInit。

        Args:
            db_manager: DBManager 实例（可选，可稍后通过 set_db_manager 设置）
            schema: 目标 schema 名称（强制为 "features"）

        Raises:
            ValueError: 如果 schema 不是 "features"
        """
        if schema != self.ALLOWED_SCHEMA:
            raise ValueError(
                f"强制迁移约定: schema 必须为 '{self.ALLOWED_SCHEMA}'，"
                f"收到 '{schema}'"
            )
        self._schema = schema
        self._db_manager = db_manager
        self._initialized = False

    @property
    def schema(self) -> str:
        """返回目标 schema 名称。"""
        return self._schema

    def set_db_manager(self, db_manager) -> None:
        """设置数据库管理器。"""
        self._db_manager = db_manager

    async def initialize(self) -> bool:
        """
        执行完整的数据库初始化。

        Returns:
            bool: 初始化是否成功

        Raises:
            RuntimeError: 如果 db_manager 未设置
        """
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置，请先调用 set_db_manager()")

        try:
            logger.info(f"开始初始化 {self._schema} schema...")

            # 1. 创建 schema
            await self._create_schema()

            # 2. 创建 mv_metadata 表
            await self._create_mv_metadata_table()

            # 3. 创建 mv_refresh_log 表
            await self._create_mv_refresh_log_table()

            self._initialized = True
            logger.info(f"{self._schema} schema 初始化完成")
            return True

        except Exception as e:
            logger.error(f"初始化 {self._schema} schema 失败: {e}")
            raise

    async def _create_schema(self) -> None:
        """创建 features schema。"""
        logger.info(f"创建 schema: {self._schema}")
        await self._db_manager.execute(CREATE_SCHEMA_SQL)

    async def _create_mv_metadata_table(self) -> None:
        """创建 mv_metadata 表。"""
        logger.info(f"创建表: {self._schema}.mv_metadata")
        await self._db_manager.execute(CREATE_MV_METADATA_TABLE_SQL)

    async def _create_mv_refresh_log_table(self) -> None:
        """创建 mv_refresh_log 表。"""
        logger.info(f"创建表: {self._schema}.mv_refresh_log")
        await self._db_manager.execute(CREATE_MV_REFRESH_LOG_TABLE_SQL)

    async def check_initialized(self) -> bool:
        """
        检查 features schema 是否已初始化。

        Returns:
            bool: 是否已初始化
        """
        if self._db_manager is None:
            return False

        try:
            # 检查 schema 是否存在
            schema_sql = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.schemata
                WHERE schema_name = 'features'
            ) AS schema_exists;
            """
            result = await self._db_manager.fetch(schema_sql)
            if not result or not result[0]["schema_exists"]:
                return False

            # 检查两个元数据表是否存在
            tables_sql = """
            SELECT COUNT(*) AS table_count
            FROM information_schema.tables
            WHERE table_schema = 'features'
              AND table_name IN ('mv_metadata', 'mv_refresh_log');
            """
            result = await self._db_manager.fetch(tables_sql)
            return result and result[0]["table_count"] == 2

        except Exception as e:
            logger.warning(f"检查初始化状态失败: {e}")
            return False

    async def ensure_initialized(self) -> bool:
        """
        确保 features schema 已初始化。
        如果未初始化则执行初始化。

        Returns:
            bool: 是否已初始化（或初始化成功）
        """
        if await self.check_initialized():
            logger.info(f"{self._schema} schema 已初始化，跳过")
            self._initialized = True
            return True

        return await self.initialize()
