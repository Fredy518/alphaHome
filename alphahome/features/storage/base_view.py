"""
Features 物化视图基类

提供物化视图定义的抽象基类，所有 MV 定义都应继承此类。

强制迁移约定: 仅允许 schema="features"
"""

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

from .refresh import MaterializedViewRefresh

logger = logging.getLogger(__name__)


class BaseFeatureView(ABC):
    """
    物化视图定义的抽象基类。

    所有物化视图定义都应继承此类并实现 get_create_sql() 方法。

    强制迁移约定:
        - schema 必须为 "features"
        - 创建后自动 upsert 到 features.mv_metadata
    """

    # 类属性（子类必须覆盖）
    # name: 作为“配方唯一标识”（recipe.name），用于注册/调用参数等。
    # 物化视图实际表名默认使用 mv_{name}（见 docs/architecture/features_module_design.md 3.2.1）。
    name: str = ""
    description: str = ""  # 视图描述
    # 可选：显式覆盖物化视图实际名称（不含 schema）。默认使用 mv_{name}
    materialized_view_name: str = ""
    refresh_strategy: str = "full"  # 刷新策略: full / concurrent
    source_tables: List[str] = []  # 数据来源表
    quality_checks: Dict[str, Any] = {}  # 质量检查配置

    # 强制约定
    ALLOWED_SCHEMA = "features"

    def __init__(self, db_manager=None, schema: str = "features"):
        """
        初始化 BaseFeatureView。

        Args:
            db_manager: DBManager 实例
            schema: 目标 schema（强制为 "features"）

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
        self._refresher: Optional[MaterializedViewRefresh] = None
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def schema(self) -> str:
        """返回目标 schema。"""
        return self._schema

    @property
    def view_name(self) -> str:
        """返回物化视图实际名称（不含 schema）。"""
        if self.materialized_view_name:
            return self.materialized_view_name
        if not self.name:
            return ""
        return f"mv_{self.name}"

    @property
    def full_name(self) -> str:
        """返回完整视图名称（schema.view_name）。"""
        return f"{self._schema}.{self.view_name}"

    def set_db_manager(self, db_manager) -> None:
        """设置数据库管理器。"""
        self._db_manager = db_manager
        if self._refresher:
            self._refresher.set_db_manager(db_manager)

    def _ensure_refresher(self) -> MaterializedViewRefresh:
        """确保刷新器已初始化。"""
        if self._refresher is None:
            self._refresher = MaterializedViewRefresh(
                db_manager=self._db_manager,
                schema=self._schema
            )
        return self._refresher

    # ==========================================================================
    # 抽象方法（子类必须实现）
    # ==========================================================================

    @abstractmethod
    def get_create_sql(self) -> str:
        """
        返回创建物化视图的 SQL。

        子类必须实现此方法，返回完整的 CREATE MATERIALIZED VIEW 语句。
        注意: SQL 中的 schema 必须使用 "features"。

        Returns:
            str: CREATE MATERIALIZED VIEW SQL
        """
        pass

    def get_post_create_sqls(self) -> List[str]:
        """返回创建物化视图后的附加 SQL（可选）。

        典型用途：为物化视图创建索引（加速查询，或为 concurrent refresh 做准备）。

        约定：
        - 返回多个独立的 SQL 语句（不需要以分号结尾）
        - 建议使用 `CREATE INDEX IF NOT EXISTS ...` 以保证幂等
        """
        return []

    # ==========================================================================
    # 视图管理方法
    # ==========================================================================

    async def exists(self) -> bool:
        """
        检查物化视图是否存在。

        Returns:
            bool: 视图是否存在
        """
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        sql = f"""
        SELECT EXISTS (
            SELECT 1 FROM pg_matviews
            WHERE schemaname = '{self._schema}'
              AND matviewname = '{self.view_name}'
        ) AS exists;
        """
        result = await self._db_manager.fetch(sql)
        return result and result[0]["exists"]

    async def create(self, if_not_exists: bool = True) -> bool:
        """
        创建物化视图。

        Args:
            if_not_exists: 如果视图已存在是否跳过

        Returns:
            bool: 创建是否成功

        Raises:
            RuntimeError: 如果 db_manager 未设置
        """
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        try:
            # 检查是否已存在
            if if_not_exists and await self.exists():
                self.logger.info(f"物化视图 {self.full_name} 已存在，跳过创建")
                # 仍然更新元数据
                await self._upsert_metadata()
                return True

            # 获取创建 SQL
            create_sql = self.get_create_sql()
            self.logger.info(f"创建物化视图: {self.full_name}")

            if self.full_name and self.full_name not in create_sql:
                self.logger.warning(
                    f"create_sql 未包含预期视图名 {self.full_name}，"
                    f"请确认 get_create_sql() 与命名规范一致"
                )

            # 执行创建
            await self._db_manager.execute(create_sql)

            # 可选：执行创建后的附加 SQL（如索引）
            post_sqls = self.get_post_create_sqls() or []
            for stmt in post_sqls:
                if not isinstance(stmt, str) or not stmt.strip():
                    continue
                await self._db_manager.execute(stmt)

            # 写入元数据
            await self._upsert_metadata()

            self.logger.info(f"物化视图 {self.full_name} 创建成功")
            return True

        except Exception as e:
            self.logger.error(f"创建物化视图 {self.full_name} 失败: {e}")
            raise

    async def drop(self, if_exists: bool = True) -> bool:
        """
        删除物化视图。

        Args:
            if_exists: 如果视图不存在是否跳过

        Returns:
            bool: 删除是否成功
        """
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        try:
            if_exists_clause = "IF EXISTS " if if_exists else ""
            sql = f"DROP MATERIALIZED VIEW {if_exists_clause}{self.full_name};"
            self.logger.info(f"删除物化视图: {self.full_name}")
            await self._db_manager.execute(sql)

            # 更新元数据状态
            await self._deactivate_metadata()

            self.logger.info(f"物化视图 {self.full_name} 删除成功")
            return True

        except Exception as e:
            self.logger.error(f"删除物化视图 {self.full_name} 失败: {e}")
            raise

    async def refresh(self, strategy: Optional[str] = None) -> Dict[str, Any]:
        """
        刷新物化视图。

        Args:
            strategy: 刷新策略（默认使用类定义的 refresh_strategy）

        Returns:
            Dict[str, Any]: 刷新结果（由 MaterializedViewRefresh.refresh 返回）
        """
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        actual_strategy = strategy or self.refresh_strategy
        refresher = self._ensure_refresher()

        self.logger.info(
            f"刷新物化视图: {self.full_name}, 策略: {actual_strategy}"
        )

        return await refresher.refresh(
            view_name=self.view_name,
            strategy=actual_strategy
        )

    async def get_row_count(self) -> int:
        """
        获取物化视图行数。

        Returns:
            int: 行数
        """
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        sql = f"SELECT COUNT(*) AS cnt FROM {self.full_name};"
        result = await self._db_manager.fetch(sql)
        return result[0]["cnt"] if result else 0

    # ==========================================================================
    # 元数据管理
    # ==========================================================================

    async def _upsert_metadata(self) -> None:
        """
        Upsert 物化视图元数据到 features.mv_metadata。
        """
        if self._db_manager is None:
            return

        try:
            source_tables = list(self.source_tables) if self.source_tables else []
            quality_checks_json = (
                json.dumps(self.quality_checks) if self.quality_checks else "{}"
            )

            sql = """
            INSERT INTO features.mv_metadata (
                view_name,
                schema_name,
                description,
                source_tables,
                refresh_strategy,
                quality_checks,
                created_at,
                updated_at,
                version,
                is_active
            ) VALUES (
                $1,
                $2,
                $3,
                $4::text[],
                $5,
                $6::jsonb,
                NOW(),
                NOW(),
                1,
                TRUE
            )
            ON CONFLICT (view_name) DO UPDATE SET
                description = EXCLUDED.description,
                source_tables = EXCLUDED.source_tables,
                refresh_strategy = EXCLUDED.refresh_strategy,
                quality_checks = EXCLUDED.quality_checks,
                updated_at = NOW(),
                version = features.mv_metadata.version + 1,
                is_active = TRUE;
            """.strip()

            await self._db_manager.execute(
                sql,
                self.view_name,
                self._schema,
                self.description,
                source_tables,
                self.refresh_strategy,
                quality_checks_json,
            )
            self.logger.debug(f"已更新元数据: {self.view_name}")

        except Exception as e:
            self.logger.warning(f"更新元数据失败: {e}")
            # 元数据更新失败不影响主流程

    async def _deactivate_metadata(self) -> None:
        """
        将元数据标记为非活跃。
        """
        if self._db_manager is None:
            return

        try:
            sql = """
            UPDATE features.mv_metadata
            SET is_active = FALSE, updated_at = NOW()
            WHERE view_name = $1;
            """.strip()
            await self._db_manager.execute(sql, self.view_name)
            self.logger.debug(f"已停用元数据: {self.view_name}")

        except Exception as e:
            self.logger.warning(f"停用元数据失败: {e}")

    # ==========================================================================
    # 工具方法
    # ==========================================================================

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__}("
            f"name='{self.name}', "
            f"schema='{self._schema}', "
            f"refresh_strategy='{self.refresh_strategy}'"
            f")>"
        )
