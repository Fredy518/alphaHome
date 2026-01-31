"""features.storage 物化视图刷新执行器

实现物化视图的刷新操作，包括：
- 执行 REFRESH MATERIALIZED VIEW 命令
- 支持 FULL 和 CONCURRENT 刷新策略
- 记录刷新元数据（写入 features.mv_refresh_log）
- 获取刷新状态

注意：本模块与 features.storage.database_init 中的表结构保持一致。

迁移自: 旧 processors.materialized_views.refresh（已删除）
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import datetime
import logging
import re

logger = logging.getLogger(__name__)


class MaterializedViewRefresh:
    """
    物化视图刷新执行器
    
    职责：
    1. 执行 REFRESH MATERIALIZED VIEW 命令
    2. 支持 FULL 和 CONCURRENT 刷新策略
    3. 记录刷新元数据（时间、状态、行数）到 features.mv_refresh_log
    4. 获取刷新状态
    
    属性：
    - db_connection: 数据库连接对象
    - logger: 日志记录器
    """

    # Phase1 强制迁移：仅允许 features schema
    DEFAULT_SCHEMA = "features"

    def __init__(self, db_manager=None, schema: str = "features", logger=None):
        """初始化刷新执行器。

        Args:
            db_manager: DBManager 异步实例（需支持 execute/fetch）
            schema: 物化视图所在 schema（Phase1 强制为 features）
            logger: 日志记录器
        """
        if schema != self.DEFAULT_SCHEMA:
            raise ValueError(
                f"Phase1 强制迁移：仅允许 schema='{self.DEFAULT_SCHEMA}'，收到: {schema!r}"
            )

        self._schema = schema
        self._db_manager = db_manager
        self.logger = logger or logging.getLogger(__name__)
        self._refresh_status: Dict[str, Any] = {}

    def set_db_manager(self, db_manager) -> None:
        self._db_manager = db_manager

    @staticmethod
    def _validate_identifier(identifier: str, kind: str) -> None:
        """
        Guardrail to prevent SQL injection via schema/view identifiers.

        We embed identifiers into SQL strings (cannot be parameterized), so we
        only allow common PostgreSQL identifier characters.
        """
        if not isinstance(identifier, str) or not identifier:
            raise ValueError(f"{kind} must be a non-empty string")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
            raise ValueError(f"Invalid {kind}: {identifier!r}")

    async def refresh(
        self,
        view_name: str,
        strategy: str = "full",
        triggered_by: str = "script"
    ) -> Dict[str, Any]:
        """
        刷新物化视图
        
        执行以下步骤：
        1. 验证物化视图是否存在
        2. 执行 REFRESH MATERIALIZED VIEW 命令
        3. 获取刷新后的行数
        4. 写入刷新日志到 features.mv_refresh_log
        5. 返回刷新结果
        
        参数：
        - view_name: 物化视图名称（不含 schema）
        - schema: 物化视图所在的 schema（默认 features，强制）
        - strategy: 刷新策略（'full' 或 'concurrent'）
        - triggered_by: 触发来源（'manual', 'scheduled', 'script'）
        
        返回：
        {
            'status': 'success' | 'failed',
            'view_name': str,
            'full_name': str,  # schema.view_name
            'refresh_time': datetime,
            'duration_seconds': float,
            'row_count': int,
            'error_message': str (if failed)
        }
        
        异常：
        - ValueError: 当 strategy 不是 'full' 或 'concurrent' 时，或 schema 不是 'features' 时
        - RuntimeError: 当数据库连接不可用时
        """
        # 验证参数
        if strategy not in ('full', 'concurrent'):
            raise ValueError(
                f"Invalid refresh strategy: {strategy}. "
                f"Must be 'full' or 'concurrent'."
            )
        
        if not self._db_manager:
            raise RuntimeError("db_manager 未设置，无法执行 refresh")

        schema = self._schema
        self._validate_identifier(schema, "schema")
        self._validate_identifier(view_name, "view_name")

        full_name = f"{schema}.{view_name}"
        start_time = datetime.now()

        try:
            self.logger.info(
                f"Starting refresh of materialized view: {full_name} "
                f"(strategy: {strategy})"
            )
            
            # 1. 验证物化视图是否存在
            exists = await self._check_matview_exists(view_name, schema)
            if not exists:
                error_msg = f"Materialized view {full_name} does not exist"
                self.logger.error(error_msg)
                result = {
                    'status': 'failed',
                    'view_name': view_name,
                    'view_schema': schema,
                    'full_name': full_name,
                    'refresh_time': start_time,
                    'duration_seconds': 0,
                    'row_count': 0,
                    'error_message': error_msg,
                    'refresh_strategy': strategy,
                    'strategy': strategy,
                }
                # 写入失败日志
                await self._log_refresh(
                    view_name, schema, start_time, datetime.now(),
                    False, 0, 0, error_msg, triggered_by, strategy
                )
                return result

            # 2. 执行 REFRESH 命令
            await self._execute_refresh(view_name, schema, strategy)

            # 3. 获取刷新后的行数
            row_count = await self._get_row_count(view_name, schema)

            # 4. 记录刷新元数据
            end_time = datetime.now()
            duration_seconds = (end_time - start_time).total_seconds()
            
            result = {
                'status': 'success',
                'view_name': view_name,
                'view_schema': schema,
                'full_name': full_name,
                'refresh_time': end_time,
                'duration_seconds': duration_seconds,
                'row_count': row_count,
                'refresh_strategy': strategy,
                'strategy': strategy,
            }
            
            self.logger.info(
                f"Successfully refreshed {full_name}: "
                f"{row_count} rows in {duration_seconds:.2f}s"
            )
            
            # 5. 写入刷新日志到 features.mv_refresh_log
            await self._log_refresh(
                view_name, schema, start_time, end_time,
                True, row_count, duration_seconds, None, triggered_by, strategy
            )
            
            # 更新内部状态
            self._refresh_status[full_name] = result
            
            return result
            
        except Exception as e:
            end_time = datetime.now()
            duration_seconds = (end_time - start_time).total_seconds()
            error_msg = f"{type(e).__name__}: {str(e)}"
            
            self.logger.error(
                f"Failed to refresh {full_name}: {error_msg}",
                exc_info=True
            )
            
            result = {
                'status': 'failed',
                'view_name': view_name,
                'view_schema': schema,
                'full_name': full_name,
                'refresh_time': end_time,
                'duration_seconds': duration_seconds,
                'row_count': 0,
                'error_message': error_msg,
                'refresh_strategy': strategy,
                'strategy': strategy,
            }
            
            # 写入失败日志
            await self._log_refresh(
                view_name, schema, start_time, end_time,
                False, 0, duration_seconds, error_msg, triggered_by, strategy
            )
            
            # 更新内部状态
            self._refresh_status[full_name] = result
            
            return result
    
    def get_refresh_status(
        self,
        view_name: str,
        schema: str = "features"
    ) -> Optional[Dict[str, Any]]:
        """
        获取物化视图的刷新状态
        
        返回最后一次刷新的状态信息。如果从未刷新过，返回 None。
        
        参数：
        - view_name: 物化视图名称（不含 schema）
        - schema: 物化视图所在的 schema（默认 features，强制）
        
        返回：
        刷新状态字典或 None
        """
        # Phase1 强制迁移
        if schema != self.DEFAULT_SCHEMA:
            raise ValueError(
                f"Phase1 强制迁移：仅允许 schema='{self.DEFAULT_SCHEMA}'，收到: {schema!r}"
            )

        full_name = f"{schema}.{view_name}"
        return self._refresh_status.get(full_name)
    
    # =========================================================================
    # 私有方法
    # =========================================================================

    async def _log_refresh(
        self,
        view_name: str,
        schema: str,
        started_at: datetime,
        completed_at: datetime,
        success: bool,
        row_count: int,
        duration_seconds: float,
        error_message: Optional[str],
        triggered_by: str,
        refresh_strategy: str,
    ) -> None:
        """
        将刷新结果写入 features.mv_refresh_log
        """
        if not self._db_manager:
            return

        # 与 features.storage.database_init 中 CREATE_MV_REFRESH_LOG_TABLE_SQL 保持一致
        sql = """
        INSERT INTO features.mv_refresh_log (
            view_name,
            schema_name,
            refresh_strategy,
            started_at,
            finished_at,
            duration_seconds,
            success,
            error_message,
            row_count
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9
        );
        """.strip()

        try:
            await self._execute(
                sql,
                view_name,
                schema,
                refresh_strategy,
                started_at,
                completed_at,
                duration_seconds,
                success,
                error_message,
                row_count,
            )
        except Exception as e:
            # 日志写入失败不应阻断主流程
            self.logger.warning(f"Failed to log refresh to features.mv_refresh_log: {e}")

    async def _check_matview_exists(
        self,
        view_name: str,
        schema: str
    ) -> bool:
        """
        检查物化视图是否存在
        """
        try:
            query = """
            SELECT EXISTS (
                SELECT 1
                FROM pg_matviews
                WHERE schemaname = $1
                  AND matviewname = $2
            );
            """
            val = await self._fetch_val(query, schema, view_name)
            return bool(val)

        except Exception as e:
            self.logger.warning(
                f"Failed to check if view {schema}.{view_name} exists: {e}"
            )
            return False
    
    async def _execute_refresh(
        self,
        view_name: str,
        schema: str,
        strategy: str
    ) -> None:
        """
        执行 REFRESH MATERIALIZED VIEW 命令
        """
        full_name = f"{schema}.{view_name}"
        
        if strategy == 'concurrent':
            # CONCURRENT 刷新不阻塞查询
            query = f"REFRESH MATERIALIZED VIEW CONCURRENTLY {full_name}"
        else:
            # FULL 刷新会阻塞查询
            query = f"REFRESH MATERIALIZED VIEW {full_name}"
        
        self.logger.debug(f"Executing: {query}")

        try:
            await self._execute(query)
        except Exception as e:
            # 如果 CONCURRENT 刷新失败（例如没有唯一索引），回退到 FULL 刷新
            if strategy == 'concurrent':
                self.logger.warning(
                    f"CONCURRENT refresh failed for {full_name}, "
                    f"falling back to FULL refresh: {e}"
                )
                query = f"REFRESH MATERIALIZED VIEW {full_name}"
                await self._execute(query)
            else:
                raise
    
    async def _get_row_count(
        self,
        view_name: str,
        schema: str
    ) -> int:
        """
        获取物化视图的行数
        """
        try:
            full_name = f"{schema}.{view_name}"
            query = f"SELECT COUNT(*) FROM {full_name}"
            val = await self._fetch_val(query)
            return int(val or 0)

        except Exception as e:
            self.logger.warning(
                f"Failed to get row count for {schema}.{view_name}: {e}"
            )
            return 0

    async def _execute(self, query: str, *args) -> Any:
        if not self._db_manager:
            raise RuntimeError("db_manager 未设置")

        try:
            if hasattr(self._db_manager, "execute"):
                return await self._db_manager.execute(query, *args)
            raise RuntimeError("db_manager does not support async execute()")
        except Exception:
            self.logger.error(f"SQL execution failed: {query}", exc_info=True)
            raise

    async def _fetch_val(self, query: str, *args) -> Any:
        if not self._db_manager:
            raise RuntimeError("db_manager 未设置")

        try:
            # DBManager v2: fetch 返回 list[dict]
            if hasattr(self._db_manager, "fetch"):
                rows = await self._db_manager.fetch(query, *args)
                if not rows:
                    return None
                row0 = rows[0]
                if isinstance(row0, dict):
                    return next(iter(row0.values())) if row0 else None
                # 兜底：若返回 tuple/list
                return row0[0] if row0 else None

            raise RuntimeError("db_manager does not support fetch()")
        except Exception:
            self.logger.error(f"SQL query failed: {query}", exc_info=True)
            raise
