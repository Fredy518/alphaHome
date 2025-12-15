"""
物化视图刷新执行器

实现物化视图的刷新操作，包括：
- 执行 REFRESH MATERIALIZED VIEW 命令
- 支持 FULL 和 CONCURRENT 刷新策略
- 记录刷新元数据
- 获取刷新状态
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import datetime
import logging
import time
import re

logger = logging.getLogger(__name__)


class MaterializedViewRefresh:
    """
    物化视图刷新执行器
    
    职责：
    1. 执行 REFRESH MATERIALIZED VIEW 命令
    2. 支持 FULL 和 CONCURRENT 刷新策略
    3. 记录刷新元数据（时间、状态、行数）
    4. 获取刷新状态
    
    属性：
    - db_connection: 数据库连接对象
    - logger: 日志记录器
    """
    
    def __init__(self, db_connection=None, logger=None):
        """
        初始化刷新执行器
        
        参数：
        - db_connection: 数据库连接对象（可选）
        - logger: 日志记录器（可选）
        """
        self.db_connection = db_connection
        self.logger = logger or logging.getLogger(__name__)
        self._refresh_status: Dict[str, Any] = {}

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
        schema: str = "materialized_views",
        strategy: str = "full"
    ) -> Dict[str, Any]:
        """
        刷新物化视图
        
        执行以下步骤：
        1. 验证物化视图是否存在
        2. 执行 REFRESH MATERIALIZED VIEW 命令
        3. 获取刷新后的行数
        4. 记录刷新元数据
        5. 返回刷新结果
        
        参数：
        - view_name: 物化视图名称（不含 schema）
        - schema: 物化视图所在的 schema（默认 materialized_views）
        - strategy: 刷新策略（'full' 或 'concurrent'）
        
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
        - ValueError: 当 strategy 不是 'full' 或 'concurrent' 时
        - RuntimeError: 当数据库连接不可用时
        """
        # 验证参数
        if strategy not in ('full', 'concurrent'):
            raise ValueError(
                f"Invalid refresh strategy: {strategy}. "
                f"Must be 'full' or 'concurrent'."
            )
        
        if not self.db_connection:
            raise RuntimeError(
                "Database connection is not available. "
                "Cannot execute refresh."
            )

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
                return {
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
            
            # 5. 更新内部状态
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
            
            # 更新内部状态
            self._refresh_status[full_name] = result
            
            return result
    
    def get_refresh_status(
        self,
        view_name: str,
        schema: str = "materialized_views"
    ) -> Optional[Dict[str, Any]]:
        """
        获取物化视图的刷新状态
        
        返回最后一次刷新的状态信息。如果从未刷新过，返回 None。
        
        参数：
        - view_name: 物化视图名称（不含 schema）
        - schema: 物化视图所在的 schema（默认 materialized_views）
        
        返回：
        {
            'status': 'success' | 'failed',
            'view_name': str,
            'full_name': str,
            'refresh_time': datetime,
            'duration_seconds': float,
            'row_count': int,
            'error_message': str (if failed)
        }
        或 None（如果从未刷新过）
        """
        full_name = f"{schema}.{view_name}"
        return self._refresh_status.get(full_name)
    
    # =========================================================================
    # 私有方法
    # =========================================================================

    async def _check_matview_exists(
        self,
        view_name: str,
        schema: str
    ) -> bool:
        """
        检查物化视图是否存在
        
        参数：
        - view_name: 物化视图名称
        - schema: schema 名称
        
        返回：
        True 如果物化视图存在，否则 False
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
        
        参数：
        - view_name: 物化视图名称
        - schema: schema 名称
        - strategy: 刷新策略（'full' 或 'concurrent'）
        
        异常：
        - Exception: 当刷新失败时
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
        
        参数：
        - view_name: 物化视图名称
        - schema: schema 名称
        
        返回：
        物化视图的行数
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
        if not self.db_connection:
            raise RuntimeError("Database connection is not available")

        try:
            if hasattr(self.db_connection, "execute"):
                return await self.db_connection.execute(query, *args)
            raise RuntimeError("db_connection does not support async execute()")
        except Exception:
            self.logger.error(f"SQL execution failed: {query}", exc_info=True)
            raise

    async def _fetch_val(self, query: str, *args) -> Any:
        if not self.db_connection:
            raise RuntimeError("Database connection is not available")

        try:
            if hasattr(self.db_connection, "fetch_val"):
                return await self.db_connection.fetch_val(query, *args)
            if hasattr(self.db_connection, "fetch_one"):
                row = await self.db_connection.fetch_one(query, *args)
                if row is None:
                    return None
                return row[0]
            raise RuntimeError("db_connection does not support fetch_val()/fetch_one()")
        except Exception:
            self.logger.error(f"SQL query failed: {query}", exc_info=True)
            raise
