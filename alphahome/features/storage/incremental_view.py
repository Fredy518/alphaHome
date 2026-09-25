"""Atomic SQL feature-table refresh, with stable target object identity."""

import logging
from abc import abstractmethod
from datetime import datetime
from time import monotonic
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional

from .base_view import BaseFeatureView
from .atomic import identifier, table_refresh_transaction
from .refresh_log import log_mv_refresh
from .recovery import begin_recovery, commit_checkpoint, incremental_window
from .quality import comparable_row_counts, validate_quality, validate_expected_keys

logger = logging.getLogger(__name__)


class IncrementalFeatureView(BaseFeatureView):
    """
    支持增量刷新的物化视图基类。

    子类需要实现：
    - get_create_sql(): 创建物化视图的 SQL
    - get_incremental_sql(start_date, end_date): 增量计算的 SQL

    配置属性：
    - incremental_days: 增量刷新的天数范围（默认 30 天）
    - date_column: 日期列名（默认 trade_date）
    """

    # 增量刷新配置
    incremental_days: int = 30  # 默认刷新最近 30 天
    date_column: str = "trade_date"  # 日期列名
    refresh_strategy: str = "incremental"
    primary_keys: tuple[str, ...] = ()
    supported_strategies = ("full", "incremental")
    allow_expected_no_data = False

    async def _is_materialized_view(self) -> bool:
        """检查当前对象是否为物化视图（而非普通表）。"""
        if self._db_manager is None:
            return False

        sql = f"""
        SELECT EXISTS (
            SELECT 1 FROM pg_matviews
            WHERE schemaname = '{self._schema}'
              AND matviewname = '{self.view_name}'
        ) AS is_matview;
        """
        result = await self._db_manager.fetch(sql)
        return result and result[0]["is_matview"]

    @abstractmethod
    def get_incremental_sql(self, start_date: str, end_date: str) -> str:
        """
        返回增量计算的 SELECT SQL（不含 INSERT）。

        Args:
            start_date: 开始日期 (YYYYMMDD 格式)
            end_date: 结束日期 (YYYYMMDD 格式)

        Returns:
            str: SELECT SQL，用于获取指定日期范围的数据
        """
        pass

    async def refresh(self, strategy: Optional[str] = None) -> Dict[str, Any]:
        """
        刷新物化视图。

        Args:
            strategy: 刷新策略
                - "incremental": 增量刷新（删除旧数据 + 插入新数据）
                - "full": 全量刷新
                - "concurrent": 并发全量刷新

        Returns:
            Dict[str, Any]: 刷新结果
        """
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        # GUI 层使用 "default" 表示“使用配方默认策略”
        if strategy == "default":
            strategy = None

        actual_strategy = strategy or self.refresh_strategy

        if actual_strategy == "incremental":
            return await self._incremental_refresh()
        else:
            # 使用父类的全量刷新逻辑
            return await super().refresh(strategy=actual_strategy)

    async def _incremental_refresh(self) -> Dict[str, Any]:
        end = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        start = await incremental_window(self, end)
        return await self._refresh_table_window("incremental", start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))

    async def expected_no_data_reason(self, connection, start, end) -> Optional[str]:
        """A recipe must explicitly prove an empty eligible universe before clearing it."""
        return None

    async def _refresh_table_window(self, strategy, start_date, end_date, *,
                                    approved_initial_baseline_rows=None):
        import asyncpg

        start = datetime.strptime(start_date, "%Y%m%d").date()
        end = datetime.strptime(end_date, "%Y%m%d").date()
        if strategy not in self.supported_strategies or start > end:
            raise ValueError("Unsupported SQL feature replacement request")
        if not self.primary_keys or self.date_column not in self.primary_keys:
            raise ValueError("SQL feature requires dated business keys")
        target = f"{identifier(self._schema)}.{identifier(self.view_name)}"
        date_column = identifier(self.date_column)
        keys = ", ".join(identifier(key) for key in self.primary_keys)
        null_keys = " OR ".join(f"{identifier(key)} IS NULL" for key in self.primary_keys)
        started = monotonic()
        connection = None
        date_range = "all" if strategy == "full" else f"{start_date}-{end_date}"
        try:
            # Large first full baselines can spend hours maintaining target indexes on NAS storage.
            connection = await asyncpg.connect(
                self._db_manager.connection_string,
                command_timeout=14400 if strategy == "full" else 7200,
            )
            lock_started = monotonic()
            # Same whole-table lock as PythonFeatureTable, held BEFORE computing.
            async with table_refresh_transaction(connection, self._schema, self.view_name):
                recovery = await begin_recovery(connection, self, strategy, start, end)
                if approved_initial_baseline_rows is not None and (
                    strategy != 'full' or recovery is None or not recovery['initial_baseline']
                ):
                    raise ValueError('Initial baseline growth approval requires a first full recovery baseline')
                lock_wait = monotonic() - lock_started
                kind = await connection.fetchval("SELECT relkind::text FROM pg_class WHERE oid=$1::regclass", self.full_name)
                if kind != "r":
                    raise RuntimeError("migration_required: SQL feature target must be an ordinary table")
                columns = await connection.fetch(
                    "SELECT attname FROM pg_attribute WHERE attrelid=$1::regclass "
                    "AND attnum>0 AND NOT attisdropped AND attgenerated='' ORDER BY attnum", self.full_name,
                )
                names = [row["attname"] for row in columns]
                if not set(self.primary_keys).issubset(names):
                    raise RuntimeError("migration_required: feature business-key columns missing")
                insert_columns = ", ".join(identifier(name) for name in names)
                select_columns = ", ".join(f"d.{identifier(name)}" for name in names)
                await connection.execute(f"CREATE TEMP TABLE feature_sql_stage (LIKE {target} INCLUDING DEFAULTS INCLUDING CONSTRAINTS) ON COMMIT DROP")
                # No client-side materialization; type/check failures happen in staging.
                select = self.get_incremental_sql(start_date, end_date).strip().rstrip(";")
                await connection.execute(
                    f"INSERT INTO pg_temp.feature_sql_stage ({insert_columns}) "
                    f"SELECT {select_columns} FROM ({select}) AS d"
                )
                count = await connection.fetchval("SELECT COUNT(*) FROM pg_temp.feature_sql_stage")
                status, empty_reason = "success", None
                if count == 0:
                    if self.allow_expected_no_data:
                        empty_reason = await self.expected_no_data_reason(connection, start, end)
                    if not empty_reason:
                        raise ValueError("Empty feature result lacks an expected_no_data contract")
                    status = "expected_no_data"
                if await connection.fetchval(
                    f"SELECT EXISTS (SELECT 1 FROM pg_temp.feature_sql_stage WHERE {null_keys} "
                    f"OR {date_column} NOT BETWEEN $1 AND $2)", start, end,
                ):
                    raise ValueError("Feature contains null keys or dates outside the replacement window")
                await connection.execute(f"CREATE UNIQUE INDEX ON pg_temp.feature_sql_stage ({keys})")
                previous_count, comparable_count = await comparable_row_counts(
                    connection, self.full_name, 'pg_temp.feature_sql_stage', self.date_column, strategy, start, end,
                )
                quality = await validate_quality(connection, self.quality_checks, 'pg_temp.feature_sql_stage',
                                                 previous_count, expected_empty=status == 'expected_no_data',
                                                 comparable_count=comparable_count,
                                                 approved_initial_baseline_rows=approved_initial_baseline_rows)
                await validate_expected_keys(connection, self, 'pg_temp.feature_sql_stage', start, end)
                if strategy == "full":
                    await connection.execute(f"DELETE FROM {target}")
                else:
                    await connection.execute(f"DELETE FROM {target} WHERE {date_column} BETWEEN $1 AND $2", start, end)
                await connection.execute(f"INSERT INTO {target} ({insert_columns}) SELECT {insert_columns} FROM pg_temp.feature_sql_stage")
                await commit_checkpoint(connection, self, recovery)
                # Commit remains inside the try; deferred failures are never success.
        except Exception as exc:
            await self._log_refresh(strategy, False, monotonic()-started, date_range=date_range, error=f"{type(exc).__name__}: {exc}")
            raise
        finally:
            if connection is not None:
                await connection.close()
        duration = monotonic()-started
        await self._log_refresh(strategy, True, duration, rows_affected=count, date_range=date_range)
        return {
            "status": status, "view_name": self.view_name, "view_schema": self._schema,
            "full_name": self.full_name, "row_count": count, "committed_rows": count,
            "duration_seconds": duration, "lock_wait_seconds": lock_wait,
            "refresh_strategy": strategy, "strategy": strategy,
            "requested_strategy": strategy, "effective_strategy": strategy,
            "fallback_reason": None, "date_range": date_range, "empty_reason": empty_reason,
            "history_coverage": "checkpointed_range" if recovery else "unverified",
            "source_consumption": "unverified",
            "quality": quality,
        }

    async def _log_refresh(
        self,
        strategy: str,
        success: bool,
        duration: float,
        rows_affected: int = 0,
        date_range: str = "",
        error: str = ""
    ) -> None:
        """记录刷新日志到 features.mv_refresh_log。"""
        if self._db_manager is None:
            return

        refresh_strategy = strategy + (f" ({date_range})" if date_range else "")
        await log_mv_refresh(
            self._db_manager,
            view_name=self.view_name,
            schema_name=self._schema,
            refresh_strategy=refresh_strategy,
            success=success,
            duration_seconds=duration,
            row_count=rows_affected,
            error_message=error if error else None,
        )


class IncrementalTableView(IncrementalFeatureView):
    """
    使用普通表（而非物化视图）的增量刷新基类。

    适用场景：
    - 需要频繁增量更新
    - 数据量大，全量刷新太慢
    - 使用单事务暂存校验与原表替换，保留依赖关系

    注意：
    - 使用 CREATE TABLE 而非 CREATE MATERIALIZED VIEW
    - get_create_sql() 应返回 CREATE TABLE 语句
    """
    storage_type: str = "数据表"

    async def refresh(self, strategy: Optional[str] = None) -> Dict[str, Any]:
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")
        actual = self.refresh_strategy if strategy in (None, "default") else strategy
        if actual not in self.supported_strategies:
            raise ValueError(f"Unsupported ordinary-table refresh strategy: {actual}")
        if actual == "full":
            return await self._full_refresh_table()
        return await self._incremental_refresh()

    async def _full_refresh_table(self) -> Dict[str, Any]:
        return await self._refresh_table_window("full", "19000101", datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d'))

    async def create(self, if_not_exists: bool = True) -> bool:
        """
        创建普通表。
        """
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        try:
            if await self._is_materialized_view():
                raise RuntimeError(
                    f"{self.full_name} 当前为物化视图（pg_matviews），但配方为数据表类型。"
                    "migration_required: 请使用经过依赖检查和备份审阅的显式存储迁移。"
                )

            # 检查表是否已存在
            check_sql = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = '{self._schema}'
                  AND table_name = '{self.view_name}'
            ) AS exists;
            """
            result = await self._db_manager.fetch(check_sql)
            table_exists = result and result[0]["exists"]

            if if_not_exists and table_exists:
                self.logger.info(f"表 {self.full_name} 已存在，跳过创建")
                await self._upsert_metadata()
                return True

            # 获取创建 SQL
            create_sql = self.get_create_sql()
            self.logger.info(f"创建表: {self.full_name}")

            # 执行创建
            await self._db_manager.execute(create_sql)

            # 执行创建后的附加 SQL（如索引）
            post_sqls = self.get_post_create_sqls() or []
            for stmt in post_sqls:
                if not isinstance(stmt, str) or not stmt.strip():
                    continue
                await self._db_manager.execute(stmt)

            # 写入元数据
            await self._upsert_metadata()

            self.logger.info(f"表 {self.full_name} 创建成功")
            return True

        except Exception as e:
            self.logger.error(f"创建表 {self.full_name} 失败: {e}")
            raise

    async def exists(self) -> bool:
        """检查表是否存在。"""
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        sql = f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{self._schema}'
              AND table_name = '{self.view_name}'
        ) AS exists;
        """
        result = await self._db_manager.fetch(sql)
        return result and result[0]["exists"]

    async def drop(self, if_exists: bool = True) -> bool:
        """删除表。"""
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        try:
            if_exists_clause = "IF EXISTS " if if_exists else ""
            sql = f"DROP TABLE {if_exists_clause}{self.full_name};"
            self.logger.info(f"删除表: {self.full_name}")
            await self._db_manager.execute(sql)
            await self._deactivate_metadata()
            self.logger.info(f"表 {self.full_name} 删除成功")
            return True

        except Exception as e:
            self.logger.error(f"删除表 {self.full_name} 失败: {e}")
            raise
