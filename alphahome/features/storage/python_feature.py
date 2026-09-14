"""
Python 计算特征基类

设计思路：
某些特征计算无法用纯 SQL 表达（如复杂的机器学习模型、自定义算法等），
需要使用 Python 进行计算，然后将结果存入数据库表。

与 SQL 物化视图的区别：
- SQL MV: 使用 CREATE MATERIALIZED VIEW，由数据库引擎执行
- Python Feature: 使用 Python 计算，结果存入普通表

刷新策略：
- incremental: 增量刷新（计算最近 N 天的数据）
- full: 全量刷新（清空重建）
"""

import logging
from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from .base_view import BaseFeatureView
from .refresh_log import log_mv_refresh
from .atomic import identifier, table_refresh_transaction

logger = logging.getLogger(__name__)


class PythonFeatureTable(BaseFeatureView):
    """
    Python 计算特征的基类。

    子类需要实现：
    - get_create_sql(): 创建表的 SQL（CREATE TABLE）
    - compute(start_date, end_date): 计算特征的 Python 方法

    配置属性：
    - incremental_days: 增量刷新的天数范围（默认 30 天）
    - date_column: 日期列名（默认 trade_date）
    """

    # 增量刷新配置
    supported_strategies = ("full", "incremental")
    incremental_days: int = 30  # 默认刷新最近 30 天
    date_column: str = "trade_date"  # 日期列名
    refresh_strategy: str = "incremental"  # 默认使用增量刷新
    primary_keys: tuple[str, ...] = ()
    allow_expected_no_data: bool = False

    # 标记这是 Python 计算的特征
    is_python_feature: bool = True
    storage_type: str = "数据表"

    @abstractmethod
    async def compute(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        计算指定日期范围的特征。

        Args:
            start_date: 开始日期 (YYYYMMDD 格式)
            end_date: 结束日期 (YYYYMMDD 格式)

        Returns:
            pd.DataFrame: 计算结果，列名应与表结构匹配
        """
        pass

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

    async def create(self, if_not_exists: bool = True) -> bool:
        """创建表。"""
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        try:
            if if_not_exists and await self.exists():
                self.logger.info(f"表 {self.full_name} 已存在，跳过创建")
                await self._upsert_metadata()
                return True

            create_sql = self.get_create_sql()
            self.logger.info(f"创建表: {self.full_name}")

            await self._db_manager.execute(create_sql)

            # 执行创建后的附加 SQL（如索引）
            post_sqls = self.get_post_create_sqls() or []
            for stmt in post_sqls:
                if isinstance(stmt, str) and stmt.strip():
                    await self._db_manager.execute(stmt)

            await self._upsert_metadata()
            self.logger.info(f"表 {self.full_name} 创建成功")
            return True

        except Exception as e:
            self.logger.error(f"创建表 {self.full_name} 失败: {e}")
            raise

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

    async def refresh(self, strategy: Optional[str] = None) -> Dict[str, Any]:
        """
        刷新特征表。

        Args:
            strategy: 刷新策略
                - "incremental": 增量刷新
                - "full": 全量刷新
                - "default" 或 None: 使用类定义的默认策略

        Returns:
            Dict[str, Any]: 刷新结果
        """
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        # GUI 层使用 "default" 表示"使用配方默认策略"
        if strategy == "default":
            strategy = None

        actual_strategy = strategy or self.refresh_strategy

        if actual_strategy == "full":
            return await self._full_refresh()
        if actual_strategy == "incremental":
            return await self._incremental_refresh()
        raise ValueError(f"Unsupported Python feature refresh strategy: {actual_strategy}")

    async def _incremental_refresh(self) -> Dict[str, Any]:
        now = datetime.now()
        return await self._refresh_window(
            "incremental", (now - timedelta(days=self.incremental_days)).strftime("%Y%m%d"),
            now.strftime("%Y%m%d"),
        )

    async def _full_refresh(self) -> Dict[str, Any]:
        return await self._refresh_window("full", "19000101", "20991231")

    def _validate_frame(self, frame, start_date, end_date):
        if not isinstance(frame, pd.DataFrame):
            raise ValueError("Feature computation must return a DataFrame")
        if frame.empty:
            if not (self.allow_expected_no_data and frame.attrs.get("expected_no_data") is True
                    and frame.attrs.get("reason")):
                raise ValueError("Empty feature result lacks an expected_no_data contract")
            return frame.copy(), "expected_no_data"
        if frame.columns.duplicated().any():
            raise ValueError("Duplicate feature output columns")
        for column in frame.columns:
            identifier(column)
        if self.date_column not in frame:
            raise ValueError("Feature date column missing")
        frame = frame.copy()
        dates = pd.to_datetime(frame[self.date_column], errors="coerce")
        if dates.isna().any() or not dates.between(pd.Timestamp(start_date), pd.Timestamp(end_date)).all():
            raise ValueError("Feature dates outside the replacement window")
        frame[self.date_column] = dates.dt.date
        keys = list(self.primary_keys)
        if keys and (not set(keys).issubset(frame) or frame[keys].isna().any().any()
                     or frame.duplicated(keys).any()):
            raise ValueError("Invalid or duplicate feature business keys")
        return frame, "success"

    async def _refresh_window(self, strategy, start_date, end_date):
        import asyncpg

        started = datetime.now()
        start = datetime.strptime(start_date, "%Y%m%d").date()
        end = datetime.strptime(end_date, "%Y%m%d").date()
        if strategy not in {"full", "incremental"} or start > end:
            raise ValueError("Invalid feature replacement request")
        target = f"{identifier(self._schema)}.{identifier(self.view_name)}"
        date_column = identifier(self.date_column)
        date_range = "all" if strategy == "full" else f"{start_date}-{end_date}"
        connection = None
        try:
            connection = await asyncpg.connect(self._db_manager.connection_string, command_timeout=7200)
            # Hold the same target lock before computing: a slower earlier run
            # must not overwrite a later run's already committed snapshot.
            async with table_refresh_transaction(connection, self._schema, self.view_name):
                frame, status = self._validate_frame(await self.compute(start_date, end_date), start, end)
                if not frame.empty:
                    await connection.execute(
                        f"CREATE TEMP TABLE feature_stage (LIKE {target} INCLUDING DEFAULTS INCLUDING CONSTRAINTS) ON COMMIT DROP"
                    )
                    await self._insert_dataframe(connection, frame, table_name="feature_stage", schema_name="pg_temp")
                    staged = await connection.fetchval("SELECT COUNT(*) FROM pg_temp.feature_stage")
                    if staged != len(frame):
                        raise ValueError("Feature staging row count differs from computed result")
                if strategy == "full":
                    await connection.execute(f"DELETE FROM {target}")
                else:
                    await connection.execute(f"DELETE FROM {target} WHERE {date_column} BETWEEN $1 AND $2", start, end)
                if not frame.empty:
                    columns = ", ".join(identifier(column) for column in frame.columns)
                    await connection.execute(f"INSERT INTO {target} ({columns}) SELECT {columns} FROM pg_temp.feature_stage")
            row_count = len(frame)
        except Exception as exc:
            await self._log_refresh(strategy, False, (datetime.now()-started).total_seconds(), date_range=date_range, error=f"{type(exc).__name__}: {exc}")
            raise
        finally:
            if connection is not None:
                await connection.close()
        duration = (datetime.now()-started).total_seconds()
        await self._log_refresh(strategy, True, duration, rows_affected=row_count, date_range=date_range)
        return {
            "status": status, "view_name": self.view_name, "view_schema": self._schema,
            "full_name": self.full_name, "row_count": row_count, "committed_rows": row_count,
            "duration_seconds": duration, "refresh_strategy": strategy, "strategy": strategy,
            "date_range": date_range,
        }

    async def _insert_dataframe(self, conn, df, *, table_name=None, schema_name=None):
        # Bound conversion memory; COPY also performs PostgreSQL type validation
        # before the live snapshot is touched.
        for offset in range(0, len(df), 10000):
            records = []
            for row in df.iloc[offset:offset+10000].itertuples(index=False, name=None):
                records.append(tuple(
                    None if pd.isna(value) else value.item() if isinstance(value, np.generic) else value
                    for value in row
                ))
            await conn.copy_records_to_table(
                table_name or self.view_name, schema_name=schema_name or self._schema,
                columns=list(df.columns), records=records,
            )

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
