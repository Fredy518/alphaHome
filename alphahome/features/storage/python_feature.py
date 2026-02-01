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
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from .base_view import BaseFeatureView
from .refresh_log import log_mv_refresh

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
    incremental_days: int = 30  # 默认刷新最近 30 天
    date_column: str = "trade_date"  # 日期列名
    refresh_strategy: str = "incremental"  # 默认使用增量刷新

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
        else:
            return await self._incremental_refresh()

    async def _incremental_refresh(self) -> Dict[str, Any]:
        """执行增量刷新。"""
        import asyncpg

        start_time = datetime.now()

        # 计算日期范围
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (
            datetime.now() - timedelta(days=self.incremental_days)
        ).strftime("%Y%m%d")

        self.logger.info(
            f"开始增量刷新 {self.full_name}, 日期范围: {start_date} - {end_date}"
        )

        try:
            # 获取长超时连接
            conn_str = self._db_manager.connection_string
            conn = await asyncpg.connect(conn_str, command_timeout=7200)

            try:
                # Step 1: 删除旧数据
                delete_sql = f"""
                DELETE FROM {self.full_name}
                WHERE {self.date_column} >= '{start_date}'
                  AND {self.date_column} <= '{end_date}';
                """
                await conn.execute(delete_sql)

                # Step 2: 计算新数据
                df = await self.compute(start_date, end_date)

                if df is not None and not df.empty:
                    # Step 3: 插入新数据
                    await self._insert_dataframe(conn, df)
                    row_count = len(df)
                else:
                    row_count = 0

            finally:
                await conn.close()

            # 记录刷新日志
            duration = (datetime.now() - start_time).total_seconds()
            await self._log_refresh(
                strategy="incremental",
                success=True,
                duration=duration,
                rows_affected=row_count,
                date_range=f"{start_date}-{end_date}"
            )

            self.logger.info(
                f"增量刷新 {self.full_name} 完成, "
                f"新增: {row_count} 行, 耗时: {duration:.2f}s"
            )

            return {
                "status": "success",
                "view_name": self.view_name,
                "view_schema": self._schema,
                "full_name": self.full_name,
                "row_count": row_count,
                "duration_seconds": duration,
                "refresh_strategy": "incremental",
                "strategy": "incremental",
                "date_range": f"{start_date}-{end_date}",
            }

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = f"{type(e).__name__}: {str(e)}"

            await self._log_refresh(
                strategy="incremental",
                success=False,
                duration=duration,
                rows_affected=0,
                date_range=f"{start_date}-{end_date}",
                error=error_msg
            )

            self.logger.error(f"增量刷新 {self.full_name} 失败: {error_msg}")
            raise

    async def _full_refresh(self) -> Dict[str, Any]:
        """执行全量刷新。"""
        import asyncpg

        start_time = datetime.now()

        self.logger.info(f"开始全量刷新 {self.full_name}")

        try:
            # 获取长超时连接
            conn_str = self._db_manager.connection_string
            conn = await asyncpg.connect(conn_str, command_timeout=7200)

            try:
                # Step 1: 清空表
                truncate_sql = f"TRUNCATE TABLE {self.full_name};"
                await conn.execute(truncate_sql)

                # Step 2: 计算全量数据（使用一个很大的日期范围）
                df = await self.compute("19000101", "20991231")

                if df is not None and not df.empty:
                    # Step 3: 插入数据
                    await self._insert_dataframe(conn, df)
                    row_count = len(df)
                else:
                    row_count = 0

            finally:
                await conn.close()

            # 记录刷新日志
            duration = (datetime.now() - start_time).total_seconds()
            await self._log_refresh(
                strategy="full",
                success=True,
                duration=duration,
                rows_affected=row_count,
                date_range="all"
            )

            self.logger.info(
                f"全量刷新 {self.full_name} 完成, "
                f"总行数: {row_count}, 耗时: {duration:.2f}s"
            )

            return {
                "status": "success",
                "view_name": self.view_name,
                "view_schema": self._schema,
                "full_name": self.full_name,
                "row_count": row_count,
                "duration_seconds": duration,
                "refresh_strategy": "full",
                "strategy": "full",
            }

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = f"{type(e).__name__}: {str(e)}"

            await self._log_refresh(
                strategy="full",
                success=False,
                duration=duration,
                rows_affected=0,
                date_range="all",
                error=error_msg
            )

            self.logger.error(f"全量刷新 {self.full_name} 失败: {error_msg}")
            raise

    async def _insert_dataframe(self, conn, df: pd.DataFrame) -> None:
        """
        将 DataFrame 插入到表中。

        Args:
            conn: asyncpg 连接
            df: 要插入的数据
        """
        if df.empty:
            return

        # 转换类型以兼容 asyncpg
        df = df.copy()
        for col in df.columns:
            # 将 numpy datetime64 转换为 Python datetime
            if df[col].dtype == 'datetime64[ns]':
                df[col] = df[col].dt.to_pydatetime()
            # 将 numpy 类型转换为 Python 原生类型
            elif hasattr(df[col].dtype, 'name') and 'int' in df[col].dtype.name:
                df[col] = df[col].astype(object).where(df[col].notna(), None)
            elif hasattr(df[col].dtype, 'name') and 'float' in df[col].dtype.name:
                df[col] = df[col].astype(object).where(df[col].notna(), None)

        # 获取列名
        columns = list(df.columns)
        col_str = ", ".join([f'"{c}"' for c in columns])
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])

        insert_sql = f"""
        INSERT INTO {self.full_name} ({col_str})
        VALUES ({placeholders})
        """

        # 转换为 records 列表
        data = []
        for _, row in df.iterrows():
            # 将每行转换为 tuple，确保类型正确
            row_data = []
            for val in row:
                if pd.isna(val):
                    row_data.append(None)
                elif isinstance(val, np.generic):
                    row_data.append(val.item())
                else:
                    row_data.append(val)
            data.append(tuple(row_data))

        # 批量插入
        await conn.executemany(insert_sql, data)

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
